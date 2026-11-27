// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! An upload stream, or a future of one, dropped at each stage, with the
//! lanes stepped by hand so that the stage is reached for certain. After
//! each: no buffer is lent again while an operation the stream left
//! running holds it, every buffer, operation slot and stream record comes
//! back, and a begun upload is aborted once its last operation has ended.
//! Each runs twice, the second time with allocation forbidden.

use super::common::BUF;
use super::returned::poll_once as poll;
use super::rig::*;
use super::settle::*;
use longshore_s3::client::{Buffer, Error, Pending};
use std::pin::{pin, Pin};
use std::task::Poll;

const SIZE: usize = BUF as usize;

/// Dropped while waiting for a stream record, and again after the record
/// came back and woke it but before it took it.
#[cfg(test)]
fn waiting_for_a_record(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let first = r.run(client.upload(k.key)).unwrap();
    {
        let mut waiting = pin!(client.upload("second"));
        for _ in 0..50 {
            assert!(poll(waiting.as_mut()).is_pending());
            r.pump();
        }
    }
    {
        let mut woken = pin!(client.upload("third"));
        assert!(poll(woken.as_mut()).is_pending());
        drop(first);
    }
    Left {
        held: 0,
        requests: before,
        stored: None,
    }
}

/// Dropped holding one full buffer and nothing more: no upload began.
#[cfg(test)]
fn after_a_short_write(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..100])).unwrap();
    r.run(up.write(&data()[100..SIZE])).unwrap();
    drop(up);
    Left {
        held: 0,
        requests: before,
        stored: None,
    }
}

/// Dropped while a write waits for a buffer, after a buffer came back and
/// woke it but before it took it.
#[cfg(test)]
fn waiting_for_a_buffer(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    let mut all: [Option<Buffer>; 4] = core::array::from_fn(|_| None);
    for b in &mut all {
        *b = Some(r.run(client.buffer()));
    }
    {
        let mut write = pin!(up.write(&data()[..10]));
        for _ in 0..50 {
            assert!(poll(write.as_mut()).is_pending());
            r.pump();
        }
        all[0] = None;
    }
    drop(up);
    drop(all);
    Left {
        held: 0,
        requests: before,
        stored: None,
    }
}

/// Dropped while waiting for the operation slot it keeps for the abort:
/// every slot holds an erase that has ended and whose result nobody took.
#[cfg(test)]
fn waiting_for_the_abort_slot(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..SIZE])).unwrap();
    let keys = ["e0", "e1", "e2", "e3"];
    let mut erases: [Option<Pending<Result<(), Error>>>; 4] = core::array::from_fn(|_| None);
    for (erase, key) in erases.iter_mut().zip(keys) {
        let mut e = client.erase(key);
        assert!(poll(Pin::new(&mut e)).is_pending());
        *erase = Some(e);
    }
    for _ in 0..500 {
        r.pump();
    }
    {
        let mut write = pin!(up.write(&data()[SIZE..SIZE + 1]));
        for _ in 0..50 {
            assert!(poll(write.as_mut()).is_pending());
            r.pump();
        }
    }
    // No begin went out.
    assert_eq!(server(r).requests(), before + 4);
    drop(up);
    for e in erases.iter_mut().flatten() {
        assert_eq!(poll(Pin::new(e)), Poll::Ready(Ok(())));
    }
    Left {
        held: 0,
        requests: before + 4,
        stored: None,
    }
}

/// Dropped with the begin sent to the I/O thread and not yet run: the
/// begin runs, its upload id lands in the record, and the abort follows.
#[cfg(test)]
fn while_the_begin_is_sent(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..SIZE])).unwrap();
    assert!(poll(pin!(up.write(&data()[SIZE..SIZE + 1]))).is_pending());
    assert_eq!(server(r).requests(), before);
    drop(up);
    Left {
        // The begin carries the buffer of part records.
        held: 1,
        requests: before + 2,
        stored: None,
    }
}

/// Dropped once the begin has ended, with its result not yet taken.
#[cfg(test)]
fn after_the_begin_ends_untaken(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..SIZE])).unwrap();
    assert!(poll(pin!(up.write(&data()[SIZE..SIZE + 1]))).is_pending());
    for _ in 0..500 {
        r.pump();
    }
    assert_eq!(server(r).uploads_in_progress(), 1);
    assert_eq!(server(r).requests(), before + 1);
    drop(up);
    Left {
        held: 0,
        requests: before + 2,
        stored: None,
    }
}

/// Dropped with three parts sent to the I/O thread and not yet run.
#[cfg(test)]
fn with_parts_sent(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..3 * SIZE + 10])).unwrap();
    assert_eq!(server(r).requests(), before + 1);
    drop(up);
    Left {
        held: 3,
        requests: before + 5,
        stored: None,
    }
}

/// Dropped after a write was dropped holding some of its bytes: the
/// stream has failed with `BadRange`, and finishing it aborts.
#[cfg(test)]
fn after_a_write_dropped_part_way(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    // One buffer's worth taken, then the begin sent.
    assert!(poll(pin!(up.write(&data()[..2 * SIZE + 10]))).is_pending());
    assert_eq!(r.run(up.write(b"x")), Err(Error::BadRange));
    assert_eq!(r.run(up.finish()), Err(Error::BadRange));
    Left {
        held: 1,
        requests: before + 2,
        stored: None,
    }
}

/// Dropped after a send was dropped part-way through copying its buffer
/// after bytes already waiting: the stream has failed with `BadRange`.
#[cfg(test)]
fn after_a_send_dropped_part_way(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..100])).unwrap();
    let mut b = r.run(client.buffer());
    b.as_mut_slice().copy_from_slice(&data()[..SIZE]);
    assert!(poll(pin!(up.send(b, SIZE))).is_pending());
    assert_eq!(r.run(up.write(b"x")), Err(Error::BadRange));
    drop(up);
    Left {
        held: 1,
        requests: before + 2,
        stored: None,
    }
}

/// `finish` dropped once its completion is sent: the completion runs and
/// stores the object, and the abort that follows it finds nothing open.
#[cfg(test)]
fn while_the_completion_is_sent(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let len = 2 * SIZE + 5;
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..len])).unwrap();
    for _ in 0..500 {
        r.pump();
    }
    assert_eq!(server(r).requests(), before + 3);
    let mut finishing = pin!(up.finish());
    // The last part goes; then, once it has ended, the completion.
    assert!(poll(finishing.as_mut()).is_pending());
    for _ in 0..500 {
        r.pump();
    }
    assert_eq!(server(r).requests(), before + 4);
    assert!(poll(finishing.as_mut()).is_pending());
    assert_eq!(server(r).requests(), before + 4);
    Left {
        // The completion carries the buffer of part records.
        held: 1,
        requests: before + 6,
        stored: Some(len),
    }
}

/// `finish` of a short stream dropped once its one plain store is sent.
#[cfg(test)]
fn while_the_short_store_is_sent(r: &mut Rig, k: Key) -> Left {
    let client = r.client.clone();
    let before = server(r).requests();
    let mut up = r.run(client.upload(k.key)).unwrap();
    r.run(up.write(&data()[..100])).unwrap();
    assert!(poll(pin!(up.finish())).is_pending());
    assert_eq!(server(r).requests(), before);
    Left {
        held: 1,
        requests: before + 1,
        stored: Some(100),
    }
}

/// Two keys for a scenario's two runs.
macro_rules! keys {
    ($name:literal) => {
        [
            Key {
                key: concat!("warm-", $name),
                path: concat!("/warm-", $name),
            },
            Key {
                key: concat!("sealed-", $name),
                path: concat!("/sealed-", $name),
            },
        ]
    };
}

#[test]
fn dropped_while_opening_waits_for_a_record() {
    let shape = Shape {
        streams: 1,
        ..Shape::new()
    };
    twice(shape, keys!("record"), waiting_for_a_record);
}

#[test]
fn dropped_before_its_upload_began() {
    twice(Shape::new(), keys!("short"), after_a_short_write);
    twice(Shape::new(), keys!("buffer"), waiting_for_a_buffer);
    twice(Shape::new(), keys!("slot"), waiting_for_the_abort_slot);
}

#[test]
fn dropped_while_its_upload_begins() {
    twice(Shape::new(), keys!("begin-sent"), while_the_begin_is_sent);
    twice(
        Shape::new(),
        keys!("begin-ended"),
        after_the_begin_ends_untaken,
    );
}

#[test]
fn dropped_with_parts_in_flight() {
    let shape = Shape {
        buffers: 6,
        ..Shape::new()
    };
    twice(shape, keys!("parts"), with_parts_sent);
}

#[test]
fn a_write_or_send_dropped_part_way_fails_the_stream_and_its_drop_aborts() {
    twice(Shape::new(), keys!("write"), after_a_write_dropped_part_way);
    twice(Shape::new(), keys!("send"), after_a_send_dropped_part_way);
}

#[test]
fn finish_dropped_while_its_last_request_is_in_flight() {
    twice(
        Shape::new(),
        keys!("complete"),
        while_the_completion_is_sent,
    );
    twice(Shape::new(), keys!("store"), while_the_short_store_is_sent);
}
