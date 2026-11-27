// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Upload streams through the shareable client, against the in-tree server
//! in the simulator.

#[cfg(test)]
#[path = "../store/common/mod.rs"]
mod common;
#[cfg(test)]
#[path = "upload/ends.rs"]
mod ends;
#[cfg(test)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod loopback;
#[cfg(test)]
#[path = "upload/rig.rs"]
mod rig;
#[cfg(test)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[path = "upload/threaded.rs"]
mod threaded;

use common::BUF;
use longshore_common::io::alloc_guard::sealed;
use longshore_s3::client::{Client, Error, Upload};
use rig::*;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

const SIZE: usize = BUF as usize;

/// Open a stream to `key` and write `data` to it in pieces of `piece`.
#[cfg(test)]
fn write_all(r: &mut Rig, client: &Client, key: &str, data: &[u8], piece: usize) -> Upload {
    let mut up = r.run(client.upload(key)).unwrap();
    for chunk in data.chunks(piece) {
        r.run(up.write(chunk)).unwrap();
    }
    up
}

#[test]
fn a_stream_larger_than_every_buffer_together_arrives_whole() {
    let mut r = rig(Shape::new());
    let client = r.client.clone();
    // Ten buffers' worth through a pool of four.
    let data = pattern(9 * SIZE + 4321, 1);
    let up = write_all(&mut r, &client, "big", &data, 7000);
    assert_eq!(r.run(up.finish()), Ok(()));
    let server = &r.lanes[0].server;
    assert_eq!(server.object("/big"), Some(&data[..]));
    assert_eq!(server.uploads_in_progress(), 0);
    // A begin, ten parts and a completion.
    assert_eq!(server.requests(), 12);
}

#[test]
fn a_short_stream_is_one_plain_store_and_begins_no_upload() {
    let mut r = rig(Shape::new());
    let client = r.client.clone();
    let mut up = r.run(client.upload("short")).unwrap();
    r.run(up.write(b"hello")).unwrap();
    r.run(up.write(b", world")).unwrap();
    assert_eq!(r.run(up.finish()), Ok(()));
    assert_eq!(
        r.lanes[0].server.object("/short"),
        Some(&b"hello, world"[..])
    );
    assert_eq!(r.lanes[0].server.requests(), 1);

    // Exactly one buffer is still one store.
    let one = pattern(SIZE, 2);
    let whole = write_all(&mut r, &client, "one", &one, SIZE);
    assert_eq!(r.run(whole.finish()), Ok(()));
    assert_eq!(r.lanes[0].server.object("/one"), Some(&one[..]));
    assert_eq!(r.lanes[0].server.requests(), 2);

    // Nothing at all stores an empty object.
    let empty = r.run(client.upload("empty")).unwrap();
    assert_eq!(r.run(empty.finish()), Ok(()));
    assert_eq!(r.lanes[0].server.object("/empty"), Some(&[][..]));

    // Dropping a short stream sends nothing.
    let never = write_all(&mut r, &client, "never", &one, 1000);
    drop(never);
    for _ in 0..100 {
        r.pump();
    }
    assert_eq!(r.lanes[0].server.requests(), 3);
    assert_eq!(r.lanes[0].server.uploads_in_progress(), 0);
}

#[test]
fn a_buffer_below_the_smallest_part_cannot_open_a_stream() {
    let min = u64::from(BUF) + 1;
    let mut r = rig(Shape {
        part_min: min,
        ..Shape::new()
    });
    let client = r.client.clone();
    let opened = r.run(client.upload("k"));
    assert_eq!(opened.err(), Some(Error::PartTooSmall { min }));
    assert_eq!(r.run(client.upload("")).err(), Some(Error::BadKey));
}

#[test]
fn opening_waits_while_every_record_is_held() {
    let mut r = rig(Shape {
        streams: 1,
        ..Shape::new()
    });
    let client = r.client.clone();
    let first = r.run(client.upload("a")).unwrap();
    let mut second = pin!(client.upload("b"));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..50 {
        assert!(second.as_mut().poll(&mut cx).is_pending());
        r.pump();
    }
    // A stream that ends gives its record back.
    assert_eq!(r.run(first.finish()), Ok(()));
    let Poll::Ready(Ok(second)) = second.as_mut().poll(&mut cx) else {
        panic!("the record did not come back");
    };
    drop(second);
    assert!(r.run(client.upload("c")).is_ok());
}

#[test]
fn buffers_handed_over_are_sent_as_they_are_and_partial_ones_filled() {
    let mut r = rig(Shape::new());
    let client = r.client.clone();
    let mut up = r.run(client.upload("lent")).unwrap();
    let mut expected = Vec::new();

    let mut full = r.run(client.buffer());
    let first = pattern(SIZE, 3);
    full.as_mut_slice().copy_from_slice(&first);
    expected.extend_from_slice(&first);
    assert_eq!(r.run(up.send(full, SIZE)), Ok(()));

    // A buffer longer than it holds is refused, and the stream goes on.
    let long = r.run(client.buffer());
    assert_eq!(r.run(up.send(long, SIZE + 1)), Err(Error::TooLong));

    // A partial buffer becomes the one writes fill.
    let mut part = r.run(client.buffer());
    part.as_mut_slice()[..100].fill(7);
    expected.extend_from_slice(&[7; 100]);
    assert_eq!(r.run(up.send(part, 100)), Ok(()));
    r.run(up.write(&[8; 50])).unwrap();
    expected.extend_from_slice(&[8; 50]);

    // With bytes waiting, a handed-over buffer is copied after them.
    let mut more = r.run(client.buffer());
    let second = pattern(SIZE, 4);
    more.as_mut_slice().copy_from_slice(&second);
    expected.extend_from_slice(&second);
    assert_eq!(r.run(up.send(more, SIZE)), Ok(()));

    assert_eq!(r.run(up.finish()), Ok(()));
    assert_eq!(r.lanes[0].server.object("/lent"), Some(&expected[..]));
}

#[test]
fn a_stream_past_the_room_for_its_list_of_parts_fails_and_aborts() {
    let mut r = rig(Shape {
        objects: 100,
        object_size: BUF,
        ..Shape::new()
    });
    let client = r.client.clone();
    let data = pattern(SIZE, 5);
    let mut up = r.run(client.upload("long")).unwrap();
    let mut buffers = 0;
    let failed = loop {
        if let Err(e) = r.run(up.write(&data)) {
            break e;
        }
        buffers += 1;
        assert!(buffers < 100, "the list of parts never ran out of room");
    };
    // Each part takes a record and an entry in the completion, both in one
    // buffer of 16 KiB: room for about sixty.
    assert_eq!(failed, Error::TooLong);
    assert!((50..80).contains(&buffers), "{buffers}");
    // The stream stays failed.
    assert_eq!(r.run(up.write(b"x")), Err(Error::TooLong));
    drop(up);
    for _ in 0..2000 {
        r.pump();
    }
    let server = &r.lanes[0].server;
    assert_eq!(server.uploads_in_progress(), 0);
    assert_eq!(server.object(&part_target("long", 1, 1)), None);
    assert_eq!(server.object("/long"), None);
}

#[test]
fn a_stream_allocates_nothing_after_warm_up() {
    let mut r = rig(Shape::new());
    let client = r.client.clone();
    let data = pattern(3 * SIZE + 99, 6);
    for (finished, dropped) in [("warm", "warm-drop"), ("sealed", "sealed-drop")] {
        let run = |lanes: &mut Rig| {
            let mut kept = lanes.run(client.upload(finished)).unwrap();
            for chunk in data.chunks(5000) {
                lanes.run(kept.write(chunk)).unwrap();
            }
            assert_eq!(lanes.run(kept.finish()), Ok(()));
            let mut gone = lanes.run(client.upload(dropped)).unwrap();
            lanes.run(gone.write(&data)).unwrap();
            drop(gone);
            for _ in 0..2000 {
                lanes.pump();
            }
        };
        if finished == "warm" {
            run(&mut r);
        } else {
            sealed(|| run(&mut r));
        }
        assert_eq!(r.lanes[0].server.uploads_in_progress(), 0);
    }
    assert_eq!(r.lanes[0].server.object("/sealed"), Some(&data[..]));
}
