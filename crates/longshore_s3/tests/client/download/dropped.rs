// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! A download, or a future of one, dropped at each stage, with the lane
//! stepped by hand so that the stage is reached for certain. After each:
//! no buffer is lent again while a range the download left running holds
//! it, a piece the reader kept stays the object's, every buffer and
//! operation slot comes back, and the next download reads the object
//! whole. Each runs twice, the second time with allocation forbidden.

use super::common::sealed;
use super::returned::{buffers_back, poll_once as poll, slots_free};
use super::rig::Rig;
use super::setup::*;
use longshore_s3::client::{Buffer, Piece};
use std::pin::pin;
use std::sync::OnceLock;
use std::task::Poll;

const KEY: &str = "dropped";
/// Four ranges, the last short.
const SIZE: usize = 3 * RANGE + 1000;
/// Operation slots on the lane.
const SLOTS: usize = 4;

/// What a scenario leaves once its download is dropped.
struct Left {
    /// Lent buffers still held by ranges the download left running.
    held: usize,
    /// Requests the server has seen once the lane has settled.
    requests: u64,
    /// A piece the reader still holds, and where in the object it starts.
    piece: Option<(Buffer, usize, usize)>,
}

fn bytes() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| object(SIZE, 11))
}

fn requests(r: &Rig) -> u64 {
    r.lanes[0].server.requests()
}

/// Run `scenario` over a stored object, then again with allocation
/// forbidden, checking after each what it left.
#[cfg(test)]
fn twice(scenario: fn(&mut Rig) -> Left) {
    let mut r = download_rig(true);
    upload(&mut r, KEY, bytes(), true);
    let warm = scenario(&mut r);
    settled(&mut r, warm);
    sealed(|| {
        let left = scenario(&mut r);
        settled(&mut r, left);
    });
}

/// `held` buffers come back only once the lane has run, a piece the reader
/// kept is unchanged by then, every slot is free, and the object reads
/// whole again.
#[cfg(test)]
fn settled(r: &mut Rig, left: Left) {
    let client = r.client.clone();
    let kept = usize::from(left.piece.is_some());
    buffers_back(&client, &mut || r.pump(), LENT as usize - kept, left.held);
    if let Some((piece, len, at)) = left.piece {
        assert!(piece.as_slice()[..len] == bytes()[at..at + len]);
    }
    for _ in 0..500 {
        r.pump();
    }
    assert_eq!(requests(r), left.requests);
    slots_free(&client, &mut || r.pump(), SLOTS);
    let mut d = open(r, KEY);
    assert_eq!(read_all(r, &mut d, bytes()), (Ok(true), SIZE, 4));
}

/// Dropped while the question about the object is in flight.
#[cfg(test)]
fn while_opening(r: &mut Rig) -> Left {
    let client = r.client.clone();
    let before = requests(r);
    assert!(poll(pin!(client.download(KEY))).is_pending());
    assert_eq!(requests(r), before);
    Left {
        held: 0,
        requests: before + 1,
        piece: None,
    }
}

/// Dropped with two ranges sent to the I/O thread and not yet run.
#[cfg(test)]
fn with_ranges_in_flight(r: &mut Rig) -> Left {
    let mut d = open(r, KEY);
    let before = requests(r);
    assert!(poll(pin!(d.read())).is_pending());
    drop(d);
    assert_eq!(requests(r), before);
    Left {
        held: 2,
        requests: before + 2,
        piece: None,
    }
}

/// Dropped while the reader holds a piece, with a range in flight.
#[cfg(test)]
fn while_the_reader_holds_a_piece(r: &mut Rig) -> Left {
    let mut d = open(r, KEY);
    let before = requests(r);
    let Ok(Piece::Bytes(first, len)) = drive(r, d.read()) else {
        panic!("the first piece");
    };
    // The second range has ended; reading takes it and asks for the third.
    for _ in 0..500 {
        r.pump();
    }
    assert_eq!(requests(r), before + 2);
    let Poll::Ready(Ok(Piece::Bytes(second, _))) = poll(pin!(d.read())) else {
        panic!("the second piece was not there");
    };
    drop(second);
    drop(d);
    assert_eq!(requests(r), before + 2);
    Left {
        held: 1,
        requests: before + 3,
        piece: Some((first, len, 0)),
    }
}

/// A read dropped while it waits for its range; the next reads go on to
/// the end and verify.
#[cfg(test)]
fn after_a_read_dropped_pending(r: &mut Rig) -> Left {
    let mut d = open(r, KEY);
    let before = requests(r);
    assert!(poll(pin!(d.read())).is_pending());
    assert_eq!(read_all(r, &mut d, bytes()), (Ok(true), SIZE, 4));
    drop(d);
    Left {
        held: 0,
        requests: before + 4,
        piece: None,
    }
}

/// Dropped while it waits for a buffer: the caller holds every one, and
/// one comes back and wakes it before it is dropped.
#[cfg(test)]
fn waiting_for_a_buffer(r: &mut Rig) -> Left {
    let mut d = open(r, KEY);
    let before = requests(r);
    let mut all: [Option<Buffer>; LENT as usize] = core::array::from_fn(|_| None);
    for b in &mut all {
        *b = Some(r.buffer());
    }
    {
        let mut read = pin!(d.read());
        for _ in 0..50 {
            assert!(poll(read.as_mut()).is_pending());
            r.pump();
        }
        all[0] = None;
    }
    drop(d);
    drop(all);
    Left {
        held: 0,
        requests: before,
        piece: None,
    }
}

#[test]
fn a_download_dropped_while_opening() {
    twice(while_opening);
}

#[test]
fn a_download_dropped_with_ranges_in_flight() {
    twice(with_ranges_in_flight);
    twice(waiting_for_a_buffer);
}

#[test]
fn a_download_dropped_while_the_reader_holds_a_piece() {
    twice(while_the_reader_holds_a_piece);
}

#[test]
fn a_read_dropped_while_pending_leaves_the_download_whole() {
    twice(after_a_read_dropped_pending);
}
