// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Fetches dropped while the kernel receives into their lent buffers, on
//! the real io_uring threads: the buffer stays away until the operation
//! has ended on its I/O thread, comes back with the whole body, sees no
//! write after, and carries the next round trip.
//!
//! The same check runs on the portable backend under `cargo test`
//! (`crates/longshore_s3/tests/client/dropped_fetch.rs`).

use crate::die::OrDie;
use crate::SIZE;
use longshore::client::{Buffer, Client};
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

const KEY: &str = "dropped-fetch";
/// Fetches dropped, alternately whole and ranged.
const ROUNDS: usize = 32;
/// The range the ranged fetches ask for.
const RANGE: (u64, u64) = (4096, SIZE as u64 - 4097);

/// Never zero at 0, so a zeroed buffer shows when the first byte arrives.
fn byte(i: usize) -> u8 {
    (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ 0x5a
}

/// Store an object of a whole buffer, hold every buffer but one, and drop
/// [`ROUNDS`] fetches into that one while their bodies arrive.
pub fn check(client: &Client) {
    let mut body = client.buffer_blocking();
    for (i, b) in body.as_mut_slice().iter_mut().enumerate() {
        *b = byte(i);
    }
    let (stored, mut into) = client.store(KEY, body, SIZE as usize).wait();
    stored.or_die("store the object to fetch");
    // Two buffers per I/O thread; all but `into` are held here.
    let aside: Vec<Buffer> = (1..2 * client.threads())
        .map(|_| client.buffer_blocking())
        .collect();
    let (mut mid_body, mut held) = (0, 0);
    for round in 0..ROUNDS {
        let range = (round % 2 == 1).then_some(RANGE);
        let dropped_mid_body = start_and_drop(client, range, into);
        mid_body += usize::from(dropped_mid_body);
        let mut taking = Box::pin(client.buffer());
        let mut cx = Context::from_waker(Waker::noop());
        into = match taking.as_mut().poll(&mut cx) {
            Poll::Ready(b) => b,
            Poll::Pending => {
                held += usize::from(dropped_mid_body);
                // Stop waiting here first, or the buffer would be handed
                // to this waiter, which nothing polls again.
                drop(taking);
                client.buffer_blocking()
            }
        };
        // Lent again only once the fetch had ended: the whole body is in it.
        let want = wanted(range);
        let whole = into.as_slice()[..want.len()]
            .iter()
            .zip(want)
            .all(|(&g, i)| g == byte(i));
        assert!(
            whole,
            "round {round}: the buffer came back without the whole body"
        );
    }
    assert!(mid_body > 0, "no fetch was dropped while its body arrived");
    assert!(held > 0, "no dropped fetch still held its buffer");
    into.as_mut_slice().fill(0x33);
    std::thread::sleep(Duration::from_millis(50));
    let untouched = into.as_slice().iter().all(|&b| b == 0x33);
    assert!(untouched, "the buffer was written after it came back");
    round_trip(client, into);
    drop(aside);
    assert_eq!(client.erase(KEY).wait(), Ok(()));
    println!("ok dropped fetches: {mid_body} of {ROUNDS} dropped mid-body, {held} still holding their buffer, every buffer back whole");
}

/// What a fetch of `range` of the object reads.
fn wanted(range: Option<(u64, u64)>) -> std::ops::Range<usize> {
    match range {
        Some((first, last)) => first as usize..last as usize + 1,
        None => 0..SIZE as usize,
    }
}

/// Start a fetch of `range` into `into`, zeroed, and drop it once the
/// first wanted byte is in the buffer and the last is not. Whether it was
/// dropped there; false if the whole body came before it could be.
fn start_and_drop(client: &Client, range: Option<(u64, u64)>, mut into: Buffer) -> bool {
    into.as_mut_slice().fill(0);
    let want = wanted(range);
    let base = into.as_slice().as_ptr();
    let (first, last) = (want.start, want.end - 1);
    let (first_byte, last_byte) = (byte(first), byte(last));
    // The fetch writes the body from offset 0 of its buffer. The kernel
    // writes these bytes while they are read here; a volatile read of one
    // byte sees either the old or the new value, and nothing else of the
    // buffer is used until it is lent back.
    let at = |i: usize| unsafe { core::ptr::read_volatile(base.add(i)) };
    let mut cx = Context::from_waker(Waker::noop());
    let mut fetching = match range {
        Some((a, b)) => Box::pin(client.fetch_range(KEY, a, b, into)),
        None => Box::pin(client.fetch(KEY, into)),
    };
    assert!(fetching.as_mut().poll(&mut cx).is_pending());
    let deadline = Instant::now() + Duration::from_secs(10);
    while at(0) != first_byte {
        assert!(Instant::now() < deadline, "no byte of the body arrived");
        std::hint::spin_loop();
    }
    // Dropped here, unless the last byte is in already.
    at(last - first) != last_byte
}

/// The buffer carries a store and a fetch of another object.
fn round_trip(client: &Client, mut buffer: Buffer) {
    for (i, b) in buffer.as_mut_slice().iter_mut().enumerate() {
        *b = byte(i) ^ 0xff;
    }
    let (stored, mut buffer) = client.store("after-drop", buffer, SIZE as usize).wait();
    stored.or_die("store through the buffer that came back");
    buffer.as_mut_slice().fill(0);
    let (fetched, buffer) = client.fetch("after-drop", buffer).wait();
    assert_eq!(fetched, Ok(Some(SIZE as usize)));
    let same = buffer
        .as_slice()
        .iter()
        .enumerate()
        .all(|(i, &b)| b == byte(i) ^ 0xff);
    assert!(same, "the round trip after the drops differs");
    drop(buffer);
    assert_eq!(client.erase("after-drop").wait(), Ok(()));
}
