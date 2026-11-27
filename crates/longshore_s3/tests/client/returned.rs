// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! What a stream dropped part-way must leave behind: every lent buffer
//! back, but none before the kernel is done with it, and every operation
//! slot free. Shared by the upload and download tests; allocates nothing.

use longshore_s3::client::{Buffer, Client, Error, Pending};
use std::future::Future;
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};

/// More than any test here lends or has slots for.
const MOST: usize = 16;

/// Keys the slot check erases, one per slot.
const KEYS: [&str; MOST] = [
    "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14",
    "s15",
];

/// Poll `f` once, with a waker that does nothing.
pub fn poll_once<F: Future>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}

/// Borrow all `buffers` of the client's lent buffers: `held` of them are
/// still held by operations a dropped stream left running, so only the
/// others are lent before the lanes run, and those come once they have.
/// Each comes back whole, not void, and is written over: nothing the
/// kernel still uses is reachable here.
#[cfg(test)]
pub fn buffers_back(client: &Client, pump: &mut dyn FnMut(), buffers: usize, held: usize) {
    assert!(buffers <= MOST && held <= buffers);
    let mut cx = Context::from_waker(Waker::noop());
    let mut lent: [Option<Buffer>; MOST] = core::array::from_fn(|_| None);
    // Buffers lent before the lanes first had to run.
    let (mut at_once, mut waited) = (0, false);
    for slot in lent.iter_mut().take(buffers) {
        let mut taking = pin!(client.buffer());
        let mut turns = 0;
        let b = loop {
            if let Poll::Ready(b) = taking.as_mut().poll(&mut cx) {
                break b;
            }
            assert!(turns < 100_000, "a lent buffer never came back");
            pump();
            turns += 1;
        };
        waited |= turns > 0;
        at_once += usize::from(!waited);
        assert_eq!(b.len(), client.buffer_size(), "a buffer came back void");
        *slot = Some(b);
    }
    assert_eq!(
        at_once,
        buffers - held,
        "lent before the operations holding them ended"
    );
    for b in lent.iter_mut().flatten() {
        b.as_mut_slice().fill(0xa5);
    }
    // Nothing writes to them while they are lent.
    for _ in 0..50 {
        pump();
    }
    for b in lent.iter().flatten() {
        assert!(b.as_slice().iter().all(|&x| x == 0xa5));
    }
}

/// Every one of the client's `slots` operation slots is free: as many
/// erases, each polled once before any is collected, are all sent at once.
/// An erase that found no free slot could get one only once another's
/// result is taken, so polling the last first shows it.
#[cfg(test)]
pub fn slots_free(client: &Client, pump: &mut dyn FnMut(), slots: usize) {
    assert!(slots <= MOST);
    let mut cx = Context::from_waker(Waker::noop());
    let mut erases: [Option<Pending<Result<(), Error>>>; MOST] = core::array::from_fn(|_| None);
    for (n, slot) in erases.iter_mut().take(slots).enumerate() {
        let mut erase = client.erase(KEYS[n]);
        assert!(Pin::new(&mut erase).poll(&mut cx).is_pending());
        *slot = Some(erase);
    }
    for _ in 0..2000 {
        pump();
    }
    for erase in erases.iter_mut().take(slots).rev() {
        let Some(e) = erase else { continue };
        let Poll::Ready(done) = Pin::new(e).poll(&mut cx) else {
            panic!("an operation slot was still held");
        };
        assert_eq!(done, Ok(()));
    }
}
