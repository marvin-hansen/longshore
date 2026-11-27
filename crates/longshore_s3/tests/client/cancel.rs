// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! A fetch whose future is dropped while the response is arriving in its
//! lent buffer, in the simulator, stepped one turn at a time: the buffer is
//! not lent again until the operation has ended on the I/O side, and the
//! next borrower gets it whole and can use it.

#[cfg(test)]
#[path = "../store/common/mod.rs"]
mod common;
#[expect(
    dead_code,
    reason = "these tests fill their buffers to their full length, so `fill` and `LEN` go unused in this binary"
)]
#[cfg(test)]
mod rig;
use common::{BufId, Faults, Io, BUF, LIMITS};
use longshore_s3::client::Buffer;
use rig::*;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// The key of the object every fetch here reads.
const KEY: &str = "cancel/whole";

/// Network faults while the fetch runs: most sends and receives move only
/// part of what they could, so the body arrives in many pieces.
const PIECES: Faults = Faults::NONE
    .with_short_send(900_000)
    .with_partial_recv(900_000);

/// The byte at `i` of the stored object. Never zero at 0, so a zeroed
/// buffer shows when the first byte has arrived.
fn byte(i: usize) -> u8 {
    (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ 0x5a
}

/// The simulator's buffer that `b` lends, found by its address.
#[cfg(test)]
fn buf_id_of(r: &mut Rig, b: &Buffer) -> BufId {
    let at = b.as_slice().as_ptr();
    (0..LIMITS.buffers())
        .map(BufId::new)
        .find(|&id| r.lanes[0].io.buf(id).as_ptr() == at)
        .expect("a lent buffer is one of the simulator's")
}

/// How many leading bytes of buffer `id` are `want`'s.
#[cfg(test)]
fn arrived(r: &mut Rig, id: BufId, want: &[u8]) -> usize {
    let got = &r.lanes[0].io.buf(id)[..want.len()];
    got.iter().zip(want).take_while(|(g, w)| g == w).count()
}

/// Store a whole buffer's worth under [`KEY`], take the lane's other
/// buffer so that the one the fetch gets is the only one, and zero the
/// fetch's buffer. Returns the stored bytes, the fetch's buffer, and the
/// buffer held aside.
#[cfg(test)]
fn set_up(r: &mut Rig) -> (Vec<u8>, Buffer, Buffer) {
    let mut body = r.buffer();
    let len = body.len();
    for (i, b) in body.as_mut_slice().iter_mut().enumerate() {
        *b = byte(i);
    }
    let sent = body.as_slice().to_vec();
    let (stored, mut body) = r.run(r.client.store(KEY, body, len));
    assert_eq!(stored, Ok(()));
    let mut aside = r.buffer();
    aside.as_mut_slice().fill(0xee);
    body.as_mut_slice().fill(0);
    (sent, body, aside)
}

/// Start the fetch of `want` (all of [`KEY`], or `range` of it) into
/// `into`, step the lane until part but not all of the body is in the
/// buffer, and drop the future there. Then check that the buffer stays
/// away while the operation runs on, that it comes back holding the whole
/// body, that nothing writes to it after, and that it works for the next
/// round trip.
#[cfg(test)]
fn drop_while_receiving(range: Option<(u64, u64)>) {
    let mut r = rig(1);
    let (sent, into, aside) = set_up(&mut r);
    let want = match range {
        Some((first, last)) => sent[first as usize..=last as usize].to_vec(),
        None => sent.clone(),
    };
    let id = buf_id_of(&mut r, &into);
    r.lanes[0].io.set_faults(PIECES);
    let mut cx = Context::from_waker(Waker::noop());

    let mut partial = 0;
    {
        let mut fetching = match range {
            Some((first, last)) => Box::pin(r.client.fetch_range(KEY, first, last, into)),
            None => Box::pin(r.client.fetch(KEY, into)),
        };
        for _ in 0..100_000 {
            if let Poll::Ready(v) = fetching.as_mut().poll(&mut cx) {
                panic!(
                    "the fetch ended before it could be dropped mid-body: {:?}",
                    v.0
                );
            }
            r.pump();
            partial = arrived(&mut r, id, &want);
            if partial > 0 && partial < want.len() {
                break;
            }
        }
        assert!(
            partial > 0 && partial < want.len(),
            "the body never stood part-way in the buffer ({partial} of {})",
            want.len()
        );
        // Dropped here, with the kernel still filling the buffer.
    }

    // The only free buffer is the one the fetch holds: not lent until the
    // fetch ends, however many turns that takes.
    let client = r.client.clone();
    let mut taking = pin!(client.buffer());
    let mut turns = 0;
    let back = loop {
        if let Poll::Ready(b) = taking.as_mut().poll(&mut cx) {
            break b;
        }
        assert!(turns < 100_000, "the buffer never came back");
        r.pump();
        turns += 1;
    };
    assert!(turns > 0, "the buffer was lent again at once");
    assert_eq!(
        back.as_slice().as_ptr(),
        r.lanes[0].io.buf(id).as_ptr(),
        "the buffer that came back is the one the fetch held"
    );
    // The fetch ran to its end before the buffer was lent again: all of
    // the body is in it, past what had come when the future was dropped.
    assert_eq!(&back.as_slice()[..want.len()], &want[..]);

    // Nothing writes to it after it came back.
    let mut back = back;
    back.as_mut_slice().fill(0x33);
    for _ in 0..200 {
        r.pump();
    }
    assert!(back.as_slice().iter().all(|&b| b == 0x33));

    round_trip(&mut r, back);
    // The buffer held aside was never touched.
    assert!(aside.as_slice().iter().all(|&b| b == 0xee));
}

/// The buffer that came back carries the next round trip, with the
/// network well again.
#[cfg(test)]
fn round_trip(r: &mut Rig, mut back: Buffer) {
    r.lanes[0].io.set_faults(Faults::NONE);
    let len = back.len();
    for (i, b) in back.as_mut_slice().iter_mut().enumerate() {
        *b = byte(i) ^ 0xff;
    }
    let again = back.as_slice().to_vec();
    let (stored, mut back) = r.run(r.client.store("cancel/again", back, len));
    assert_eq!(stored, Ok(()));
    back.as_mut_slice().fill(0);
    let (fetched, back) = r.run(r.client.fetch("cancel/again", back));
    assert_eq!(fetched, Ok(Some(len)));
    assert_eq!(back.as_slice(), &again[..]);
}

#[test]
fn a_fetch_dropped_while_its_body_arrives_keeps_its_buffer_until_it_ends() {
    drop_while_receiving(None);
}

#[test]
fn a_ranged_fetch_dropped_while_its_body_arrives_keeps_its_buffer_until_it_ends() {
    let last = u64::from(BUF) - 101;
    drop_while_receiving(Some((100, last)));
}
