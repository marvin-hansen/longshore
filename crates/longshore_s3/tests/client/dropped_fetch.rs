// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! A fetch dropped while the kernel receives into its lent buffer, on real
//! sockets: the portable backend over loopback.
//!
//! The test watches the first and last bytes of the buffer the fetch holds,
//! with volatile reads, as the kernel writes it from the I/O thread, and
//! drops the future once the first has come and the last has not. The
//! buffer must then stay away until the operation ends, come back with the
//! whole body, see no write after, and carry the next round trip.

use crate::loopback::ServerThread;
use longshore_common::io::{Limits, Sock};
use longshore_http_client::server;
use longshore_s3::client::{Buffer, Client};
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// The size of the object and of every buffer: large enough that loopback
/// takes many reads to deliver it.
const SIZE: u32 = 1 << 20;
const CONNS: u32 = 2;
const KEY: &str = "dropped-fetch";
/// Fetches dropped per test.
const ROUNDS: usize = 16;

/// Never zero at 0, so a zeroed buffer shows when the first byte arrives.
fn byte(i: usize) -> u8 {
    (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ 0x5a
}

#[cfg(test)]
fn server() -> ServerThread {
    ServerThread::start(
        Limits::BASE
            .with_sockets_max(1 + CONNS)
            .with_buffers(2 * CONNS)
            .with_buffer_size(SIZE)
            .with_ops_max(CONNS * 3 + 3),
        server::ServerConfig::builder(Sock::new(0), CONNS, 1, 0, SIZE, 2 << 40)
            .head_max(4096)
            .objects_max(4)
            .build()
            .unwrap(),
    )
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
#[cfg(test)]
fn start_and_drop(client: &Client, range: Option<(u64, u64)>, mut into: Buffer) -> bool {
    into.as_mut_slice().fill(0);
    let want = wanted(range);
    let base = into.as_slice().as_ptr();
    let (first, last) = (want.start, want.end - 1);
    let (first_byte, last_byte) = (byte(first), byte(last));
    // The fetch writes the body from offset 0 of its buffer. The I/O
    // thread's kernel writes these bytes while they are read here; a
    // volatile read of one byte sees either the old or the new value, and
    // the test uses nothing else of the buffer until it is lent back.
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

/// Drop [`ROUNDS`] fetches of `range` mid-body, each with the only free
/// buffer, and check what comes back each time.
#[cfg(test)]
fn drop_while_receiving(range: Option<(u64, u64)>) {
    let server = server();
    let client = server
        .builder(CONNS, SIZE)
        .buffers(2)
        .operations(4)
        .build()
        .expect("client build on portable backend");
    let mut body = client.buffer_blocking();
    for (i, b) in body.as_mut_slice().iter_mut().enumerate() {
        *b = byte(i);
    }
    let (stored, mut into) = client.store(KEY, body, SIZE as usize).wait();
    assert_eq!(stored, Ok(()));
    let aside = client.buffer_blocking();
    let want = wanted(range);
    let (mut mid_body, mut held) = (0, 0);
    for _ in 0..ROUNDS {
        let dropped_mid_body = start_and_drop(&client, range, into);
        mid_body += usize::from(dropped_mid_body);
        // The fetch holds the only free buffer until it ends, which it
        // usually has not yet.
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
        let got = &into.as_slice()[..want.len()];
        assert!(got.iter().zip(want.clone()).all(|(&g, i)| g == byte(i)));
    }
    assert!(mid_body > 0, "no fetch was dropped while its body arrived");
    assert!(held > 0, "no dropped fetch still held its buffer");
    // Nothing writes to it after it came back.
    into.as_mut_slice().fill(0x33);
    std::thread::sleep(Duration::from_millis(50));
    assert!(into.as_slice().iter().all(|&b| b == 0x33));
    round_trip(&client, into);
    drop(aside);
    drop(client);
    server.stop();
}

/// The buffer carries a store and a fetch of another object.
#[cfg(test)]
fn round_trip(client: &Client, mut buffer: Buffer) {
    for (i, b) in buffer.as_mut_slice().iter_mut().enumerate() {
        *b = byte(i) ^ 0xff;
    }
    let (stored, mut buffer) = client.store("after-drop", buffer, SIZE as usize).wait();
    assert_eq!(stored, Ok(()));
    buffer.as_mut_slice().fill(0);
    let (fetched, buffer) = client.fetch("after-drop", buffer).wait();
    assert_eq!(fetched, Ok(Some(SIZE as usize)));
    let back = buffer.as_slice();
    assert!(back.iter().enumerate().all(|(i, &b)| b == byte(i) ^ 0xff));
}

#[test]
fn a_fetch_dropped_while_the_kernel_receives_keeps_its_buffer_until_it_ends() {
    drop_while_receiving(None);
}

#[test]
fn a_ranged_fetch_dropped_while_the_kernel_receives_keeps_its_buffer_until_it_ends() {
    drop_while_receiving(Some((4096, u64::from(SIZE) - 4097)));
}
