// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! An upload stream and a download dropped mid-transfer on real sockets:
//! the portable backend over loopback, with two I/O threads. Afterwards
//! every lent buffer comes back, none is written once lent, a piece the
//! reader kept stays the object's, both stream records come back, and the
//! client still moves whole objects.

use crate::loopback::ServerThread;
use longshore_common::io::{Limits, Sock};
use longshore_http_client::server;
use longshore_s3::client::{Buffer, Client, Piece};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

const SIZE: u32 = 128 * 1024;
const CONNS: u32 = 2;
const THREADS: u32 = 2;
/// Lent buffers per thread.
const BUFFERS: u32 = 4;
const STREAMS: u32 = 2;
/// Times each drop is repeated, so some land while bytes are moving.
const ROUNDS: usize = 8;

fn byte(i: usize, seed: u8) -> u8 {
    (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ seed
}

fn data(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| byte(i, seed)).collect()
}

/// Poll `f` until it ends, for ten seconds at most: a leak fails the test
/// instead of hanging it.
#[cfg(test)]
fn within<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        assert!(Instant::now() < deadline, "it never ended");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
fn setup() -> (ServerThread, Client) {
    let server = ServerThread::start(
        Limits::BASE
            .with_sockets_max(1 + CONNS * THREADS)
            .with_buffers(2 * CONNS * THREADS)
            .with_buffer_size(SIZE)
            .with_ops_max(CONNS * THREADS * 3 + 3),
        server::ServerConfig::builder(Sock::new(0), CONNS * THREADS, 1, 0, SIZE, 2 << 40)
            .head_max(4096)
            .checksum(|body: &[u8]| {
                longshore_crc::crc64_header_value(longshore_crc::crc64_nvme(body))
            })
            .objects_max(16)
            .object_size(8 * SIZE)
            .build()
            .unwrap(),
    );
    let client = server
        .builder(CONNS, SIZE)
        .threads(THREADS)
        .buffers(BUFFERS)
        .operations(8)
        .streams(STREAMS)
        .part_min(u64::from(SIZE))
        .build()
        .expect("client build on portable backend");
    (server, client)
}

/// Every lent buffer but `kept` comes back, not void; written over, none
/// changes while lent.
#[cfg(test)]
fn buffers_back(client: &Client, kept: usize) {
    let all = (THREADS * BUFFERS) as usize - kept;
    let mut lent: Vec<Buffer> = (0..all).map(|_| within(client.buffer())).collect();
    for b in &mut lent {
        assert_eq!(b.len(), SIZE as usize, "a buffer came back void");
        b.as_mut_slice().fill(0x33);
    }
    std::thread::sleep(Duration::from_millis(50));
    for b in &lent {
        assert!(b.as_slice().iter().all(|&x| x == 0x33));
    }
}

/// Stream `bytes` to `key` and finish.
#[cfg(test)]
fn stream(client: &Client, key: &str, bytes: &[u8]) {
    let mut up = client.upload(key).wait().unwrap();
    for chunk in bytes.chunks(5000) {
        up.write(chunk).wait().unwrap();
    }
    up.finish().wait().unwrap();
}

/// Read `key` whole, checking it against `bytes` and verifying it.
#[cfg(test)]
fn read_whole(client: &Client, key: &str, bytes: &[u8]) {
    let mut d = client.download(key).wait().unwrap().expect("it exists");
    let mut at = 0;
    loop {
        match d.read_blocking().expect("a piece") {
            Piece::Bytes(b, len) => {
                assert!(b.as_slice()[..len] == bytes[at..at + len]);
                at += len;
            }
            Piece::End { verified } => {
                assert!(verified);
                break;
            }
        }
    }
    assert_eq!(at, bytes.len());
}

#[test]
fn an_upload_stream_dropped_with_parts_in_flight_leaves_nothing_held() {
    let (server, client) = setup();
    let size = SIZE as usize;
    let bytes = data(3 * size + 77, 1);
    for _ in 0..ROUNDS {
        let mut up = client.upload("dropped-up").wait().unwrap();
        // The begin, then each full buffer sent as a part as the next
        // bytes arrive: dropped with parts on their way.
        up.write(&bytes).wait().unwrap();
        drop(up);
        buffers_back(&client, 0);
        // Both records come back, the dropped one once its abort is sent.
        let opened: Vec<_> = (0..STREAMS)
            .map(|n| within(client.upload(["r0", "r1"][n as usize])).unwrap())
            .collect();
        drop(opened);
    }
    stream(&client, "after-up", &bytes);
    read_whole(&client, "after-up", &bytes);
    drop(client);
    server.stop();
}

#[test]
fn a_download_dropped_mid_transfer_leaves_nothing_held() {
    let (server, client) = setup();
    let size = SIZE as usize;
    let bytes = data(4 * size + 99, 2);
    stream(&client, "object", &bytes);
    for _ in 0..ROUNDS {
        let mut d = client
            .download("object")
            .wait()
            .unwrap()
            .expect("it exists");
        let Piece::Bytes(first, len) = d.read_blocking().expect("a piece") else {
            panic!("the first piece");
        };
        // The window is asked for again, and dropped with ranges in flight.
        let asked = pin!(d.read()).poll(&mut Context::from_waker(Waker::noop()));
        // A second piece, if it had come already, goes back here.
        drop(asked);
        drop(d);
        buffers_back(&client, 1);
        assert!(first.as_slice()[..len] == bytes[..len]);
    }
    read_whole(&client, "object", &bytes);
    drop(client);
    server.stop();
}
