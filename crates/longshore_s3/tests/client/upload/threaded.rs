// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Upload streams through a threaded client on loopback, with the blocking
//! forms, and a shutdown that waits for a dropped stream's abort.

use super::loopback::ServerThread;
use super::rig::pattern;
use longshore_common::io::{Limits, Sock};
use longshore_http_client::server;

const SIZE: u32 = 16 * 1024;
const CONNS: u32 = 2;

#[test]
fn blocking_calls_stream_an_object_and_a_shutdown_waits_for_an_abort() {
    let server = ServerThread::start(
        Limits::BASE
            .with_sockets_max(1 + CONNS * 2)
            .with_buffers(2 * CONNS * 2)
            .with_buffer_size(SIZE)
            .with_ops_max(CONNS * 6 + 3),
        server::ServerConfig::builder(Sock::new(0), CONNS * 2, 1, 0, SIZE, 2 << 40)
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
        .threads(2)
        .buffers(4)
        .operations(4)
        .part_min(u64::from(SIZE))
        .build()
        .unwrap();
    let size = SIZE as usize;
    let data = pattern(3 * size + 77, 9);

    let mut up = client.upload("blocking").wait().unwrap();
    for chunk in data.chunks(5000) {
        up.write(chunk).wait().unwrap();
    }
    up.finish().wait().unwrap();
    // The object is larger than a buffer: read it back a buffer at a time.
    let mut into = client.buffer_blocking();
    for (n, expected) in data.chunks(size).enumerate() {
        let first = (n * size) as u64;
        let last = first + expected.len() as u64 - 1;
        let (fetched, back) = client.fetch_range("blocking", first, last, into).wait();
        assert_eq!(fetched, Ok(Some(expected.len())));
        assert_eq!(&back.as_slice()[..expected.len()], expected);
        into = back;
    }
    drop(into);

    // Dropped with parts in flight, then the client goes: its threads stay
    // until the abort has gone out.
    let mut dropped = client.upload("dropped").wait().unwrap();
    dropped.write(&data).wait().unwrap();
    drop(dropped);
    drop(client);
    // The server keeps eight uploads at most: had the abort been lost, one
    // of these could not begin.
    let after = server.builder(CONNS, SIZE).build().unwrap();
    for n in 0..8 {
        let begun = after.upload_begin(&format!("after/{n}")).wait();
        assert!(begun.is_ok(), "{n}: {begun:?}");
    }
    drop(after);
    server.stop();
}
