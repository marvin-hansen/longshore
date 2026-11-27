// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Lanes driven by hand in the simulator for upload streams: as many lent
//! buffers, operation slots and stream records as a test asks for, and a
//! server whose objects may be larger than one buffer.

use crate::common::*;
use longshore_s3::client::{Client, Layout, Worker};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// The server's buffers come first, two per connection.
const SERVER_BUFFERS: u32 = CONNS * 2;

/// What a test needs of its lanes.
#[derive(Clone, Copy)]
pub struct Shape {
    pub lanes: usize,
    /// Buffers lent to callers, per lane.
    pub buffers: u32,
    pub operations: u32,
    pub streams: u32,
    pub part_min: u64,
    /// Objects each server holds, parts included, and the bytes each holds.
    pub objects: u32,
    pub object_size: u32,
}

impl Shape {
    /// One lane of four buffers and four slots, two streams, parts of a
    /// buffer at least, and objects of up to sixteen buffers.
    pub const fn new() -> Shape {
        Shape {
            lanes: 1,
            buffers: 4,
            operations: 4,
            streams: 2,
            part_min: BUF as u64,
            objects: 16,
            object_size: 16 * BUF,
        }
    }
}

/// One I/O lane, with the server it talks to.
pub struct Lane {
    pub io: SimIo,
    pub worker: Worker,
    pub server: Server,
    pub out: Completions,
}

pub struct Rig {
    pub lanes: Vec<Lane>,
    pub client: Client,
}

pub fn rig(shape: Shape) -> Rig {
    let limits = Limits::BASE
        .with_sockets_max(1 + CONNS * 2)
        .with_buffers(SERVER_BUFFERS + shape.buffers)
        .with_buffer_size(BUF)
        .with_small_buffers(CONNS)
        .with_small_buffer_size(HEAD_MAX)
        .with_ops_max(CONNS * 3 + 4);
    let mut handles = Vec::new();
    let lanes = (0..shape.lanes)
        .map(|n| {
            let mut io = SimIo::new(limits, 70 + n as u64, Faults::default());
            let mut server = Server::new(
                server::ServerConfig::builder(Sock::new(0), CONNS, 1 + CONNS, 0, BUF, 2 << 40)
                    .head_max(2048)
                    .objects_max(shape.objects)
                    .object_size(shape.object_size)
                    .checksum(|body: &[u8]| {
                        longshore_crc::crc64_header_value(longshore_crc::crc64_nvme(body))
                    })
                    .build()
                    .unwrap(),
            );
            let _: SocketAddr = server.start(&mut io, ADDR).unwrap();
            let layout = Layout::new(
                shape.operations,
                BufId::new(SERVER_BUFFERS),
                shape.buffers,
                7 << 40,
                10 * MS,
            )
            .with_streams(shape.streams)
            .with_part_min(shape.part_min);
            let config = s3_config(3, false)
                .with_http(http_config(1, limits.buffers(), 1 << 40))
                .with_requests_max(shape.operations);
            let (mut worker, lane) = Worker::new(&mut io, S3::new(config), layout).unwrap();
            worker.start(&mut io);
            handles.push(lane);
            Lane {
                io,
                worker,
                server,
                out: Completions::new(16),
            }
        })
        .collect();
    Rig {
        lanes,
        // The lanes' simulators live in the rig beside the client.
        client: unsafe { Client::over(handles) },
    }
}

impl Rig {
    /// One turn of lane `n`'s loop.
    pub fn pump_lane(&mut self, n: usize) {
        let l = &mut self.lanes[n];
        l.worker.turn(&mut l.io);
        l.io.tick(true, &mut l.out).unwrap();
        while let Some(c) = l.out.pop() {
            if !l.worker.on_completion(&mut l.io, c) {
                assert!(l.server.on_completion(&mut l.io, c), "unrouted {c:?}");
            }
        }
        l.worker.turn(&mut l.io);
    }

    /// One turn of every lane's loop.
    pub fn pump(&mut self) {
        for n in 0..self.lanes.len() {
            self.pump_lane(n);
        }
    }

    /// Drive every lane until `f` ends.
    pub fn run<F: Future>(&mut self, f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
            self.pump();
        }
        panic!("the future did not end");
    }

    /// Drive only lane `n` until `f` ends.
    pub fn run_on<F: Future>(&mut self, n: usize, f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
            self.pump_lane(n);
        }
        panic!("the future did not end");
    }
}

/// `len` bytes of a pattern that differs from buffer to buffer, so a part
/// out of place shows.
pub fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31) ^ ((i / 1000) as u8) ^ seed)
        .collect()
}

/// A part of the upload `upload` (as the server numbers uploads) of `key`.
pub fn part_target(key: &str, number: u32, upload: u32) -> String {
    format!("/{key}?partNumber={number}&uploadId=sim-upload-{upload:X}")
}
