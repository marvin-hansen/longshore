// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! What an upload stream dropped part-way must leave, and the harness the
//! drop tests share: each scenario runs twice, the second time with
//! allocation forbidden, and then every buffer, operation slot and stream
//! record must be back and no upload left open.

use super::common::{sealed, Server, BUF};
use super::returned::{buffers_back, slots_free};
use super::rig::*;
use longshore_s3::client::Upload;
use std::future::Future;
use std::pin::pin;
use std::sync::OnceLock;
use std::task::{Context, Poll, Waker};

const SIZE: usize = BUF as usize;

/// The key a run uses, and the object's path at the server.
#[derive(Clone, Copy)]
pub struct Key {
    pub key: &'static str,
    pub path: &'static str,
}

/// What a scenario leaves once its stream is dropped.
pub struct Left {
    /// Lent buffers still held by operations the stream left running.
    pub held: usize,
    /// Requests lane 0's server has seen once the lanes have settled.
    pub requests: u64,
    /// An object the stream stored after all, and its length.
    pub stored: Option<usize>,
}

/// The bytes every scenario writes, made before allocation is forbidden.
pub fn data() -> &'static [u8] {
    static DATA: OnceLock<Vec<u8>> = OnceLock::new();
    DATA.get_or_init(|| pattern(4 * SIZE, 7))
}

pub fn server(r: &Rig) -> &Server {
    &r.lanes[0].server
}

/// Run `scenario` on a fresh rig of `shape`, then again with allocation
/// forbidden, checking after each what it left.
#[cfg(test)]
pub fn twice(shape: Shape, keys: [Key; 2], scenario: fn(&mut Rig, Key) -> Left) {
    let _: &[u8] = data();
    let mut r = rig(shape);
    let warm = scenario(&mut r, keys[0]);
    settled(&mut r, shape, keys[0], &warm);
    sealed(|| {
        let left = scenario(&mut r, keys[1]);
        settled(&mut r, shape, keys[1], &left);
    });
}

/// Check what a dropped stream left: `held` buffers come back only once
/// the lanes have run, then every buffer, every operation slot and every
/// stream record is free, and no upload is left open.
#[cfg(test)]
pub fn settled(r: &mut Rig, shape: Shape, key: Key, left: &Left) {
    let client = r.client.clone();
    let lent = shape.lanes * shape.buffers as usize;
    buffers_back(&client, &mut || r.pump(), lent, left.held);
    for _ in 0..2000 {
        r.pump();
    }
    for lane in &r.lanes {
        assert_eq!(lane.server.uploads_in_progress(), 0);
    }
    assert_eq!(server(r).requests(), left.requests);
    assert_eq!(server(r).object(key.path).map(<[u8]>::len), left.stored);
    let slots = shape.lanes * shape.operations as usize;
    slots_free(&client, &mut || r.pump(), slots);
    records_free(r, shape.streams as usize);
}

/// Every one of `streams` stream records is free: as many streams open at
/// once without the lanes running, and one more waits.
#[cfg(test)]
pub fn records_free(r: &mut Rig, streams: usize) {
    const KEYS: [&str; 4] = ["r0", "r1", "r2", "r3"];
    let client = r.client.clone();
    let mut cx = Context::from_waker(Waker::noop());
    let mut open: [Option<Upload>; 4] = core::array::from_fn(|_| None);
    for (n, slot) in open.iter_mut().take(streams).enumerate() {
        let Poll::Ready(Ok(up)) = pin!(client.upload(KEYS[n])).poll(&mut cx) else {
            panic!("a stream record was still held");
        };
        *slot = Some(up);
    }
    assert!(pin!(client.upload("r-more")).poll(&mut cx).is_pending());
}
