// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! A lane whose lent buffers are smaller than the objects its server
//! holds, so that one object takes several ranges and outgrows every lent
//! buffer together; and the steps the download tests share.

use crate::common::*;
use crate::rig::{LaneRig, Rig};
use longshore_s3::client::{Client, Download, Error, Layout, Piece, Worker};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// The size of a lent buffer, and so of a range.
pub const RANGE: usize = 4096;
/// Lent buffers: together smaller than the server's largest object.
pub const LENT: u32 = 3;
/// Server pairs 0..6 and two spare large buffers, then the client's head
/// buffers and the lent buffers among the small ones.
const LIMITS_DL: Limits = Limits::BASE
    .with_sockets_max(1 + CONNS * 2)
    .with_buffers(8)
    .with_buffer_size(BUF)
    .with_small_buffers(CONNS + LENT)
    .with_small_buffer_size(RANGE as u32)
    .with_ops_max(CONNS * 3 + 4);

/// One lane; its server keeps a CRC-64/NVME of each object if `checksums`.
#[cfg(test)]
pub fn download_rig(checksums: bool) -> Rig {
    let mut io = SimIo::new(LIMITS_DL, 41, Faults::default());
    let built = server::ServerConfig::builder(Sock::new(0), CONNS, 1 + CONNS, 0, BUF, 2 << 40)
        .head_max(2048)
        .objects_max(16);
    let built = if checksums {
        built.checksum(|body: &[u8]| {
            longshore_crc::crc64_header_value(longshore_crc::crc64_nvme(body))
        })
    } else {
        built
    };
    let mut server = Server::new(built.build().unwrap());
    let _: SocketAddr = server.start(&mut io, ADDR).unwrap();
    let http = client::ClientConfig::builder(CONNS, 1, LIMITS_DL.buffers(), RANGE as u32, 1 << 40)
        .head_max(RANGE as u32)
        .max_age_ns(60_000 * MS)
        .max_requests(1000)
        .request_timeout_ns(1_000 * MS)
        .build()
        .unwrap();
    let cfg = Config::new(
        longshore_s3::s3::Vendor::S3,
        "bucket.objects.test".into(),
        "us-test-1".into(),
        Credentials::new("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY").unwrap(),
        http,
        Endpoint::Fixed(ADDR),
    )
    .with_requests_max(4)
    .with_attempts_max(3)
    .with_backoff_ns(100 * MS)
    .with_backoff_max_ns(800 * MS);
    let lent = BufId::new(LIMITS_DL.buffers() + CONNS);
    let layout = Layout::new(4, lent, LENT, 7 << 40, 10 * MS);
    let (mut worker, lane) = Worker::new(&mut io, S3::new(cfg), layout).unwrap();
    worker.start(&mut io);
    let lane_rig = LaneRig {
        io,
        worker,
        server,
        out: Completions::new(16),
    };
    Rig {
        lanes: vec![lane_rig],
        // The lane's simulator lives in the rig beside the client.
        client: unsafe { Client::over(vec![lane]) },
    }
}

/// Drive the lanes until `f` ends.
#[cfg(test)]
pub fn drive<F: Future>(r: &mut Rig, f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100_000 {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        r.pump();
    }
    panic!("it did not end");
}

/// `len` bytes that differ from range to range and from `seed` to `seed`.
pub fn object(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2654435761).to_le_bytes()[1] ^ seed)
        .collect()
}

/// Store `bytes` under `key` as a multipart upload of one range per part,
/// so that it may be larger than a lent buffer, with checksums or not.
#[cfg(test)]
pub fn upload(r: &mut Rig, key: &str, bytes: &[u8], checksums: bool) {
    let client = r.client.clone().with_checksums(checksums);
    let id = drive(r, client.upload_begin(key)).expect("begin");
    let mut parts = Vec::new();
    for (n, chunk) in bytes.chunks(RANGE).enumerate() {
        let mut body = r.buffer();
        body.as_mut_slice()[..chunk.len()].copy_from_slice(chunk);
        let (part, _body) = drive(
            r,
            client.upload_part(key, &id, n as u32 + 1, body, chunk.len()),
        );
        parts.push(part.expect("part"));
    }
    let scratch = r.buffer();
    let (done, _scratch) = drive(r, client.upload_complete(key, &id, &parts, scratch));
    assert_eq!(done, Ok(()));
    assert_eq!(r.lanes[0].server.object(&format!("/{key}")), Some(bytes));
}

/// Store `bytes`, no longer than one buffer, under `key` with one plain
/// store, sending a checksum or not.
#[cfg(test)]
pub fn store(r: &mut Rig, key: &str, bytes: &[u8], checksum: bool) {
    let client = r.client.clone().with_checksums(checksum);
    let mut body = r.buffer();
    body.as_mut_slice()[..bytes.len()].copy_from_slice(bytes);
    let (stored, _body) = drive(r, client.store(key, body, bytes.len()));
    assert_eq!(stored, Ok(()));
}

/// Open a download of `key`, which must exist.
#[cfg(test)]
pub fn open(r: &mut Rig, key: &str) -> Download {
    let client = r.client.clone();
    drive(r, client.download(key))
        .expect("the store answered")
        .expect("the object exists")
}

/// Read `d` to its end, checking each piece against `expected` in order.
/// Returns how it ended and how many bytes and pieces came before that.
/// Allocates nothing.
#[cfg(test)]
pub fn read_all(
    r: &mut Rig,
    d: &mut Download,
    expected: &[u8],
) -> (Result<bool, Error>, usize, usize) {
    let (mut at, mut pieces) = (0, 0);
    loop {
        match drive(r, d.read()) {
            Ok(Piece::Bytes(buffer, len)) => {
                assert!(len <= RANGE && len > 0, "a piece of {len} bytes");
                assert!(
                    buffer.as_slice()[..len] == expected[at..at + len],
                    "the bytes at {at} are the object's, in order"
                );
                at += len;
                pieces += 1;
            }
            Ok(Piece::End { verified }) => return (Ok(verified), at, pieces),
            Err(e) => return (Err(e), at, pieces),
        }
    }
}
