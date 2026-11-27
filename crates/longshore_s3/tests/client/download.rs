// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The download stream through the shareable client: ranges a window
//! ahead, handed out in order, `If-Match` on every range, and the whole
//! object's checksum checked at the end. In the simulator, and once on
//! real sockets for the blocking forms.

#![cfg(not(longshore_model))]

#[cfg(test)]
#[path = "../store/common/mod.rs"]
mod common;
#[cfg(test)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod loopback;
#[cfg(test)]
#[expect(
    dead_code,
    reason = "the shared rig's own lanes and helpers; these tests build a lane of their own"
)]
mod rig;
#[cfg(test)]
#[path = "download/setup.rs"]
mod setup;

use common::{sealed, Behavior, Fatal};
use longshore_s3::client::{Error, Piece};
use setup::*;

/// Three full ranges and a short one: more than every lent buffer holds.
const SIZE: usize = 3 * RANGE + 1000;

#[test]
fn an_object_larger_than_every_lent_buffer_arrives_in_order_and_verified() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 1);
    assert!(SIZE > LENT as usize * RANGE);
    upload(&mut r, "big", &bytes, true);
    let mut d = open(&mut r, "big");
    fn can_move_to_another_thread<T: Send>(_: &T) {}
    can_move_to_another_thread(&d);
    assert_eq!(d.stat().size(), SIZE as u64);
    assert!(d.stat().checksum().is_some());
    assert_eq!(read_all(&mut r, &mut d, &bytes), (Ok(true), SIZE, 4));
    // The end stays the end.
    assert!(matches!(
        drive(&mut r, d.read()),
        Ok(Piece::End { verified: true })
    ));
}

#[test]
fn every_range_of_the_window_is_fetched_before_the_reader_reaches_it() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 10);
    upload(&mut r, "ahead", &bytes, true);
    let mut d = open(&mut r, "ahead");
    let before = r.lanes[0].server.requests();
    // One read asks for the window: two ranges, leaving a buffer of the
    // three for the reader. Both reach the store, not only the first.
    let mut read = std::pin::pin!(d.read());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(read.as_mut(), &mut cx).is_pending());
    for _ in 0..500 {
        r.pump();
    }
    assert_eq!(r.lanes[0].server.requests(), before + 2);
    assert!(std::future::Future::poll(read.as_mut(), &mut cx).is_ready());
}

#[test]
fn a_flipped_bit_in_one_range_ends_with_a_checksum_error() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 2);
    upload(&mut r, "damaged", &bytes, true);
    // The question about the object goes first, then a range: whichever
    // range the server reads next has one bit flipped on the way.
    r.lanes[0].server.script(Behavior::Normal);
    r.lanes[0].server.script(Behavior::FlipBit(100));
    let mut d = open(&mut r, "damaged");
    let mut seen = vec![0u8; 0];
    let mut end = None;
    while end.is_none() {
        match drive(&mut r, d.read()) {
            Ok(Piece::Bytes(buffer, len)) => seen.extend_from_slice(&buffer.as_slice()[..len]),
            other => end = Some(other.map(|_| ())),
        }
    }
    assert_eq!(end, Some(Err(Error::Store(Fatal::ChecksumMismatch))));
    // Every byte was handed out, one of them damaged, before the end said so.
    assert_eq!(seen.len(), SIZE);
    let wrong = seen.iter().zip(&bytes).filter(|(a, b)| a != b).count();
    assert_eq!(wrong, 1);
    assert!(matches!(
        drive(&mut r, d.read()),
        Err(Error::Store(Fatal::ChecksumMismatch))
    ));
}

#[test]
fn an_object_replaced_mid_download_fails_the_next_range_as_a_precondition() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 3);
    upload(&mut r, "moving", &bytes, true);
    let mut d = open(&mut r, "moving");
    let Ok(Piece::Bytes(first, len)) = drive(&mut r, d.read()) else {
        panic!("the first range");
    };
    assert_eq!(first.as_slice()[..len], bytes[..len]);
    drop(first);
    store(&mut r, "moving", &object(RANGE, 4), true);
    // Ranges fetched before the replacement may still be handed out, and
    // are the old object's; none of the new one's are.
    let (end, at, _) = read_all(&mut r, &mut d, &bytes[len..]);
    assert_eq!(end, Err(Error::Store(Fatal::Status(412))));
    assert!(len + at < SIZE);
}

#[test]
fn an_object_the_store_keeps_no_checksum_of_ends_unverified() {
    let mut r = download_rig(false);
    let bytes = object(SIZE, 5);
    upload(&mut r, "plain", &bytes, false);
    let mut d = open(&mut r, "plain");
    assert_eq!(d.stat().checksum(), None);
    assert_eq!(read_all(&mut r, &mut d, &bytes), (Ok(false), SIZE, 4));
}

#[test]
fn an_object_within_one_range_is_one_piece() {
    let mut r = download_rig(true);
    let bytes = object(RANGE - 1, 6);
    store(&mut r, "small", &bytes, true);
    let mut d = open(&mut r, "small");
    assert_eq!(read_all(&mut r, &mut d, &bytes), (Ok(true), RANGE - 1, 1));
}

#[test]
fn an_empty_object_ends_at_once_and_verified() {
    let mut r = download_rig(true);
    store(&mut r, "empty", b"", true);
    let mut d = open(&mut r, "empty");
    assert_eq!(d.stat().size(), 0);
    assert_eq!(read_all(&mut r, &mut d, b""), (Ok(true), 0, 0));
}

#[test]
fn a_missing_key_opens_as_none_and_a_bad_key_fails() {
    let mut r = download_rig(true);
    let client = r.client.clone();
    assert!(matches!(drive(&mut r, client.download("absent")), Ok(None)));
    assert!(matches!(
        drive(&mut r, client.download("")),
        Err(Error::BadKey)
    ));
}

#[test]
fn a_store_that_answers_a_range_with_the_whole_object_fails_the_download() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 7);
    upload(&mut r, "whole", &bytes, true);
    r.lanes[0].server.script(Behavior::Normal);
    r.lanes[0].server.script(Behavior::Raw(
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nwhat",
    ));
    let mut d = open(&mut r, "whole");
    let (end, _, _) = read_all(&mut r, &mut d, &bytes);
    assert_eq!(end, Err(Error::Store(Fatal::RangeIgnored)));
}

#[test]
fn a_download_after_warm_up_allocates_nothing() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 8);
    upload(&mut r, "sealed", &bytes, true);
    let mut warm = open(&mut r, "sealed");
    assert_eq!(read_all(&mut r, &mut warm, &bytes), (Ok(true), SIZE, 4));
    drop(warm);
    let client = r.client.clone();
    let mut missing = false;
    let ended = sealed(|| {
        let mut d = drive(&mut r, client.download("sealed")).unwrap().unwrap();
        missing = matches!(drive(&mut r, client.download("absent")), Ok(None));
        read_all(&mut r, &mut d, &bytes)
    });
    assert!(missing);
    assert_eq!(ended, (Ok(true), SIZE, 4));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn the_blocking_forms_read_an_object_over_loopback() {
    use longshore_common::io::{Limits, Sock};
    use longshore_http_client::server;
    use loopback::ServerThread;

    // The server holds objects of 64 KiB, the client lends two of 16.
    const SERVER: u32 = 64 * 1024;
    const LENT: usize = 16 * 1024;
    const CONNS: u32 = 2;
    let server = ServerThread::start(
        Limits::BASE
            .with_sockets_max(1 + CONNS)
            .with_buffers(2 * CONNS)
            .with_buffer_size(SERVER)
            .with_ops_max(CONNS * 3 + 3),
        server::ServerConfig::builder(Sock::new(0), CONNS, 1, 0, SERVER, 2 << 40)
            .head_max(4096)
            .objects_max(8)
            .checksum(|body: &[u8]| {
                longshore_crc::crc64_header_value(longshore_crc::crc64_nvme(body))
            })
            .build()
            .unwrap(),
    );
    let client = server
        .builder(CONNS, LENT as u32)
        .buffers(2)
        .operations(4)
        .build()
        .expect("client build on portable backend");
    let bytes = object(3 * LENT + 99, 9);
    let id = client.upload_begin("loop").wait().expect("begin");
    let mut parts = Vec::new();
    for (n, chunk) in bytes.chunks(LENT).enumerate() {
        let mut body = client.buffer_blocking();
        body.as_mut_slice()[..chunk.len()].copy_from_slice(chunk);
        let (part, _body) = client
            .upload_part("loop", &id, n as u32 + 1, body, chunk.len())
            .wait();
        parts.push(part.expect("part"));
    }
    let scratch = client.buffer_blocking();
    let (done, _scratch) = client.upload_complete("loop", &id, &parts, scratch).wait();
    assert_eq!(done, Ok(()));

    let mut d = client.download("loop").wait().unwrap().expect("it exists");
    let mut seen = Vec::new();
    let verified = loop {
        match d.read_blocking().expect("a piece") {
            Piece::Bytes(buffer, len) => seen.extend_from_slice(&buffer.as_slice()[..len]),
            Piece::End { verified } => break verified,
        }
    };
    assert!(verified);
    assert!(seen == bytes, "the object came back whole and in order");
    assert!(client.download("nothing").wait().unwrap().is_none());
    drop(d);
    drop(client);
    server.stop();
}
