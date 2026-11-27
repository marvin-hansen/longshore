// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The copying forms through the shareable client: a slice in, a slice
//! out, one buffer when it fits and the streams when it does not. In the
//! simulator, on a lane whose lent buffers are smaller than its objects,
//! and once on real sockets for the blocking forms.

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
#[expect(
    dead_code,
    reason = "the download tests' helpers; these tests use only some of them"
)]
#[path = "download/setup.rs"]
mod setup;

use common::{sealed, Behavior, Fatal};
use longshore_s3::client::Error;
use setup::*;

/// Three full ranges and a short one: more than every lent buffer holds.
const SIZE: usize = 3 * RANGE + 1000;

#[test]
fn a_short_slice_goes_in_and_comes_out_through_one_buffer() {
    let mut r = download_rig(true);
    let bytes = object(1000, 3);
    let client = r.client.clone();
    assert_eq!(drive(&mut r, client.store_from("short", &bytes)), Ok(()));
    assert_eq!(r.lanes[0].server.object("/short"), Some(&bytes[..]));
    let mut out = [0u8; RANGE];
    assert_eq!(
        drive(&mut r, client.fetch_into("short", &mut out)),
        Ok(Some(1000))
    );
    assert!(out[..1000] == bytes[..]);
}

#[test]
fn a_long_slice_goes_in_through_an_upload_stream_and_comes_back_whole() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 2);
    let client = r.client.clone();
    assert_eq!(drive(&mut r, client.store_from("long", &bytes)), Ok(()));
    assert_eq!(r.lanes[0].server.object("/long"), Some(&bytes[..]));
    assert_eq!(r.lanes[0].server.uploads_in_progress(), 0);
    let mut out = vec![0u8; SIZE];
    assert_eq!(
        drive(&mut r, client.fetch_into("long", &mut out)),
        Ok(Some(SIZE))
    );
    assert!(out == bytes, "every byte, in order");
}

#[test]
fn a_long_slice_is_stored_with_nothing_allocated() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 3);
    let client = r.client.clone();
    // Warm up once, then seal.
    assert_eq!(drive(&mut r, client.store_from("warm", &bytes)), Ok(()));
    let stored = sealed(|| drive(&mut r, client.store_from("sealed", &bytes)));
    assert_eq!(stored, Ok(()));
    assert_eq!(r.lanes[0].server.object("/sealed"), Some(&bytes[..]));
}

#[test]
fn a_long_slice_is_filled_through_a_download() {
    let mut r = download_rig(true);
    let bytes = object(SIZE, 4);
    upload(&mut r, "long", &bytes, true);
    let mut out = vec![0u8; SIZE + 7];
    let client = r.client.clone();
    let got = sealed(|| drive(&mut r, client.fetch_into("long", &mut out)));
    assert_eq!(got, Ok(Some(SIZE)));
    assert!(out[..SIZE] == bytes[..], "every byte, in order");
}

#[test]
fn an_object_longer_than_the_slice_is_too_long_whichever_way_it_is_fetched() {
    let mut r = download_rig(true);
    upload(&mut r, "long", &object(SIZE, 5), true);
    store(&mut r, "small", &object(1000, 6), true);
    let client = r.client.clone();
    // Fits a buffer, but not the slice.
    let mut short = [0u8; 999];
    assert_eq!(
        drive(&mut r, client.fetch_into("small", &mut short)),
        Err(Error::TooLong)
    );
    // Longer than a buffer, and the slice is one buffer.
    let mut one = [0u8; RANGE];
    assert_eq!(
        drive(&mut r, client.fetch_into("long", &mut one)),
        Err(Error::TooLong)
    );
    // Longer than a buffer, and the slice is longer too, but not enough.
    let mut almost = vec![0u8; SIZE - 1];
    assert_eq!(
        drive(&mut r, client.fetch_into("long", &mut almost)),
        Err(Error::TooLong)
    );
}

#[test]
fn a_missing_key_is_none_through_one_buffer_and_through_a_download() {
    let mut r = download_rig(true);
    let client = r.client.clone();
    let mut short = [0u8; 10];
    assert_eq!(
        drive(&mut r, client.fetch_into("absent", &mut short)),
        Ok(None)
    );
    let mut long = vec![0u8; SIZE];
    assert_eq!(
        drive(&mut r, client.fetch_into("absent", &mut long)),
        Ok(None)
    );
}

#[test]
fn a_damaged_range_fails_the_long_fetch_with_a_checksum_error() {
    let mut r = download_rig(true);
    upload(&mut r, "long", &object(SIZE, 8), true);
    // The question about the object goes first, then one damaged range.
    r.lanes[0].server.script(Behavior::Normal);
    r.lanes[0].server.script(Behavior::FlipBit(17));
    let mut out = vec![0u8; SIZE];
    let client = r.client.clone();
    let got = drive(&mut r, client.fetch_into("long", &mut out));
    assert_eq!(got, Err(Error::Store(Fatal::ChecksumMismatch)));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn the_blocking_forms_copy_over_loopback() {
    use longshore_common::io::{Limits, Sock};
    use longshore_http_client::server;
    use loopback::ServerThread;

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
    let short = object(LENT - 1, 9);
    client.store_from_blocking("short", &short).expect("stored");
    let mut out = vec![0u8; 3 * LENT];
    assert_eq!(
        client.fetch_into_blocking("short", &mut out),
        Ok(Some(LENT - 1))
    );
    assert!(out[..LENT - 1] == short[..]);
    assert_eq!(client.fetch_into_blocking("absent", &mut out), Ok(None));
    drop(client);
    server.stop();
}
