// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The copying forms: `store_from` a byte slice and `fetch_into` one, for
//! bytes that already live in the caller's own memory. Each copies the
//! body once, into or out of a lent buffer, and allocates nothing.
//!
//! A slice that fits in one buffer is one request. A longer one goes
//! through the streams, as a multipart upload and a download in ranges,
//! so the client's buffers must be at least the store's smallest part,
//! 5 MiB. A slice too short for the object fails with `TooLong` before any
//! of the object is fetched.
//!
//! ```text
//! S3_ENDPOINT=... S3_REGION=... S3_BUCKET=... S3_ACCESS_KEY=... S3_SECRET_KEY=... \
//!     cargo run -p longshore_s3 --example copying
//! ```
//!
//! `examples/common/mod.rs` lists every variable read.

mod common;

use common::{why, Failure};
use longshore_s3::client::{Client, Error};

/// The buffer size: the smallest part S3 takes.
const BUFFER: usize = 5 << 20;

fn main() -> Result<(), Failure> {
    let client = common::store()?
        .buffer_size(BUFFER as u32)
        // A stream holds a few buffers at once: see `Builder::streams`.
        .buffers(8)
        .connections(8)
        .operations(8)
        .build()?;
    let prefix = common::prefix("copying");
    let (small, large) = (format!("{prefix}small"), format!("{prefix}large"));
    let result = copy(&client, &small, &large);
    let erased = common::erase(&client, &[&small, &large]);
    result.and(erased)
}

fn copy(client: &Client, small: &str, large: &str) -> Result<(), Failure> {
    // Fits one buffer: one store, and one fetch checked against the
    // checksum the store keeps.
    let bytes = pattern(100_000);
    client
        .store_from_blocking(small, &bytes)
        .map_err(why("store_from"))?;
    let mut out = vec![0u8; bytes.len()];
    let n = client
        .fetch_into_blocking(small, &mut out)
        .map_err(why("fetch_into"))?;
    same(n, &out, &bytes)?;
    println!("{} bytes through one buffer each way", bytes.len());

    // Two and a half buffers: three parts up, three ranges down. The forms
    // are async functions; the `_blocking` forms above wrap them.
    let long = pattern(BUFFER * 5 / 2);
    let mut back = vec![0u8; long.len()];
    let got = futures_executor::block_on(async {
        client.store_from(large, &long).await?;
        client.fetch_into(large, &mut back).await
    })
    .map_err(why("store_from and fetch_into"))?;
    same(got, &back, &long)?;
    let stat = client
        .stat(large)
        .wait()
        .map_err(why("stat"))?
        .ok_or("gone")?;
    let tag = stat.tag().ok_or("the store gave no tag")?;
    // A multipart object's tag ends in the number of its parts.
    println!(
        "{} bytes as a multipart upload (tag {}) and a download",
        long.len(),
        String::from_utf8_lossy(tag.as_bytes())
    );

    // One byte short: refused before any range is fetched.
    let mut short = vec![0u8; long.len() - 1];
    match client.fetch_into_blocking(large, &mut short) {
        Err(Error::TooLong) => println!("a slice one byte short: TooLong"),
        other => return Err(format!("a slice too short gave {other:?}").into()),
    }
    // A missing object is not an error.
    let missing = format!("{small}-never-stored");
    match client.fetch_into_blocking(&missing, &mut short) {
        Ok(None) => println!("a key never stored: None"),
        other => return Err(format!("a missing key gave {other:?}").into()),
    }
    Ok(())
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1])
        .collect()
}

fn same(fetched: Option<usize>, out: &[u8], sent: &[u8]) -> Result<(), Failure> {
    match fetched {
        Some(n) if out[..n] == *sent => Ok(()),
        other => Err(format!("fetched {other:?} bytes, not the ones stored").into()),
    }
}
