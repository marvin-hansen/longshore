// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The lending round trip: borrow a buffer, store from it, fetch into it,
//! fetch part of it, and erase. The client makes no copy of the body. The
//! caller writes into a registered buffer, and each operation takes the
//! buffer and gives it back with its result.
//!
//! ```text
//! S3_ENDPOINT=s3.us-east-1.amazonaws.com S3_REGION=us-east-1 S3_BUCKET=my-bucket \
//! S3_ACCESS_KEY=... S3_SECRET_KEY=... S3_TLS=1 \
//!     cargo run -p longshore_s3 --example round_trip
//! ```
//!
//! `examples/common/mod.rs` lists every variable read.

mod common;

use common::{why, Failure};
use longshore_s3::client::Client;

fn main() -> Result<(), Failure> {
    let client = common::store()?
        // One operation at a time needs little: two buffers of 1 MiB, and
        // two connections.
        .buffers(2)
        .buffer_size(1 << 20)
        .connections(2)
        .operations(2)
        .build()?;
    let key = format!("{}greeting", common::prefix("round_trip"));
    let result = round_trip(&client, &key);
    let erased = common::erase(&client, &[&key]);
    result.and(erased)
}

fn round_trip(client: &Client, key: &str) -> Result<(), Failure> {
    let text = b"hello from longshore";

    // Borrow a buffer, waiting if every one is lent, and write the body
    // into it.
    let mut body = client.buffer_blocking();
    body.as_mut_slice()[..text.len()].copy_from_slice(text);

    // The store takes the buffer and gives it back with the result.
    let (stored, body) = client.store(key, body, text.len()).wait();
    stored.map_err(why("store"))?;
    println!("stored {} bytes under {key}", text.len());

    // The same buffer carries the fetch. The object is its first n bytes.
    let (fetched, body) = client.fetch(key, body).wait();
    let n = fetched.map_err(why("fetch"))?.ok_or("the object is gone")?;
    if body.as_slice()[..n] != text[..] {
        return Err("fetched other bytes than were stored".into());
    }
    println!(
        "fetched {n} bytes: {}",
        String::from_utf8_lossy(&body.as_slice()[..n])
    );

    // Bytes 6 to 9, both ends included.
    let (ranged, body) = client.fetch_range(key, 6, 9, body).wait();
    let got = ranged
        .map_err(why("fetch_range"))?
        .ok_or("the object is gone")?;
    println!(
        "bytes 6 to 9: {}",
        String::from_utf8_lossy(&body.as_slice()[..got])
    );

    // A conditional store stores only when the key is free.
    let (first, body) = client.store_no_clobber(key, body, 0).wait();
    if first.map_err(why("store_no_clobber"))? {
        return Err("store_no_clobber replaced an object".into());
    }
    println!("store_no_clobber: an object was there, nothing stored");

    client.erase(key).wait().map_err(why("erase"))?;
    // A missing object is not an error: the fetch says None.
    let (gone, _body) = client.fetch(key, body).wait();
    match gone.map_err(why("fetch"))? {
        None => println!("erase: a fetch now finds nothing"),
        Some(_) => return Err("the object is still there after erase".into()),
    }
    // Dropping the buffer gives it back to the client.
    Ok(())
}
