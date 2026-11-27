// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Upload and download streams, for an object larger than one buffer or of
//! a length not known up front.
//!
//! An upload stream sends each full buffer as one part of a multipart
//! upload, a few parts in flight at once. `send` lends it a buffer the
//! caller filled, with no copy; `write` copies bytes into its buffers. A
//! download asks the store about the object, fetches it in ranges of one
//! buffer, a few ahead of the reader, and checks the whole object against
//! the store's checksum at the end. Dropping an upload stream before it
//! finishes aborts its upload.
//!
//! ```text
//! S3_ENDPOINT=... S3_REGION=... S3_BUCKET=... S3_ACCESS_KEY=... S3_SECRET_KEY=... \
//!     cargo run -p longshore_s3 --example streams
//! ```
//!
//! `examples/common/mod.rs` lists every variable read.

mod common;

use common::{why, Failure};
use longshore_s3::client::{Client, Piece};

/// The buffer size, which is the part size: the smallest part S3 takes.
const BUFFER: usize = 5 << 20;

fn main() -> Result<(), Failure> {
    let client = common::store()?
        .buffer_size(BUFFER as u32)
        // An upload stream holds up to six buffers and a download five.
        .buffers(10)
        .connections(8)
        .operations(8)
        .build()?;
    let prefix = common::prefix("streams");
    let (key, dropped) = (format!("{prefix}object"), format!("{prefix}dropped"));
    let body = pattern(BUFFER * 5 / 2 + 12_345);
    let result = upload(&client, &key, &body)
        .and_then(|()| download(&client, &key, &body))
        .and_then(|()| drop_upload(&client, &dropped));
    let erased = common::erase(&client, &[&key, &dropped]);
    result.and(erased)
}

fn upload(client: &Client, key: &str, body: &[u8]) -> Result<(), Failure> {
    let mut upload = client.upload(key).wait().map_err(why("upload"))?;
    let (whole, tail) = body.split_at(2 * BUFFER);
    // Whole buffers are lent: each becomes one part, sent without a copy.
    for chunk in whole.chunks(BUFFER) {
        let mut buffer = client.buffer_blocking();
        // A real program fills the buffer in place, from a file or a socket.
        buffer.as_mut_slice().copy_from_slice(chunk);
        upload.send(buffer, BUFFER).wait().map_err(why("send"))?;
    }
    // Bytes in the caller's own memory are copied into the stream.
    upload.write(tail).wait().map_err(why("write"))?;
    // Sends the last part and completes the upload.
    upload.finish().wait().map_err(why("finish"))?;
    println!(
        "uploaded {} bytes: 2 buffers sent, {} bytes written",
        body.len(),
        tail.len()
    );
    Ok(())
}

fn download(client: &Client, key: &str, body: &[u8]) -> Result<(), Failure> {
    let opened = client.download(key).wait().map_err(why("download"))?;
    let mut download = opened.ok_or("the object is not there")?;
    let (mut at, mut pieces) = (0, 0);
    loop {
        match download.read_blocking().map_err(why("read"))? {
            Piece::Bytes(buffer, n) => {
                // These bytes are not verified until the end.
                if body.get(at..at + n) != Some(&buffer.as_slice()[..n]) {
                    return Err(format!("bytes {at}.. differ from those uploaded").into());
                }
                (at, pieces) = (at + n, pieces + 1);
                // Dropping the buffer gives it back for the next range.
            }
            Piece::End { verified } => {
                println!(
                    "downloaded {at} of {} bytes in {pieces} pieces, verified: {verified}",
                    download.stat().size()
                );
                return if at == body.len() && verified {
                    Ok(())
                } else {
                    Err("the download ended short or unverified".into())
                };
            }
        }
    }
}

fn drop_upload(client: &Client, key: &str) -> Result<(), Failure> {
    let mut upload = client.upload(key).wait().map_err(why("upload"))?;
    // A buffer and a half: the multipart upload begins and its first part
    // is sent.
    upload
        .write(&pattern(BUFFER * 3 / 2))
        .wait()
        .map_err(why("write"))?;
    // Dropped before `finish`: the client aborts the upload once the part
    // in flight has ended, so no part lands after the abort.
    drop(upload);
    match client.stat(key).wait().map_err(why("stat"))? {
        None => println!("an upload stream dropped half way: no object"),
        Some(_) => return Err("a dropped upload stream left an object".into()),
    }
    Ok(())
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[2])
        .collect()
}
