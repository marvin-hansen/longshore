// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Asking about objects without fetching them: `stat` and `stat_head` for
//! one object, `list` and `list_next` for the keys under a prefix.
//!
//! `stat` needs no buffer: its answer is small and fixed in size. A listing
//! comes back as the store's document in a lent buffer, which `Listing`
//! reads in place, copying and allocating nothing. To go past a page that
//! was cut short, `list_next` takes that same buffer back: the I/O thread
//! reads the continuation token from it before the next page overwrites it.
//!
//! ```text
//! S3_ENDPOINT=... S3_REGION=... S3_BUCKET=... S3_ACCESS_KEY=... S3_SECRET_KEY=... \
//!     cargo run -p longshore_s3 --example stat_and_list
//! ```
//!
//! `examples/common/mod.rs` lists every variable read.

mod common;

use common::{why, Failure};
use longshore_s3::client::Client;
use longshore_s3::list::{unescape, Listing};
use longshore_s3::stat::header_in;

/// The objects stored, under the run's own prefix.
const NAMES: [&str; 4] = ["docs/a.txt", "docs/b.txt", "images/c.png", "readme.txt"];

fn main() -> Result<(), Failure> {
    let client = common::store()?
        .buffers(2)
        .buffer_size(1 << 20)
        .connections(2)
        .operations(2)
        .build()?;
    let prefix = common::prefix("stat_and_list");
    let keys = NAMES.map(|name| format!("{prefix}{name}"));
    let result = keys
        .iter()
        .try_for_each(|key| client.store_from_blocking(key, key.as_bytes()))
        .map_err(|e| why("store_from")(e).into())
        .and_then(|()| stat(&client, &keys[0]))
        .and_then(|()| one_level(&client, &prefix))
        .and_then(|()| pages(&client, &prefix));
    let erased = common::erase(&client, &keys);
    result.and(erased)
}

fn stat(client: &Client, key: &str) -> Result<(), Failure> {
    let found = client.stat(key).wait().map_err(why("stat"))?;
    let stat = found.ok_or("the object is not there")?;
    let shown = |b: Option<&[u8]>| match b {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None => "none".to_owned(),
    };
    println!("stat: {} bytes", stat.size());
    println!("stat: modified {:?} (Unix seconds)", stat.modified());
    println!(
        "stat: tag {}",
        shown(stat.tag().as_ref().map(|t| t.as_bytes()))
    );
    println!(
        "stat: checksum {}",
        shown(stat.checksum().as_ref().map(|c| &c[..]))
    );
    // For headers `Stat` has no field for, `stat_head` copies the response
    // head into a lent buffer.
    let (described, head) = client.stat_head(key, client.buffer_blocking()).wait();
    let len = described
        .map_err(why("stat_head"))?
        .ok_or("gone")?
        .head_len();
    let head = &head.as_slice()[..len as usize];
    for name in ["content-type", "last-modified"] {
        println!("stat_head: {name}: {}", shown(header_in(head, name)));
    }
    // A missing object is not an error.
    match client
        .stat(&format!("{key}-missing"))
        .wait()
        .map_err(why("stat"))?
    {
        None => println!("stat of a key never stored: None"),
        Some(_) => return Err("a key never stored has an object".into()),
    }
    Ok(())
}

/// One level of the prefix: the keys directly in it, and the prefixes one
/// level down that stand for the keys below them.
fn one_level(client: &Client, prefix: &str) -> Result<(), Failure> {
    let into = client.buffer_blocking();
    let (len, page) = client.list(prefix, Some(b'/'), 1000, into).wait();
    let n = len.map_err(why("list"))?;
    let listing = Listing::parse(&page.as_slice()[..n]).map_err(|e| format!("{e:?}"))?;
    for entry in listing.entries() {
        let entry = entry.map_err(|e| format!("{e:?}"))?;
        let key = text(entry.key_raw(), prefix);
        println!("list with '/': key {key}, {} bytes", entry.size());
    }
    for below in listing.common_prefixes() {
        let below = below.map_err(|e| format!("{e:?}"))?;
        println!("list with '/': prefix {}", text(below, prefix));
    }
    Ok(())
}

/// Every key under the prefix, two to a page.
fn pages(client: &Client, prefix: &str) -> Result<(), Failure> {
    let into = client.buffer_blocking();
    let (mut len, mut page) = client.list(prefix, None, 2, into).wait();
    let mut count = 0;
    loop {
        let n = len.map_err(why("list"))?;
        let listing = Listing::parse(&page.as_slice()[..n]).map_err(|e| format!("{e:?}"))?;
        count += 1;
        let mut keys = Vec::new();
        for entry in listing.entries() {
            keys.push(text(entry.map_err(|e| format!("{e:?}"))?.key_raw(), prefix));
        }
        println!("page {count}: {}", keys.join(", "));
        if !listing.is_truncated() {
            return Ok(());
        }
        // The next page is written over this one.
        (len, page) = client.list_next(prefix, None, 2, page, n).wait();
    }
}

/// A key or prefix as the listing holds it, with XML character references
/// decoded and the run's prefix taken off.
fn text(raw: &[u8], prefix: &str) -> String {
    let mut out = [0u8; 1024];
    let decoded = match unescape(raw, &mut out) {
        Some(n) => String::from_utf8_lossy(&out[..n]).into_owned(),
        None => String::from_utf8_lossy(raw).into_owned(),
    };
    match decoded.strip_prefix(prefix) {
        Some(rest) => rest.to_owned(),
        None => decoded,
    }
}
