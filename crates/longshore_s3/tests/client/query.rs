// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Stat and listing through the shareable client, over workers driven by
//! hand in the simulator.

#![cfg(not(longshore_model))]

#[cfg(test)]
#[path = "../store/common/mod.rs"]
mod common;
#[cfg(test)]
mod rig;
use longshore_common::io::alloc_guard::sealed;
use longshore_s3::client::{Buffer, Error};
use longshore_s3::list::{unescape, Listing};
use longshore_s3::stat::header_in;
use rig::*;

#[cfg(test)]
fn put(r: &mut Rig, key: &str) {
    let mut body = r.buffer();
    fill(&mut body, key.len() as u8);
    let (stored, _body) = r.run(r.client.store(key, body, LEN));
    assert_eq!(stored, Ok(()), "storing {key}");
}

/// Keys and common prefixes of one page, written into fixed arrays so a
/// sealed caller can read them.
#[cfg(test)]
fn read_page(page: &[u8], keys: &mut [[u8; 8]; 8], nk: &mut usize, cps: &mut usize) -> bool {
    let listing = Listing::parse(page).expect("the server's listing parses");
    for e in listing.entries() {
        let mut out = [0u8; 8];
        let n = unescape(e.unwrap().key_raw(), &mut out).unwrap();
        keys[*nk] = [0; 8];
        keys[*nk][..n].copy_from_slice(&out[..n]);
        *nk += 1;
    }
    *cps += listing.common_prefixes().count();
    listing.is_truncated()
}

#[cfg(test)]
fn key_of(k: &[u8; 8]) -> &str {
    let n = k.iter().position(|b| *b == 0).unwrap_or(8);
    std::str::from_utf8(&k[..n]).unwrap()
}

#[test]
fn stat_reports_a_stored_object_and_none_for_a_missing_key() {
    let mut r = rig(1);
    put(&mut r, "present");
    let stat = r
        .run(r.client.stat("present"))
        .unwrap()
        .expect("it was stored");
    assert_eq!(stat.size(), LEN as u64);
    assert!(stat.tag().is_some(), "the store answered with an ETag");
    assert_eq!(stat.head_len(), 0, "no head is copied without a buffer");
    assert_eq!(r.run(r.client.stat("absent")), Ok(None));
}

#[test]
fn stat_head_copies_the_response_head_into_the_lent_buffer() {
    let mut r = rig(1);
    put(&mut r, "present");
    let into = r.buffer();
    let (stat, head) = r.run(r.client.stat_head("present", into));
    let stat = stat.unwrap().expect("it was stored");
    let n = stat.head_len() as usize;
    assert!(n > 0);
    assert!(header_in(&head.as_slice()[..n], "etag").is_some());
}

#[test]
fn a_listing_over_several_pages_returns_every_key_once_in_order_and_allocates_nothing() {
    let mut r = rig(1);
    for k in ["k1", "k2", "k3", "k4", "k5"] {
        put(&mut r, k);
    }
    let page = r.buffer();
    let mut keys = [[0u8; 8]; 8];
    let (mut nk, mut cps, mut pages) = (0, 0, 0);
    sealed(|| {
        let (mut len, mut page) = r.run(r.client.list("k", None, 2, page));
        loop {
            let n = len.expect("a page");
            pages += 1;
            if !read_page(&page.as_slice()[..n], &mut keys, &mut nk, &mut cps) {
                break;
            }
            (len, page) = r.run(r.client.list_next("k", None, 2, page, n));
        }
    });
    assert_eq!(pages, 3);
    assert_eq!(cps, 0);
    let got: Vec<&str> = keys[..nk].iter().map(key_of).collect();
    assert_eq!(got, ["k1", "k2", "k3", "k4", "k5"]);
}

#[test]
fn a_delimiter_returns_common_prefixes_and_pages_past_them() {
    let mut r = rig(1);
    for k in ["a/1", "a/2", "b/1", "c"] {
        put(&mut r, k);
    }
    let mut keys = [[0u8; 8]; 8];
    let (mut nk, mut cps) = (0, 0);
    let into = r.buffer();
    let (mut len, mut page) = r.run(r.client.list("", Some(b'/'), 1, into));
    loop {
        let n = len.expect("a page");
        if !read_page(&page.as_slice()[..n], &mut keys, &mut nk, &mut cps) {
            break;
        }
        (len, page) = r.run(r.client.list_next("", Some(b'/'), 1, page, n));
    }
    assert_eq!(cps, 2, "a/ and b/");
    assert_eq!(keys[..nk].iter().map(key_of).collect::<Vec<_>>(), ["c"]);
}

#[test]
fn list_next_refuses_a_page_that_was_not_cut_short() {
    let mut r = rig(1);
    put(&mut r, "only");
    let into = r.buffer();
    let (len, page) = r.run(r.client.list("", None, 10, into));
    let n = len.unwrap();
    let (next, _page): (Result<usize, Error>, Buffer) =
        r.run(r.client.list_next("", None, 10, page, n));
    assert_eq!(next, Err(Error::BadKey));
}

#[test]
fn a_delimiter_that_is_not_ascii_is_refused_before_sending() {
    let mut r = rig(1);
    let into = r.buffer();
    let (len, _page) = r.run(r.client.list("", Some(0xC3), 10, into));
    assert_eq!(len, Err(Error::BadKey));
}

#[test]
fn list_next_refuses_a_length_beyond_the_buffer() {
    let mut r = rig(1);
    let page = r.buffer();
    let too_long = r.client.buffer_size() + 1;
    let (next, _page) = r.run(r.client.list_next("", None, 10, page, too_long));
    assert_eq!(next, Err(Error::TooLong));
}
