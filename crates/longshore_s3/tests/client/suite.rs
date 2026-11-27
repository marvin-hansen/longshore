// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! One suite of client operations, written once and built twice: as an
//! `async fn` that awaits every operation, for the executors, and as a
//! plain `fn` that waits on every operation with the blocking forms, for
//! threads with no runtime.
//!
//! Each run works under its own prefix, so runs can share a client and a
//! server, and leaves nothing behind.

use longshore_s3::client::{Buffer, Client};
use longshore_s3::list::{unescape, Listing};

/// The length of every object the suite stores.
pub const LEN: usize = 3000;

/// The byte at `i` of the object `seed` names.
pub fn byte(seed: usize, i: usize) -> u8 {
    (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ (seed as u8).wrapping_mul(37)
}

pub fn fill(buf: &mut Buffer, seed: usize, len: usize) {
    for (i, b) in buf.as_mut_slice()[..len].iter_mut().enumerate() {
        *b = byte(seed, i);
    }
}

pub fn holds(buf: &Buffer, seed: usize, from: usize, len: usize) -> bool {
    buf.as_slice()[..len]
        .iter()
        .enumerate()
        .all(|(i, &b)| b == byte(seed, from + i))
}

/// The keys of a listing page, in order.
#[cfg(test)]
pub fn keys_of(page: &[u8]) -> Vec<String> {
    let listing = Listing::parse(page).expect("the server's listing parses");
    let mut out = [0u8; 256];
    listing
        .entries()
        .map(|e| {
            let n = unescape(e.unwrap().key_raw(), &mut out).unwrap();
            String::from_utf8(out[..n].to_vec()).unwrap()
        })
        .collect()
}

/// Waits on a client call: `.await` in the async suite.
macro_rules! awaited {
    ($e:expr) => {
        $e.await
    };
}

/// Waits on a client call: the blocking forms in the plain suite.
macro_rules! waited {
    ($e:expr) => {
        $e.wait()
    };
}

/// Borrows a buffer, by awaiting or by blocking.
macro_rules! borrow_awaited {
    ($c:expr) => {
        $c.buffer().await
    };
}

macro_rules! borrow_waited {
    ($c:expr) => {
        $c.buffer_blocking()
    };
}

/// The suite itself, around a call-waiting and a buffer-borrowing macro.
macro_rules! suite {
    ($name:ident, [$($asyncness:tt)*], $go:ident, $borrow:ident) => {
        /// Store, conditional store, fetch, ranged fetch, stat, list, a
        /// multipart upload, and erase, under `prefix`, with `seed`'s bytes.
        #[cfg(test)]
        pub $($asyncness)* fn $name(client: Client, prefix: String, seed: usize) {
            let (a, b, up) = (format!("{prefix}/a"), format!("{prefix}/b"), format!("{prefix}/up"));
            let mut buf = $borrow!(client);
            fill(&mut buf, seed, LEN);
            let (stored, buf) = $go!(client.store(&a, buf, LEN));
            assert_eq!(stored, Ok(()), "store {a}");
            let (again, buf) = $go!(client.store_no_clobber(&a, buf, LEN));
            assert_eq!(again, Ok(false), "a conditional store over {a}");
            let (fresh, mut buf) = $go!(client.store_no_clobber(&b, buf, LEN));
            assert_eq!(fresh, Ok(true), "a conditional store of {b}");

            buf.as_mut_slice().fill(0);
            let (fetched, buf) = $go!(client.fetch(&a, buf));
            assert_eq!(fetched, Ok(Some(LEN)), "fetch {a}");
            assert!(holds(&buf, seed, 0, LEN), "the bytes of {a}");
            let (ranged, buf) = $go!(client.fetch_range(&b, 100, 1099, buf));
            assert_eq!(ranged, Ok(Some(1000)), "a range of {b}");
            assert!(holds(&buf, seed, 100, 1000), "the range's bytes");

            let stat = $go!(client.stat(&a)).unwrap().expect("a stat of what was stored");
            assert_eq!(stat.size(), LEN as u64);
            let (listed, buf) = $go!(client.list(&format!("{prefix}/"), None, 100, buf));
            let n = listed.expect("a listing");
            assert_eq!(keys_of(&buf.as_slice()[..n]), [a.clone(), b.clone()]);

            let id = $go!(client.upload_begin(&up)).expect("begin an upload");
            let mut buf = buf;
            fill(&mut buf, seed + 1, LEN);
            let (one, mut buf) = $go!(client.upload_part(&up, &id, 1, buf, LEN));
            fill(&mut buf, seed + 2, LEN);
            let (two, buf) = $go!(client.upload_part(&up, &id, 2, buf, LEN));
            let parts = [one.expect("part 1"), two.expect("part 2")];
            let (completed, buf) = $go!(client.upload_complete(&up, &id, &parts, buf));
            assert_eq!(completed, Ok(()), "complete {up}");
            let (whole, buf) = $go!(client.fetch(&up, buf));
            assert_eq!(whole, Ok(Some(2 * LEN)), "the assembled {up}");
            let (first, second) = buf.as_slice()[..2 * LEN].split_at(LEN);
            assert!(first.iter().enumerate().all(|(i, &x)| x == byte(seed + 1, i)));
            assert!(second.iter().enumerate().all(|(i, &x)| x == byte(seed + 2, i)));

            for key in [&a, &b, &up] {
                assert_eq!($go!(client.erase(key)), Ok(()), "erase {key}");
            }
            assert_eq!($go!(client.stat(&a)), Ok(None), "{a} after its erase");
            let (gone, _buf) = $go!(client.fetch(&b, buf));
            assert_eq!(gone, Ok(None), "{b} after its erase");
        }
    };
}

suite!(run_async, [async], awaited, borrow_awaited);
suite!(run_blocking, [], waited, borrow_waited);
