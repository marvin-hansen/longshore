// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! `client_s3 check`: a correctness run of the client's own forms against
//! a real store, step by step. Stat, paged and delimited listing, the
//! copying forms, and the download stream, with what the store answered
//! printed for each step, and the upload stream: a short stream as one
//! plain store, a long one written in pieces as a multipart upload, a long
//! slice through `store_from`, and a stream dropped half way. Every key it stores is under one prefix of its
//! own and is erased at the end. Not a benchmark: it uses the blocking
//! forms and one caller.
//!
//! The big object is `--conns` + 1 buffers of `--size` bytes and a short
//! tail, so that a download asks for at least one range only after the
//! object has been replaced (see `replaced`).

use longshore::client::{Client, Error, Piece};
use longshore::store::list::{unescape, Listing};
use longshore::store::s3::Fatal;
use longshore::store::stat::header_in;

#[path = "check/streams.rs"]
mod streams;

/// The tail of the big object, after its whole buffers.
const TAIL: usize = 1 << 20;
/// The small object, which fits one buffer.
const SMALL: usize = 1 << 20;

fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2654435761).to_le_bytes()[1] ^ seed)
        .collect()
}

/// Run every step; true if all passed.
pub fn run(client: &Client, size: usize, conns: u32) -> bool {
    let prefix = format!("check/{}-{}/", std::process::id(), unix_secs());
    let mut c = Check {
        client,
        prefix,
        keys: Vec::new(),
        failed: 0,
    };
    println!("== client_s3 check under {}", c.prefix);
    c.small();
    c.stat();
    c.listing();
    let big = size * (conns as usize + 1) + TAIL;
    if c.put_big(big, size) {
        c.download(big);
        c.fetch_long(big);
        c.too_short(big);
        c.replaced(big, size);
    }
    c.streams(big, size);
    c.erase_all();
    if c.failed == 0 {
        println!("ok client_s3 check");
    } else {
        println!("FAIL client_s3 check: {} steps failed", c.failed);
    }
    c.failed == 0
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

struct Check<'a> {
    client: &'a Client,
    prefix: String,
    keys: Vec<String>,
    failed: u32,
}

impl Check<'_> {
    fn key(&mut self, name: &str) -> String {
        let k = format!("{}{name}", self.prefix);
        if !self.keys.contains(&k) {
            self.keys.push(k.clone());
        }
        k
    }

    fn report(&mut self, step: &str, result: Result<String, String>) {
        match result {
            Ok(said) => println!("ok   {step}: {said}"),
            Err(why) => {
                self.failed += 1;
                println!("FAIL {step}: {why}");
            }
        }
    }

    /// A slice that fits one buffer, in and out.
    fn small(&mut self) {
        let key = self.key("small");
        let bytes = pattern(1, SMALL);
        let r1 = (|| {
            self.client
                .store_from_blocking(&key, &bytes)
                .map_err(|e| format!("store_from: {e:?}"))?;
            let mut out = vec![0u8; SMALL + 1];
            match self.client.fetch_into_blocking(&key, &mut out) {
                Ok(Some(n)) if n == SMALL && out[..n] == bytes[..] => {
                    Ok(format!("{n} bytes back, equal"))
                }
                other => Err(format!("fetch_into: {other:?}")),
            }
        })();
        self.report("copy a slice that fits one buffer", r1);
    }

    /// Stat of a present and a missing key, and the copied head.
    fn stat(&mut self) {
        let key = self.key("small");
        let r2 = match self.client.stat(&key).wait() {
            Ok(Some(s)) if s.size() == SMALL as u64 && s.tag().is_some() => Ok(format!(
                "size {}, tag present, stored checksum {}",
                s.size(),
                if s.checksum().is_some() {
                    "present"
                } else {
                    "absent"
                }
            )),
            other => Err(format!("{other:?}")),
        };
        self.report("stat a stored key", r2);
        let missing = self.key("never-stored");
        let r3 = match self.client.stat(&missing).wait() {
            Ok(None) => Ok("none".into()),
            other => Err(format!("{other:?}")),
        };
        self.report("stat a missing key", r3);
        let into = self.client.buffer_blocking();
        let (stat, head) = self.client.stat_head(&key, into).wait();
        let r4 = match stat {
            Ok(Some(s)) => {
                let n = s.head_len() as usize;
                match header_in(&head.as_slice()[..n], "content-length") {
                    Some(v) if v == SMALL.to_string().as_bytes() => {
                        Ok(format!("{n} bytes of head, content-length {SMALL}"))
                    }
                    other => Err(format!("content-length {other:?} in {n} bytes of head")),
                }
            }
            other => Err(format!("{other:?}")),
        };
        self.report("stat with the response head", r4);
    }

    /// Five keys listed two to a page, and one level by delimiter.
    fn listing(&mut self) {
        let dir = format!("{}l/", self.prefix);
        let mut stored = Vec::new();
        for i in 0..5 {
            let k = self.key(&format!("l/k{i}"));
            if let Err(e) = self.client.store_from_blocking(&k, &pattern(i, 1024)) {
                return self.report("store keys to list", Err(format!("{k}: {e:?}")));
            }
            stored.push(k);
        }
        let r5 = (|| {
            let (mut keys, mut pages) = (Vec::new(), 0);
            let into = self.client.buffer_blocking();
            let (mut len, mut page) = self.client.list(&dir, None, 2, into).wait();
            loop {
                let n = len.map_err(|e| format!("page {}: {e:?}", pages + 1))?;
                pages += 1;
                let listing = Listing::parse(&page.as_slice()[..n])
                    .map_err(|e| format!("parse page {pages}: {e:?}"))?;
                for e in listing.entries() {
                    keys.push(text(e.map_err(|e| format!("{e:?}"))?.key_raw()));
                }
                if !listing.is_truncated() {
                    break;
                }
                (len, page) = self.client.list_next(&dir, None, 2, page, n).wait();
            }
            if keys == stored {
                Ok(format!("{} keys in {pages} pages, in order", keys.len()))
            } else {
                Err(format!("{keys:?} in {pages} pages"))
            }
        })();
        self.report("list two keys a page", r5);
        let r6 = (|| {
            let into = self.client.buffer_blocking();
            let (len, page) = self
                .client
                .list(&self.prefix, Some(b'/'), 1000, into)
                .wait();
            let n = len.map_err(|e| format!("{e:?}"))?;
            let listing = Listing::parse(&page.as_slice()[..n]).map_err(|e| format!("{e:?}"))?;
            let prefixes: Vec<String> = listing
                .common_prefixes()
                .map(|p| p.map(text).map_err(|e| format!("{e:?}")))
                .collect::<Result<_, _>>()?;
            if prefixes.contains(&dir) {
                Ok(format!("common prefixes {prefixes:?}"))
            } else {
                Err(format!("common prefixes {prefixes:?}, no {dir}"))
            }
        })();
        self.report("list one level by delimiter", r6);
    }

    /// The big object, as a multipart upload of one buffer per part.
    fn put_big(&mut self, big: usize, size: usize) -> bool {
        let key = self.key("big");
        let bytes = pattern(7, big);
        let r7 = (|| {
            let id = self
                .client
                .upload_begin(&key)
                .wait()
                .map_err(|e| format!("begin: {e:?}"))?;
            let mut parts = Vec::new();
            let mut last = None;
            for (n, chunk) in bytes.chunks(size).enumerate() {
                let mut body = self.client.buffer_blocking();
                body.as_mut_slice()[..chunk.len()].copy_from_slice(chunk);
                let (part, body) = self
                    .client
                    .upload_part(&key, &id, n as u32 + 1, body, chunk.len())
                    .wait();
                parts.push(part.map_err(|e| format!("part {}: {e:?}", n + 1))?);
                last = Some(body);
            }
            let scratch = last.ok_or("no parts")?;
            let (done, _) = self
                .client
                .upload_complete(&key, &id, &parts, scratch)
                .wait();
            done.map_err(|e| format!("complete: {e:?}"))?;
            Ok(format!("{big} bytes in {} parts", parts.len()))
        })();
        let ok = r7.is_ok();
        self.report("store the big object (multipart)", r7);
        ok
    }

    /// Every byte of the big object through the download stream.
    fn download(&mut self, big: usize) {
        let key = self.key("big");
        let bytes = pattern(7, big);
        let r8 = (|| {
            let mut d = match self.client.download(&key).wait() {
                Ok(Some(d)) => d,
                Ok(None) => return Err("open: no such object".into()),
                Err(e) => return Err(format!("open: {e:?}")),
            };
            let (mut at, mut pieces) = (0, 0);
            loop {
                match d.read_blocking() {
                    Ok(Piece::Bytes(buffer, n)) => {
                        if buffer.as_slice()[..n] != bytes[at..at + n] {
                            return Err(format!("bytes at {at} differ"));
                        }
                        at += n;
                        pieces += 1;
                    }
                    Ok(Piece::End { verified }) if at == big => {
                        return Ok(format!(
                            "{at} bytes in {pieces} pieces, in order, {}",
                            if verified {
                                "verified against the stored checksum"
                            } else {
                                "unverified: the store kept no full-object checksum"
                            }
                        ))
                    }
                    other => return Err(format!("after {at} bytes: {}", said(&other))),
                }
            }
        })();
        self.report("download the big object", r8);
    }

    /// The big object into a slice longer than one buffer.
    fn fetch_long(&mut self, big: usize) {
        let key = self.key("big");
        let mut out = vec![0u8; big + 3];
        let r9 = match self.client.fetch_into_blocking(&key, &mut out) {
            Ok(Some(n)) if n == big && out[..n] == pattern(7, big)[..] => {
                Ok(format!("{n} bytes, equal"))
            }
            other => Err(format!("{other:?}")),
        };
        self.report("fetch the big object into a slice", r9);
    }

    /// Slices too short for the object, one buffer long and longer.
    fn too_short(&mut self, big: usize) {
        let key = self.key("big");
        let mut one = vec![0u8; 1024];
        let mut almost = vec![0u8; big - 1];
        let r10 = match (
            self.client.fetch_into_blocking(&key, &mut one),
            self.client.fetch_into_blocking(&key, &mut almost),
        ) {
            (Err(Error::TooLong), Err(Error::TooLong)) => Ok("TooLong both ways".into()),
            other => Err(format!("{other:?}")),
        };
        self.report("fetch into slices too short", r10);
    }

    /// The object replaced while a download holds its first piece. The
    /// window has asked for at most `--conns` ranges by then, so at least
    /// one range is asked for after the replacement and must be refused.
    fn replaced(&mut self, big: usize, size: usize) {
        let key = self.key("big");
        let r11 = (|| {
            let mut d = match self.client.download(&key).wait() {
                Ok(Some(d)) => d,
                Ok(None) => return Err("open: no such object".into()),
                Err(e) => return Err(format!("open: {e:?}")),
            };
            let first = d
                .read_blocking()
                .map_err(|e| format!("first piece: {e:?}"))?;
            self.client
                .store_from_blocking(&key, &pattern(9, 1000))
                .map_err(|e| format!("replace: {e:?}"))?;
            drop(first);
            let mut at = size;
            loop {
                match d.read_blocking() {
                    Ok(Piece::Bytes(_, n)) => at += n,
                    Err(Error::Store(Fatal::Status(code))) => {
                        return Ok(format!("status {code} after {at} of {big} bytes"))
                    }
                    other => return Err(format!("after {at} bytes: {}", said(&other))),
                }
            }
        })();
        self.report("download while the object is replaced", r11);
    }

    fn erase_all(&mut self) {
        let keys = std::mem::take(&mut self.keys);
        let r12 = keys
            .iter()
            .map(|k| {
                self.client
                    .erase(k)
                    .wait()
                    .map_err(|e| format!("{k}: {e:?}"))
            })
            .collect::<Result<Vec<()>, _>>()
            .map(|v| format!("{} keys erased", v.len()));
        self.report("erase everything stored", r12);
    }
}

/// What a read gave, for a step that did not expect it.
fn said(read: &Result<Piece, Error>) -> String {
    match read {
        Ok(Piece::Bytes(_, n)) => format!("{n} more bytes"),
        Ok(Piece::End { verified }) => format!("the end, verified {verified}"),
        Err(e) => format!("{e:?}"),
    }
}

fn text(raw: &[u8]) -> String {
    let mut out = vec![0u8; raw.len()];
    let n = unescape(raw, &mut out).unwrap_or(0);
    String::from_utf8_lossy(&out[..n]).into_owned()
}
