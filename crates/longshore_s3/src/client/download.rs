// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Reading an object of any size as a stream of lent buffers.
//!
//! A download first asks about the object, then fetches it in ranges of
//! one buffer each, a few ranges ahead of the reader, and hands the
//! buffers to the reader in order. Every range names the object's `ETag`
//! in `If-Match`, so an object replaced while it is read fails the next
//! range instead of splicing two objects together. A running checksum of
//! the bytes handed out is compared with the store's checksum of the
//! whole object at the end.

use super::buffer::Borrow;
use super::{Budget, Buffer, Client, Error, Pending, KEY_MAX};
use crate::report::{ETag, Expect, Report};
use crate::s3::Fatal;
use crate::stat::Stat;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

mod check;
use check::{range_of, Running};

/// Ranges a download fetches ahead of its reader at most. Fewer when the
/// client lends fewer buffers: a download leaves at least one of them for
/// the buffer its reader holds.
const WINDOW: usize = 4;

/// A range asked for: its report and the buffer it was fetched into.
type Asked = Pending<(Result<Report, Error>, Buffer)>;

/// What [`Download::read`] gives.
pub enum Piece {
    /// The object's next bytes: the first `usize` of the buffer. They are
    /// not verified until the download ends; see [`Download`].
    Bytes(Buffer, usize),
    /// Every byte has been handed out. `verified` is true when they match
    /// the store's checksum of the whole object, and false when the store
    /// keeps none for it.
    End {
        /// Whether the whole object was checked.
        verified: bool,
    },
}

/// Opening a download: asking the store about the object. Await it, or
/// call [`Opening::wait`].
#[must_use = "the download comes back through this"]
pub struct Opening {
    client: Client,
    key: [u8; KEY_MAX],
    key_len: u16,
    stat: Pending<Result<Option<Stat>, Error>>,
}

/// An object read in order, one lent buffer at a time.
///
/// Each [`Download::read`] gives the next bytes in a buffer, which the
/// reader keeps as long as it likes and drops to return. Up to four ranges
/// are fetched ahead of the reader, but no more than leave one of the
/// client's lent buffers for the reader to hold (one range for a client
/// that lends one buffer). Each range waits for a free buffer, so a reader
/// that keeps every buffer it is given makes the download wait for one.
/// Other callers of the client share the same buffers.
///
/// Bytes handed out before the end are unverified. Only the end compares
/// the bytes read with the store's checksum of the whole object, and only
/// a reader that reads to the end learns whether they were the object's.
/// The checksum is CRC-64/NVME on S3 and CRC-32C on Google Cloud Storage,
/// as [`Stat::checksum`] holds it. An object with no checksum of the whole
/// object, such as one uploaded with a composite checksum or none, ends
/// with `verified: false`.
///
/// The download ends with an error, and keeps returning it, when:
///
/// - the object changed since the download opened:
///   `Error::Store(Fatal::Status(412))`, or `Fatal::Status(404)` if it is
///   gone;
/// - the bytes do not match the store's checksum:
///   `Error::Store(Fatal::ChecksumMismatch)`, at the end;
/// - the store answered a range with the whole object:
///   `Error::Store(Fatal::RangeIgnored)`, or with other bytes than asked
///   for: `Fatal::BadResponse`;
/// - a range failed as a fetch fails.
///
/// Ranges are fetched with [`Client::get`], which Google Cloud Storage does
/// not have yet, so on that store every range fails.
pub struct Download {
    client: Client,
    key: [u8; KEY_MAX],
    key_len: u16,
    stat: Stat,
    expect: Expect,
    /// Bytes one range asks for: one buffer's.
    range: u64,
    window: usize,
    /// Where the next range to ask for starts.
    fetched: u64,
    /// Bytes handed to the reader.
    delivered: u64,
    /// Ranges asked for and not yet handed out, the oldest at `head`.
    ring: [Option<Asked>; WINDOW],
    head: usize,
    count: usize,
    /// Waiting for a buffer to fetch the next range into.
    borrow: Option<Borrow>,
    crc: Running,
    /// How the download ended, once it has.
    end: Option<Result<bool, Error>>,
}

impl Client {
    /// Open a download of the object under `key`: ask the store about it,
    /// and then read it with [`Download::read`]. `Ok(None)` means there is
    /// no such object.
    pub fn download(&self, key: &str) -> Opening {
        // A key too long fails the question about it, and is never used.
        let n = key.len().min(KEY_MAX);
        let mut copy = [0u8; KEY_MAX];
        copy[..n].copy_from_slice(&key.as_bytes()[..n]);
        Opening {
            client: self.clone(),
            key: copy,
            key_len: n as u16,
            stat: self.stat(key),
        }
    }
}

impl Opening {
    /// Block the calling thread until the store has answered.
    pub fn wait(self) -> Result<Option<Download>, Error> {
        let Opening {
            client,
            key,
            key_len,
            stat,
        } = self;
        open(client, &key, key_len, stat.wait())
    }
}

impl Future for Opening {
    type Output = Result<Option<Download>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let stat = core::task::ready!(Pin::new(&mut me.stat).poll(cx));
        Poll::Ready(open(me.client.clone(), &me.key, me.key_len, stat))
    }
}

fn open(
    client: Client,
    key: &[u8; KEY_MAX],
    key_len: u16,
    stat: Result<Option<Stat>, Error>,
) -> Result<Option<Download>, Error> {
    let Some(stat) = stat? else {
        return Ok(None);
    };
    // Without its tag a changed object could not be told from this one.
    let etag = stat
        .tag()
        .and_then(|tag| ETag::new(tag.as_bytes()))
        .ok_or(Error::Store(Fatal::BadResponse))?;
    let lent: u32 = client.lanes.iter().map(|l| l.bufs.slots()).sum();
    Ok(Some(Download {
        key: *key,
        key_len,
        stat,
        expect: Expect::default().with_etag(Some(etag)),
        range: client.buffer_size().max(1) as u64,
        window: WINDOW.min(lent.saturating_sub(1) as usize).max(1),
        fetched: 0,
        delivered: 0,
        ring: Default::default(),
        head: 0,
        count: 0,
        borrow: None,
        crc: Running::new(stat.checksum()),
        end: None,
        client,
    }))
}

impl Download {
    /// What the store said about the object when the download opened.
    pub fn stat(&self) -> Stat {
        self.stat
    }

    /// The object's next bytes, or its end. Once the download has ended,
    /// it gives that end again.
    pub fn read(&mut self) -> impl Future<Output = Result<Piece, Error>> + '_ {
        core::future::poll_fn(move |cx| self.poll_read(cx))
    }

    /// [`Download::read`], blocking the calling thread.
    pub fn read_blocking(&mut self) -> Result<Piece, Error> {
        longshore_mailbox::block_on(self.read())
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<Piece, Error>> {
        if let Some(end) = self.end {
            return Poll::Ready(end.map(|verified| Piece::End { verified }));
        }
        let size = self.stat.size();
        if self.delivered == size {
            let end = self.crc.end();
            self.end = Some(end);
            return Poll::Ready(end.map(|verified| Piece::End { verified }));
        }
        while self.count < self.window && self.fetched < size {
            let borrow = self.borrow.get_or_insert_with(|| {
                Borrow::new(self.client.lanes.clone(), self.client.alive.clone())
            });
            let Poll::Ready(buffer) = Pin::new(borrow).poll(cx) else {
                break;
            };
            self.borrow = None;
            self.ask(buffer);
        }
        // With nothing asked for, the download waits for a buffer.
        let Some(oldest) = self.ring[self.head].as_mut() else {
            return Poll::Pending;
        };
        let (result, buffer) = core::task::ready!(Pin::new(oldest).poll(cx));
        self.ring[self.head] = None;
        self.head = (self.head + 1) % WINDOW;
        self.count -= 1;
        // A waiter left queued while the reader holds the stream would keep
        // a returned buffer from the client's other callers.
        self.borrow = None;
        let first = self.delivered;
        let last = (first + self.range).min(size) - 1;
        let tag = self.expect.etag();
        match result.and_then(|report| range_of(&report, first, last, tag.as_ref())) {
            Ok(len) => {
                self.crc.update(&buffer.as_slice()[..len]);
                self.delivered += len as u64;
                Poll::Ready(Ok(Piece::Bytes(buffer, len)))
            }
            Err(e) => {
                self.end = Some(Err(e));
                Poll::Ready(Err(e))
            }
        }
    }

    /// Ask for the next range into `buffer`.
    fn ask(&mut self, buffer: Buffer) {
        let first = self.fetched;
        let last = (first + self.range).min(self.stat.size()) - 1;
        // The key came in as a `&str` and was copied whole.
        let key = core::str::from_utf8(&self.key[..self.key_len as usize]).unwrap_or("");
        let at = (self.head + self.count) % WINDOW;
        let range = Some((first, last));
        self.ring[at] = Some(
            self.client
                .get(key, range, &self.expect, buffer, Budget::UNLIMITED),
        );
        self.count += 1;
        self.fetched = last + 1;
    }
}

#[cfg(test)]
mod tests;
