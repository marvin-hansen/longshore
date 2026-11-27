// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Store from a byte slice, and fetch into one, for a caller that does not
//! want to handle lent buffers.
//!
//! These forms copy the body once, into or out of a registered buffer.
//! That copy is the whole cost of the convenience: the caller owns the
//! memory on both sides, so nothing is allocated to hold a body. A slice
//! that fits in one buffer is one request. A longer one goes through the
//! streams: an upload stream for a store, a download for a fetch. For no
//! copy at all, use [`Client::store`] and [`Client::fetch`] with lent
//! buffers.

use super::download::Piece;
use super::{Client, Error};
use crate::s3::Fatal;
use longshore_http_client::client::Error as Http;

impl Client {
    /// Store `body` under `key`, replacing any object there. One copy into
    /// a registered buffer; a body longer than one buffer is sent as a
    /// multipart upload, through [`Client::upload`].
    pub async fn store_from(&self, key: &str, body: &[u8]) -> Result<(), Error> {
        if body.len() > self.buffer_size() {
            return self.store_streamed(key, body).await;
        }
        let mut buffer = self.buffer().await;
        buffer.as_mut_slice()[..body.len()].copy_from_slice(body);
        let (result, _buffer) = self.store(key, buffer, body.len()).await;
        result
    }

    /// [`Client::store_from`], blocking the calling thread.
    pub fn store_from_blocking(&self, key: &str, body: &[u8]) -> Result<(), Error> {
        longshore_mailbox::block_on(self.store_from(key, body))
    }

    /// Fetch the object under `key` into `out`. `Ok(Some(n))` is its
    /// length, in the first `n` bytes of `out`; `Ok(None)` means there is
    /// no such object. [`Error::TooLong`] if the object is longer than
    /// `out`, and then nothing in `out` should be read.
    ///
    /// When `out` fits in one buffer this is one fetch, checked against the
    /// store's checksum, and one copy out. A longer `out` is filled
    /// through [`Client::download`], which learns the object's size first,
    /// so an object longer than `out` fails before any of it is fetched.
    pub async fn fetch_into(&self, key: &str, out: &mut [u8]) -> Result<Option<usize>, Error> {
        if out.len() > self.buffer_size() {
            return self.fetch_downloaded(key, out).await;
        }
        let buffer = self.buffer().await;
        let (result, buffer) = self.fetch(key, buffer).await;
        match result {
            Ok(Some(n)) if n <= out.len() => {
                out[..n].copy_from_slice(&buffer.as_slice()[..n]);
                Ok(Some(n))
            }
            Ok(Some(_)) | Err(Error::Store(Fatal::Transport(Http::BodyTooLarge))) => {
                Err(Error::TooLong)
            }
            other => other,
        }
    }

    /// [`Client::fetch_into`], blocking the calling thread.
    pub fn fetch_into_blocking(&self, key: &str, out: &mut [u8]) -> Result<Option<usize>, Error> {
        longshore_mailbox::block_on(self.fetch_into(key, out))
    }

    async fn fetch_downloaded(&self, key: &str, out: &mut [u8]) -> Result<Option<usize>, Error> {
        let Some(mut download) = self.download(key).await? else {
            return Ok(None);
        };
        if download.stat().size() > out.len() as u64 {
            return Err(Error::TooLong);
        }
        let mut at = 0;
        loop {
            match download.read().await? {
                Piece::Bytes(buffer, n) => {
                    // The size was checked against `out` when the download
                    // opened, and every range is checked against the size.
                    out[at..at + n].copy_from_slice(&buffer.as_slice()[..n]);
                    at += n;
                }
                Piece::End { .. } => return Ok(Some(at)),
            }
        }
    }
}

impl Client {
    /// A body longer than one buffer, through an upload stream: copied
    /// into one buffer after another, each sent as a part.
    async fn store_streamed(&self, key: &str, body: &[u8]) -> Result<(), Error> {
        let mut upload = self.upload(key).await?;
        upload.write(body).await?;
        upload.finish().await
    }
}
