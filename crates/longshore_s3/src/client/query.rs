// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Asking about an object without fetching it, and listing keys one page
//! at a time.
//!
//! A listing comes back as the store's document in a lent buffer, which
//! the caller reads in place with [`Listing`]: nothing is copied out of it
//! and nothing is allocated. To go on past a truncated page, the caller
//! hands that same buffer to [`Client::list_next`]. The I/O thread reads
//! the continuation token out of the page before the next page overwrites
//! it, so the token never crosses the request queue.

use super::lane::{Finished, Kind};
use super::{error_of, Buffer, Client, Error, Pending};
use crate::list::{unescape, Listing};
use crate::s3::{Outcome, SubmitError, LIST_TOKEN_ENCODED_MAX, S3};
use crate::stat::Stat;
use longshore_common::io::{BufId, Io};

impl Client {
    /// What the store knows about the object under `key`, without fetching
    /// it. `Ok(None)` means there is no such object. Uses no lent buffer,
    /// so it runs on whichever I/O thread is open and least busy.
    pub fn stat(&self, key: &str) -> Pending<Result<Option<Stat>, Error>> {
        self.unbuffered(key, Kind::Stat { head: false }, stat_of)
    }

    /// [`Client::stat`], also copying the response head into `into`, for
    /// headers [`Stat`] has no field for. Read them with
    /// [`crate::stat::header_in`] over the first [`Stat::head_len`] bytes.
    pub fn stat_head(
        &self,
        key: &str,
        into: Buffer,
    ) -> Pending<(Result<Option<Stat>, Error>, Buffer)> {
        let (lane, buf, alive) = into.into_parts();
        let kind = Kind::Stat { head: true };
        Pending::new((lane, alive), key, kind, Some(buf), 0, None, stat_of)
    }

    /// List up to `max_keys` keys that start with `prefix` into `into`, and
    /// with a `delimiter`, the common prefixes one level down. `Ok(n)` is
    /// the length of the listing document at the start of the buffer;
    /// read it with [`Listing::parse`]. An empty prefix lists the bucket.
    ///
    /// The delimiter is one ASCII byte, `/` in practice; any other byte is
    /// refused with [`Error::BadKey`]. A page of zero keys is asked for as
    /// a page of one, as the store requires.
    pub fn list(
        &self,
        prefix: &str,
        delimiter: Option<u8>,
        max_keys: u16,
        into: Buffer,
    ) -> Pending<(Result<usize, Error>, Buffer)> {
        self.list_as(prefix, delimiter, max_keys, into, None)
    }

    /// The page after the one in `page`, whose listing document is its
    /// first `len` bytes, as the call that filled it returned. `prefix`,
    /// `delimiter` and `max_keys` must be what the listing was started
    /// with. The next page is written over the previous one.
    ///
    /// [`Error::BadKey`] if the page is not a listing that was cut short:
    /// it has no continuation token, or one too long to send.
    pub fn list_next(
        &self,
        prefix: &str,
        delimiter: Option<u8>,
        max_keys: u16,
        page: Buffer,
        len: usize,
    ) -> Pending<(Result<usize, Error>, Buffer)> {
        self.list_as(prefix, delimiter, max_keys, page, Some(len))
    }

    fn list_as(
        &self,
        prefix: &str,
        delimiter: Option<u8>,
        max_keys: u16,
        into: Buffer,
        page_len: Option<usize>,
    ) -> Pending<(Result<usize, Error>, Buffer)> {
        let early = if delimiter.is_some_and(|d| !d.is_ascii()) {
            Some(Error::BadKey)
        } else if page_len.is_some_and(|n| n > into.lane.buf_size) {
            Some(Error::TooLong)
        } else {
            None
        };
        let (lane, buf, alive) = into.into_parts();
        let kind = Kind::List {
            delimiter,
            max_keys,
            next: page_len.is_some(),
        };
        let page_bytes = page_len.unwrap_or(0) as u32;
        Pending::new(
            (lane, alive),
            prefix,
            kind,
            Some(buf),
            page_bytes,
            early,
            |f, _| match f {
                Finished::Outcome(Outcome::Listed { len }) => Ok(*len as usize),
                other @ (Finished::Outcome(_) | Finished::Rejected(_) | Finished::Closed) => {
                    Err(error_of(other))
                }
            },
        )
    }
}

fn stat_of(f: &Finished, _: u32) -> Result<Option<Stat>, Error> {
    match f {
        Finished::Outcome(Outcome::Stat(stat)) => Ok(Some(*stat)),
        Finished::Outcome(Outcome::NotFound) => Ok(None),
        other @ (Finished::Outcome(_) | Finished::Rejected(_) | Finished::Closed) => {
            Err(error_of(other))
        }
    }
}

/// On the I/O thread: send a listing request. With `page`, `buf` holds the
/// previous page, that many bytes, and its continuation token is decoded
/// onto the stack before the request that overwrites the buffer is sent.
#[expect(
    clippy::too_many_arguments,
    reason = "the store, the `Io` and the token, then the listing's own parts, as every request method of the store takes them"
)]
pub(super) fn submit_list<IO: Io>(
    s3: &mut S3,
    io: &mut IO,
    token: u64,
    prefix: &str,
    delimiter: Option<u8>,
    max_keys: u16,
    page: Option<u32>,
    buf: BufId,
) -> Result<(), SubmitError> {
    let one = delimiter.map(|d| [d]);
    let delimiter_text = match &one {
        Some(d) => Some(core::str::from_utf8(d).map_err(|_| SubmitError::BadKey)?),
        None => None,
    };
    let Some(len) = page else {
        return s3.list_with(io, token, prefix, None, max_keys, delimiter_text, buf);
    };
    // A decoded token is never longer than its encoded form, which the
    // store refuses beyond this length.
    let mut decoded = [0u8; LIST_TOKEN_ENCODED_MAX];
    let n = {
        let xml = io.buf(buf).get(..len as usize).ok_or(SubmitError::BadKey)?;
        let listing = Listing::parse(xml).map_err(|_| SubmitError::BadKey)?;
        let raw = listing.next_token_raw().ok_or(SubmitError::BadKey)?;
        unescape(raw, &mut decoded).ok_or(SubmitError::BadKey)?
    };
    let continuation = core::str::from_utf8(&decoded[..n]).map_err(|_| SubmitError::BadKey)?;
    s3.list_continue(
        io,
        token,
        prefix,
        continuation,
        max_keys,
        delimiter_text,
        buf,
    )
}
