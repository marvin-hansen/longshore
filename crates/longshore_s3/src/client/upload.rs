// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Upload streams: an object of unknown length, sent one lent buffer at a
//! time. See [`Upload`].

use super::buffer::Borrow;
use super::pending::Pending;
use super::{open, Buffer, Client, Error, KEY_MAX};
use crate::multipart::{Part, UploadId};
use longshore_mailbox::{block_on, SlotWaiter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

mod end;
mod flow;
mod table;
#[cfg(test)]
mod tests;

pub(super) use table::{ended, Streams};

/// A part of the upload in flight.
type PartOp = Pending<(Result<Part, Error>, Buffer)>;

/// Parts of one upload stream in flight at most.
pub const PARTS_IN_FLIGHT: usize = 4;

/// An object of unknown length being uploaded a buffer at a time.
///
/// [`Client::upload`] opens one. Bytes go in with [`Upload::write`], which
/// copies them into a lent buffer, or with [`Upload::send`], which takes a
/// buffer the caller filled. Each full buffer is sent as one part of a
/// multipart upload, so a part is one zero-copy send, and up to
/// [`PARTS_IN_FLIGHT`] parts are in flight at once; writing waits while
/// that many are. The parts may run on different I/O threads.
/// [`Upload::finish`] sends the last part and completes the upload.
///
/// The multipart upload begins only when bytes arrive beyond the first
/// buffer. A stream finished before that is stored with one plain store,
/// so it costs no more than storing one buffer, and dropping it costs
/// nothing. Dropping a stream whose upload has begun aborts the upload once
/// every part still in flight has ended, so no part lands after the abort.
/// A completion the store refuses aborts it the same way. A client that
/// shuts down waits for those aborts to be sent.
///
/// Every buffer is one part, so the client's buffers must be at least the
/// store's smallest part ([`Builder::part_min`](super::Builder::part_min),
/// 5 MiB on Amazon S3), and a stream holds at most 10,000 buffers' worth of
/// bytes. From the begin to the end of the upload, the stream holds one
/// more lent buffer for its list of parts.
///
/// ```no_run
/// # fn run(client: longshore_s3::client::Client) -> Result<(), longshore_s3::client::Error> {
/// let mut upload = client.upload("logs/today").wait()?;
/// for line in ["one\n", "two\n"] {
///     upload.write(line.as_bytes()).wait()?;
/// }
/// upload.finish().wait()?;
/// # Ok(()) }
/// ```
pub struct Upload {
    client: Client,
    /// This stream's record in the client's table.
    record: u32,
    key: [u8; KEY_MAX],
    key_len: usize,
    /// The buffer being filled, and how many of its bytes are the stream's.
    current: Option<Buffer>,
    filled: usize,
    /// A buffer a part gave back, for the next one.
    spare: Option<Buffer>,
    /// A buffer being waited for.
    borrow: Option<Borrow>,
    /// Waiting for the operation slot kept for the abort.
    reserving: SlotWaiter,
    /// The buffer of part records, from the begin to the end of the upload,
    /// when no operation holds it.
    records: Option<Buffer>,
    /// The lane, as an index in the client's, and slot kept for the abort.
    reserved: Option<(u32, u32)>,
    begin: Option<Pending<(Result<UploadId, Error>, Buffer)>>,
    upload: Option<UploadId>,
    parts: [Option<PartOp>; PARTS_IN_FLIGHT],
    /// Parts sent so far, which is the last part's number.
    sent: u32,
    /// Bytes the completion lists for the parts that have ended.
    manifest: usize,
    /// The completion, or the one plain store of a short stream.
    ending: Option<Pending<(Result<(), Error>, Buffer)>>,
    /// Why the stream cannot go on, once it cannot.
    failed: Option<Error>,
    /// The record has been given back.
    settled: bool,
}

impl Client {
    /// Open an upload stream to `key`, waiting while every stream record
    /// is in use (see [`super::Builder::streams`]).
    ///
    /// [`Error::PartTooSmall`] if this client's buffers are smaller than
    /// the store's smallest part, and [`Error::BadKey`] for a key that
    /// cannot be sent. Whether the upload sends checksums follows this
    /// handle (see [`Client::with_checksums`]).
    pub fn upload<'a>(&'a self, key: &'a str) -> OpeningUpload<'a> {
        OpeningUpload {
            client: self,
            key,
            waiter: SlotWaiter::new(),
        }
    }
}

/// Opening an upload stream: a future, or [`OpeningUpload::wait`].
#[must_use = "the stream comes back through this"]
pub struct OpeningUpload<'a> {
    client: &'a Client,
    key: &'a str,
    waiter: SlotWaiter,
}

impl OpeningUpload<'_> {
    /// Block the calling thread until the stream is open.
    pub fn wait(self) -> Result<Upload, Error> {
        block_on(self)
    }
}

impl Future for OpeningUpload<'_> {
    type Output = Result<Upload, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let client = me.client;
        if me.key.is_empty() || me.key.len() > KEY_MAX {
            return Poll::Ready(Err(Error::BadKey));
        }
        let min = client.streams.part_min;
        if (client.buffer_size() as u64) < min {
            return Poll::Ready(Err(Error::PartTooSmall { min }));
        }
        let lanes = &client.lanes;
        let record = core::task::ready!(me.waiter.poll_one(
            &client.streams.free,
            || lanes.iter().any(|l| open(l)),
            cx
        ));
        let Some(record) = record else {
            return Poll::Ready(Err(Error::Closed));
        };
        client.streams.set_key(record, me.key);
        let mut key = [0; KEY_MAX];
        key[..me.key.len()].copy_from_slice(me.key.as_bytes());
        Poll::Ready(Ok(Upload {
            client: client.clone(),
            record,
            key,
            key_len: me.key.len(),
            current: None,
            filled: 0,
            spare: None,
            borrow: None,
            reserving: SlotWaiter::new(),
            records: None,
            reserved: None,
            begin: None,
            upload: None,
            parts: core::array::from_fn(|_| None),
            sent: 0,
            manifest: 0,
            ending: None,
            failed: None,
            settled: false,
        }))
    }
}

impl Drop for OpeningUpload<'_> {
    fn drop(&mut self) {
        self.waiter.forget_one(&self.client.streams.free);
    }
}

impl Upload {
    /// Copy `bytes` into the stream. Each buffer it fills is sent as a part
    /// when more bytes follow it; the future waits while
    /// [`PARTS_IN_FLIGHT`] parts are in flight, or for a buffer.
    ///
    /// An error ends the stream: later calls return it too, and dropping
    /// the stream aborts its upload. A write dropped after it has taken
    /// some of its bytes and before it ended leaves the stream unsure of
    /// its contents, so the stream then fails with [`Error::BadRange`].
    pub fn write<'a>(&'a mut self, bytes: &'a [u8]) -> Writing<'a> {
        Writing {
            upload: self,
            bytes,
            done: 0,
            ended: false,
        }
    }

    /// Hand the stream a buffer the caller filled with its next `len`
    /// bytes, without copying them. A full buffer becomes one part. A
    /// buffer with room left becomes the one later writes fill, unless the
    /// stream already has such a buffer, in which case its bytes are
    /// copied there and the buffer goes back to the pool.
    ///
    /// [`Error::TooLong`] if `len` is more than the buffer holds; that
    /// leaves the stream as it was. Other errors end the stream, as with
    /// [`Upload::write`].
    pub fn send(&mut self, buffer: Buffer, len: usize) -> Sending<'_> {
        Sending {
            upload: self,
            buffer: Some(buffer),
            len,
            copied: 0,
            ended: false,
        }
    }

    /// Send what is left and complete the upload; or, for a stream that
    /// never filled more than one buffer, store it with one plain store.
    pub fn finish(self) -> Finishing {
        Finishing { upload: self }
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        self.settle_now();
    }
}

/// A write to an upload stream: a future, or [`Writing::wait`].
#[must_use = "a write does nothing until it is awaited or waited for"]
pub struct Writing<'a> {
    upload: &'a mut Upload,
    bytes: &'a [u8],
    done: usize,
    ended: bool,
}

impl Writing<'_> {
    /// Block the calling thread until every byte is in the stream.
    pub fn wait(self) -> Result<(), Error> {
        block_on(self)
    }
}

impl Future for Writing<'_> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let result = core::task::ready!(me.upload.poll_write(me.bytes, &mut me.done, cx));
        me.ended = true;
        Poll::Ready(result)
    }
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        if !self.ended && self.done > 0 && self.done < self.bytes.len() {
            self.upload.fail(Error::BadRange);
        }
    }
}

/// A lent buffer handed to an upload stream: a future, or
/// [`Sending::wait`].
#[must_use = "a send does nothing until it is awaited or waited for"]
pub struct Sending<'a> {
    upload: &'a mut Upload,
    buffer: Option<Buffer>,
    len: usize,
    /// Bytes copied, when the buffer's bytes are copied into the stream's.
    copied: usize,
    ended: bool,
}

impl Sending<'_> {
    /// Block the calling thread until the buffer is in the stream.
    pub fn wait(self) -> Result<(), Error> {
        block_on(self)
    }
}

impl Future for Sending<'_> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let result =
            core::task::ready!(me
                .upload
                .poll_send(&mut me.buffer, me.len, &mut me.copied, cx));
        me.ended = true;
        // A buffer not kept by the stream goes back to the pool.
        me.buffer = None;
        Poll::Ready(result)
    }
}

impl Drop for Sending<'_> {
    fn drop(&mut self) {
        if !self.ended && self.copied > 0 && self.copied < self.len {
            self.upload.fail(Error::BadRange);
        }
    }
}

/// The end of an upload stream: a future, or [`Finishing::wait`]. Dropping
/// it before it ends drops the stream, which aborts a begun upload.
#[must_use = "the upload is only complete once this has ended"]
pub struct Finishing {
    upload: Upload,
}

impl Finishing {
    /// Block the calling thread until the object is stored.
    pub fn wait(self) -> Result<(), Error> {
        block_on(self)
    }
}

impl Future for Finishing {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().upload.poll_finish(cx)
    }
}
