// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Moving bytes through an upload stream: filling buffers, beginning the
//! upload, and sending full buffers as parts.

use super::super::buffer::Borrow;
use super::super::lane::{Finished, Kind};
use super::super::pending::Pending;
use super::super::{error_of, open, Buffer, Error};
use super::{Upload, PARTS_IN_FLIGHT};
use crate::multipart::{write_manifest, Part, UploadId, PART_NUMBER_MAX, PART_TAG_MAX};
use crate::s3::{Checksum, Outcome};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Bytes a part's record takes in the buffer of records.
pub(super) const RECORD: usize = core::mem::size_of::<Part>();
/// Bytes of the completion document around its list of parts.
pub(super) const FRAME: usize =
    "<CompleteMultipartUpload>".len() + "</CompleteMultipartUpload>".len();
/// The most bytes one part's entry in the completion document can take:
/// the largest number, and a tag that is all quotes, each written as
/// `&quot;`.
pub(super) const ENTRY_MAX: usize = "<Part><PartNumber>".len()
    + 5
    + "</PartNumber><ETag>".len()
    + 6 * PART_TAG_MAX
    + "</ETag>".len()
    + "<ChecksumCRC64NVME>".len()
    + 12
    + "</ChecksumCRC64NVME>".len()
    + "</Part>".len();

/// Whether part `number` may be sent, given a buffer of records of `size`
/// bytes, `manifest` bytes of entries for the parts that have ended, and
/// `running` parts in flight whose entries are not known yet. The records
/// and the completion document are laid side by side in that buffer.
pub(super) fn room(number: u32, size: usize, manifest: usize, running: usize) -> Result<(), Error> {
    if number > PART_NUMBER_MAX {
        return Err(Error::BadPart);
    }
    let need = number as usize * RECORD + FRAME + manifest + (running + 1) * ENTRY_MAX;
    if need > size {
        return Err(Error::TooLong);
    }
    Ok(())
}

/// Bytes the completion document gives `part`.
pub(super) fn entry_len(part: Part) -> usize {
    let mut scratch = [0u8; FRAME + ENTRY_MAX];
    write_manifest(&mut scratch, &[part]).map_or(ENTRY_MAX, |n| n - FRAME)
}

fn begun(f: &Finished, _: u32) -> Result<UploadId, Error> {
    match f {
        Finished::Outcome(Outcome::Begun { upload }) => Ok(*upload),
        other @ (Finished::Outcome(_) | Finished::Rejected(_) | Finished::Closed) => {
            Err(error_of(other))
        }
    }
}

fn part_stored(f: &Finished, number: u32) -> Result<Part, Error> {
    match f {
        Finished::Outcome(Outcome::PartStored { tag, checksum }) => Ok(Part::new(
            number,
            *tag,
            checksum.map(longshore_crc::crc64_header_value),
        )),
        other @ (Finished::Outcome(_) | Finished::Rejected(_) | Finished::Closed) => {
            Err(error_of(other))
        }
    }
}

impl Upload {
    pub(super) fn key(&self) -> &str {
        core::str::from_utf8(&self.key[..self.key_len]).unwrap_or("")
    }

    /// The address of the client's table of stream records, which the
    /// stream's requests carry.
    pub(super) fn table(&self) -> usize {
        Arc::as_ptr(&self.client.streams) as usize
    }

    pub(super) fn running(&self) -> usize {
        self.parts.iter().filter(|p| p.is_some()).count()
    }

    /// End the stream with `e`, unless it has already ended with an error.
    pub(super) fn fail(&mut self, e: Error) {
        if self.failed.is_none() {
            self.failed = Some(e);
        }
    }

    /// Keep a buffer a part gave back for the next one, or return it.
    fn keep(&mut self, buffer: Buffer) {
        if self.spare.is_none() && !buffer.is_void() {
            self.spare = Some(buffer);
        }
    }

    /// Poll every part in flight, sending those still waiting for a slot
    /// and recording those that have ended.
    pub(super) fn harvest(&mut self, cx: &mut Context<'_>) {
        for i in 0..PARTS_IN_FLIGHT {
            let Some(op) = &mut self.parts[i] else {
                continue;
            };
            if let Poll::Ready((result, buffer)) = Pin::new(op).poll(cx) {
                self.parts[i] = None;
                self.keep(buffer);
                match result {
                    Ok(part) => self.record(part),
                    Err(e) => self.fail(e),
                }
            }
        }
    }

    /// Write a part's record at the place its number gives it.
    fn record(&mut self, part: Part) {
        self.manifest += entry_len(part);
        let at = (part.number() as usize - 1) * RECORD;
        let fits = self.records.as_mut().is_some_and(|records| {
            let all = records.as_mut_slice();
            let fits = at + RECORD <= all.len();
            if fits {
                // In bounds, and a `Part` is plain data.
                unsafe {
                    core::ptr::write_unaligned(all.as_mut_ptr().add(at).cast::<Part>(), part)
                };
            }
            fits
        });
        if !fits {
            self.fail(Error::TooLong);
        }
    }

    /// A buffer for the stream: one a part gave back, or one from the pool
    /// while fewer than [`PARTS_IN_FLIGHT`] parts are in flight.
    pub(super) fn need_buffer(&mut self, cx: &mut Context<'_>) -> Poll<Result<Buffer, Error>> {
        self.harvest(cx);
        if let Some(e) = self.failed {
            return Poll::Ready(Err(e));
        }
        if let Some(b) = self.spare.take() {
            // A wait for the pool no longer needed gives its turn on.
            self.borrow = None;
            return Poll::Ready(Ok(b));
        }
        if self.running() == PARTS_IN_FLIGHT {
            return Poll::Pending;
        }
        let (lanes, alive) = (self.client.lanes.clone(), self.client.alive.clone());
        let borrow = self.borrow.get_or_insert_with(|| Borrow::new(lanes, alive));
        let b = core::task::ready!(Pin::new(borrow).poll(cx));
        self.borrow = None;
        if b.is_void() {
            self.fail(Error::Closed);
            return Poll::Ready(Err(Error::Closed));
        }
        Poll::Ready(Ok(b))
    }

    /// Begin the upload, if it has not begun: take the buffer of records,
    /// keep an operation slot on its lane for the abort, and ask the store.
    pub(super) fn poll_begun(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            if self.upload.is_some() {
                return Poll::Ready(Ok(()));
            }
            if let Some(e) = self.failed {
                return Poll::Ready(Err(e));
            }
            if let Some(begin) = &mut self.begin {
                let (result, records) = core::task::ready!(Pin::new(begin).poll(cx));
                self.begin = None;
                self.records = Some(records);
                match result {
                    Ok(upload) => {
                        self.upload = Some(upload);
                        self.client.streams.set_upload(self.record, &upload);
                    }
                    Err(e) => self.fail(e),
                }
                continue;
            }
            let Some(held) = &self.records else {
                let b = core::task::ready!(self.need_buffer(cx))?;
                self.records = Some(b);
                continue;
            };
            if self.reserved.is_none() {
                let lane = held.lane.clone();
                let slot = core::task::ready!(self.reserving.poll_one(
                    lane.sender.pool(),
                    || open(&lane),
                    cx
                ));
                match slot {
                    Some(slot) => {
                        let at = self
                            .client
                            .lanes
                            .iter()
                            .position(|l| Arc::ptr_eq(l, &lane))
                            .unwrap_or(0) as u32;
                        self.reserved = Some((at, slot));
                        self.client.streams.set_reserved(self.record, at, slot);
                    }
                    None => self.fail(Error::Closed),
                }
                continue;
            }
            // The begin carries the buffer of records, so it runs on the
            // reserved slot's lane and the stream knows when it was left
            // running.
            let Some(records) = self.records.take() else {
                continue;
            };
            let (lane, buf, alive) = records.into_parts();
            let kind = Kind::Begin {
                checksums: self.client.checksum == Checksum::Compute,
            };
            let begin = Pending::new((lane, alive), self.key(), kind, Some(buf), 0, None, begun)
                .of_stream(self.table(), self.record);
            self.begin = Some(begin);
        }
    }

    /// Send the current buffer as the next part, once the upload has begun
    /// and fewer than [`PARTS_IN_FLIGHT`] parts are in flight.
    pub(super) fn send_current(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        core::task::ready!(self.poll_begun(cx))?;
        self.harvest(cx);
        if let Some(e) = self.failed {
            return Poll::Ready(Err(e));
        }
        let Some(free) = self.parts.iter().position(|p| p.is_none()) else {
            return Poll::Pending;
        };
        let (Some(upload), Some(size)) = (self.upload, self.current.as_ref().map(Buffer::len))
        else {
            return Poll::Ready(Ok(()));
        };
        let number = self.sent + 1;
        if let Err(e) = room(number, size, self.manifest, self.running()) {
            self.fail(e);
            return Poll::Ready(Err(e));
        }
        let Some(current) = self.current.take() else {
            return Poll::Ready(Ok(()));
        };
        let len = core::mem::take(&mut self.filled);
        let (lane, buf, alive) = current.into_parts();
        let kind = Kind::Part {
            checksum: self.client.checksum,
        };
        let part = Pending::new(
            (lane, alive),
            self.key(),
            kind,
            Some(buf),
            len as u32,
            None,
            part_stored,
        )
        .of_upload(&upload, number)
        .of_stream(self.table(), self.record);
        self.parts[free] = Some(part);
        self.sent = number;
        // Send it now if a slot is free.
        self.harvest(cx);
        Poll::Ready(Ok(()))
    }

    /// Copy `bytes[*done..]` into the stream.
    pub(super) fn poll_write(
        &mut self,
        bytes: &[u8],
        done: &mut usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        loop {
            if let Some(e) = self.failed {
                return Poll::Ready(Err(e));
            }
            if *done == bytes.len() {
                return Poll::Ready(Ok(()));
            }
            match &mut self.current {
                Some(current) if self.filled < current.len() => {
                    let n = (current.len() - self.filled).min(bytes.len() - *done);
                    current.as_mut_slice()[self.filled..self.filled + n]
                        .copy_from_slice(&bytes[*done..*done + n]);
                    self.filled += n;
                    *done += n;
                }
                // Full, and more bytes follow: it is a part.
                Some(_) => core::task::ready!(self.send_current(cx))?,
                None => {
                    let b = core::task::ready!(self.need_buffer(cx))?;
                    self.current = Some(b);
                    self.filled = 0;
                }
            }
        }
    }

    /// Take `buffer`, filled with the stream's next `len` bytes.
    pub(super) fn poll_send(
        &mut self,
        buffer: &mut Option<Buffer>,
        len: usize,
        copied: &mut usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        if let Some(e) = self.failed {
            return Poll::Ready(Err(e));
        }
        let Some(given) = buffer.as_ref() else {
            return Poll::Ready(Ok(()));
        };
        if len > given.len() {
            return Poll::Ready(Err(Error::TooLong));
        }
        if given.is_void() {
            self.fail(Error::Closed);
            return Poll::Ready(Err(Error::Closed));
        }
        loop {
            let copying = *copied > 0
                || self
                    .current
                    .as_ref()
                    .is_some_and(|c| self.filled > 0 && self.filled < c.len());
            if copying {
                // Bytes already wait in a buffer with room: these follow
                // them there.
                let Some(b) = buffer.as_ref() else {
                    return Poll::Ready(Ok(()));
                };
                return self.poll_write(&b.as_slice()[..len], copied, cx);
            }
            if self.current.is_some() && self.filled > 0 {
                // Full, and more bytes follow: it is a part.
                core::task::ready!(self.send_current(cx))?;
                continue;
            }
            self.current = buffer.take();
            self.filled = len;
            return Poll::Ready(Ok(()));
        }
    }
}
