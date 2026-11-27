// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! How an upload stream ends: one plain store, a completion, or an abort.

use super::super::lane::{Finished, Kind};
use super::super::pending::{Left, Pending};
use super::super::{error_of, Error, Output};
use super::flow::RECORD;
use super::Upload;
use crate::multipart::{manifest_from, Part};
use crate::s3::Outcome;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// One if `p` holds an operation sent and not taken.
fn sent<T: Output>(p: &Option<Pending<T>>) -> u32 {
    u32::from(p.as_ref().is_some_and(|p| p.is_sent()))
}

fn stored(f: &Finished, _: u32) -> Result<(), Error> {
    match f {
        Finished::Outcome(Outcome::Stored) => Ok(()),
        other @ (Finished::Outcome(_) | Finished::Rejected(_) | Finished::Closed) => {
            Err(error_of(other))
        }
    }
}

impl Upload {
    /// Send what is left and end the upload.
    pub(super) fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            if let Some(ending) = &mut self.ending {
                let (result, buffer) = core::task::ready!(Pin::new(ending).poll(cx));
                self.ending = None;
                if self.reserved.is_some() {
                    self.records = Some(buffer);
                } else {
                    drop(buffer);
                }
                match result {
                    Ok(()) => self.release(),
                    Err(e) => self.fail(e),
                }
                if result.is_ok() {
                    return Poll::Ready(Ok(()));
                }
            }
            if let Some(e) = self.failed {
                // A refused completion, or any other end, aborts now.
                self.settle_now();
                return Poll::Ready(Err(e));
            }
            if self.settled {
                return Poll::Ready(Err(Error::Closed));
            }
            if self.begin.is_none() && self.upload.is_none() {
                core::task::ready!(self.store_whole(cx))?;
                continue;
            }
            core::task::ready!(self.poll_begun(cx))?;
            if self.filled > 0 {
                core::task::ready!(self.send_current(cx))?;
                continue;
            }
            self.current = None;
            self.harvest(cx);
            if self.failed.is_some() {
                continue;
            }
            if self.running() > 0 {
                return Poll::Pending;
            }
            if let Err(e) = self.complete() {
                self.fail(e);
            }
        }
    }

    /// A stream that never filled more than one buffer: store it with one
    /// plain store, giving back what a begin that had started holds.
    fn store_whole(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if let Some((at, slot)) = self.reserved.take() {
            self.client.lanes[at as usize].sender.pool().give(slot);
        } else if let Some(records) = &self.records {
            self.reserving.forget_one(records.lane.sender.pool());
        }
        self.records = None;
        let buffer = match self.current.take() {
            Some(b) => b,
            None => core::task::ready!(self.need_buffer(cx))?,
        };
        let len = core::mem::take(&mut self.filled);
        self.ending = Some(self.client.store(self.key(), buffer, len));
        Poll::Ready(Ok(()))
    }

    /// Every part has ended: send the completion, with the list of parts
    /// in the buffer of records.
    fn complete(&mut self) -> Result<(), Error> {
        let len = self.render()?;
        let (Some(records), Some(upload)) = (self.records.take(), self.upload) else {
            return Err(Error::Closed);
        };
        let (lane, buf, alive) = records.into_parts();
        let complete = Pending::new(
            (lane, alive),
            self.key(),
            Kind::Complete,
            Some(buf),
            len as u32,
            None,
            stored,
        )
        .of_upload(&upload, 0)
        .of_stream(self.table(), self.record);
        self.ending = Some(complete);
        Ok(())
    }

    /// Write the completion document from the part records, at the start
    /// of the buffer of records. The records lie first, so the document is
    /// written after them and then moved to the start.
    fn render(&mut self) -> Result<usize, Error> {
        let n = self.sent as usize;
        let records = self.records.as_mut().ok_or(Error::Closed)?;
        let all = records.as_mut_slice();
        let start = n * RECORD;
        if start > all.len() {
            return Err(Error::TooLong);
        }
        let (recs, out) = all.split_at_mut(start);
        // Every part from 1 to `n` ended well, so each record was written.
        let parts = (0..n).map(|i| unsafe {
            core::ptr::read_unaligned(recs.as_ptr().add(i * RECORD).cast::<Part>())
        });
        let len = manifest_from(out, parts).ok_or(Error::TooLong)?;
        all.copy_within(start..start + len, 0);
        Ok(len)
    }

    /// The upload ended well: give back the reserved slot and the record.
    fn release(&mut self) {
        if let Some((at, slot)) = self.reserved.take() {
            self.client.lanes[at as usize].sender.pool().give(slot);
        }
        if !self.settled {
            self.settled = true;
            self.client.streams.free.give(self.record);
        }
    }

    /// The stream ends without completing: abort a begun upload once every
    /// operation of the stream still running has ended, and give the record
    /// back. See `table` for who sends the abort.
    pub(super) fn settle_now(&mut self) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.borrow = None;
        let streams = self.client.streams.clone();
        let record = self.record;
        if self.reserved.take().is_none() {
            // Nothing begun, so nothing runs and nothing is to abort.
            if let Some(records) = &self.records {
                self.reserving.forget_one(records.lane.sender.pool());
            }
            streams.free.give(record);
            return;
        }
        let running =
            sent(&self.begin) + self.parts.iter().map(sent).sum::<u32>() + sent(&self.ending);
        streams.dropping(record, running);
        if running > 0 {
            // Kept for the I/O threads that end what still runs; the one
            // that ends the last gives it back.
            core::mem::forget(streams.clone());
        }
        let mut ended = 0;
        if let Some(begin) = self.begin.take() {
            if let Left::Ended((result, _records)) = begin.abandon() {
                ended += 1;
                if let Ok(upload) = result {
                    streams.set_upload(record, &upload);
                }
            }
        }
        for part in &mut self.parts {
            if let Some(Left::Ended(_)) = part.take().map(Pending::abandon) {
                ended += 1;
            }
        }
        if let Some(Left::Ended(_)) = self.ending.take().map(Pending::abandon) {
            ended += 1;
        }
        if streams.dropped(record, ended) {
            streams.settle(record);
            if running > 0 {
                // Nothing was left running after all: give the kept count back.
                unsafe { Arc::decrement_strong_count(Arc::as_ptr(&streams)) };
            }
        }
    }
}
