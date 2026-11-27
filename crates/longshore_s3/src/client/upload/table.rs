// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The fixed table of upload stream records a client holds, and how a
//! dropped stream's upload is aborted once its last operation has ended.
//!
//! While a stream lives, it alone touches its record. When it is dropped
//! with operations still running, it writes their count into the record's
//! state, leaves one count on the table's `Arc` to keep the table alive,
//! and detaches them. Each I/O thread that then ends one of them takes one
//! off the state; the side that takes the state to zero with the stream
//! dropped sends the abort through the operation slot the stream reserved,
//! frees the record and gives the table's count back. So the abort never
//! waits for a slot and goes after every part has ended, on whichever
//! threads they ran.

use super::super::lane::{Finished, Kind, Lane, Streamed};
use super::super::sync::{AtomicU32, UnsafeCell};
use super::super::{request, KEY_MAX};
use crate::multipart::UploadId;
use crate::s3::Outcome;
use longshore_mailbox::Pool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Set in a record's state once its stream is dropped. The rest of the
/// state counts the stream's operations still running, two per operation.
const DROPPED: u32 = 1;

/// Waiters for a free record preallocated, as for the other pools.
const WAITERS: usize = 256;

/// The records of a client's upload streams.
pub(in crate::client) struct Streams {
    lanes: Arc<[Arc<Lane>]>,
    records: Box<[Record]>,
    /// Records no stream holds.
    pub(super) free: Pool,
    /// The smallest part the store takes.
    pub(super) part_min: u64,
}

struct Record {
    state: AtomicU32,
    key: UnsafeCell<([u8; KEY_MAX], usize)>,
    upload: UnsafeCell<Option<UploadId>>,
    /// The lane, as an index in the client's, and the operation slot on it
    /// kept for the abort.
    reserved: UnsafeCell<(u32, u32)>,
}

// A record's cells are written by its stream while the stream holds it,
// and after the stream is dropped by the one I/O thread that ends its last
// operation; the state's read-modify-writes order the two.
unsafe impl Sync for Streams {}
unsafe impl Send for Streams {}

impl Streams {
    /// `count` records over `lanes`. Initialization: this allocates.
    pub(in crate::client) fn new(lanes: Arc<[Arc<Lane>]>, count: u32, part_min: u64) -> Streams {
        let count = count.max(1);
        let records = (0..count)
            .map(|_| Record {
                state: AtomicU32::new(0),
                key: UnsafeCell::new(([0; KEY_MAX], 0)),
                upload: UnsafeCell::new(None),
                reserved: UnsafeCell::new((0, 0)),
            })
            .collect();
        Streams {
            lanes,
            records,
            free: Pool::new(count, WAITERS),
            part_min,
        }
    }

    fn record(&self, record: u32) -> &Record {
        &self.records[record as usize]
    }

    /// Write the key of a record this stream holds.
    pub(super) fn set_key(&self, record: u32, key: &str) {
        let n = key.len().min(KEY_MAX);
        self.record(record).key.with_mut(|k| {
            let k = unsafe { &mut *k };
            k.0[..n].copy_from_slice(&key.as_bytes()[..n]);
            k.1 = n;
        });
        self.record(record)
            .upload
            .with_mut(|u| unsafe { *u = None });
    }

    /// Note the upload a record's stream has begun.
    pub(super) fn set_upload(&self, record: u32, upload: &UploadId) {
        self.record(record)
            .upload
            .with_mut(|u| unsafe { *u = Some(*upload) });
    }

    /// Note the operation slot a record's stream keeps for its abort.
    pub(super) fn set_reserved(&self, record: u32, lane: u32, slot: u32) {
        self.record(record)
            .reserved
            .with_mut(|r| unsafe { *r = (lane, slot) });
    }

    /// A stream with a reserved slot is being dropped with `running` of its
    /// operations sent and not taken. From here the abort is owed to the
    /// reserved slot's lane.
    pub(super) fn dropping(&self, record: u32, running: u32) {
        let r = self.record(record);
        r.state.store(running * 2, Ordering::Release);
        let (lane, _) = r.reserved.with(|p| unsafe { *p });
        let _: u32 = self.lanes[lane as usize]
            .owed
            .fetch_add(1, Ordering::AcqRel);
    }

    /// The stream has detached its operations, `ended` of which it found
    /// already ended. True if none still runs: the caller settles.
    pub(super) fn dropped(&self, record: u32, ended: u32) -> bool {
        let state = &self.record(record).state;
        if ended > 0 {
            let _: u32 = state.fetch_sub(ended * 2, Ordering::AcqRel);
        }
        state.fetch_or(DROPPED, Ordering::AcqRel) < 2
    }

    /// One detached operation ended. True if it was the dropped stream's
    /// last: the caller settles.
    fn ended_one(&self, record: u32, finished: &Finished) -> bool {
        if let Finished::Outcome(Outcome::Begun { upload }) = finished {
            // The stream went before its upload's id came back.
            self.set_upload(record, upload);
        }
        self.record(record).state.fetch_sub(2, Ordering::AcqRel) == 2 | DROPPED
    }

    /// Send the abort of a dropped stream's upload through its reserved
    /// slot, or give the slot back if no upload began, and free the record.
    pub(super) fn settle(&self, record: u32) {
        let r = self.record(record);
        let (at, slot) = r.reserved.with(|p| unsafe { *p });
        let upload = r.upload.with(|p| unsafe { *p });
        let lane = &self.lanes[at as usize];
        let sent = upload.is_some_and(|upload| {
            let built = r.key.with(|k| {
                let (key, n) = unsafe { &*k };
                let key = core::str::from_utf8(&key[..*n]).unwrap_or("");
                request(slot, key, Kind::Abort, 0, 0)
            });
            let Ok(mut abort) = built else {
                return false;
            };
            abort.upload = Some(upload);
            abort.stream = Streamed::Abort;
            let _: u32 = lane.in_flight.fetch_add(1, Ordering::Relaxed);
            if lane.sender.send_slot(slot, None, abort).is_ok() {
                // Nobody waits for it: the I/O thread frees the slot.
                let _: Option<Finished> = lane.sender.detach(slot);
                true
            } else {
                let _: u32 = lane.in_flight.fetch_sub(1, Ordering::Relaxed);
                false
            }
        });
        if !sent {
            lane.sender.pool().give(slot);
            let _: u32 = lane.owed.fetch_sub(1, Ordering::AcqRel);
        }
        r.state.store(0, Ordering::Relaxed);
        self.free.give(record);
    }
}

/// An operation of an upload stream ended after the stream was dropped.
///
/// # Safety
///
/// `table` is the address of the [`Streams`] the operation's request
/// named, and the operation is one its stream left running when dropped,
/// so the count the stream left on the table keeps it alive until here.
pub(in crate::client) unsafe fn ended(table: usize, record: u32, finished: &Finished) {
    let streams = table as *const Streams;
    if (*streams).ended_one(record, finished) {
        (*streams).settle(record);
        // The count the dropped stream left; the table may go with it.
        Arc::decrement_strong_count(streams);
    }
}
