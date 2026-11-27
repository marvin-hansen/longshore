// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Turning one caller's request into the store call it asks for.

use super::super::lane::{Finished, Kind, Request, Streamed};
use super::Worker;
use longshore_common::io::{BufId, Io};
use std::sync::atomic::Ordering;

impl Worker {
    /// Note what the request is to an upload stream. True for the abort a
    /// dropped stream owes, which runs even while stopping, and which a
    /// shutdown waits for.
    fn note_stream(&mut self, r: &Request) -> bool {
        self.streamed[r.op as usize] = match r.stream {
            Streamed::Op { table, record } => Some((table, record)),
            Streamed::No | Streamed::Abort => None,
        };
        let owed = r.stream == Streamed::Abort;
        if owed {
            let _: u32 = self.lane.owed.fetch_sub(1, Ordering::AcqRel);
        }
        owed
    }

    pub(super) fn begin<IO: Io>(&mut self, io: &mut IO, r: &Request) {
        if !self.note_stream(r) && self.stopping {
            self.answer(r.op, &Finished::Closed);
            return;
        }
        let key = core::str::from_utf8(&r.key[..r.key_len as usize]).unwrap_or("");
        let buf = BufId::new(self.layout.first_buffer.get() + r.buf);
        let token = u64::from(r.op);
        let sent = match r.kind {
            Kind::Store {
                no_clobber: false,
                checksum,
            } => self.s3.store(io, token, key, buf, r.len, checksum),
            Kind::Store {
                no_clobber: true,
                checksum,
            } => self
                .s3
                .store_no_clobber(io, token, key, buf, r.len, checksum),
            Kind::Fetch { range } => self.s3.fetch(io, token, key, range, buf),
            Kind::Erase => self.s3.erase(io, token, key),
            Kind::Begin { checksums } => self.s3.multipart_begin(io, token, key, checksums),
            Kind::Part { checksum } => match &r.upload {
                Some(upload) => self
                    .s3
                    .multipart_store_part(io, token, key, upload, r.number, buf, r.len, checksum),
                None => Err(crate::s3::SubmitError::BadPart),
            },
            Kind::Complete => match &r.upload {
                Some(upload) => self
                    .s3
                    .multipart_complete(io, token, key, upload, buf, r.len, None),
                None => Err(crate::s3::SubmitError::BadPart),
            },
            Kind::Put { options } => {
                let limit = r.budget.limit(io);
                self.s3.put(io, token, key, buf, r.len, &options, limit)
            }
            Kind::Head => {
                let limit = r.budget.limit(io);
                self.s3.head(io, token, key, &r.expect, limit)
            }
            Kind::Get { range } => {
                let limit = r.budget.limit(io);
                self.s3.get(io, token, key, range, &r.expect, buf, limit)
            }
            Kind::Stat { head } => self.s3.stat(io, token, key, head.then_some(buf)),
            Kind::List {
                delimiter,
                max_keys,
                next,
            } => super::super::query::submit_list(
                &mut self.s3,
                io,
                token,
                key,
                delimiter,
                max_keys,
                next.then_some(r.len),
                buf,
            ),
            Kind::Abort => match &r.upload {
                Some(upload) => self.s3.multipart_abort(io, token, key, upload),
                None => Err(crate::s3::SubmitError::BadPart),
            },
        };
        match sent {
            Ok(()) => self.running[r.op as usize] = true,
            Err(e) => self.answer(r.op, &Finished::Rejected(e)),
        }
    }
}
