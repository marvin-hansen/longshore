// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! What a download checks: each range's answer, and the whole object's
//! checksum at the end.

use super::Error;
use crate::report::{Cause, ETag, End, Report};
use crate::s3::Fatal;
use longshore_crc::{Crc32c, Crc64Nvme};
use longshore_http_client::client::Error as Transport;

/// The bytes of a range `first..=last` that the store answered as asked,
/// or why the answer is not that range of the object the download opened.
pub(super) fn range_of(
    report: &Report,
    first: u64,
    last: u64,
    tag: Option<&ETag>,
) -> Result<usize, Error> {
    let answer = match report.end() {
        End::Answered(answer) => answer,
        End::NotSent(cause) | End::Unknown(cause) => return Err(error_of(cause)),
    };
    match answer.status() {
        206 => {}
        // The whole object, or what a store that ignores ranges sends.
        200 => return Err(Error::Store(Fatal::RangeIgnored)),
        status => return Err(Error::Store(Fatal::Status(status))),
    }
    // A store that ignored `If-Match` still names the object it served.
    let named = answer.etag();
    if let (Some(named), Some(tag)) = (named.as_ref(), tag) {
        if named.as_bytes() != tag.as_bytes() {
            return Err(Error::Store(Fatal::Status(412)));
        }
    }
    let len = last - first + 1;
    let served = answer
        .content_range()
        .is_some_and(|r| r.first() == first && r.last() == last);
    if !served || u64::from(answer.body_len()) != len {
        return Err(Error::Store(Fatal::BadResponse));
    }
    Ok(len as usize)
}

/// Why a range got no answer, as the client's other operations say it.
fn error_of(cause: Cause) -> Error {
    match cause {
        Cause::Deadline => Error::Store(Fatal::Transport(Transport::Timeout)),
        Cause::NoAddress => Error::Store(Fatal::NoAddress),
        Cause::Transport(e) => Error::Store(Fatal::Transport(e)),
        Cause::Closed => Error::Closed,
    }
}

/// The checksum of the bytes handed out so far, in the algorithm of the
/// store's checksum of the whole object, if it keeps one.
pub(super) enum Running {
    /// Twelve base64 characters: CRC-64/NVME, as S3 sends it.
    Crc64 { crc: Crc64Nvme, stored: [u8; 12] },
    /// Eight base64 characters and four zeros: CRC-32C, as Google Cloud
    /// Storage sends it.
    Crc32c { crc: Crc32c, stored: [u8; 12] },
    /// The store keeps no checksum of the whole object.
    Unchecked,
}

impl Running {
    /// A running checksum to compare with `stored`, in the form
    /// [`crate::stat::Stat::checksum`] holds it.
    pub(super) fn new(stored: Option<[u8; 12]>) -> Running {
        match stored {
            None => Running::Unchecked,
            Some(stored) if stored[8..] == [0; 4] => Running::Crc32c {
                crc: Crc32c::new(),
                stored,
            },
            Some(stored) => Running::Crc64 {
                crc: Crc64Nvme::new(),
                stored,
            },
        }
    }

    /// Take in the next bytes handed out.
    pub(super) fn update(&mut self, bytes: &[u8]) {
        match self {
            Running::Crc64 { crc, .. } => crc.update(bytes),
            Running::Crc32c { crc, .. } => crc.update(bytes),
            Running::Unchecked => {}
        }
    }

    /// How a download that has handed out every byte ends: `Ok(true)`
    /// verified, `Ok(false)` with nothing to check against.
    pub(super) fn end(&self) -> Result<bool, Error> {
        let (ours, stored) = match self {
            Running::Crc64 { crc, stored } => {
                (longshore_crc::crc64_header_value(crc.finish()), stored)
            }
            Running::Crc32c { crc, stored } => {
                let mut ours = [0u8; 12];
                ours[..8].copy_from_slice(&longshore_crc::crc32c_header_value(crc.finish()));
                (ours, stored)
            }
            Running::Unchecked => return Ok(false),
        };
        if ours == *stored {
            Ok(true)
        } else {
            Err(Error::Store(Fatal::ChecksumMismatch))
        }
    }
}
