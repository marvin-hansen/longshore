// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Why an operation did not do what was asked, as a value, a message and
//! an error whose cause is the store's.

use crate::s3::Fatal;

/// Why an operation did not do what was asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The key is empty, or longer than [`KEY_MAX`](super::KEY_MAX) or than the store allows,
    /// or an expected `ETag` holds a byte that is not visible ASCII.
    BadKey,
    /// The byte range ends before it starts, or the parts of an upload are
    /// not in ascending order, or a write to an upload stream was dropped
    /// part-way, so the stream no longer knows which bytes it holds.
    BadRange,
    /// The part number is not 1 to 10000.
    BadPart,
    /// More bytes than the buffer holds.
    TooLong,
    /// The client's buffers are smaller than the store's smallest part,
    /// `min` bytes (5 MiB on Amazon S3). An upload stream sends each
    /// buffer as one part, so it cannot be opened. See
    /// [`Builder::part_min`](super::Builder::part_min).
    PartTooSmall {
        /// The smallest part the store takes, in bytes.
        min: u64,
    },
    /// This client opens no upload streams: it was built with
    /// [`Builder::streams`](super::Builder::streams) 0, or over a lane with a single operation slot
    /// (see [`Layout`](super::Layout)). A begun stream keeps one slot of a lane for its
    /// abort, which on such a lane would leave none for its requests.
    NoStreams,
    /// The store refused or could not be reached. Retries have been made.
    Store(Fatal),
    /// The operation did not run: the client was shut down, or the I/O
    /// thread it was given to failed. An operation without a buffer goes to
    /// an I/O thread still running when it is sent, but one can fail just
    /// after that, and an operation with a buffer stays on the buffer's
    /// thread. Asking again may succeed on another thread. If the failed
    /// thread's kernel work could not be stopped, a buffer that comes back
    /// with this error is void: see [`Buffer`](super::Buffer).
    Closed,
    /// The store answered with something this operation does not expect.
    Unexpected,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::BadKey => f.write_str("the key is empty, too long, or holds a byte that cannot be sent"),
            Error::BadRange => f.write_str("the byte range or the order of the parts is wrong, or a write to the stream was dropped part-way"),
            Error::BadPart => f.write_str("the part number is not 1 to 10000"),
            Error::TooLong => f.write_str("more bytes than the buffer or slice holds"),
            Error::PartTooSmall { min } => write!(
                f,
                "the client's buffers are smaller than the store's smallest part, {min} bytes"
            ),
            Error::NoStreams => f.write_str("this client opens no upload streams"),
            Error::Store(_) => f.write_str("the store refused or could not be reached"),
            Error::Closed => f.write_str("the operation did not run: the client was shut down or its I/O thread failed"),
            Error::Unexpected => f.write_str("the store answered with something this operation does not expect"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Store(fatal) => Some(fatal),
            Error::BadKey
            | Error::BadRange
            | Error::BadPart
            | Error::TooLong
            | Error::PartTooSmall { .. }
            | Error::NoStreams
            | Error::Closed
            | Error::Unexpected => None,
        }
    }
}
