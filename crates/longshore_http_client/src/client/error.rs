// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Why a request failed.

use crate::head::HttpError;
use longshore_stream::TlsError;

/// Why a request failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Every connection is in use.
    Busy,
    /// The request head is invalid or does not fit.
    BadRequest,
    /// The response head is malformed or too large.
    Head(HttpError),
    /// A socket operation failed with this errno.
    Io(i32),
    /// The peer closed mid-exchange. `reused` is true when the connection had
    /// served earlier requests, the usual sign of a stale keep-alive.
    Closed {
        /// Whether the connection had served earlier requests.
        reused: bool,
    },
    /// The request took longer than the client allows.
    Timeout,
    /// The response body does not fit the buffer, or it is too large to
    /// discard when no buffer was given.
    BodyTooLarge,
    /// A response body with neither a length nor chunked encoding (it would
    /// end only when the connection closes) is not supported.
    UnsupportedFraming,
    /// The TLS handshake failed or a record did not authenticate.
    Tls(TlsError),
}

impl core::fmt::Display for HttpError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            HttpError::HeadTooLarge => "the head has no end within the limit",
            HttpError::TooManyHeaders => "the head has too many header fields",
            HttpError::BadStartLine => "the request or status line is malformed",
            HttpError::BadHeader => "a header field is malformed",
            HttpError::BadContentLength => {
                "the Content-Length is malformed or given twice differently"
            }
            HttpError::BadFraming => "the body's framing is not Content-Length or chunked",
            HttpError::BadRequest => {
                "a request head field holds CR, LF or NUL, or the head does not fit"
            }
        })
    }
}

impl std::error::Error for HttpError {}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::Busy => f.write_str("every connection is in use"),
            Error::BadRequest => f.write_str("the request head is invalid or does not fit"),
            Error::Head(_) => f.write_str("the response head was not accepted"),
            Error::Io(errno) => write!(
                f,
                "a socket operation failed: {}",
                std::io::Error::from_raw_os_error(*errno)
            ),
            Error::Closed { reused: true } => {
                f.write_str("the peer closed a reused connection mid-exchange")
            }
            Error::Closed { reused: false } => {
                f.write_str("the peer closed the connection mid-exchange")
            }
            Error::Timeout => f.write_str("the request took longer than the client allows"),
            Error::BodyTooLarge => f.write_str("the response body does not fit the buffer"),
            Error::UnsupportedFraming => {
                f.write_str("the response body has neither a length nor chunked encoding")
            }
            Error::Tls(_) => f.write_str("TLS failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Head(e) => Some(e),
            Error::Tls(e) => Some(e),
            Error::Busy
            | Error::BadRequest
            | Error::Io(_)
            | Error::Closed { .. }
            | Error::Timeout
            | Error::BodyTooLarge
            | Error::UnsupportedFraming => None,
        }
    }
}
