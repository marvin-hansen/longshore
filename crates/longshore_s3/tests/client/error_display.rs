// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The client's error as a message, and its causes down to the transport.

use longshore_http_client::client::Error as Http;
use longshore_s3::client::Error;
use longshore_s3::s3::Fatal;
use longshore_stream::TlsError;
use std::error::Error as _;

/// Every message in the chain from `e` down, joined by ": ".
#[cfg(test)]
fn chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        out.push_str(": ");
        out.push_str(&c.to_string());
        cause = c.source();
    }
    out
}

#[test]
fn a_store_error_names_each_cause_down_to_the_transport() {
    let e = Error::Store(Fatal::Transport(Http::Tls(TlsError::BadCertificate)));
    assert_eq!(
        chain(&e),
        "the store refused or could not be reached: the exchange with the store failed: \
         TLS failed: the peer's certificate did not validate"
    );
    let status = Error::Store(Fatal::Status(403));
    assert_eq!(
        chain(&status),
        "the store refused or could not be reached: the store answered with status 403"
    );
}

#[test]
fn an_error_without_a_cause_has_none_and_boxes_as_a_std_error() {
    let e = Error::PartTooSmall { min: 5 << 20 };
    assert!(e.source().is_none());
    assert_eq!(
        e.to_string(),
        "the client's buffers are smaller than the store's smallest part, 5242880 bytes"
    );
    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(Error::NoStreams);
    assert_eq!(boxed.to_string(), "this client opens no upload streams");
}

#[test]
fn an_errno_is_named_as_the_system_names_it() {
    let io = Http::Io(32);
    assert!(io.to_string().starts_with("a socket operation failed: "));
    assert!(io.to_string().len() > "a socket operation failed: ".len());
}
