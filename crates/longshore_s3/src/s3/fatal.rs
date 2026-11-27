// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! [`Fatal`] as a message, and as an error whose cause is the transport's.

use super::Fatal;

impl core::fmt::Display for Fatal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Fatal::Status(code) => write!(f, "the store answered with status {code}"),
            Fatal::Transport(_) => f.write_str("the exchange with the store failed"),
            Fatal::RetriesExhausted => f.write_str("every attempt failed"),
            Fatal::ChecksumMismatch => f.write_str("the bytes did not match the store's checksum"),
            Fatal::NoAddress => f.write_str("the store's name did not resolve to an address"),
            Fatal::RangeIgnored => {
                f.write_str("the store answered a ranged request with the whole object")
            }
            Fatal::BadResponse => f.write_str("the store's answer lacked what it must carry"),
            Fatal::ErrorDocument => f.write_str("the store answered 200 with an error document"),
        }
    }
}

impl std::error::Error for Fatal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Fatal::Transport(e) => Some(e),
            Fatal::Status(_)
            | Fatal::RetriesExhausted
            | Fatal::ChecksumMismatch
            | Fatal::NoAddress
            | Fatal::RangeIgnored
            | Fatal::BadResponse
            | Fatal::ErrorDocument => None,
        }
    }
}
