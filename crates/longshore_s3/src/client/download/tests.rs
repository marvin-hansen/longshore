// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

use super::check::{range_of, Running};
use super::Error;
use crate::report::{Answer, Cause, ContentRange, ETag, End, Report};
use crate::s3::Fatal;

const BODY: &[u8] = b"the object's bytes, read in three pieces";

fn crc64_form(body: &[u8]) -> [u8; 12] {
    longshore_crc::crc64_header_value(longshore_crc::crc64_nvme(body))
}

fn crc32c_form(body: &[u8]) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..8].copy_from_slice(&longshore_crc::crc32c_header_value(longshore_crc::crc32c(
        body,
    )));
    out
}

/// Hand `body` to `running` in three pieces, as a download would.
fn run(mut running: Running, body: &[u8]) -> Result<bool, Error> {
    for piece in body.chunks(body.len().div_ceil(3).max(1)) {
        running.update(piece);
    }
    running.end()
}

#[test]
fn the_checksum_form_picks_the_algorithm_and_pieces_add_up_to_the_whole() {
    assert_eq!(run(Running::new(Some(crc64_form(BODY))), BODY), Ok(true));
    assert_eq!(run(Running::new(Some(crc32c_form(BODY))), BODY), Ok(true));
    assert_eq!(run(Running::new(Some(crc64_form(b""))), b""), Ok(true));
    assert_eq!(run(Running::new(Some(crc32c_form(b""))), b""), Ok(true));
}

#[test]
fn a_flipped_bit_fails_either_checksum() {
    let mut damaged = BODY.to_vec();
    damaged[17] ^= 1;
    let mismatch = Err(Error::Store(Fatal::ChecksumMismatch));
    assert_eq!(
        run(Running::new(Some(crc64_form(BODY))), &damaged),
        mismatch
    );
    assert_eq!(
        run(Running::new(Some(crc32c_form(BODY))), &damaged),
        mismatch
    );
}

#[test]
fn no_stored_checksum_ends_unverified() {
    assert_eq!(run(Running::new(None), BODY), Ok(false));
}

fn tag(text: &str) -> ETag {
    ETag::new(text.as_bytes()).unwrap()
}

fn answered(status: u16, etag: Option<&str>, range: Option<(u64, u64)>, body_len: u32) -> Report {
    Report {
        end: End::Answered(Answer {
            status,
            etag: etag.map(tag),
            version: None,
            request_id: None,
            content_length: Some(u64::from(body_len)),
            content_range: range.map(|(first, last)| ContentRange {
                first,
                last,
                size: Some(100),
            }),
            encryption: None,
            sha256: None,
            crc64: None,
            body_len,
            verified: None,
            malformed: false,
        }),
        request: None,
        sent: 1,
        unanswered: 0,
    }
}

#[test]
fn a_range_answered_as_asked_gives_its_length() {
    let ours = tag("\"a\"");
    let named = answered(206, Some("\"a\""), Some((10, 19)), 10);
    assert_eq!(range_of(&named, 10, 19, Some(&ours)), Ok(10));
    // A store that names no tag is taken at its word on `If-Match`.
    let unnamed = answered(206, None, Some((10, 19)), 10);
    assert_eq!(range_of(&unnamed, 10, 19, Some(&ours)), Ok(10));
}

#[test]
fn answers_other_than_the_range_asked_for_fail() {
    let ours = tag("\"a\"");
    let cases = [
        (
            answered(200, Some("\"a\""), None, 100),
            Error::Store(Fatal::RangeIgnored),
        ),
        (
            answered(412, None, None, 0),
            Error::Store(Fatal::Status(412)),
        ),
        (
            answered(404, None, None, 0),
            Error::Store(Fatal::Status(404)),
        ),
        // Another object, from a store that ignored `If-Match`.
        (
            answered(206, Some("\"b\""), Some((10, 19)), 10),
            Error::Store(Fatal::Status(412)),
        ),
        // Less than asked for, or no range named.
        (
            answered(206, Some("\"a\""), Some((10, 14)), 5),
            Error::Store(Fatal::BadResponse),
        ),
        (
            answered(206, Some("\"a\""), None, 10),
            Error::Store(Fatal::BadResponse),
        ),
    ];
    for (report, error) in cases {
        assert_eq!(range_of(&report, 10, 19, Some(&ours)), Err(error));
    }
}

#[test]
fn a_range_without_an_answer_fails_as_the_cause_says() {
    let report = |end| Report {
        end,
        request: None,
        sent: 0,
        unanswered: 0,
    };
    assert_eq!(
        range_of(&report(End::NotSent(Cause::Closed)), 0, 9, None),
        Err(Error::Closed)
    );
    assert_eq!(
        range_of(&report(End::Unknown(Cause::NoAddress)), 0, 9, None),
        Err(Error::Store(Fatal::NoAddress))
    );
}
