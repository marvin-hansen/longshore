// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The limits on an upload stream's parts.

use super::super::Error;
use super::flow::{entry_len, room, ENTRY_MAX, FRAME, RECORD};
use crate::multipart::{Part, PartTag, PART_NUMBER_MAX};

/// A part as Amazon S3 tags it: an MD5 in hex, in quotes, with a checksum.
fn s3_part(number: u32) -> Part {
    let tag = PartTag::new(b"\"0123456789abcdef0123456789abcdef\"").unwrap();
    Part::new(number, tag, Some(*b"AAAAAAAAAAA="))
}

#[test]
fn a_five_mib_buffer_lists_ten_thousand_parts_and_refuses_one_more() {
    let size = 5 << 20;
    let entry = entry_len(s3_part(PART_NUMBER_MAX));
    let ended = |n: u32| (n as usize) * entry;
    // The last part, with three more still in flight.
    assert_eq!(room(PART_NUMBER_MAX, size, ended(9_996), 3), Ok(()));
    assert_eq!(
        room(PART_NUMBER_MAX + 1, size, ended(PART_NUMBER_MAX), 0),
        Err(Error::BadPart)
    );
}

#[test]
fn a_small_buffer_refuses_the_part_whose_record_would_not_fit() {
    let size = 16 * 1024;
    let entry = entry_len(s3_part(1));
    let fits = (1..=PART_NUMBER_MAX)
        .take_while(|&n: &u32| room(n, size, (n as usize - 1) * entry, 0).is_ok())
        .count() as u32;
    assert!(fits > 10, "{fits}");
    assert_eq!(
        room(fits + 1, size, fits as usize * entry, 0),
        Err(Error::TooLong)
    );
}

#[test]
fn an_entry_is_as_long_as_the_document_writes_it_and_never_longer_than_the_bound() {
    let entry = entry_len(s3_part(10_000));
    assert_eq!(
        entry,
        "<Part><PartNumber>10000</PartNumber><ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag><ChecksumCRC64NVME>AAAAAAAAAAA=</ChecksumCRC64NVME></Part>".len()
    );
    let quotes = PartTag::new(&[b'"'; 64]).unwrap();
    assert_eq!(
        entry_len(Part::new(10_000, quotes, Some([b'A'; 12]))),
        ENTRY_MAX
    );
    assert_eq!(FRAME, 51);
    const { assert!(RECORD <= 96) };
}
