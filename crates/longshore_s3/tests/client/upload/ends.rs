// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Upload streams that end without completing: dropped, or refused at
//! completion. Their uploads are aborted, and only once every part in
//! flight has ended.

use super::common::{Behavior, Server, BUF};
use super::rig::*;
use longshore_s3::client::Error;

const SIZE: usize = BUF as usize;

#[test]
fn a_dropped_stream_aborts_its_upload_after_its_parts_end() {
    // Room for the records, three parts and the buffer being filled.
    let mut r = rig(Shape {
        buffers: 6,
        ..Shape::new()
    });
    let client = r.client.clone();
    let data = pattern(3 * SIZE + 10, 3);
    let mut up = r.run(client.upload("gone")).unwrap();
    // The lanes are driven only while the upload begins, so the three
    // parts are sent to the I/O thread and have not yet run.
    r.run(up.write(&data)).unwrap();
    drop(up);
    let first = part_target("gone", 1, 1);
    fn server(r: &Rig) -> &Server {
        &r.lanes[0].server
    }
    assert_eq!(server(&r).uploads_in_progress(), 1);
    assert_eq!(server(&r).requests(), 1);

    let mut stored_while_open = 0;
    for _ in 0..2000 {
        r.pump();
        if server(&r).uploads_in_progress() == 1 {
            stored_while_open = stored_while_open.max(
                (1..=3)
                    .filter(|n| server(&r).object(&part_target("gone", *n, 1)).is_some())
                    .count(),
            );
        }
    }
    // Every part was stored before the abort took the upload away.
    assert_eq!(stored_while_open, 3);
    assert_eq!(server(&r).uploads_in_progress(), 0);
    assert_eq!(server(&r).object(&first), None);
    assert_eq!(server(&r).object("/gone"), None);
    // The begin, three parts, and the abort.
    assert_eq!(server(&r).requests(), 5);
}

#[test]
fn the_abort_waits_for_a_part_on_another_lane() {
    // Two buffers a lane: the first lane lends the part records' buffer and
    // the first part's, so the second part goes out on the second lane.
    let mut r = rig(Shape {
        lanes: 2,
        buffers: 2,
        ..Shape::new()
    });
    let client = r.client.clone();
    let data = pattern(2 * SIZE + 1, 4);
    let mut up = r.run(client.upload("two")).unwrap();
    r.run(up.write(&data[..SIZE])).unwrap();
    // Beginning runs on the first lane alone; then part 1 is sent there.
    r.run_on(0, up.write(&data[SIZE..SIZE + 1])).unwrap();
    // Part 2 is sent to the second lane, which has not been driven.
    r.run_on(0, up.write(&data[SIZE + 1..])).unwrap();
    drop(up);

    // Only the first lane runs: its part ends, but the abort waits for
    // the part still in flight on the second lane.
    for _ in 0..2000 {
        r.pump_lane(0);
    }
    let first = part_target("two", 1, 1);
    assert!(r.lanes[0].server.object(&first).is_some());
    assert_eq!(r.lanes[0].server.uploads_in_progress(), 1);
    assert_eq!(r.lanes[0].server.requests(), 2);
    assert_eq!(r.lanes[1].server.requests(), 0);

    // The second lane's part ends. Each lane is its own simulated machine
    // with its own server, which does not know the upload, so the part
    // fails there; what matters is that it ends, and that ending it sends
    // the abort to the first lane.
    for _ in 0..2000 {
        r.pump_lane(1);
    }
    assert_eq!(r.lanes[1].server.requests(), 1);
    assert_eq!(r.lanes[0].server.uploads_in_progress(), 1);
    for _ in 0..2000 {
        r.pump_lane(0);
    }
    assert_eq!(r.lanes[0].server.uploads_in_progress(), 0);
    assert_eq!(r.lanes[0].server.object(&first), None);
    assert_eq!(r.lanes[0].server.requests(), 3);
}

#[test]
fn a_refused_completion_aborts_the_upload() {
    let mut r = rig(Shape::new());
    let client = r.client.clone();
    let data = pattern(2 * SIZE + 5, 5);
    let mut up = r.run(client.upload("refused")).unwrap();
    r.run(up.write(&data)).unwrap();
    // Let the begin and both full parts reach the server.
    while r.lanes[0].server.requests() < 3 {
        r.pump();
    }
    // The last part is stored; the completion is refused.
    r.lanes[0].server.script(Behavior::Normal);
    r.lanes[0]
        .server
        .script(Behavior::ErrorDocument(400, "InvalidPart"));
    let finished = r.run(up.finish());
    assert!(matches!(finished, Err(Error::Store(_))), "{finished:?}");
    for _ in 0..2000 {
        r.pump();
    }
    let server = &r.lanes[0].server;
    assert_eq!(server.uploads_in_progress(), 0);
    assert_eq!(server.object(&part_target("refused", 1, 1)), None);
    assert_eq!(server.object("/refused"), None);
    // The begin, three parts, the completion and the abort.
    assert_eq!(server.requests(), 6);
}
