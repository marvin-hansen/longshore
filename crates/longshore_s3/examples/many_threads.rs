// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! Callers on many threads sharing one client. A `Client` is a handle that
//! is cheap to clone and safe to share. Here eight plain threads call the
//! blocking forms through a shared reference, and four tasks on a tokio
//! runtime await the same calls through clones, all at once, served by two
//! I/O threads. Each caller stores and fetches its own object, round after
//! round, through one buffer it keeps.
//!
//! ```text
//! S3_ENDPOINT=... S3_REGION=... S3_BUCKET=... S3_ACCESS_KEY=... S3_SECRET_KEY=... \
//!     cargo run -p longshore_s3 --example many_threads
//! ```
//!
//! `examples/common/mod.rs` lists every variable read.

mod common;

use common::{why, Failure};
use longshore_s3::client::{Buffer, Client};
use std::time::Instant;

const PLAIN_THREADS: u8 = 8;
const TASKS: u8 = 4;
const ROUNDS: u8 = 16;
/// Bytes each round stores and fetches.
const SIZE: usize = 64 << 10;

fn main() -> Result<(), Failure> {
    let client = common::store()?
        .threads(2)
        // The limits are per I/O thread: 8 buffers and 8 connections on
        // each, so 16 buffers in all for 12 callers. A caller that finds
        // none free waits for one.
        .buffers(8)
        .buffer_size(256 << 10)
        .connections(8)
        .operations(8)
        .build()?;
    let prefix = common::prefix("many_threads");
    let keys: Vec<String> = (0..PLAIN_THREADS + TASKS)
        .map(|caller| format!("{prefix}caller-{caller}"))
        .collect();
    let started = Instant::now();
    let result = run(&client, &keys);
    if result.is_ok() {
        println!(
            "{PLAIN_THREADS} plain threads and {TASKS} tokio tasks on {} I/O threads: \
             {} round trips of {} KiB in {} ms",
            client.threads(),
            u32::from(PLAIN_THREADS + TASKS) * u32::from(ROUNDS),
            SIZE >> 10,
            started.elapsed().as_millis(),
        );
    }
    let erased = common::erase(&client, &keys);
    result.and(erased)
}

/// Every caller at once: plain threads and tokio tasks.
fn run(client: &Client, keys: &[String]) -> Result<(), Failure> {
    let (plain, tasks) = keys.split_at(usize::from(PLAIN_THREADS));
    std::thread::scope(|scope| {
        // Plain threads borrow the client.
        let threads: Vec<_> = plain
            .iter()
            .zip(0u8..)
            .map(|(key, id)| scope.spawn(move || blocking(client, key, id)))
            .collect();
        // Tasks own a clone. The calls are futures that need no runtime of
        // their own, so any executor runs them.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()?;
        let spawned: Vec<_> = tasks
            .iter()
            .zip(PLAIN_THREADS..)
            .map(|(key, id)| runtime.spawn(awaiting(client.clone(), key.clone(), id)))
            .collect();
        for thread in threads {
            thread.join().map_err(|_| "a caller panicked")??;
        }
        runtime.block_on(async {
            for task in spawned {
                task.await??;
            }
            Ok(())
        })
    })
}

/// A plain thread, with the blocking forms.
fn blocking(client: &Client, key: &str, id: u8) -> Result<(), String> {
    let mut body = client.buffer_blocking();
    for round in 0..ROUNDS {
        fill(&mut body, id, round);
        let (stored, mut back) = client.store(key, body, SIZE).wait();
        stored.map_err(why("store"))?;
        // Cleared, so that what the check sees is what the fetch wrote.
        back.as_mut_slice()[..SIZE].fill(0);
        let (fetched, back) = client.fetch(key, back).wait();
        check(&back, fetched.map_err(why("fetch"))?, id, round)?;
        body = back;
    }
    Ok(())
}

/// An async task: the same calls, awaited.
async fn awaiting(client: Client, key: String, id: u8) -> Result<(), String> {
    let mut body = client.buffer().await;
    for round in 0..ROUNDS {
        fill(&mut body, id, round);
        let (stored, mut back) = client.store(&key, body, SIZE).await;
        stored.map_err(why("store"))?;
        back.as_mut_slice()[..SIZE].fill(0);
        let (fetched, back) = client.fetch(&key, back).await;
        check(&back, fetched.map_err(why("fetch"))?, id, round)?;
        body = back;
    }
    Ok(())
}

/// Bytes that differ by caller and by round.
fn byte(at: usize, id: u8, round: u8) -> u8 {
    (at as u8).wrapping_mul(31) ^ id.wrapping_mul(7) ^ round
}

fn fill(body: &mut Buffer, id: u8, round: u8) {
    for (at, b) in body.as_mut_slice()[..SIZE].iter_mut().enumerate() {
        *b = byte(at, id, round);
    }
}

fn check(body: &Buffer, fetched: Option<usize>, id: u8, round: u8) -> Result<(), String> {
    let same = |(at, b): (usize, &u8)| *b == byte(at, id, round);
    match fetched {
        Some(SIZE) if body.as_slice()[..SIZE].iter().enumerate().all(same) => Ok(()),
        other => Err(format!(
            "caller {id} round {round}: fetched {other:?}, not what it stored"
        )),
    }
}
