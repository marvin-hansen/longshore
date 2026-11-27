// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! `client_s3 download`: throughput of the download stream. Each caller
//! stores one object of `--parts` buffers of `--size` bytes, then
//! downloads it again and again for the measured window: ranges of one
//! buffer, fetched ahead of the reader, handed out in order, and the whole
//! object checked against the stored checksum at the end. Compare with
//! `client_s3 get`, which fetches whole objects of one buffer.

use crate::report::process_cpu_secs;
use crate::Args;
use longshore::client::{Client, Piece};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Counts {
    measuring: AtomicBool,
    stop: AtomicBool,
    bytes: AtomicU64,
    downloads: AtomicU64,
    unverified: AtomicU64,
    failed: AtomicU64,
}

pub fn run(client: &Client, a: &Args) {
    let object = a.parts as usize * a.size as usize;
    let pid = std::process::id();
    let keys: Vec<String> = (0..a.callers)
        .map(|c| format!("bench/{pid}-download-{c}.bin"))
        .collect();
    for (c, key) in keys.iter().enumerate() {
        let body: Vec<u8> = (0..object).map(|i| (i as u32 ^ c as u32) as u8).collect();
        if let Err(e) = client.store_from_blocking(key, &body) {
            eprintln!("client_s3: store {key}: {e}");
            std::process::exit(3);
        }
    }
    let counts = Arc::new(Counts::default());
    let ready = Arc::new(Barrier::new(keys.len() + 1));
    let callers: Vec<_> = keys
        .iter()
        .map(|key| {
            let (client, key, counts, ready) =
                (client.clone(), key.clone(), counts.clone(), ready.clone());
            std::thread::spawn(move || {
                let _: std::sync::BarrierWaitResult = ready.wait();
                while !counts.stop.load(Ordering::Relaxed) {
                    download(&client, &key, object, &counts);
                }
            })
        })
        .collect();
    let _: std::sync::BarrierWaitResult = ready.wait();
    std::thread::sleep(Duration::from_secs(2));
    let cpu0 = process_cpu_secs();
    counts.measuring.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs(a.secs));
    counts.measuring.store(false, Ordering::Relaxed);
    let wall = t0.elapsed().as_secs_f64();
    let cpu_s = process_cpu_secs() - cpu0;
    counts.stop.store(true, Ordering::Relaxed);
    for c in callers {
        if c.join().is_err() {
            eprintln!("client_s3: a caller panicked");
        }
    }
    for key in &keys {
        if let Err(e) = client.erase(key).wait() {
            eprintln!("client_s3: erase {key}: {e}");
        }
    }
    let gb = counts.bytes.load(Ordering::Relaxed) as f64 / 1e9;
    println!(
        "{{\"cmd\":\"client-download\",\"arch\":\"{}\",\"threads\":{},\"callers\":{},\"conns\":{},\"size\":{},\"object\":{object},\"secs\":{wall:.2},\"downloads\":{},\"unverified\":{},\"failed\":{},\"gb_per_s\":{:.3},\"process_cpu_cores\":{:.2},\"gb_per_cpu_s\":{:.2}}}",
        std::env::consts::ARCH,
        a.threads,
        a.callers,
        a.conns,
        a.size,
        counts.downloads.load(Ordering::Relaxed),
        counts.unverified.load(Ordering::Relaxed),
        counts.failed.load(Ordering::Relaxed),
        gb / wall,
        cpu_s / wall,
        gb / cpu_s.max(1e-9),
    );
}

/// One whole download of `key`, counting its bytes while the window is
/// being measured.
fn download(client: &Client, key: &str, object: usize, counts: &Counts) {
    let measured = |n: usize| {
        if counts.measuring.load(Ordering::Relaxed) {
            let _: u64 = counts.bytes.fetch_add(n as u64, Ordering::Relaxed);
        }
    };
    let Ok(Some(mut d)) = client.download(key).wait() else {
        let _: u64 = counts.failed.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut got = 0;
    loop {
        match d.read_blocking() {
            Ok(Piece::Bytes(_, n)) => {
                got += n;
                measured(n);
            }
            Ok(Piece::End { verified }) => {
                if got != object {
                    let _: u64 = counts.failed.fetch_add(1, Ordering::Relaxed);
                } else if !verified {
                    let _: u64 = counts.unverified.fetch_add(1, Ordering::Relaxed);
                }
                let _: u64 = counts.downloads.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(_) => {
                let _: u64 = counts.failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }
}
