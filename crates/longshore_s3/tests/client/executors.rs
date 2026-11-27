// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The same suite of client operations under two async executors, tokio
//! (its current-thread and its multi-thread runtime) and futures-executor,
//! and from plain threads with the blocking forms.
//!
//! A real client on the portable backend over loopback, against the
//! in-tree server: the operations end on the client's I/O threads, which
//! wake each executor's own wakers from there. Several suites run at once
//! on each executor, so a task's wake-up has to reach the right task.

#![cfg(any(target_os = "linux", target_os = "macos"))]

#[cfg(test)]
mod loopback;
#[cfg(test)]
mod suite;

use longshore_common::io::{Limits, Sock};
use longshore_http_client::server;
use longshore_s3::client::Client;
use loopback::ServerThread;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

/// Buffer size, I/O threads, connections per thread, and buffers per thread.
const SIZE: u32 = 64 * 1024;
const THREADS: u32 = 2;
const CONNS: u32 = 2;
const BUFFERS: u32 = 4;
/// Suites run at once on each executor: one buffer each, and fewer than
/// the client lends.
const SUITES: usize = 6;

/// A server, and a client of two I/O threads pointed at it.
#[cfg(test)]
fn client() -> (ServerThread, Client) {
    let conns = THREADS * CONNS;
    let server = ServerThread::start(
        Limits::BASE
            .with_sockets_max(1 + conns)
            .with_buffers(2 * conns)
            .with_buffer_size(SIZE)
            .with_ops_max(conns * 3 + 3),
        server::ServerConfig::builder(Sock::new(0), conns, 1, 0, SIZE, 2 << 40)
            .head_max(4096)
            .objects_max(8 * SUITES as u32)
            .checksum(|body: &[u8]| {
                longshore_crc::crc64_header_value(longshore_crc::crc64_nvme(body))
            })
            .build()
            .unwrap(),
    );
    let client = server
        .builder(CONNS, SIZE)
        .threads(THREADS)
        .buffers(BUFFERS)
        .operations(8)
        .build()
        .expect("client build on portable backend");
    (server, client)
}

/// [`SUITES`] async suites, each under its own prefix.
#[cfg(test)]
fn suites(client: &Client, executor: &str) -> Vec<Pin<Box<dyn Future<Output = ()> + Send>>> {
    (0..SUITES)
        .map(|n| {
            let run = suite::run_async(client.clone(), format!("{executor}/{n}"), n);
            Box::pin(run) as Pin<Box<dyn Future<Output = ()> + Send>>
        })
        .collect()
}

/// All of `futures`, polled together in one task until each has ended.
#[cfg(test)]
async fn join(mut futures: Vec<Pin<Box<dyn Future<Output = ()> + Send>>>) {
    core::future::poll_fn(|cx| {
        futures.retain_mut(|f| f.as_mut().poll(cx).is_pending());
        if futures.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

#[test]
fn every_operation_completes_on_tokio_current_thread() {
    let (server, client) = client();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // Tasks spawned onto the one thread, and suites joined in one task.
    rt.block_on(async {
        let tasks: Vec<_> = suites(&client, "tokio-ct-task")
            .into_iter()
            .map(tokio::spawn)
            .collect();
        join(suites(&client, "tokio-ct-joined")).await;
        for t in tasks {
            t.await.unwrap();
        }
    });
    drop(rt);
    drop(client);
    server.stop();
}

#[test]
fn every_operation_completes_on_tokio_multi_thread() {
    let (server, client) = client();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .build()
        .unwrap();
    for round in 0..2 {
        let tasks: Vec<_> = suites(&client, &format!("tokio-mt-{round}"))
            .into_iter()
            .map(|s| rt.spawn(s))
            .collect();
        rt.block_on(async {
            for t in tasks {
                t.await.unwrap();
            }
        });
    }
    drop(rt);
    drop(client);
    server.stop();
}

#[test]
fn every_operation_completes_on_futures_executor() {
    let (server, client) = client();
    // Several suites joined in one task on this thread.
    futures_executor::block_on(join(suites(&client, "futures-joined")));
    // And one suite on each of several threads, each with its own executor.
    let threads: Vec<_> = (0..SUITES)
        .map(|n| {
            let client = client.clone();
            std::thread::spawn(move || {
                futures_executor::block_on(suite::run_async(client, format!("futures/{n}"), n));
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    drop(client);
    server.stop();
}

#[test]
fn every_operation_completes_from_plain_threads_with_the_blocking_forms() {
    let (server, client) = client();
    let threads: Vec<_> = (0..SUITES)
        .map(|n| {
            let client = client.clone();
            std::thread::spawn(move || suite::run_blocking(client, format!("plain/{n}"), n))
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    drop(client);
    server.stop();
}
