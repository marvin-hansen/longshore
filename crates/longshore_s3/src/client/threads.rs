// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The I/O threads behind a [`Client`]: building them, and stopping them
//! when the last handle, buffer and operation are gone.

use super::lane::Lane;
use super::{Client, TlsProvider};
use crate::creds::Credentials;
use crate::s3::{KeyPrefix, Validate};
use longshore_dns::Family;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;

/// Why a client could not be built.
#[derive(Debug)]
pub enum BuildError {
    /// This platform has no supported I/O backend (Linux and macOS are supported).
    Unsupported,
    /// A setting is out of range. The text says which.
    Setting(&'static str),
    /// No nameserver was given and none was found in `/etc/resolv.conf`.
    NoNameserver,
    /// An I/O thread could not set up its backend.
    Io(std::io::Error),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Unsupported => write!(f, "this client runs on Linux and macOS only"),
            BuildError::Setting(what) => write!(f, "{what}"),
            BuildError::NoNameserver => write!(f, "no nameserver configured or found"),
            BuildError::Io(e) => write!(f, "I/O thread set-up failed: {e}"),
        }
    }
}

impl std::error::Error for BuildError {}

/// Settings for a [`Client`]. Start from [`Builder::new`], change what
/// differs with the setters, which are named after the settings, then call
/// [`Builder::build`]:
///
/// ```no_run
/// # use longshore_s3::client::Builder;
/// # use longshore_s3::creds::Credentials;
/// # let credentials = Credentials::new("K", "S").unwrap();
/// let client = Builder::new("my-bucket.s3.us-east-1.amazonaws.com", "us-east-1", credentials)
///     .tls(true)
///     .port(443)
///     .threads(4)
///     .build();
/// ```
///
/// The setters only record a value; [`Builder::build`] checks them all and
/// returns [`BuildError::Setting`] for one out of range.
#[derive(Clone)]
pub struct Builder {
    host: String,
    bucket_in_path: Option<String>,
    port: u16,
    region: String,
    credentials: Credentials,
    credentials_file: Option<std::path::PathBuf>,
    credentials_reread_secs: u64,
    credentials_validate: Validate,
    checksums: bool,
    tls: bool,
    tls_provider: Option<TlsProvider>,
    threads: u32,
    pin: Option<(usize, usize)>,
    portable: bool,
    connections: u32,
    buffers: u32,
    buffer_size: u32,
    operations: u32,
    waiters: u32,
    /// `None` until set: then one fewer than `operations`, at most 4.
    streams: Option<u32>,
    part_min: u64,
    family: Family,
    prefer_large_segments: Option<usize>,
    nameservers: Vec<SocketAddr>,
    address: Option<SocketAddr>,
}

impl Builder {
    /// One I/O thread, 64 connections, 64 buffers of 8 MiB, plain HTTP on
    /// port 80, IPv4.
    pub fn new(host: &str, region: &str, credentials: Credentials) -> Builder {
        Builder {
            host: host.into(),
            port: 80,
            region: region.into(),
            credentials,
            credentials_file: None,
            credentials_reread_secs: 60,
            credentials_validate: Validate::List(KeyPrefix::EMPTY),
            checksums: true,
            bucket_in_path: None,
            tls: false,
            tls_provider: None,
            threads: 1,
            pin: None,
            portable: false,
            connections: 64,
            buffers: 64,
            buffer_size: 8 << 20,
            operations: 64,
            waiters: 256,
            streams: None,
            part_min: 5 << 20,
            family: Family::V4,
            prefer_large_segments: None,
            nameservers: Vec::new(),
            address: None,
        }
    }

    /// Upload streams the client opens: as set, or one fewer than
    /// `operations`, at most 4.
    pub(super) fn streams_or_default(&self) -> u32 {
        self.streams
            .unwrap_or(4.min(self.operations.saturating_sub(1)))
    }

    fn check(&self) -> Result<(), BuildError> {
        let bad = |what| Err(BuildError::Setting(what));
        if self.tls {
            match self.tls_provider.or(TlsProvider::preferred()) {
                None => return bad(
                    "TLS requires the `tls` feature or a provider: `aws-lc`, `ring` or `rustls`",
                ),
                Some(p) if !p.compiled() => return bad(p.missing()),
                Some(_) => {}
            }
        }
        #[cfg(target_os = "macos")]
        if self.pin.is_some() {
            return bad("pinning threads to cores is not supported on macOS");
        }
        if self.threads == 0 || self.threads > 1024 {
            return bad("threads must be 1 to 1024");
        }
        if self.connections == 0 || self.buffers == 0 || self.operations == 0 || self.waiters == 0 {
            return bad("connections, buffers, operations and waiters must be at least 1");
        }
        if self.streams.is_some_and(|s| s > 1024) {
            return bad("streams must be 0 to 1024");
        }
        // A begun stream keeps one slot for its abort on the thread its
        // list of parts came from, and every stream may be on one thread.
        // With as many streams as slots, their requests would have none
        // left there, and every stream would wait for ever.
        if self.streams.is_some_and(|s| s > 0 && s >= self.operations) {
            return bad("streams must be below operations, or 0: each begun upload stream keeps an operation slot for its abort, and all of them may be on one thread");
        }
        if self.buffer_size < 8192 {
            return bad("buffer_size must be at least 8192");
        }
        if self.host.is_empty() || self.host.len() > 253 {
            return bad("host must be 1 to 253 bytes");
        }
        if self
            .credentials_file
            .as_ref()
            .is_some_and(|p| !p.is_absolute() || p.to_str().is_none())
        {
            return bad("credentials_file must be an absolute path in UTF-8");
        }
        if let Some(bucket) = &self.bucket_in_path {
            let valid = (3..=63).contains(&bucket.len())
                && bucket.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.'
                });
            if !valid {
                return bad("bucket_in_path is not a bucket name");
            }
        }
        if self.region.len() > 64 {
            return bad("region must be at most 64 bytes");
        }
        if self.nameservers.len() > longshore_dns::resolver::SERVERS_MAX {
            return bad("at most two nameservers");
        }
        let mut query = [0u8; 512];
        if self.address.is_none()
            && longshore_dns::codec::encode_query(&self.host, 0, &mut query).is_err()
        {
            return bad("host is not a name DNS can look up");
        }
        Ok(())
    }

    /// Start the I/O threads and return a handle to them. Everything the
    /// client will use is allocated here.
    pub fn build(mut self) -> Result<Client, BuildError> {
        self.check()?;
        if self.address.is_none() && self.nameservers.is_empty() {
            let conf = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
            self.nameservers = longshore_dns::codec::parse_resolv_conf(&conf)
                .into_iter()
                .take(2)
                .map(|ip| SocketAddr::new(ip, 53))
                .collect();
            if self.nameservers.is_empty() {
                return Err(BuildError::NoNameserver);
            }
        }
        self.start()
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn start(self) -> Result<Client, BuildError> {
        Err(BuildError::Unsupported)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn start(self) -> Result<Client, BuildError> {
        use longshore_common::io::posix::PosixIo;
        #[cfg(target_os = "linux")]
        use longshore_common::io::uring::UringIo;

        let (ready, started) = std::sync::mpsc::channel();
        let mut joins = Vec::new();
        for n in 0..self.threads {
            let (settings, report) = (self.clone(), ready.clone());
            let spawned = std::thread::Builder::new()
                .name(format!("longshore-io-{n}"))
                .spawn(move || {
                    #[cfg(target_os = "linux")]
                    if settings.portable {
                        run::run::<PosixIo>(&settings, n, report);
                    } else {
                        run::run::<UringIo>(&settings, n, report);
                    }
                    #[cfg(target_os = "macos")]
                    run::run::<PosixIo>(&settings, n, report);
                });
            match spawned {
                Ok(j) => joins.push(j),
                Err(e) => {
                    // The threads already started stop when their lanes do.
                    drop(ready);
                    let lanes: Vec<Arc<Lane>> = started.iter().flatten().collect();
                    drop(Threads::new(lanes, joins));
                    return Err(BuildError::Io(e));
                }
            }
        }
        drop(ready);
        let mut lanes = Vec::new();
        let mut failed = None;
        for reply in started.iter().take(self.threads as usize) {
            match reply {
                Ok(lane) => lanes.push(lane),
                Err(e) => failed = Some(e),
            }
        }
        if failed.is_none() && lanes.len() != self.threads as usize {
            failed = Some(std::io::Error::other(
                "an I/O thread stopped while starting",
            ));
        }
        let threads = Threads::new(lanes.clone(), joins);
        match failed {
            Some(e) => Err(BuildError::Io(e)),
            None => Ok(Client::assemble(
                lanes,
                Some(Arc::new(threads)),
                if self.checksums {
                    crate::s3::Checksum::Compute
                } else {
                    crate::s3::Checksum::None
                },
            )),
        }
    }
}

/// Owns the I/O threads. Dropped when the last client handle, buffer and
/// operation are gone, since each of those keeps it alive.
pub(super) struct Threads {
    lanes: Vec<Arc<Lane>>,
    joins: Vec<JoinHandle<()>>,
}

impl Threads {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn new(lanes: Vec<Arc<Lane>>, joins: Vec<JoinHandle<()>>) -> Threads {
        Threads { lanes, joins }
    }
}

impl Drop for Threads {
    fn drop(&mut self) {
        // An upload stream dropped with parts in flight owes an abort,
        // which the I/O thread that ends its last part sends through a
        // lane's queue, and a stop closes the queues. So the aborts are let
        // arrive first. Parts end, by retries and time limits if not
        // otherwise, so this ends; a thread that has died stops the wait.
        while self
            .lanes
            .iter()
            .any(|l| l.owed.load(std::sync::atomic::Ordering::Acquire) > 0)
            && !self.joins.iter().any(JoinHandle::is_finished)
        {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        for lane in &self.lanes {
            lane.sender.stop();
        }
        for j in self.joins.drain(..) {
            // A panicked I/O thread has already reported it; Drop must not panic again.
            let _: Option<()> = j.join().ok();
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod run;
mod setters;
