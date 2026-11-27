// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The in-tree object server (`longshore::http::server`) as a program, for
//! measuring the client on one machine without a store in the way.
//!
//!   s3_serve [--port P] [--conns N] [--size BYTES] [--object BYTES] [--objects N]
//!
//! Serves plain HTTP on `0.0.0.0:P` (80 by default) from one thread on the
//! portable backend until it is killed. Objects live in memory: `--objects`
//! of them, each at most `--object` bytes; one request body or range at
//! most `--size` bytes, the size of each of the server's buffers. Every
//! stored body is checked against its CRC-64/NVME, which the server keeps
//! and returns, as Amazon S3 does when asked. It does not check signatures.
//!
//! Point a client at it with S3_ENDPOINT set to its host name and
//! S3_PATH_STYLE=1; any bucket name and keys do.

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("s3_serve needs Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    use longshore::http::server::{Server, ServerConfig};
    use longshore::io::posix::PosixIo;
    use longshore::io::{Completions, Io, Limits, Sock};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let mut port = 80u64;
    let mut conns = 16u64;
    let mut size = 8u64 << 20;
    let mut object = 64u64 << 20;
    let mut objects = 64u64;
    let mut args = std::env::args().skip(1);
    while let Some(k) = args.next() {
        let v: u64 = args
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| die(&format!("bad value for {k}")));
        match k.as_str() {
            "--port" => port = v,
            "--conns" => conns = v,
            "--size" => size = v,
            "--object" => object = v,
            "--objects" => objects = v,
            _ => die(&format!("unknown flag {k}")),
        }
    }
    let conns = conns as u32;
    let limits = Limits::BASE
        .with_sockets_max(1 + conns)
        .with_buffers(2 * conns)
        .with_buffer_size(size as u32)
        .with_ops_max(conns * 3 + 3);
    let config = ServerConfig::builder(Sock::new(0), conns, 1, 0, size as u32, 2 << 40)
        .head_max(8192)
        .objects_max(objects as u32)
        .object_size(object as u32)
        .checksum(|body: &[u8]| {
            longshore::checksum::crc64_header_value(longshore::checksum::crc64_nvme(body))
        })
        .build()
        .unwrap_or_else(|e| die(&e.to_string()));
    let mut io = PosixIo::new(limits).unwrap_or_else(|e| die(&e.to_string()));
    let mut server = Server::new(config);
    let at = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port as u16);
    let bound = server
        .start(&mut io, at)
        .unwrap_or_else(|e| die(&e.to_string()));
    eprintln!(
        "s3_serve: listening on {bound}, {conns} connections, buffers of {size} bytes, {objects} objects of at most {object} bytes"
    );
    let mut out = Completions::new(64);
    loop {
        if let Err(e) = io.tick(true, &mut out) {
            die(&e.to_string());
        }
        while let Some(c) = out.pop() {
            let _: bool = server.on_completion(&mut io, c);
        }
    }
}

/// Print `s3_serve: <msg>` and exit with status 2.
#[cfg(target_os = "linux")]
fn die(msg: &str) -> ! {
    eprintln!("s3_serve: {msg}");
    std::process::exit(2);
}
