// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! What every example shares: the store the environment names, a prefix of
//! its own for the keys it stores, and a client error as a message.
//!
//! The store is read from the variables the bench programs use:
//!
//! - `S3_ENDPOINT`: the store's host name, without the bucket.
//! - `S3_REGION` and `S3_BUCKET`.
//! - `S3_ACCESS_KEY` and `S3_SECRET_KEY`, and `S3_SESSION_TOKEN` for
//!   temporary keys.
//! - `S3_TLS=1` for HTTPS on port 443. Plain HTTP on port 80 otherwise.
//! - `S3_PATH_STYLE=1` for the bucket in the path instead of the host name,
//!   as self-hosted stores commonly expect.

use longshore_s3::client::{Builder, Client, Error};
use longshore_s3::creds::Credentials;

/// Why an example failed. Returned from `main`, it ends the program with a
/// non-zero status.
pub type Failure = Box<dyn std::error::Error>;

/// The settings for the store the environment names. Each example adds the
/// sizes it needs and builds the client.
pub fn store() -> Result<Builder, Failure> {
    let var = |name: &str| std::env::var(name).map_err(|_| format!("set {name}"));
    let on = |name: &str| std::env::var(name).is_ok_and(|v| v == "1");
    let credentials = match std::env::var("S3_SESSION_TOKEN") {
        Ok(token) if !token.is_empty() => {
            Credentials::with_session(&var("S3_ACCESS_KEY")?, &var("S3_SECRET_KEY")?, &token)
        }
        _ => Credentials::new(&var("S3_ACCESS_KEY")?, &var("S3_SECRET_KEY")?),
    }
    .ok_or("the credentials are empty, too long or not printable ASCII")?;
    let (endpoint, bucket) = (var("S3_ENDPOINT")?, var("S3_BUCKET")?);
    let path_style = on("S3_PATH_STYLE");
    let host = if path_style {
        endpoint
    } else {
        format!("{bucket}.{endpoint}")
    };
    let tls = on("S3_TLS");
    Ok(Builder::new(&host, &var("S3_REGION")?, credentials)
        .bucket_in_path(path_style.then_some(bucket))
        .tls(tls)
        .port(if tls { 443 } else { 80 }))
}

/// A prefix no other run uses, so that an example finds only its own keys
/// and can erase all of them.
pub fn prefix(example: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("longshore-examples/{example}/{now}/")
}

/// Erase every key, even after one fails, and say how many were erased.
/// Erasing a key that is not there succeeds, so this cleans up after a run
/// that failed before it stored them all, too.
pub fn erase(client: &Client, keys: &[impl AsRef<str>]) -> Result<(), Failure> {
    let mut first = Ok(());
    for key in keys {
        let erased = client.erase(key.as_ref()).wait().map_err(why("erase"));
        first = first.and(erased);
    }
    if first.is_ok() {
        let s = if keys.len() == 1 { "" } else { "s" };
        println!("cleaned up: erased {} key{s}", keys.len());
    }
    Ok(first?)
}

/// Turn a client [`Error`] into a message that says which step failed,
/// followed by each cause in turn. The message is a `String`, so it can
/// cross threads.
pub fn why(step: &'static str) -> impl Fn(Error) -> String {
    move |e| {
        let mut message = format!("{step}: {e}");
        let mut cause = std::error::Error::source(&e);
        while let Some(c) = cause {
            message.push_str(&format!(": {c}"));
            cause = c.source();
        }
        message
    }
}
