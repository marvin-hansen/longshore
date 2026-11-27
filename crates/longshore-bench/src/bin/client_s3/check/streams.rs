// Copyright 2026 Carver Automation Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// SPDX-License-Identifier: Apache-2.0

//! The upload stream steps of `client_s3 check`.

use super::{pattern, Check};

impl Check<'_> {
    /// Upload streams: short, long in pieces, a long slice, and dropped.
    pub(super) fn streams(&mut self, big: usize, size: usize) {
        let short = self.key("stream-short");
        let r1 = (|| {
            let mut up = self
                .client
                .upload(&short)
                .wait()
                .map_err(|e| format!("open: {e:?}"))?;
            up.write(&pattern(3, 1000))
                .wait()
                .map_err(|e| format!("write: {e:?}"))?;
            up.finish().wait().map_err(|e| format!("finish: {e:?}"))?;
            let tag = self.tag(&short)?;
            if tag.contains('-') {
                Err(format!("tag {tag}: a multipart upload"))
            } else {
                Ok(format!("1000 bytes, tag {tag}: one plain store"))
            }
        })();
        self.report("upload a short stream", r1);

        let long = self.key("stream-long");
        let bytes = pattern(4, big);
        let r2 = (|| {
            let mut up = self
                .client
                .upload(&long)
                .wait()
                .map_err(|e| format!("open: {e:?}"))?;
            // Pieces that do not line up with the buffers.
            for piece in bytes.chunks(size / 3 + 17) {
                up.write(piece)
                    .wait()
                    .map_err(|e| format!("write: {e:?}"))?;
            }
            up.finish().wait().map_err(|e| format!("finish: {e:?}"))?;
            let tag = self.tag(&long)?;
            self.same(&long, &bytes)?;
            Ok(format!(
                "{big} bytes in pieces of {}, tag {tag}, read back equal",
                size / 3 + 17
            ))
        })();
        self.report("upload a long stream written in pieces", r2);

        let slice = self.key("store-from-long");
        let r3 = (|| {
            self.client
                .store_from_blocking(&slice, &bytes)
                .map_err(|e| format!("store_from: {e:?}"))?;
            let tag = self.tag(&slice)?;
            self.same(&slice, &bytes)?;
            Ok(format!("{big} bytes, tag {tag}, read back equal"))
        })();
        self.report("store a long slice", r3);

        let dropped = self.key("stream-dropped");
        let r4 = (|| {
            let mut up = self
                .client
                .upload(&dropped)
                .wait()
                .map_err(|e| format!("open: {e:?}"))?;
            up.write(&bytes[..2 * size + size / 2])
                .wait()
                .map_err(|e| format!("write: {e:?}"))?;
            drop(up);
            match self.client.stat(&dropped).wait() {
                Ok(None) => {
                    Ok("no object; the upload was aborted (not visible through the client)".into())
                }
                other => Err(format!("stat after the drop: {other:?}")),
            }
        })();
        self.report("drop a stream half way", r4);
    }

    /// The tag the store gives the object under `key`.
    fn tag(&self, key: &str) -> Result<String, String> {
        match self.client.stat(key).wait() {
            Ok(Some(s)) => Ok(s
                .tag()
                .map(|t| String::from_utf8_lossy(t.as_bytes()).into_owned())
                .unwrap_or_default()),
            other => Err(format!("stat: {other:?}")),
        }
    }

    /// Whether the object under `key` holds exactly `bytes`.
    fn same(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let mut out = vec![0u8; bytes.len() + 1];
        match self.client.fetch_into_blocking(key, &mut out) {
            Ok(Some(n)) if n == bytes.len() && out[..n] == *bytes => Ok(()),
            Ok(Some(n)) => Err(format!("{n} bytes back, not equal")),
            other => Err(format!("fetch_into: {other:?}")),
        }
    }
}
