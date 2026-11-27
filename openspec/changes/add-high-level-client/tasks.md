## 1. Core
- [x] 1.1 Request and completion queues allocated at build time; eventfd wake-up of the I/O thread; simulation and real-kernel tests
- [x] 1.2 `Client` builder: endpoint, region, credentials, limits, TLS; start-up and orderly shutdown
- [x] 1.3 Buffer lending with a guard type; waiting when the pool is empty
- [x] 1.4 Error type and outcome mapping
- [x] 1.5 More than one I/O thread: thread count and CPU pinning in the builder, placement of operations and lent buffers, shutdown of every thread
- [x] ~~1.6 A client driven by the caller's event loop~~ *(moved to `add-driven-client`)*
- [x] 1.7 Bucket in the path as a store setting (`Config::bucket_in_path`), run against Linode Object Storage in both forms
- [x] 1.8 Run path addressing against a self-hosted store that requires it, MinIO or Ceph (`bench/results/m27-self-hosted-stores.md`: Ceph 20.2.5 and RustFS 1.0.6, each serving only path-style requests)
- [x] 1.9 Builder settings for a credentials file that is reread, the bucket in the path, and checksums on or off; real-kernel check of each

## 2. Operations
- [x] 2.1 Async and blocking forms of store, conditional store, fetch, ranged fetch and erase on the lending path
- [x] 2.6 Multipart upload through the client: begin, part, complete, abort (`client/multipart.rs`)
- [x] 2.5 Async and blocking forms of stat (with and without the response head) and list; `list_next` reading the continuation token from the previous page's buffer; one-byte delimiter; simulation tests including a missing key and a listing over several pages under `alloc_guard` (`client/query.rs`, `tests/client/query.rs`)
- [ ] 2.3 Upload stream: stream record table (`Builder::streams`), lazy begin with a plain store for a short stream, part records in a held buffer and the manifest rendered from them, fixed parts in flight, 5 MiB and 10,000-part checks, abort after the last part in flight through the reserved slot on drop and on failed completion; simulation tests for each
- [ ] 2.4 Download stream: stat first, ranges of one buffer a fixed window ahead with `If-Match`, delivery in order, running CRC-64/NVME (S3) or CRC-32C (GCS) checked at the end, unverified end for objects without a full-object checksum; simulation tests with a flipped bit and an object replaced mid-download
- [ ] 2.2 Copying forms `store_from` and `fetch_into`: one buffer for a slice that fits, the streams for a longer one, `Error::TooLong` for a slice shorter than the object
- [ ] 2.7 Run 2.5, 2.3, 2.4 and 2.2 against Amazon S3 and Linode Object Storage, recorded in `bench/results/`

## 3. Quality
- [ ] 3.1 Cancellation tests: futures dropped at every stage never free a buffer the kernel still holds. Covered: a store dropped after it was sent (`tests/client/driven.rs`), dropped while waiting for a slot or before sending (`tests/client/waits_adversarial.rs`), dropped unfinished in the loom model; a fetch and a ranged fetch dropped while the body arrives in the lent buffer, stepped in the simulator (`tests/client/cancel.rs`), on the portable backend over loopback (`tests/client/dropped_fetch.rs`, under `client_portable`) and on io_uring (`client_uring`, `client_uring/cancel.rs`): the buffer is not lent again until the operation ends, comes back with the whole body, sees no later write, and carries the next round trip. Still to add: a stream dropped at each stage, once 2.3 and 2.4 land
- [ ] 3.2 Tests under at least two async executors and from plain threads
- [x] 3.3 Overhead benchmark against the low-level API, recorded in `bench/results/` (`m22-client-and-soak.md`: Amazon S3)
- [x] 3.4 Runs against Amazon S3 (`m22-client-and-soak.md`, `m23-client-eight-hours.md`). Google Cloud Storage through the client *(moved to `update-gcs-xml-coverage` 5.1, which also needs its 1.1 for the client to reach that store)*
- [x] 3.5 Throughput by thread count for HTTPS upload, download and HTTPS download, recorded in `bench/results/` (`m22-client-and-soak.md`)

## 4. Packaging
- [x] ~~4.1 Compile on non-Linux targets with an unsupported error at build of a client~~ *(superseded by `add-portable-io-backend`: macOS now builds and runs the client on the portable `kqueue` backend; other non-Linux targets still return `BuildError::Unsupported`)*
- [ ] 4.2 Crate documentation with runnable examples, and an `examples/` directory: lending round trip, several threads, copying forms, upload and download streams, stat and listing
- [ ] 4.3 Stated minimum Rust version (`rust-version` in the workspace), ~~a feature that turns TLS off~~ *(superseded by `add-server-networking`: default-on `tls` feature across `longshore`, `longshore_s3`, `longshore_http_client`, and `longshore_stream`)*, and a note on the C toolchain AWS-LC needs
- [x] 4.4 README: quick start with the high-level client (`README.md`, "Using it")
