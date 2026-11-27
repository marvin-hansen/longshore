## Context
The low-level store is single-threaded and not `Send` across its event loop.
Most callers are on other threads, often inside an async runtime.

## Goals / Non-Goals
- Goals: a client that is `Send + Sync + Clone`; no dependency on a
  particular async runtime; the zero-copy path stays reachable; back pressure
  instead of unbounded queues; more than one core when the caller asks for it
- Non-Goals: replacing the low-level API, or depending on a caller's
  runtime. A client driven by the caller's own event loop, with no thread
  of its own, is a separate change, `add-driven-client`

## Decisions
- The client owns a fixed number of I/O threads, set when it is built. The
  default is one. Each thread is a complete single-threaded store with its
  own ring, connections, buffers and resolver, and the threads share
  nothing. More threads are for work one core cannot carry: HTTPS and
  downloads are bound by the client's core, at about 5 and 4 GB/s on
  Graviton4, and eight independent clients side by side gave 34.5 GB/s of
  HTTPS upload against 5.0 for one. A plain upload already fills a
  300 Gbit/s card from one thread, so more threads add nothing there.
  Alternative considered: one ring shared by several threads. Rejected
  because it brings locks and shared state onto the data path, and the
  simulator could no longer replay a run exactly.
- The store's bucket addressing is a setting: the bucket in the host name,
  as today, or in the path, which self-hosted S3-compatible stores commonly
  expect. Signing covers the path as sent.
- The builder takes the number of threads and, optionally, the CPU each is
  pinned to. On a machine with more than one NUMA node the threads belong
  on the network card's node: off it, one thread's plain upload fell from
  about 19 GB/s to about 4. macOS cannot pin a thread to a CPU, so the
  builder refuses the setting there.
- The number of threads is fixed for the life of a client. A caller that
  needs more for a while, such as a restore, builds a second client for
  that job and drops it afterwards.
- An operation runs on one thread from start to finish. A new operation
  goes to the thread with the fewest in progress, or, if that thread has
  no free operation slot, to any open thread that has one. A lent buffer
  belongs to the thread that registered it, so an operation that carries a
  lent buffer runs on that buffer's thread, and `client.buffer()` hands out
  the first free buffer on any open thread. The parts of one streamed upload may
  run on different threads, since each part is its own request. Beginning
  and completing the upload are ordinary operations.
- Limits given to the builder (connections, buffers, operations) are per
  thread, so memory grows with the thread count and is still fixed at build.
- Callers submit to a thread through a bounded queue of fixed-size request
  messages, and the thread is woken through its backend's waker: an eventfd
  registered with the ring under `io_uring`, the `epoll` or `kqueue` wake-up
  on the portable backend. Completion wakes the caller's `Waker`, or unparks
  the thread for blocking calls.
  Alternative considered: integrate with a specific runtime's reactor.
  Rejected because it ties the crate to that runtime and puts its scheduler
  on the data path.
- Buffers are lent, not shared. `client.buffer().await` gives exclusive
  ownership of one registered buffer as a guard that returns it on drop.
  While an operation holds a buffer the caller cannot touch it, which the
  type system enforces by moving the guard into the operation and handing it
  back with the result.
- When no buffer or operation slot is free, calls wait. Nothing is queued
  without bound and nothing is allocated to hold the backlog.
- Cancellation: dropping a future detaches the caller but lets the operation
  finish on the I/O thread, so a buffer is never returned while the kernel
  may still write to it.
- Errors are one public enum. Low-level outcomes that are not errors, such as
  `NotFound`, are mapped to `Option` or a dedicated variant per method.
- Credentials can come from a file that each I/O thread rereads on its own
  timer. Changed credentials are tried with one request (by default a
  one-key listing of the bucket) and adopted only if the store accepts
  them, so a half-written or wrong file never replaces working credentials.
  The client counts adopted and failed reloads so that a rotation that is
  not taking can be seen.
- A CRC-64/NVME is sent with every plain store and part by default. It is
  one more pass over the body (16 GB/s with it, 37 GB/s without, one
  thread), so the builder and each handle can turn it off. A conditional
  store always sends one, because telling a retry's own earlier write from
  someone else's compares checksums.

### Stat and list
- `stat(key)` returns `Option<Stat>`. `Stat` is `Copy` and fixed-size
  (size, modification time, stored checksum, tag), so it comes back in the
  completion message and needs no buffer; like `erase`, it runs on any
  open thread. `stat_head(key, buffer)` also copies the response head into
  a lent buffer, for headers `Stat` has no field for, read with
  `stat::header_in`.
- `list(prefix, delimiter, max_keys, buffer)` returns the listing document
  in the lent buffer with its length. The caller parses it in place with
  `list::Listing`, which allocates and copies nothing.
- A truncated listing is resumed by `list_next(prefix, delimiter, max_keys,
  buffer, len)`, given the buffer that holds the previous page and that
  page's length as the call that filled it returned. The I/O thread
  reads the continuation token out of that page and builds the next
  request's query from it before the response overwrites the buffer. No
  token crosses the queue, so the request message keeps its size (one key
  field of `KEY_MAX` bytes, which holds the prefix), and the caller cannot
  pass a token that does not belong to the listing.
  Alternative considered: a token argument copied into the request.
  Rejected because a token is up to 1024 bytes once encoded, which would
  double every request message to carry a field only listings use.
- The delimiter is one byte, `/` in practice. A longer delimiter is
  refused with `Error::BadKey`; the low-level `S3::list_continue` takes
  any.

### Copying forms
- `store_from(key, &[u8])` and `fetch_into(key, &mut [u8]) -> Option<usize>`.
  The caller owns the memory on both sides, so the client never allocates
  to hold a body; that is why there is no form that returns a `Vec`.
- A slice no longer than one buffer costs one copy and one request:
  `store_from` borrows a buffer, copies and stores; `fetch_into` fetches the
  whole object (checked against its stored checksum) and copies out. An
  object larger than the buffer fails that fetch as too large for the
  buffer, and the call then reports `Error::TooLong`.
- A longer slice goes through the streams below: `store_from` writes it to
  an upload stream and finishes it, `fetch_into` reads a download stream
  into the slice. A slice shorter than the object fails with
  `Error::TooLong` before any range is fetched, since the download learns
  the size first.

### Upload stream
- `client.upload(key)` opens a stream. It writes into lent buffers, with
  `write(&[u8])` copying and `send(buffer, len)` lending, and sends each
  full buffer as one part, so each part is one zero-copy send. A fixed
  number of parts are in flight; `write` and `send` wait when that many
  are. Opening fails with a clear error when the client's buffer size is
  below the store's 5 MiB minimum part size, and the stream fails with a
  clear error before it would exceed 10,000 parts. A streamed object is
  therefore capped at 10,000 times the buffer size.
- The multipart upload begins only when the first full buffer is sent. A
  stream finished before that is stored with one plain store, so a short
  stream costs no more than `store_from`, and dropping it costs nothing.
- Each stream takes a record from a fixed table set when the client is
  built (`Builder::streams`, per client). The record holds the upload id,
  the count of parts in flight, a dropped flag, and an operation slot
  reserved when the upload begins. Opening a stream waits when no record
  is free. The record also holds one lent buffer, taken at begin, in which
  each finished part's number, tag and checksum are written at an offset
  set by the part number; at finish the manifest is rendered from those
  records into the same buffer and sent with the completion. The records
  for 10,000 parts take well under 1 MiB, and the buffer is at least
  5 MiB.
- Dropping an unfinished stream sets the dropped flag. Whichever thread
  ends the stream's last part in flight, or the drop itself if none is in
  flight, sends the abort through the reserved slot, so the abort needs no
  slot it might have to wait for and comes after every part has ended.
  S3 notes that a part still uploading when its upload is aborted may land
  anyway; aborting last avoids that. A failed completion aborts the same
  way. Shutting down the client counts a pending abort as an operation in
  progress.
  Alternatives considered: sending the abort at once from `Drop`, which can
  find no free slot and cannot wait there; keeping all of one stream's
  parts on one thread, which caps one HTTPS upload at one core.

### Download stream
- `client.download(key)` first asks about the object (a HEAD with
  `x-amz-checksum-mode: ENABLED`), learning its size, tag and stored
  checksum. It then fetches ranges of one buffer each, a fixed window
  ahead of the reader, and hands the buffers to the reader in order. The
  window is bounded by the buffer pool.
- Every range is fetched with `If-Match` on the tag from the first answer,
  so an object replaced mid-download fails the next range with status 412
  instead of splicing two objects together.
- A ranged answer carries no checksum, so ranges cannot be checked one by
  one. Instead the stream keeps a running CRC, CRC-64/NVME on S3 and
  CRC-32C on Google Cloud Storage, updated as each buffer is handed to the
  reader. Delivery is in order, so one running CRC is enough and no CRC
  combining is needed. At the end it is compared with the stored
  full-object checksum: the stream ends verified, or with
  `Error::Store(Fatal::ChecksumMismatch)`. Bytes handed out before the end
  are unverified until the stream ends; this is documented on the stream.
  An object with no full-object checksum, such as one uploaded with a
  composite checksum or none, ends reported as unverified.
  The CRC runs on the reader's thread, not the I/O thread, because HTTPS
  download is already bound by the I/O thread's core.
- `client.download(key)` resolves to `Ok(None)` for a missing key, as
  `fetch` and `stat` do, and otherwise to a `Download`. Each
  `read()` (or `read_blocking()`) gives `Piece::Bytes(buffer, len)`, the
  lent buffer itself, which the reader returns by dropping it, and at the
  end `Piece::End { verified }`. An end or an error is given again on
  every later read.
- The window is four ranges, and never more than the client's lent
  buffers less one, which is left for the buffer the reader holds (one
  range for a client that lends one buffer). A range is asked for as soon
  as a buffer is free; the stream gives up its place in the buffer queue
  whenever it hands a buffer out, so it never keeps a returned buffer
  from the client's other callers while the reader is busy.
- Ranges are fetched with the reported `get`, not the plain fetch, so
  each answer can be checked against the range asked for: status 206,
  that `Content-Range`, that many bytes, and the tag from the first
  answer, which also catches a store that ignores `If-Match`. Status 200
  is `Fatal::RangeIgnored`, a range other than the one asked for is
  `Fatal::BadResponse`, and a changed object is the store's own
  `Fatal::Status(412)` (404 if it is gone); no new error variant is
  added. The reported `get` exists only for S3 so far, so on Google Cloud
  Storage every range fails until `update-gcs-xml-coverage` 1.1 and 2.2
  land.
- Which CRC to run is read from the stored checksum's form, which the
  vendor decides: twelve base64 characters are S3's CRC-64/NVME, eight
  followed by zeros are Google Cloud Storage's CRC-32C. The stream needs
  no vendor setting of its own.
- An empty object asks for no range and ends at once, checked against
  the stored checksum of no bytes.

## Risks / Trade-offs
- A queue hop between threads costs time on small requests. The overhead
  benchmark measures it, and the low-level API remains for callers that
  cannot afford it.
- Every thread opens its own connections and resolves names itself, so a
  store sees the client's connection count multiplied by its threads.
- Threads do not rebalance. A thread whose operations are slow keeps them,
  and new work goes elsewhere.
- A caller that is itself one thread per core can skip all of this and
  give each of its threads a low-level store. Nothing in the low-level API
  changes.
- A download reader that stops early never learns whether the bytes it read
  were the object's; only reading to the end verifies.
- Each upload stream holds one buffer for its part records from begin to
  finish, so `Builder::streams` streams in progress take that many buffers
  out of the pool.

## Open Questions
- Whether the async methods should also be offered as a `Stream` and
  `AsyncRead`/`AsyncWrite` from a specific ecosystem crate behind a feature.
  Proposed: start with the crate's own lending types and add adapters behind
  features once a user asks for one.
