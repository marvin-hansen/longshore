## Why
A caller that already runs one event loop per core, such as a storage
engine, pays a thread and a queue hop for every request when it uses the
threaded client. It needs a client that its own loop drives, so a request
stays on the core that asked for it. Moved out of `add-high-level-client`
(its task 1.6), which closes without it.

## What Changes
- A client with no thread of its own and no queue, not `Send`, built over
  an `Io` on the caller's thread
- The `Io` exposes a descriptor that becomes readable when there are
  completions to deliver: the ring's own under `io_uring`, the `epoll` or
  `kqueue` descriptor on the portable backend
- A non-blocking `turn` that feeds completions and runs the client's work;
  operations are the same async methods as the threaded client's, and their
  futures complete inside `turn`

## Impact
- Affected specs: `high-level-client` (created by `add-high-level-client`,
  which is archived first)
- Affected code: `longshore_common::io` (descriptor), `longshore_s3::client`
  (a driven client beside `Worker` and `Client::over`)
- First principles: nothing crosses a thread, nothing is allocated after
  build, and the protocol code is the same `Worker` the I/O threads run
