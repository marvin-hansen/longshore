## Why
Using the library today means owning the event loop, registering buffers,
submitting operations with tokens and polling for outcomes. That suits a
storage engine and is a barrier for everyone else. Other projects need a
client they can build and call.

## What Changes
- A `Client` that starts and owns its I/O thread, or several independent
  ones when asked, for work one core cannot carry
- Buckets addressed in the path as well as in the host name
- Credentials reread from a file, and checksums on stored bodies that can
  be turned off
- Async methods that work under any executor, and blocking equivalents
- A copying path that takes a byte slice and fetches into one, and a
  lending path that hands the caller a registered buffer so the body is
  never copied
- Stat and paged listing, multipart upload through the client, streaming
  upload built on multipart and streaming download built on ranged fetches
  with whole-object checksum verification
- Runnable examples, and a benchmark of what the layer costs over the
  low-level API
- The crate compiles everywhere; the client runs on Linux and on macOS (the
  portable backend), and building one elsewhere returns an unsupported error
- Moved out: a client with no thread of its own, driven by the caller's
  event loop, is now `add-driven-client`. Running the client against Google
  Cloud Storage is `update-gcs-xml-coverage` 5.1

## Impact
- Affected specs: `high-level-client` (new)
- Affected code: a new `client` module; `store` gains the addressing
  setting. No other change to the low-level API
- Depends on: `add-multipart-upload` for streaming upload, and
  `add-object-operations` for `stat` and listing continuation (both built)
- First principles: each I/O thread is unchanged, one ring driven by one
  thread with nothing shared. The caller's thread and an I/O thread exchange
  fixed-size messages through bounded queues that are allocated when the
  client is built, and upload streams take records from a table also
  allocated then. The lending path adds no copy. The copying path adds
  exactly one, into or out of a registered buffer, into memory the caller
  owns, and is documented as the convenience path.
