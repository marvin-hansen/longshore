A client that callers on any thread can share.

A [`Client`] is a handle to one or more I/O threads. Each I/O thread is a
complete single-threaded store with its own ring, connections and
buffers, and the threads share nothing with each other. One thread is
enough to fill a fast network card with plain uploads. More threads are
for work that one core cannot carry, such as HTTPS and downloads.

Every operation returns a future that works under any executor and also
has a blocking form: [`Pending::wait`] on most calls, a `_blocking`
method on the rest.

```no_run
use longshore_s3::client::Builder;
use longshore_s3::creds::Credentials;

let credentials = Credentials::new("ACCESS_KEY", "SECRET_KEY").unwrap();
let client = Builder::new("my-bucket.s3.us-east-1.amazonaws.com", "us-east-1", credentials)
    .tls(true)
    .port(443)
    .threads(4) // four independent I/O threads
    .build()
    .unwrap();

let mut body = client.buffer_blocking();
body.as_mut_slice()[..5].copy_from_slice(b"hello");
let (stored, body) = client.store("greeting", body, 5).wait();
stored.unwrap();
let (fetched, body) = client.fetch("greeting", body).wait();
assert_eq!(fetched.unwrap(), Some(5));
assert_eq!(&body.as_slice()[..5], b"hello");
```

# Choosing a form

| Form | Calls | Copies of the body | For |
|---|---|---|---|
| Lending | [`store`], [`store_no_clobber`], [`fetch`], [`fetch_range`] | none | a body that fits in one buffer and can be written or read there |
| Copying | [`store_from`], [`fetch_into`] | one | bytes already in the caller's own memory |
| Streams | [`upload`], [`download`] | none, or one with [`Upload::write`] | an object larger than one buffer, or of unknown length |
| Multipart | [`upload_begin`], [`upload_part`], [`upload_complete`], [`upload_abort`] | none | a caller that places the parts itself |
| Queries | [`stat`], [`stat_head`], [`list`], [`list_next`], [`erase`] | no body | asking about objects, and erasing them |
| Reported | [`put`], [`head`], [`get`] | none | a caller that must know exactly what the store answered |

"Copies" counts copies the client makes. By default the client also reads
each stored body once to compute its checksum ([`Builder::checksums`]), and
over HTTPS sealing reads it once more.

## Lending: no copy

The body lives in a [`Buffer`] borrowed from the client, which is memory
registered with the kernel. An operation takes the buffer and gives it back
with its result, so the caller cannot touch it while the kernel may, and the
same buffer can carry the next request. [`Client::buffer_size`] is the
largest body one operation moves. A missing object is `Ok(None)`, not an
error.

## Copying: one copy

[`Client::store_from`] and [`Client::fetch_into`] take the caller's slice.
A slice that fits in one buffer is one request and one copy. A longer one
goes through the streams below. The caller owns the memory on both sides,
so nothing is allocated to hold a body, and there is no form that returns
a `Vec`.

```no_run
# async fn run(client: longshore_s3::client::Client) -> Result<(), longshore_s3::client::Error> {
client.store_from("notes/today", b"two lines\nof notes\n").await?;
let mut out = [0u8; 64];
let n = client.fetch_into("notes/today", &mut out).await?.expect("stored above");
assert_eq!(&out[..n], b"two lines\nof notes\n");
# Ok(()) }
```

## Streams: objects of any size

An [`Upload`] sends each full buffer as one part of a multipart upload,
with up to [`PARTS_IN_FLIGHT`] parts in flight. [`Upload::send`] lends it a
buffer the caller filled, with no copy; [`Upload::write`] copies bytes into
its buffers. A stream that never fills more than one buffer is stored with
one plain store. Every buffer is a part, so the client's buffers must be at
least the store's smallest part ([`Builder::part_min`], 5 MiB on Amazon S3).

A [`Download`] asks the store about the object, then fetches it in ranges of
one buffer, a few ahead of the reader, each bound to the object's `ETag` so
that a replaced object fails instead of being spliced. [`Download::read`]
gives each [`Piece`] in order, in a lent buffer the reader drops to give
back. The bytes are checked against the store's checksum of the whole
object at the end, so they are unverified until [`Piece::End`].

```no_run
# use longshore_s3::client::{Client, Error, Piece};
# fn consume(_: &[u8]) {}
# async fn run(client: Client, bytes: &[u8]) -> Result<(), Error> {
let mut upload = client.upload("logs/today").await?;
upload.write(bytes).await?;
upload.finish().await?;

let mut download = client.download("logs/today").await?.expect("stored above");
loop {
    match download.read().await? {
        // Not verified yet. Dropping the buffer gives it back.
        Piece::Bytes(buffer, n) => consume(&buffer.as_slice()[..n]),
        Piece::End { verified } => {
            println!("every byte read; checked against the store's checksum: {verified}");
            break;
        }
    }
}
# Ok(()) }
```

## Asking about objects

[`Client::stat`] returns a [`Stat`](crate::stat::Stat), which is small and
`Copy`, so it needs no buffer. [`Client::stat_head`] also copies the
response head into a buffer, for headers `Stat` has no field for, read
with [`header_in`](crate::stat::header_in). [`Client::list`] returns the
store's listing document in a buffer, which
[`Listing`](crate::list::Listing) reads in place; [`Client::list_next`]
takes that buffer back to fetch the page after it.

## Knowing exactly what the store answered

[`Client::put`], [`Client::head`] and [`Client::get`] are for a caller
that must know exactly what happened: they take a limit of attempts and
time, and report what the store answered instead of deciding what it
means. See [`crate::report`].

# Waiting instead of queueing

Everything a client uses is allocated when it is built, so a call that
needs something in use waits for it instead of queueing without bound:

- [`Client::buffer`] waits for a free buffer on any I/O thread
  ([`Builder::buffers`] per thread).
- Every operation waits for an operation slot on its I/O thread
  ([`Builder::operations`] per thread).
- [`Client::upload`] waits for a stream record ([`Builder::streams`] per
  client), and a write to a stream waits while [`PARTS_IN_FLIGHT`] parts
  are in flight.
- A download waits for buffers to fetch its ranges into.

Buffers are shared by every caller of a client, and an upload stream holds
up to [`PARTS_IN_FLIGHT`] + 2 of them and a download up to five. A caller
that keeps every buffer and asks for one more waits for ever, so size
[`Builder::buffers`] for the callers and streams at work at once.

# Cancellation

A [`Pending`] sends its operation the first time it is polled or waited
on and a slot is free. Dropped before that, the operation never runs. Once
sent, the operation runs to its end on the I/O thread even if the future is
dropped, and only then gives back the buffer it holds, because the kernel
may read or write that buffer until then. So a dropped store may still
store its object. The async forms, such as [`Client::store_from`], behave
the same.

Dropping an [`Upload`] before [`Upload::finish`] ends aborts its multipart
upload, once every part still in flight has ended, so no part lands after
the abort; a stream that never began an upload costs nothing to drop. A
write or send dropped part-way leaves the stream unsure of its bytes, so it
fails with [`Error::BadRange`] and is aborted when dropped. Dropping a
[`Download`] stops it asking for ranges; ranges in flight end first and
give their buffers back.

# Threads

[`Client`] is `Send`, `Sync` and cheap to clone: share a reference across
scoped threads, or give each task a clone. It brings no async runtime, and
its futures are woken by the I/O threads, so they run under any executor;
[`Pending::wait`] parks the calling thread instead, and belongs on a plain
thread rather than inside an async task.

[`Builder::threads`] sets the number of I/O threads, fixed for the life of
the client, and [`Builder::pin`] the CPUs they run on. The limits
([`Builder::connections`], [`Builder::buffers`], [`Builder::operations`])
are per I/O thread. An operation runs on one I/O thread from start to end:
one that carries a buffer runs on the thread the buffer belongs to, and one
without, such as [`Client::stat`] or [`Client::erase`], on the least busy
open thread. The parts of one upload stream may run on different threads.
The I/O threads stop when the last handle, buffer and operation are gone,
after any aborts owed by dropped upload streams have been sent.

[`Builder::build`] checks every setting before it starts a thread:

```
use longshore_s3::client::{BuildError, Builder};
use longshore_s3::creds::Credentials;

let credentials = Credentials::new("ACCESS_KEY", "SECRET_KEY").unwrap();
let refused = Builder::new("objects.example.com", "us-east-1", credentials)
    .threads(0)
    .build();
assert!(matches!(refused, Err(BuildError::Setting(_))));
```

# Examples

The crate's `examples/` directory has a program for each form. Each reads
the store from `S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY` and
`S3_SECRET_KEY` (and `S3_SESSION_TOKEN`, `S3_TLS=1`, `S3_PATH_STYLE=1`),
stores under a prefix of its own and erases what it stored:

- `round_trip`: borrow a buffer, store, fetch, fetch a range, conditional
  store, erase.
- `many_threads`: plain threads and tokio tasks sharing one client with two
  I/O threads.
- `copying`: `store_from` and `fetch_into`, within one buffer and beyond it.
- `streams`: an upload stream fed by `send` and `write`, a verified
  download, and a dropped stream.
- `stat_and_list`: `stat`, `stat_head`, a listing by delimiter, and a
  listing in pages.

```text
cargo run -p longshore_s3 --example round_trip
```

[`Worker`] is one I/O thread's logic with no thread around it, for a
caller that drives the I/O itself: the simulator in tests, or an event
loop that already owns the core.

[`store`]: Client::store
[`store_no_clobber`]: Client::store_no_clobber
[`fetch`]: Client::fetch
[`fetch_range`]: Client::fetch_range
[`store_from`]: Client::store_from
[`fetch_into`]: Client::fetch_into
[`upload`]: Client::upload
[`download`]: Client::download
[`upload_begin`]: Client::upload_begin
[`upload_part`]: Client::upload_part
[`upload_complete`]: Client::upload_complete
[`upload_abort`]: Client::upload_abort
[`stat`]: Client::stat
[`stat_head`]: Client::stat_head
[`list`]: Client::list
[`list_next`]: Client::list_next
[`erase`]: Client::erase
[`put`]: Client::put
[`head`]: Client::head
[`get`]: Client::get
