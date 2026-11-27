## ADDED Requirements

### Requirement: Self-contained client
The library SHALL provide a client that starts and owns its I/O thread, can
be cloned and shared across threads, and requires no event loop, buffer
registration or token bookkeeping from the caller.

#### Scenario: First use
- **WHEN** a caller builds a client from an endpoint, a region and credentials, and stores then fetches an object
- **THEN** both calls succeed without the caller creating an `Io` or polling for completions

#### Scenario: Shutdown
- **WHEN** the last clone of a client is dropped
- **THEN** operations in progress finish or fail, the I/O thread exits and its resources are released

### Requirement: Configurable number of I/O threads
The client SHALL run on one I/O thread by default and SHALL run on a number
of independent I/O threads chosen when it is built, each with its own
connections and buffers and with no state shared between them on the data
path. The builder SHALL accept a CPU for each thread to be pinned to, and
SHALL refuse pinning where the platform cannot pin.

#### Scenario: Default
- **WHEN** a client is built without a thread count
- **THEN** it starts exactly one I/O thread

#### Scenario: Several threads
- **WHEN** a client is built with four threads and many operations are started at once
- **THEN** operations run on all four threads and every one completes

#### Scenario: Lent buffer stays on its thread
- **WHEN** a caller takes a buffer from a client with several threads and stores it
- **THEN** the operation runs on the thread that owns the buffer and the body is not copied

#### Scenario: Shutdown with several threads
- **WHEN** the last clone of a client with several threads is dropped
- **THEN** every I/O thread exits and its resources are released

#### Scenario: Pinning refused on macOS
- **WHEN** a client is built on macOS with its threads pinned to CPUs
- **THEN** building fails with `BuildError::Setting` naming the setting

### Requirement: Bucket addressing
The store SHALL address a bucket either in the host name or in the request
path, as configured, and SHALL sign requests for the form it sends.

#### Scenario: Bucket in the path
- **WHEN** the store is configured for path addressing and stores then fetches an object against a store that accepts only that form
- **THEN** both requests are accepted

### Requirement: Credentials reread from a file
The client SHALL accept a credentials file that each I/O thread rereads at a
fixed interval, SHALL try changed credentials with one request before
adopting them, and SHALL count adopted and failed reloads.

#### Scenario: Rotated credentials
- **WHEN** the credentials file is rewritten with new valid credentials while the client runs
- **THEN** each I/O thread adopts them within one reread interval, later operations succeed, and the count of adopted reloads grows

#### Scenario: Credentials the store refuses
- **WHEN** the credentials file is rewritten with credentials the store refuses
- **THEN** the client keeps the credentials in use and the count of failed reloads grows

### Requirement: Checksums on stored bodies
The client SHALL send a CRC-64/NVME with every plain stored body and
multipart part by default, SHALL let the builder or a handle turn this off,
and SHALL always send one with a conditional store.

#### Scenario: Checksums off
- **WHEN** a client built with checksums off stores an object
- **THEN** the request carries no checksum header and the store accepts it

#### Scenario: Conditional store keeps its checksum
- **WHEN** a client with checksums off stores an object only if none exists
- **THEN** the request carries a CRC-64/NVME

### Requirement: Async and blocking calls
Every operation SHALL be available as an async method that works under any
executor and as a blocking method.

#### Scenario: Two executors
- **WHEN** the same test suite runs under two different async executors
- **THEN** every operation completes under both

#### Scenario: Plain thread
- **WHEN** a thread with no async runtime calls the blocking form of fetch
- **THEN** the call returns the object

### Requirement: Zero-copy path
The client SHALL lend registered buffers to callers so that a body is sent
and received without being copied, and SHALL prevent access to a buffer
while an operation holds it.

#### Scenario: Lent buffer round trip
- **WHEN** a caller takes a buffer, fills it, stores it and receives the buffer back with the result
- **THEN** the body was not copied by the client between the caller's write and the socket

#### Scenario: Future dropped mid-operation
- **WHEN** a caller drops the future of a fetch while the kernel is still receiving into its buffer
- **THEN** the buffer returns to the pool only after the operation has finished on the I/O thread

### Requirement: Object queries
The client SHALL ask about one object without fetching it, returning a
fixed-size result, and SHALL list keys under a prefix one page at a time into
a lent buffer that the caller parses in place, resuming a truncated listing
from the page held in that buffer.

#### Scenario: Stat of a present and a missing key
- **WHEN** a caller asks about a stored key and about a key that is not there
- **THEN** the first returns the object's size, tag and stored checksum, and the second returns no object

#### Scenario: Listing across pages
- **WHEN** a caller lists a prefix holding more keys than one page, passing the buffer of each page to the call for the next
- **THEN** every key under the prefix is returned exactly once, in order, and no call allocates

### Requirement: Multipart upload through the client
The client SHALL offer beginning, storing parts of, completing and aborting a
multipart upload, with each part sent from one lent buffer and parts of one
upload free to run on different I/O threads.

#### Scenario: Parts on several threads
- **WHEN** a caller begins an upload on a client with two threads, stores parts from buffers of both threads and completes it
- **THEN** the object equals the parts in order

### Requirement: Copying convenience path
The client SHALL offer forms of store and fetch that take a byte slice and
fetch into a byte slice the caller provides, at the cost of one copy into or
out of a registered buffer, and SHALL NOT allocate to hold the body. A body
larger than one buffer SHALL go through the streaming transfers.

#### Scenario: Slice in, slice out
- **WHEN** a caller stores a byte slice and fetches the key into a slice of its own
- **THEN** the fetched bytes equal the stored bytes

#### Scenario: Larger than one buffer
- **WHEN** a caller stores a slice several buffers long and fetches it into a slice of that length
- **THEN** the store went through a multipart upload, the fetch through ranged fetches, and the bytes match

#### Scenario: Slice too short
- **WHEN** a caller fetches an object into a slice shorter than the object
- **THEN** the call fails with `Error::TooLong` and the slice holds nothing the caller should read

### Requirement: Streaming transfer
The client SHALL upload a stream of unknown length through multipart upload,
up to the store's multipart limits, and download an object of any size
through ranged fetches, each using a bounded number of buffers. A download
SHALL verify the whole object against the store's full-object checksum when
the store keeps one, and SHALL fail if the object changes while it is read.

#### Scenario: Upload larger than the buffer pool
- **WHEN** a caller writes more bytes to an upload stream than all registered buffers hold together, in a stream that stays within the store's part size and part count limits
- **THEN** the upload completes and the object equals what was written

#### Scenario: Short upload stream
- **WHEN** a caller finishes an upload stream before it has filled one buffer
- **THEN** the object is stored with one plain store and no multipart upload is begun

#### Scenario: Buffer below the minimum part size
- **WHEN** a caller opens an upload stream on a client whose buffer size is below the store's 5 MiB minimum part size
- **THEN** opening the stream fails with an error that names the minimum part size

#### Scenario: Upload stream dropped
- **WHEN** an upload stream is dropped before it is finished
- **THEN** its multipart upload is aborted after its last part in flight has ended

#### Scenario: Download larger than the buffer pool
- **WHEN** a caller reads an object larger than all registered buffers together to the end
- **THEN** every byte is delivered in order

#### Scenario: Download verified
- **WHEN** a caller reads to the end an object stored with a full-object CRC-64/NVME
- **THEN** the stream ends reporting the object verified, and with a flipped bit in one range it ends with a checksum error

#### Scenario: Object replaced mid-download
- **WHEN** the object is replaced after a download has read its first range
- **THEN** the next range fails as a precondition failure instead of delivering bytes of the new object

### Requirement: Back pressure
The client SHALL make callers wait when no buffer, operation slot or stream
record is free, and SHALL NOT queue work without bound or allocate after it
is built.

#### Scenario: More callers than slots
- **WHEN** more operations are started concurrently than the client has operation slots
- **THEN** the extra calls wait and complete as slots free up, and memory use does not grow

### Requirement: Builds everywhere, runs on Linux and macOS
The crate SHALL compile on every platform. Building a client SHALL succeed on
Linux (`io_uring`, or `epoll` when portable is chosen) and on macOS
(`kqueue`), and SHALL fail elsewhere with an error stating that the platform
is unsupported.

#### Scenario: Documentation build
- **WHEN** the crate's documentation is built on a host that is neither Linux nor macOS
- **THEN** the build succeeds

#### Scenario: Client on macOS
- **WHEN** a client is built on macOS and stores then fetches an object over loopback
- **THEN** both calls succeed on the portable backend

#### Scenario: Unsupported platform
- **WHEN** a client is built on a platform other than Linux or macOS
- **THEN** building fails with `BuildError::Unsupported`
