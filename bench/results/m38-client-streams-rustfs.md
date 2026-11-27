# Milestone 38: the client's stat, listing, streams and copying forms against RustFS

Run 2026-11-27 with `client_s3 check` at commit `69b7f73`, on an Apple M3
Max under OrbStack (Docker 29.8.2, Linux `7.2.12-orbstack` on AArch64).
RustFS 1.0.6 (`rustfs/rustfs:1.0.6`, one node, one volume) ran in its own
container, started by `scripts/self-hosted-s3.sh up rustfs`, and the program
ran in a `rust:1-bookworm` container on the same Docker network, built
there, with the seccomp profile off and locked memory unlimited, so the
client ran on `io_uring`. Plain HTTP on port 80, the bucket in the path.
One I/O thread, 4 connections and 4 lent buffers of 8 MiB (the defaults of
`client_s3 check`). Every line the program printed is in
`m38-client-streams-rustfs.txt`.

RustFS stands in here for Amazon S3 and Linode Object Storage, which the
client already ran against for store, fetch and multipart upload
(`m22-client-and-soak.md`, `m23-client-eight-hours.md`, and
`m27-self-hosted-stores.md` for RustFS and Ceph with the bucket in the
path).

## Result

Every step passed, and the program ended `ok client_s3 check` with exit
status 0.

| Step | What RustFS answered |
|---|---|
| `store_from` and `fetch_into`, 1 MiB (fits one buffer) | equal |
| `stat` of a stored key | size, tag and stored CRC-64/NVME present |
| `stat` of a missing key | none |
| `stat_head` | 393 bytes of head, `content-length` as stored |
| `list` two keys a page, then `list_next` | 5 keys in 3 pages, in order |
| `list` with delimiter `/` | the one common prefix |
| multipart upload of the big object (6 parts) | 42,991,616 bytes |
| `download` of the big object | 6 pieces in order, every byte checked, verified against the stored checksum |
| `fetch_into` a slice longer than one buffer | equal |
| `fetch_into` slices too short, one buffer long and one byte short | `TooLong` both ways |
| `download` while the object is replaced by `store_from` | status 412 on the next read after the first piece |
| upload stream of 1000 bytes | one plain store: the tag has no `-N` part |
| upload stream of the big object written in pieces of 2,796,219 bytes | a multipart upload of 6 parts (tag ends `-6`), read back equal |
| `store_from` of the big object | a multipart upload of 6 parts, read back equal |
| upload stream dropped after 2.5 buffers | no object, and no upload left in progress |
| erase of every key stored | 12 keys erased |

## Reading it

- **A whole-object checksum after a multipart upload.** The client asks for
  a full-object CRC-64/NVME when it begins an upload, and RustFS kept one:
  the download of the multipart object ended verified, not unverified.
- **The replaced object.** The download had asked for up to three ranges
  ahead before the object was replaced, yet the very next read already got
  412. RustFS checks `If-Match` when it serves a request, not when it
  receives it, so ranges asked for before the replacement but served after
  it are refused too. Either way the download stopped rather than hand out
  bytes of two objects.
- **The dropped stream.** The client cannot list uploads in progress, so
  after the run a `ListMultipartUploads` under the run's prefix was sent
  with `curl --aws-sigv4` (the last lines of the `.txt`): it listed none.
  As a control, an upload started by hand under the same prefix was
  listed, and gone after its abort (status 204).
- **Lazy begin.** A store's tag for a multipart object ends in `-` and the
  part count, and a plain store's does not. The short stream's tag shows it
  was stored with one plain request, as the design says.

## What this does not tell us

- Anything about Amazon S3, Linode or Google Cloud Storage for these forms.
  On Google Cloud Storage the download fails every range until the reported
  `get` exists there (`update-gcs-xml-coverage` 2.2).
- HTTPS, more than one I/O thread, or throughput: this is a correctness run
  of one caller with the blocking forms.
- Whether parts were still in flight when the stream was dropped, so
  whether the abort waited for one: the output does not say. That
  ordering is covered in the simulator.
- Repeatability beyond two full runs on one machine, both passing, and an
  earlier run without the upload steps, also passing.
