# Milestone 39: the client before and after the high-level client work, on one machine

Run 2026-11-27 and 2026-11-28 on an Apple M3 Max under OrbStack (Docker
29.8.2, Linux `7.2.12-orbstack` on AArch64, 4 CPUs and 8 GB given to Docker).
Every line the program printed is in `m39-client-local-ab.jsonl`, each
tagged with the build it came from (`build`) and the set it belongs to
(`set`).

Two builds of `client_s3` are compared:

- **base**, commit `9a304a5`, before the work of `add-high-level-client`
  was completed;
- **new**, commit `554e81f` plus the `download` mode of `client_s3` and the
  server fix below, which do not touch the client library.

Both ran against `s3_serve` (this milestone), the in-tree object server as a
program, built from the new commit for both. It ran alone in one container,
on the portable backend, with 32 connections, buffers of 8 MiB and 24 object
slots of up to 32 MiB, checking every stored body's CRC-64/NVME and returning
it. `client_s3` ran in another container on the same Docker network, on
`io_uring`, one I/O thread, 16 connections and 16 buffers of 8 MiB, plain
HTTP, the bucket in the path. Each run started a fresh server and measured
8 s after 2 s of settling. GB = 10^9 bytes. "GB per CPU-second" divides the
bytes by the CPU time of the client process alone, callers included, read
from `/proc/self/stat` (10 ms ticks).

This setup measures what the client costs, not what a store can take: the
client and server share 4 CPUs and loopback, and the server is one thread.

## Result: no measurable change

| | base | new |
|---|---|---|
| `put` 8 MiB, GB/s | 0.65 to 0.74 | 0.65 to 0.74 |
| `put`, GB per CPU-second, 7 pairs | 6.26 to 6.93, mean 6.63 | 6.06 to 6.85, mean 6.48 |
| `get` 8 MiB, GB/s, 3 pairs | 3.60, 3.68, 3.73 | 3.69, 3.55, 3.79 |
| `get`, GB per CPU-second | 3.94, 3.73, 4.04 | 3.71, 3.90, 3.93 |

No request failed. A `put` is bound by the server, which runs a CRC over
every body it stores: 0.1 of a core on the client moves what the server
takes. A `get` is bound by the client's I/O thread at about one core.

In the first three `put` pairs, with base run first, new came out 2 to 8 %
lower per CPU-second. Four more pairs with new run first put new ahead in
two and behind in two, so the gap followed the order, not the build. Over
all seven pairs the means differ by 2 %, inside a spread of up to 13 % between
runs of the same build.

What changed on the path of a plain store and fetch since base is a field
naming the stream on every request message (which already carries a key of
1024 bytes), one look into a small table when an operation ends, and an
atomic load when a stopping I/O thread decides whether it can exit.

## The download stream

New only: each caller stores one object of 4 buffers (32 MiB) and downloads
it again and again, every object read to its end and checked against the
stored CRC-64/NVME.

| Callers | GB/s | Client cores | GB per CPU-second |
|---|---|---|---|
| 1 | 3.52 to 3.57 | 0.90 | 3.93 to 3.94 |
| 2 | 3.63 to 3.77 | 0.95 to 0.97 | 3.80 to 3.91 |
| 4 | 3.82 to 3.83 | 1.00 | 3.81 to 3.83 |

Every download ended verified. One download keeps 4 ranges in flight and
already reaches what `get` reaches with 16 requests in flight, at the same
cost per byte; more callers add little because the one I/O thread is then
at a full core. The running CRC on the reader's thread is included in the
cost.

## The server's tags

The first runs of this milestone showed the download stream at 0.09 GB/s,
and `get` at 0.7. Timing each piece of one download showed its 4 ranges in
flight together and all arriving about 300 ms after they were asked for. The
in-tree server worked out an object's `ETag` from all of its bytes whenever a
request named it: once to check `If-Match` and once to write the `ETag`
header, about 37 ms each for 32 MiB, on its one thread. It now works the tag
out once, when the object is written. Those first runs measured the server,
not the client, and are not in the `.jsonl`.

The server also stopped answering, even plain `curl`, after a long series of
runs: `client_s3 put` and `get` leave their objects behind under new names
each run, and the object slots filled. Each run here starts a fresh server.

## How to run it

```sh
docker network create longshore-s3
# Build both programs from the tree into a volume (and client_s3 from the
# base commit into another), with `cargo build --release -p longshore-bench
# --bin s3_serve --bin client_s3` in a `rust:1-bookworm` container.
docker run -d --name longshore-serve --hostname longshore-serve \
    --network longshore-s3 --security-opt seccomp=unconfined \
    -v longshore-target:/target:ro rust:1-bookworm \
    /target/release/s3_serve --conns 32 --size 8388608 --object 33554432 --objects 24
docker run --rm --network longshore-s3 --security-opt seccomp=unconfined \
    --ulimit memlock=-1:-1 -v longshore-target:/target:ro \
    -e S3_ENDPOINT=longshore-serve -e S3_REGION=us-east-1 -e S3_BUCKET=bench \
    -e S3_ACCESS_KEY=x -e S3_SECRET_KEY=y -e S3_PATH_STYLE=1 rust:1-bookworm \
    /target/release/client_s3 download --conns 16 --parts 4 --callers 1 --secs 8
```

## What this does not tell us

- Anything about a real store, HTTPS, more than one I/O thread, or a
  machine with a fast network card. The figures for those are in
  `m22-client-and-soak.md` and `m26-s3-aws-large-instances-current.md`; a
  rerun of `m22` on the same instances is what would show a change there.
- Small objects, where the request message and the queue hop weigh more
  than they do at 8 MiB.
- The upload stream's own throughput.
