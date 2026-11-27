## Context
`add-high-level-client` built `Worker`, one I/O thread's logic with no
thread around it, and `Client::over`, a client over workers the caller
drives. That path still goes through a mailbox lane, needs `Io::waker`, and
`Client::over` is `unsafe` because the caller must keep each `Io` alive.
No `Io` exposes a descriptor a caller's reactor could wait on.

## Goals / Non-Goals
- Goals: a client one event loop per core can run with no thread, no queue
  hop and no `unsafe` at the call site; the same async methods as the
  threaded client
- Non-Goals: code of any particular runtime in this crate. The caller
  registers a descriptor with its own reactor

## Decisions
- The driven client is not `Send`. It owns its `Io` and its worker, so the
  lifetime rule `Client::over` leaves to the caller is enforced by
  ownership. Its futures borrow it, and complete inside `turn`.
- It gives the caller a file descriptor that becomes readable when the
  client has work to do, and a `turn` call that does that work without
  blocking. The caller registers the descriptor with its own reactor and
  calls `turn` when it fires. Nothing crosses a thread, so there is no
  queue: a request goes from the method straight into the worker.
- Under `io_uring` the descriptor is the ring's own: a ring set up for
  deferred task work is readable when it has completions to deliver. On the
  portable backend it is the `epoll` or `kqueue` descriptor, which is
  itself pollable.
  Alternative considered: I/O threads beside the caller's cores. Works
  with the threaded client as it is, and costs a thread and a queue hop
  for every request.

## Risks / Trade-offs
- A caller that forgets to call `turn` stalls every operation of that
  client; the documentation and the example show the loop.

## Open Questions
- Whether polling the ring's descriptor is enough on every supported
  kernel with `IORING_SETUP_DEFER_TASKRUN`, or an eventfd registered with
  the ring is needed. To be settled by a real-kernel test on Linux 6.1.
  None of the current hosts runs 6.1, so one has to be found or set up
  (a 6.1 VM is enough, since nothing here needs a fast card).
