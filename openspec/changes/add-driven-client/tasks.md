## 1. Descriptor
- [ ] 1.1 `Io` exposes a descriptor readable when completions are waiting: the ring's under `io_uring`, the `epoll`/`kqueue` descriptor under `PosixIo`; the simulator's equivalent for tests
- [ ] 1.2 Real-kernel test on Linux 6.1 settling whether the ring's descriptor is enough with deferred task work, or an eventfd registered with the ring is needed

## 2. Client
- [ ] 2.1 Driven client owning its `Io` and worker, not `Send`, with no queue; the threaded client's async methods
- [ ] 2.2 Non-blocking `turn`; returns at once when there is nothing to do
- [ ] 2.3 Simulation tests, and real-kernel tests under `io_uring` and `epoll` with a caller that only polls the descriptor
- [ ] 2.4 Example: one driven client per core in a plain `poll` loop
