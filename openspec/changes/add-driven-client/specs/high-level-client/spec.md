## ADDED Requirements

### Requirement: Client driven by the caller's event loop
The library SHALL provide a client that starts no thread, exposes a file
descriptor that is readable when the client has work to do, and does that
work in a call that never blocks, so that a caller with one event loop per
core can run a client on each core.

#### Scenario: Driven by polling
- **WHEN** a caller builds such a client, starts a store and a fetch, and calls `turn` each time the descriptor is readable
- **THEN** both operations complete and no thread was started by the library

#### Scenario: Nothing to do
- **WHEN** `turn` is called while the client has no completions to process
- **THEN** it returns at once
