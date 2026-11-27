# TASKS.md

Work top to bottom. Each task lists what "done" means. Stop at every STOP.

## Phase 0: Prove the payoff

Goal: measure whether parallel retrieve beats coordinator fetch by enough to justify the SDK. This phase may use an existing Tokio-based Postgres driver; it is throwaway measurement code, not the SDK.

- [ ] **P0.1 Benchmark harness** in `bench/`.
  - Mode A: one coordinator connection, `SELECT ...` streaming all rows.
  - Mode B: parallel retrieve cursor, one connection per segment endpoint, all retrieved concurrently.
  - Parameters: row count (1M, 10M, 100M), row width (narrow, wide), number of segments in the cluster.
  - Setup script creates and loads test tables, distributed across all segments.
  - Output: CSV with mode, rows, width, segments, wall time, rows per second.
  - Done when: both modes return identical row counts and checksums for every size.
- [ ] **P0.2 Results report** in `bench/BENCH.md`: table of results, cluster spec, method, and anything that looked wrong.
- **STOP.** Ed makes the go or no-go call from BENCH.md.

## Phase 1: Coordinator client on any runtime

- [ ] **P1.1 I/O spike** (timebox 3 days). Decide how `crates/core` does network I/O without a runtime: a `futures-io` style stream trait, a crate-defined transport trait, or another option. Prove one connect-and-query path runs under both Tokio and smol. Write the decision and tradeoffs in `docs/SPIKE.md`.
- **STOP.** Ed reviews SPIKE.md.
- [ ] **P1.2 Wire protocol.** Startup, authentication (cleartext, MD5, SCRAM-SHA-256), simple query, extended query, error and notice handling. Evaluate the `postgres-protocol` crate for message encoding before writing your own; verify it has no runtime dependency.
  - Done when: unit tests cover each message type; integration test connects and queries.
- [ ] **P1.3 TLS** via rustls, feature gated.
- [ ] **P1.4 Pool** built on Asyncband (pool and semaphore). Configurable max size, acquire timeout, health check on checkout.
  - Done when: a test with 100 concurrent tasks and pool size 10 never exceeds 10 open connections.
- [ ] **P1.5 Conformance suite.** Same integration tests pass against SynxDB and upstream Cloudberry 2.1, on Tokio and on smol (4 combinations).
- **STOP.** Public API freeze review with Ed.

## Phase 2: Parallel retrieve

- [ ] **P2.1 Read the reference C program** (see CLAUDE.md). Write `docs/RETRIEVE.md` describing the exact sequence, including the wait-mode monitor, and any differences from the docs.
- [ ] **P2.2 Declare and discover.** API to declare a parallel retrieve cursor inside a transaction and return typed endpoint descriptors.
- [ ] **P2.3 Fan out.** Open retrieve sessions concurrently, capped by an Asyncband semaphore. Offer two consumer APIs: one stream per segment, and one merged stream. Rows flow through Asyncband channels with bounded capacity for backpressure.
- [ ] **P2.4 Completion and cancellation.** Use Asyncband wait group or latch for completion and Shutdown for cancellation. On error or on drop of the result stream: close all retrieve connections, close the cursor, roll back. No leaked connections.
- [ ] **P2.5 Fault tests.** Kill a segment or drop a retrieve connection mid-stream. The error surfaces to the caller within a set timeout; nothing hangs; coordinator transaction is cleaned up.
- [ ] **P2.6 Reproduce P0.** Re-run the Phase 0 benchmark through the SDK. Report results next to the P0 numbers in BENCH.md.
- **STOP.** Review before Phase 3 (docs, examples, crates.io release).

## Ongoing

- Every Asyncband gap or bug goes in `UPSTREAM.md` the moment it is found.
- Keep `DEPENDENCIES.md` current.
