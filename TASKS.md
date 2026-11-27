# TASKS.md

Ordered. Each task has done criteria. Stop at every STOP and wait for Ed.

Prerequisites from Ed (ask if missing):

- A Cloudberry or SynxDB cluster with as many segments as possible (8 or more makes the race convincing), reachable from where you run.
- Credentials in the `CB_*` env vars.
- The Asyncband 0.8.1 release candidate source tarball, extracted under `vendor/`.

## T0: Python spike (prove the speedup before building Rust)

- [x] Load the demo table with `trial/setup.sql`. Confirm rows are spread evenly across segments.
- [x] Write `spike/spike.py` (psycopg, one thread per endpoint) that runs a parallel retrieve and, separately, a coordinator fetch of the same query using a regular cursor in batches.
- [x] Print row counts, wall time, rows per second for both. Row counts must match.
- Done when: both paths return identical counts; results written to `spike/RESULTS.md` with cluster size and segment count.
- **STOP.** Ed decides whether the speedup justifies the Rust demo.

## T1: Get the existing Rust trial running

`trial/` is a first pass written without compiling. Asyncband calls were checked against the 0.8.0 docs. tokio-postgres calls (`Config::options`, `simple_query`, `SimpleQueryRow::get`) were written from memory and are the likeliest compile fixes.

- [x] Point `trial/Cargo.toml` at the vendored RC crate (the directory holding asyncband's own Cargo.toml).
- [x] Make it compile. Fix only what is needed. Record any Asyncband API difference from 0.8.0 in `docs/UPSTREAM.md`.
- [x] Run happy path: all endpoints finish, total equals `SELECT count(*)`, leak check passes.
- [x] Run with `FAIL_ENDPOINT=2`: that stream fails, others report cancelled, coordinator rolls back, leak check passes, nothing hangs.
- Done when: both runs behave as described. Paste outputs into `trial/RESULTS.md`.

## T2: Demo CLI skeleton

Build `demo/` (can start as a copy of `trial/`).

- [x] One binary, two modes: `--mode coordinator` (regular cursor, batched FETCH through the coordinator) and `--mode parallel` (parallel retrieve, Asyncband coordination).
- [x] Same query, same row count check in both modes.
- Done when: both modes run on the demo table and counts match.

## T3: Parquet output and Polars check

- [x] `--to out.parquet` writes results with existing crates (verify the arrow and parquet crate APIs first). Parallel mode may write one file per segment if that is simpler; say so in output.
- [x] A two-line Python check reads the output with Polars and prints the row count.
- Done when: Polars count equals the database count.

## T4: The race visuals

- [x] Parallel mode: one progress bar per segment, updating live (use an existing progress bar crate; verify its API).
- [x] Coordinator mode: one bar.
- [x] Side-by-side or back-to-back run script, `demo/race.sh`, ending with one comparison line: times and speedup.
- Done when: the race is readable in a standard terminal and finishes cleanly.

## T5: The failure act

- [ ] Client-side failure via `FAIL_ENDPOINT` shows on screen: failing bar turns red, others stop within a second, rollback message, leak check line.
- [ ] Investigate a real server-side failure (for example terminating one retrieve session's backend from another session). Document the exact method in `demo/FAILURE.md`; implement only if reliable.
- Done when: the failure run never hangs across 10 consecutive attempts.

## T6: Honest benchmark

- [ ] Three paths on the same table and cluster:
    1. Coordinator fetch.
    2. Parallel retrieve (this demo).
    3. The existing parallel file export path (gpfdist writable external table to the client host, or another parallel export the cluster supports). Ask Ed which one before building it.
- [ ] Table sizes: at least 10M and 100M rows.
- [ ] Write `bench/BENCH.md`: cluster spec, segment count, method, results table, anything odd. Plain writing, no dashes of the em or en kind.
- Done when: BENCH.md is complete and reproducible from a script in `bench/`.
- **STOP.** Ed reviews numbers against the kill criteria in docs/BACKGROUND.md.

## T7: Package the demo

- [ ] `demo/README.md`: what it shows, how to run, requirements (segment reachability), limits (scan and filter queries only).
- [ ] Record the race and the failure act as terminal recordings for talks, so a live failure on stage is never a risk.
- **STOP.** Ed decides next step: publish results, ConnectorX source design, or stop.

## Not now (recorded so nobody builds it early)

- ConnectorX source for Cloudberry using a two-layer design (Asyncband-based retrieve core plus thin ConnectorX source). See docs/BACKGROUND.md.
- Runtime-agnostic I/O, TLS, pooling, general query support, full SDK. See `archive/` for the superseded SDK plan.
