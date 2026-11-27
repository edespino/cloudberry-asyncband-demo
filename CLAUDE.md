# CLAUDE.md

Read this file, then `docs/BACKGROUND.md` (full context and every decision made so far), then `TASKS.md`. Work tasks in order and stop at every STOP.

## What this is

A fast, throwaway demo: parallel export of query results straight from Apache Cloudberry / SynxDB segments, using Cloudberry's parallel retrieve cursors, with Apache Asyncband coordinating the concurrent streams.

The demo has two acts:

1. **The race.** One progress bar per segment filling at once, beside a single bar crawling through the coordinator. Final line compares times. Output lands in Parquet and opens in Polars.
2. **The failure.** One segment stream fails mid-export. Every other stream stops promptly, the transaction rolls back, nothing hangs, no connection leaks. This act is where Asyncband is visible.

Owner: Ed (technical co-founder, Synx Data Labs). Ed is also reviewing the Asyncband 0.8.1 release vote, so the first runs build against the 0.8.1 release candidate source.

## Ground rules

1. **Real Cloudberry only.** Never simulate segments or fake database behavior. A deliberate client-side failure switch (`FAIL_ENDPOINT`) on real data is allowed.
2. **Verify before you call.** Read docs.rs (or the vendored source) for every crate before writing code against it. Never guess a function, feature flag, trait, or signature. When docs and memory disagree, the docs win. In every task summary, list what you verified and what you could not.
3. **Demo code, not product code.** Optimize for speed and what shows on screen. Error handling may be "print and exit." No config files, pooling, retries, TLS variety, or general query support. If the audience cannot see it, do not build it.
4. **Tokio for I/O, Asyncband for coordination.** Tokio and tokio-postgres handle networking. All concurrency coordination uses Asyncband: Semaphore for capping open sessions, `shutdown` for cancellation and leak checking, plus WaitGroup or Latch if needed. Do not use `tokio::sync` for coordination. If Asyncband lacks something, record it in `docs/UPSTREAM.md` and ask before working around it.
5. **Asyncband source.** Build against the extracted release candidate under `vendor/` (path dependency) while the 0.8.1 vote is open. Afterward, pin the released version exactly (`=x.y.z`). Any API difference from the 0.8.0 docs goes in `docs/UPSTREAM.md`; it may matter for the vote.
6. **No credentials in code.** Use env vars: `CB_HOST`, `CB_PORT`, `CB_USER`, `CB_PASSWORD`, `CB_DB`, `CB_QUERY`, `MAX_OPEN`, `FAIL_ENDPOINT`.
7. **Naming.** No "apache" in any crate, binary, or repo name. Apache is an ASF trademark.
8. **License.** Apache License 2.0 for anything written here.
9. **Writing style for anything Ed reads** (READMEs, BENCH.md, summaries, commit messages): never use em dashes or en dashes. Write plainly; avoid generic AI phrasing.
10. **Throwaway means throwaway.** If the demo succeeds, the next step is a proper rewrite, not polishing this code. Do not add product features.

## Parallel retrieve protocol (from Cloudberry and Greenplum docs)

1. Coordinator: `BEGIN`. The cursor must live inside a transaction block.
2. `DECLARE <name> PARALLEL RETRIEVE CURSOR FOR <query>;` Returns when endpoints are ready. `WITH HOLD` unsupported; `BINARY` ignored.
3. Endpoints: `SELECT hostname, port, auth_token, endpointname, username FROM gp_endpoints WHERE cursorname = '<name>';` (Some docs and the C example use `gp_get_endpoints()`.)
4. Per endpoint, a separate connection to `hostname:port`, user = `username` (the declaring session user), password = the endpoint `auth_token`, startup option `-c gp_retrieve_conn=true`. Only one retrieve session per endpoint at a time.
5. `RETRIEVE <count> FROM ENDPOINT <endpointname>;` repeatedly until zero rows, or `RETRIEVE ALL`.
6. Optional on the coordinator: `gp_wait_parallel_retrieve_cursor(cursorname text)` or `(cursorname text, timeout_sec int4)` reports status or blocks until all endpoints finish; returns true only when every endpoint is fully retrieved.
7. Finish: `CLOSE <name>; COMMIT;`. If the transaction ends early, remaining retrieve sessions are terminated.
8. Endpoint states in `gp_endpoints.state`: READY, ATTACHED, RETRIEVING, FINISHED, RELEASED.

Reference C program: `src/test/examples/test_parallel_retrieve_cursor_wait.c` in github.com/apache/cloudberry.

Known limits to respect in the demo:

- Queries with a gather motion (global ORDER BY, final aggregates) produce a single coordinator endpoint. Use plain scans and filters.
- The client must reach every segment host and port. Run from the coordinator host or inside the cluster network first.
- Retrieve auth uses the token, not pg_hba.conf.
- Some Greenplum-lineage builds need `CREATE EXTENSION gp_parallel_retrieve_cursor` first.

## Layout

```
CLAUDE.md             this file
TASKS.md              ordered tasks with done criteria
docs/BACKGROUND.md    full context, research, decisions, open questions
docs/UPSTREAM.md      Asyncband gaps or API differences found (create when needed)
trial/                existing first-run code: Rust, real Cloudberry, Asyncband RC
spike/                Python spike (task T0)
demo/                 the demo CLI (tasks T2 onward; may grow out of trial/)
bench/                BENCH.md and scripts
vendor/               extracted Asyncband 0.8.1 RC source (not committed)
archive/              superseded SDK plan, for reference only
```

## Commands

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo build --release
```

Every network wait in code has a timeout or a cancellation path. A hang is a failure.

## Working style

- One task per commit, message starts with the task id (`T2: per-segment progress bars`).
- End each task with: what was built, what was verified and how, what is unverified, anything added to `docs/UPSTREAM.md`.
- Tick the checkbox in TASKS.md when done criteria pass.
