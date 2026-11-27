# CLAUDE.md

## Project

A Rust client SDK for SynxDB and Apache Cloudberry. The core feature is parallel retrieve cursors: fetching query results directly from segment endpoints in parallel, bypassing the coordinator. All internal concurrency uses Apache Asyncband. The library must run on any executor that polls standard Rust futures.

Working crate name: `synxdb-client` (placeholder). Do not put "apache" in any crate, package, or binary name. "Apache" is an ASF trademark and the final name needs Cloudberry PPMC review.

Work through `TASKS.md` in order. Stop at every point marked STOP and wait for Ed.

## Hard rules

1. **Verify before you call.** Read docs.rs for `asyncband` and for every other crate before writing code against it. Never guess a function name, feature flag, trait, or signature. When docs and your memory disagree, the docs win. If you cannot verify something, say so in your summary instead of writing around it.
2. **No runtime lock-in in library code.** No `tokio::`, `smol::`, or `async_std::` imports in `crates/core`. Runtime glue lives only in `crates/adapters`, `bench/`, tests, and examples.
3. **Asyncband for all coordination.** Locks, semaphores, wait groups, latches, channels, shutdown, pooling, one-time init. If Asyncband lacks something you need, stop and record the gap in `UPSTREAM.md` (what, why, proposed API). Do not hand-roll a replacement without asking. Gaps become upstream contributions.
4. **Pin Asyncband exactly.** It is pre-1.0. Use `=x.y.z` in Cargo.toml and record the version in README.
5. **Apache License 2.0.** Every source file carries the Apache 2.0 header. No code copied from GPL or unknown-license sources. List every dependency and its license in `DEPENDENCIES.md`. The code may later be donated to the ASF, so provenance must stay clean.
6. **No hardcoded credentials.** Connection info comes from env vars: `SYNX_HOST`, `SYNX_PORT`, `SYNX_USER`, `SYNX_PASSWORD`, `SYNX_DB`.
7. **Ask first** before adding a dependency not already in Cargo.toml, changing public API after the Phase 1 freeze, or contacting any host other than the test cluster.

## Parallel retrieve cursor protocol

From the Cloudberry docs. Confirm every step against the reference C program before implementing Phase 2.

1. On the coordinator connection, `BEGIN`. A cursor outside a transaction block is an error.
2. `DECLARE <name> PARALLEL RETRIEVE CURSOR FOR <query>;` (`WITH HOLD` is not supported; `BINARY` is ignored.)
3. Discover endpoints: `SELECT hostname, port, auth_token, endpointname, username FROM gp_endpoints WHERE cursorname = '<name>';` The C example uses `gp_get_endpoints()` instead; check which the target versions support.
4. For each endpoint, open a separate connection to `hostname:port` as `username` (the session user), with password = `auth_token` and startup option `-c gp_retrieve_conn=true`.
5. On each retrieve connection: `RETRIEVE ALL FROM ENDPOINT <endpointname>;` or `RETRIEVE <count> FROM ENDPOINT ...` for batches. One retrieve session may read multiple endpoints.
6. Endpoints exist only for the life of the transaction or until the cursor closes. After all retrieves finish: `CLOSE <name>; COMMIT;` on the coordinator.
7. Endpoint states in `gp_endpoints.state`: READY, ATTACHED, RETRIEVING, FINISHED, RELEASED (RELEASED means an error closed it).

Reference implementation: `src/test/examples/test_parallel_retrieve_cursor_wait.c` in github.com/apache/cloudberry. Read it fully before Phase 2. It also shows a wait-mode monitor the coordinator session runs while retrieves proceed; find the exact function it calls rather than assuming.

Docs: cloudberry.apache.org/docs/sql-stmts/declare, /retrieve, and /sys-catalogs/sys-views/gp-endpoints.

## Layout

```
Cargo.toml            workspace
crates/core/          protocol, connection, pool, retrieve (runtime agnostic)
crates/adapters/      runtime glue, feature gated (tokio first, then smol)
bench/                Phase 0 benchmark harness
tests/                integration tests against a live cluster
TASKS.md              task list with acceptance criteria
UPSTREAM.md           Asyncband gaps and proposed patches
DEPENDENCIES.md       dependency and license list
```

## Commands

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
SYNX_IT=1 cargo test --test integration   # needs a live cluster
```

All four must pass before a task is marked done.

## Working style

- One task per branch and PR. Commit messages start with the task id, e.g. `P1.2: SCRAM-SHA-256 auth`.
- Tick the checkbox in `TASKS.md` when acceptance criteria pass.
- End each task with a short summary: what was built, what was verified and how, what is unverified, any `UPSTREAM.md` entries added.
- Every integration test that waits on the network has a timeout. A hang is a failure.
