# Asyncband findings

Notes from building against the Asyncband 0.8.1 release candidate (commit `1cadff0ab35723be2fd50d7187c1dc3fb93d1a41`, source archive `apache-asyncband-0.8.1-incubating-src.tar.gz`). For Ed. Nothing here has been sent upstream or to the vote thread.

## T1, 2026-11-27

### API differences from the 0.8.0 docs

None found in what the trial uses. `trial/src/main.rs` was written against the 0.8.0 docs and compiled against the release candidate without a single Asyncband change.

Checked in the vendored source:

- `asyncband::semaphore::Semaphore::new(permits: usize)`
- `Semaphore::acquire_owned(self: Arc<Self>, permits: usize) -> OwnedSemaphorePermit`
- `asyncband::shutdown::new() -> (Shutdown, ShutdownGuard)`
- `Shutdown::request_shutdown(&self)`, and `Shutdown` as a future
- `ShutdownGuard::is_shutdown_requested(&self) -> bool`
- `ShutdownGuard::shutdown_requested(&self)` (async)
- `Shutdown` and `ShutdownGuard` are both `Clone`
- Feature `shutdown` enables `latch` and `waitgroup`, as `asyncband/Cargo.toml` states

The 0.8.1 section of `CHANGELOG.md` lists no change to `semaphore` or `shutdown` behavior. It does list a speedup for `WaitGroup`, which `shutdown` is built on.

Not checked: the 0.8.0 docs were not read side by side with the release candidate source. `ShutdownWatch`, `ShutdownGuard::watch`, `ShutdownGuard::into_watch`, and `shutdown_requested_owned` are in the release candidate. The trial does not use them, and whether each one was already in 0.8.0 was not confirmed.

### Observations from real use (not defects)

1. **Awaiting `Shutdown` always requests shutdown.** The first poll calls `request_shutdown`. There is no way to use a `Shutdown` handle to wait for the guards to drop without also telling every worker to stop. The docs say this clearly. In the trial it means the "all guards released" check can only run after the work is finished, when the request is harmless. A caller who wants "wait for workers, do not cancel them" has to use `WaitGroup` directly.

2. **A guard held only by the worker task proves little.** In the first version of the trial each worker task owned one guard and the check ran after every task was joined, so it could not fail. The trial now also gives a guard clone to each spawned connection task, so the check waits for every retrieve connection to close. This is a usage point, not an Asyncband problem, but an example in the docs showing guards held by the resource (a connection, a file) and not only by the task would have pointed to the stronger pattern.

3. **Build.** The crate uses workspace inheritance (`edition.workspace = true` and others), so a path dependency has to point inside the full extracted source tree. Pointing at a copy of the `asyncband/` directory alone would not build. Edition 2024, `rust-version` 1.86.0.
