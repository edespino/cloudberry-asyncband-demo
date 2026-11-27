# T1 trial results

Run on 2026-11-27 on the same cluster as `spike/RESULTS.md`: Apache Cloudberry 2.2.0-incubating, one c7i.4xlarge host, 3 primary segments, 10,000,000 rows in `retrieve_demo`. The binary ran on the cluster host.

Asyncband is the 0.8.1 release candidate source, extracted under `vendor/`. The sha512 of the archive matches the published `.sha512` file. The signature was not verified: the signing key (RSA `023A42E7299A010FEE7AE021393AFB9D52BB37B8`) is not in the local keyring.

Build: `cargo build --release --locked`, cargo 1.100.0 on the host. `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass (cargo 1.97.1 on the laptop).

## Happy path

`SELECT count(*) FROM retrieve_demo` returns 10000000.

```
$ CB_HOST=localhost CB_PORT=7000 CB_USER=rocky CB_DB=postgres trial/target/release/asyncband-cloudberry-trial
3 endpoints, at most 64 open at once
endpoint 1 cdw:7003  3331883 rows in 2.46s
endpoint 0 cdw:7002  3333278 rows in 2.54s
endpoint 2 cdw:7004  3334839 rows in 2.55s
10000000 rows in 2.67s
all guards released, no leaked retrieve connections

real    0m2.675s
user    0m2.339s
sys     0m0.649s
```

Exit code 0. The total equals `count(*)`. Each endpoint count equals its segment's row count.

## Failure path

```
$ FAIL_ENDPOINT=2 trial/target/release/asyncband-cloudberry-trial
3 endpoints, at most 64 open at once
endpoint 2 cdw:7004  FAILED: injected failure after first batch; requesting shutdown
endpoint 1  cancelled, session closed
endpoint 0  cancelled, session closed
coordinator: canceling MPP operation: "Endpoint retrieve session is quitting. All unfinished parallel retrieve cursors on the session will be terminated."  (seg1 10.0.1.179:7003 pid=79740)
rolled back after failure
all guards released, no leaked retrieve connections

real    0m0.049s
user    0m0.010s
sys     0m0.010s
```

Exit code 0. The whole run ends 49 ms after start.

Five more failure runs, with `FAIL_ENDPOINT` set to 1, 2, 0, 1, 2, each under a 60 second limit: all exited 0. None hung.

After all runs, checked from a new session:

- `gp_endpoints`: 0 rows.
- Sessions idle in a transaction: 0.
- Sessions on any segment with a `RETRIEVE` query: 0.

## Semaphore cap

`MAX_OPEN=1` makes the Asyncband Semaphore admit one retrieve session at a time:

```
$ MAX_OPEN=1 trial/target/release/asyncband-cloudberry-trial
3 endpoints, at most 1 open at once
endpoint 0 cdw:7002  3333278 rows in 2.29s
endpoint 1 cdw:7003  3331883 rows in 1.85s
endpoint 2 cdw:7004  3334839 rows in 2.04s
10000000 rows in 6.30s
all guards released, no leaked retrieve connections
```

Exit code 0. One at a time takes 6.30 s; all three at once takes 2.67 s. Same client, same data, so this 2.36x is a like for like figure for what running the streams together buys on this host.

`MAX_OPEN=1 FAIL_ENDPOINT=0` also exits 0: endpoint 0 fails, the two tasks still waiting on the Semaphore never open a session, the coordinator rolls back.

## What changed in the trial

The trial compiled as written. No Asyncband or tokio-postgres call needed a fix. Three changes were made anyway:

1. **Connect timeouts.** Both the coordinator and the retrieve connections now set a 10 second `connect_timeout`.
2. **A leak check that covers connections.** Each spawned connection task now holds its own `ShutdownGuard` clone until the socket closes. Before this, only the worker tasks held guards and the check ran after they were joined, so it could not fail.
3. **Rollback after a failure.** The first failure run exited with an error. When a retrieve session quits early, Cloudberry cancels the cursor on the coordinator session, and that error (SQLSTATE 25M01) comes back as the reply to the next statement, which was `ROLLBACK`. The trial now prints that message and sends `ROLLBACK` again.

## Not verified

- The leak check was never seen to fail. No run was made with a connection deliberately held open to prove it reports a leak.
- A failure of the first kind only: the client drops one stream. A server side failure is T5.
- The 2.67 s figure is not comparable with the Python spike numbers. The Rust client counts rows without converting them, and there is no Rust coordinator fetch yet to compare against. That is T2.
