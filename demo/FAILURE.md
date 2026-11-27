# The failure act

One segment stream fails in the middle of an export. Every other stream stops, the coordinator transaction rolls back, nothing hangs, and no connection is left open. There are two ways to cause the failure. Both run on real data.

Tested on 2026-11-27 on Apache Cloudberry 2.2.0-incubating, 8 primary segments on two hosts, 100,000,000 rows, client on the coordinator host.

## 1. Client side: `FAIL_ENDPOINT`

The client drops one stream on purpose.

```
CB_QUERY='SELECT * FROM retrieve_demo_100m' FAIL_ENDPOINT=2 FAIL_AFTER=4000000 \
    demo/target/release/asyncband-cloudberry-demo --mode parallel
```

- `FAIL_ENDPOINT` is the bar number (`seg2`).
- `FAIL_AFTER` is how many rows that stream reads first. Without it the stream fails after one batch, which is too fast to see.

On screen: the `seg2` bar turns red and says FAILED. The other seven bars turn yellow and say cancelled. Then:

```
seg2 FAILED: injected failure after 4000000 rows; shutdown requested, other streams cancelled
all 7 other streams stopped 0 ms after the failure
coordinator: canceling MPP operation: "Endpoint retrieve session is quitting. All unfinished parallel retrieve cursors on the session will be terminated."  (seg4 10.0.1.187:6000 pid=35703)
rolled back after failure
all guards released, no leaked retrieve connections
export failed, nothing committed
```

Exit code 1.

Ten runs in a row, a different segment and row count each time, each under a 60 second limit: all ten exited 1, none hit the limit. Afterward the server had no endpoints, no open transactions, and no retrieve sessions.

## 2. Server side: terminate one retrieve backend

A second session terminates the backend that serves one retrieve session. The client does nothing special. It sees its connection die.

Exact method:

1. Open a session straight to one segment in utility mode: `PGOPTIONS='-c gp_role=utility' psql -h <segment host> -p <segment port> -d postgres`.
2. Find the retrieve session there: `SELECT pid FROM pg_stat_activity WHERE query ILIKE 'retrieve%' AND pid <> pg_backend_pid();`
3. `SELECT pg_terminate_backend(<pid>);`

`demo/kill-one.sh` does these three steps. It waits for a retrieve session to appear, lets it run for a few seconds, then terminates it:

```
demo/kill-one.sh sdw1 6001 3 &
CB_QUERY='SELECT * FROM retrieve_demo_100m' \
    demo/target/release/asyncband-cloudberry-demo --mode parallel
```

On screen: the `seg5` bar turns red, the other seven turn yellow. Then:

```
seg5 FAILED: connection closed; shutdown requested, other streams cancelled
all 7 other streams stopped 1 ms after the failure
coordinator: canceling MPP operation: "Endpoint retrieve session is quitting. All unfinished parallel retrieve cursors on the session will be terminated."  (seg1 10.0.1.179:6001 pid=236128)
rolled back after failure
all guards released, no leaked retrieve connections
export failed, nothing committed
```

Exit code 1. The reason text is either `connection closed` or `terminating connection due to administrator command`, depending on whether the server's message arrives before the socket closes.

This was run 8 times against 8 different segments on both hosts, with delays from 1 to 5 seconds. Every run failed the targeted stream, cancelled the other seven within 16 ms, rolled back, and exited 1. With `--to`, the partial Parquet files were removed. It is reliable enough to use in the demo.

Requirements: run it as the database owner from a cluster host. A utility mode session to a segment is an administrator action and is not something an ordinary client can do.

## What Asyncband does here

- The failing task calls `Shutdown::request_shutdown()`.
- Every other task is waiting in a `select!` on `ShutdownGuard::shutdown_requested()` beside its `RETRIEVE`. It wakes, drops its session, and returns.
- Tasks still waiting for a Semaphore permit check `is_shutdown_requested()` when they get one and never open a session.
- Each retrieve connection task holds a `ShutdownGuard` until its socket is closed. Awaiting `Shutdown` at the end returns only when all of them are gone. That is the "no leaked retrieve connections" line. It has a 5 second limit and prints `LEAK` if it is exceeded.

## What Cloudberry does here

When a retrieve session ends before its endpoint is finished, Cloudberry cancels the whole parallel retrieve cursor on the coordinator session. The coordinator reports that as an error (SQLSTATE 25M01) on the next statement the client sends. So the first `ROLLBACK` fails with that message and the client sends `ROLLBACK` again. The `coordinator:` line on screen is that message.

## Not tested

- The `LEAK` line has never been seen. No run held a connection open on purpose.
- A segment host going away (network loss, instance stop). Only a terminated backend was tested.
- A failure of the coordinator session itself.
- The stop times are measured inside the client, from the failure to the last task returning. They are not a measure of when the server freed its resources. The server was checked a second or more later.
