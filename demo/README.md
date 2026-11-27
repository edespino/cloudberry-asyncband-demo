# Parallel export from Cloudberry segments, coordinated with Asyncband

A small command line demo. It exports the result of one query from an Apache Cloudberry (or SynxDB) cluster in two ways and lets you watch both:

- **Through the coordinator.** One session, one cursor. This is the path pandas, Polars, JDBC, and ConnectorX use today.
- **Straight from the segments.** A parallel retrieve cursor, with one session per segment, all running at once.

Apache Asyncband coordinates the segment streams. Tokio and tokio-postgres do the network I/O.

This is throwaway demo code. It is not a product and not a library.

## What it shows

**The race.** One progress bar for the coordinator, then one bar per segment. The last line compares the two times.

```
coordinator 36.69s, parallel 7.70s across 8 segments: 4.8x faster
```

**The failure.** One segment stream dies in the middle of an export. Every other stream stops, the transaction rolls back, nothing hangs, and no connection is left open. This is where Asyncband is visible.

```
seg5 FAILED: connection closed
all 7 other streams stopped 0 ms after the failure
rolled back after failure
all guards released, no leaked retrieve connections
```

**Waiting for a turn.** With `MAX_OPEN=4` an Asyncband Semaphore lets four retrieve sessions run while the other four bars say "waiting for a permit".

## What it does not show

It does not beat gpfdist. On 100M rows, from a client on its own machine:

| Path | Seconds |
|---|---|
| Coordinator fetch | 38.26 |
| Parallel retrieve (this demo) | 7.64 |
| gpfdist writable external table | 6.13 |

So the honest claim is this: about 5 times faster than reading through the coordinator, and within 25 percent of gpfdist, with nothing to set up on the server. gpfdist needs a process on the client host, a writable external table, and segments that can connect back to the client. This needs an ordinary login and outbound connections from the client to the segments.

Every number, the cluster, and the method are in [`../bench/BENCH.md`](../bench/BENCH.md).

## Requirements

- **The client must reach every segment host and port.** The coordinator hands out segment host names and ports, and the client opens a connection to each one. From outside the cluster network this usually fails. In Kubernetes deployments segment pods are normally not reachable from outside. Run the demo from a machine inside the cluster network.
- A Cloudberry build with parallel retrieve cursors. Apache Cloudberry 2.2.0-incubating needs nothing extra. Some builds in the Greenplum family need `CREATE EXTENSION gp_parallel_retrieve_cursor` first.
- A login that can `SELECT` from the table. The retrieve sessions authenticate with a token the coordinator issues, not with a `pg_hba.conf` entry on the segments.
- Rust, to build. The first runs were built with cargo 1.100.0.
- The Asyncband 0.8.1 source extracted under `vendor/`, so that `vendor/apache-asyncband-0.8.1-incubating-src/asyncband` exists. `demo/Cargo.toml` points there.

## Limits

- **Scans and filters only.** A query that needs a gather step (a global `ORDER BY`, a final aggregate) produces a single endpoint on the coordinator and gains nothing. Check with `EXPLAIN`.
- No TLS, no connection pooling, no retries, no config file.
- Integers, floats, and booleans are typed in the Parquet output. Every other column, timestamps included, is written as the text the server sent.
- Parallel mode writes one Parquet file per segment, not one file.
- Tested on one cluster shape: 8 segments on two hosts, up to 100M rows of a three column table.

## Run it

Load the demo table (10M rows):

```
psql -d postgres -f trial/setup.sql
```

Build:

```
cd demo && cargo build --release && cd ..
```

Point it at the cluster:

```
export CB_HOST=<coordinator host> CB_PORT=5432 CB_USER=<user> CB_DB=postgres
```

The race:

```
demo/race.sh
```

Either mode alone, with or without Parquet output:

```
demo/target/release/asyncband-cloudberry-demo --mode coordinator
demo/target/release/asyncband-cloudberry-demo --mode parallel --to out.parquet
python demo/check.py 'out.*.parquet'      # Polars row count
```

The failure, caused by the client:

```
FAIL_ENDPOINT=2 FAIL_AFTER=4000000 demo/target/release/asyncband-cloudberry-demo --mode parallel
```

The failure, caused on the server by terminating one retrieve backend. See [`FAILURE.md`](FAILURE.md) for what it needs:

```
demo/failure.sh sdw1 6001 4
```

## Settings

| Variable | Meaning | Default |
|---|---|---|
| `CB_HOST` `CB_PORT` `CB_USER` `CB_PASSWORD` `CB_DB` | Coordinator connection | `localhost` `5432` `gpadmin` none `postgres` |
| `CB_QUERY` | The query to export | `SELECT * FROM retrieve_demo` |
| `MAX_OPEN` | Most retrieve sessions open at once | 64 |
| `BATCH` | Rows per `RETRIEVE` or `FETCH`, or `all` | 100000 |
| `FAIL_ENDPOINT` | Bar number of a stream to drop on purpose | not set |
| `FAIL_AFTER` | Rows that stream reads before it is dropped | 0 |
| `SUMMARY_FILE` | File to append "mode seconds rows streams" to | not set |

No credentials go in code or in files that are committed.

## Recordings

So that a talk never depends on a live run, the three scenes are recorded as asciinema casts, 80 columns by 24 rows, in [`recordings/`](recordings):

| File | Scene | Length |
|---|---|---|
| `race.cast` | `demo/race.sh` on 100M rows | 47 s |
| `permits.cast` | `MAX_OPEN=4`, four bars wait for a permit | 16 s |
| `failure.cast` | `demo/failure.sh`: one retrieve backend is terminated 4 seconds in | 6 s |

Play one with `asciinema play demo/recordings/race.cast`. `demo/record.sh` makes them again.

They were recorded on 2026-11-27 from a client machine in the cluster network, against 8 segments on two hosts. The numbers in them are from that run and differ a little from the benchmark tables.

## Where Asyncband is used

Only in parallel mode, and only for coordination:

- `Semaphore` caps how many retrieve sessions are open (`MAX_OPEN`).
- `shutdown` carries cancellation. The stream that fails requests shutdown. Every other stream is waiting on that request beside its `RETRIEVE` and stops when it comes.
- Each retrieve connection holds a `ShutdownGuard` until its socket is closed. Awaiting `Shutdown` at the end returns only when all of them are gone. That is the "no leaked retrieve connections" line.

Asyncband adds no measurable time to an export. It does not make anything faster. Its job here is the failure path.

## Files

| File | What it is |
|---|---|
| `src/main.rs` | The whole client |
| `race.sh` | Coordinator, then parallel, then one comparison line |
| `failure.sh`, `kill-one.sh` | The server side failure |
| `FAILURE.md` | How both failures work and what was tested |
| `check.py` | Polars row count of the Parquet output |
| `record.sh`, `recordings/` | The terminal recordings |
