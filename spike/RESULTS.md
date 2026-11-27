# T0 spike results

Run on 2026-11-27 with `spike/spike.py`.

## Result

Row counts match on every run: 10,000,000 from `count(*)`, from the coordinator fetch, and from the parallel retrieve.

Best of 3 runs per path, 10,000 rows per batch:

| Path | Workers | Best time | Rows per second | Speedup |
|---|---|---|---|---|
| Coordinator fetch | one session | 8.87 s | 1,127,065 | 1.00x |
| Parallel retrieve | one thread per endpoint | 5.15 s | 1,941,875 | 1.72x |
| Parallel retrieve | one process per endpoint | 3.72 s | 2,686,417 | 2.38x |

The cluster has 3 segments, so 3x is the ceiling here. Threads reach 1.72x and processes reach 2.38x. The gap between the two is the Python interpreter lock, not the database: each thread spends most of its time turning rows into Python objects, and only one thread can do that at a time.

## Cluster

- Apache Cloudberry 2.2.0-incubating (PostgreSQL 14.10), build 13d3c58eaa0.
- One host, AWS c7i.4xlarge (16 vCPU, 30 GB), Rocky Linux 9. The environment has two instances, but this cluster uses only the first.
- A gpdemo cluster: coordinator, standby, 3 primary segments, 3 mirror segments, all on that one host.
- Segment count: 3.
- The client ran on the same host. The environment has no inbound access, so the laptop cannot reach segment ports.
- Other containers (Texera services, litellm) were running on the host during the runs. Load average before the first run was 0.81.

## Table and query

- `trial/setup.sql`: `retrieve_demo`, 10,000,000 rows, `DISTRIBUTED BY (id)`.
- Rows per segment: 3,333,278 / 3,331,883 / 3,334,839.
- Query: `SELECT * FROM retrieve_demo`.
- `DECLARE ... PARALLEL RETRIEVE CURSOR` produced 3 endpoints, one per primary segment, on ports 7002 to 7004. Each endpoint returned exactly its segment's row count.

## Method

- Coordinator path: `BEGIN`, `DECLARE ... NO SCROLL CURSOR`, `FETCH 10000` until empty, `CLOSE`, `COMMIT`.
- Parallel path: `BEGIN`, `DECLARE ... PARALLEL RETRIEVE CURSOR`, read `gp_endpoints`, one worker per endpoint running `RETRIEVE 10000 FROM ENDPOINT` until empty, then `gp_wait_parallel_retrieve_cursor(name, 5)`, `CLOSE`, `COMMIT`.
- Both paths pull every row into Python as text and count it. Nothing is written to disk.
- Timing covers DECLARE through the last row. It excludes connecting to the coordinator.
- The paths alternate, coordinator first, 3 runs each.
- psycopg 3.2.13 (binary build), Python 3.9.25.

## All runs

Thread workers:

```
run 1 coordinator  rows=10000000     9.10s     1,098,614 rows/s
run 1 parallel     rows=10000000     5.15s     1,941,875 rows/s  endpoints=[3333278, 3331883, 3334839]  all_finished=True
run 2 coordinator  rows=10000000     8.87s     1,126,768 rows/s
run 2 parallel     rows=10000000     5.37s     1,862,435 rows/s  endpoints=[3333278, 3331883, 3334839]  all_finished=True
run 3 coordinator  rows=10000000     8.87s     1,127,065 rows/s
run 3 parallel     rows=10000000     5.16s     1,937,139 rows/s  endpoints=[3333278, 3331883, 3334839]  all_finished=True

best coordinator 8.87s, best parallel 5.15s, speedup 1.72x
row counts match
```

Process workers (`SPIKE_WORKERS=process`):

```
run 1 coordinator  rows=10000000     9.12s     1,095,963 rows/s
run 1 parallel     rows=10000000     4.27s     2,342,404 rows/s  endpoints=[3333278, 3331883, 3334839]  all_finished=True
run 2 coordinator  rows=10000000     8.89s     1,125,381 rows/s
run 2 parallel     rows=10000000     3.98s     2,510,270 rows/s  endpoints=[3333278, 3331883, 3334839]  all_finished=True
run 3 coordinator  rows=10000000     8.85s     1,130,309 rows/s
run 3 parallel     rows=10000000     3.72s     2,686,417 rows/s  endpoints=[3333278, 3331883, 3334839]  all_finished=True

best coordinator 8.85s, best parallel 3.72s, speedup 2.38x
row counts match
```

## What this does and does not show

Shown:

- The protocol works end to end on stock Cloudberry 2.2.0. No extension was needed.
- A retrieve session connects with the endpoint token as the password and `-c gp_retrieve_conn=true` as a startup option.
- `gp_wait_parallel_retrieve_cursor` returned true after every parallel run.
- Parallel retrieve is faster than the coordinator fetch on this cluster, and the speedup follows the number of segments when the client is not the limit.

Not shown:

- Anything about a real network. Client, coordinator, and segments share one host, so no path pays a network cost, and all of them compete for the same 16 vCPUs.
- Anything beyond 3 segments. TASKS.md asks for 8 or more for a convincing race.
- Where the coordinator fetch is limited. It may be the single Python client as much as the coordinator. CPU use per process was not measured.
- A comparison with an existing parallel export path such as gpfdist. That is T6.
- Larger tables. Only 10M rows were run.

## Reproduce

On the cluster host, with the Cloudberry environment sourced:

```
psql -d postgres -f trial/setup.sql
python3 -m venv venv && venv/bin/pip install 'psycopg[binary]'
export CB_HOST=localhost CB_PORT=7000 CB_USER=rocky CB_DB=postgres
venv/bin/python spike/spike.py
SPIKE_WORKERS=process venv/bin/python spike/spike.py
```
