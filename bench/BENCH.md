# T6 benchmark

Run on 2026-11-27 with `bench/bench.sh`, three times:

1. First demo client, run on the coordinator host.
2. Tuned demo client, run on the coordinator host.
3. Tuned demo client, run on a third machine that holds no part of the database.

Every one of the 48 runs in each pass exported the full row count. Pass 3 is the fairest and is the one reported first.

## Result

Client on a third machine, best of 3 runs, seconds:

| Path | 10M discard | 10M to disk | 100M discard | 100M to disk |
|---|---|---|---|---|
| 1. Coordinator fetch (demo binary) | 3.77 | 8.54 | 38.26 | 92.49 |
| 2. Parallel retrieve (demo binary) | 1.03 | 1.50 | 7.64 | 12.26 |
| 3. gpfdist writable external table | 0.84 | 0.99 | 6.13 | 6.04 |
| 4. COPY ON SEGMENT | 0.82 | 0.91 | 3.87 | 3.77 |

Rows per second on 100M rows, discard: coordinator 2.6M, parallel retrieve 13.1M, gpfdist 16.3M, COPY ON SEGMENT 25.8M.

Ratios on 100M rows, discard:

- Parallel retrieve against coordinator fetch: 5.0 times faster.
- gpfdist against parallel retrieve: 1.25 times faster.
- COPY ON SEGMENT against parallel retrieve: 2.0 times faster.

Against the kill criterion in `docs/BACKGROUND.md` ("stop if parallel retrieve does not clearly beat the existing parallel export path"): parallel retrieve does not beat gpfdist in any of the three passes. From a separate client machine the gap is 25 percent. With the client on the coordinator host it was 65 percent.

## What this means

The claim "faster than the existing parallel export" is not supported. The claim that is supported:

- **Against the coordinator, which is what a notebook user gets today:** 5 times faster on 100M rows. pandas, Polars, JDBC, and ConnectorX all read through the coordinator.
- **Against gpfdist:** 25 percent slower from a separate client machine, and it needs nothing set up on the server. gpfdist needs a process running on the client host, a writable external table (so the right to create one), and segments that can open connections back to the client. Parallel retrieve needs an ordinary login and outbound connections from the client to the segments.

So the honest pitch is not a new fastest path. It is most of the speed of the parallel path, from a plain client connection, for someone who cannot or will not set up gpfdist. That is the notebook user in `docs/BACKGROUND.md`.

The requirement that does not go away: the client must be able to reach every segment host and port.

## What each path does

1. **Coordinator fetch.** `demo --mode coordinator`. One session, `DECLARE CURSOR`, `FETCH 100000` until empty. Every row passes through the coordinator.
2. **Parallel retrieve.** `demo --mode parallel`. `DECLARE PARALLEL RETRIEVE CURSOR`, then one retrieve session per segment, `RETRIEVE 100000` until empty, all 8 at once. Asyncband coordinates the streams.
3. **gpfdist.** A writable external table with `LOCATION ('gpfdist://cdw:8081/...')`, `FORMAT 'TEXT'`, distributed by the same key as the source. `INSERT INTO ... SELECT * FROM` the table. Each segment pushes its rows over HTTP to one gpfdist process on the client host. The plan has no redistribute step.
4. **COPY ON SEGMENT.** `COPY <table> TO ... ON SEGMENT`. Each segment writes on its own host. Nothing is delivered to the client. It is here as a ceiling: how fast the segments can produce the rows when no client is involved.

Paths 1 to 3 deliver the data to the client host. Path 4 does not.

## Sinks

- **Discard.** Nothing reaches a disk. The demo binary reads and counts rows. gpfdist writes to a file that is a link to `/dev/null`. COPY ON SEGMENT uses `TO PROGRAM 'cat > /dev/null'`.
- **To disk.** The demo binary writes Parquet with snappy compression (one file in coordinator mode, one per segment in parallel mode). gpfdist writes one text file. COPY ON SEGMENT writes one text file per segment.

The disk outputs are not the same thing. Parquet is typed and compressed and costs CPU to build. Text is what the server already produces.

## Where the client runs

Passes 2 and 3 use the same binary. Only the client machine differs. 100M rows, best of 3:

| Path | Client on the coordinator host | Client on a third machine |
|---|---|---|
| Coordinator fetch, discard | 34.59 | 38.26 |
| Parallel retrieve, discard | 8.98 | 7.64 |
| gpfdist, discard | 5.44 | 6.13 |
| COPY ON SEGMENT, discard | 4.08 | 3.87 |
| Coordinator fetch, to disk | 70.99 | 92.49 |
| Parallel retrieve, to disk | 12.49 | 12.26 |

Moving the client off the coordinator host made parallel retrieve 15 percent faster and gpfdist 13 percent slower. On the coordinator host the client competes for CPU with the coordinator and four segments, and parallel retrieve, which does its work in the client, pays the most for that. gpfdist lost its loopback path for half the segments.

The coordinator fetch got slower from the third machine, most of all when writing Parquet (70.99 s to 92.49 s). That run uses one task for both the network and the Parquet encoding. Why the same single task is slower on an idle machine was not looked into.

## Client tuning

This section compares passes 1 and 2, both on the coordinator host.

The first pass used a client that asked for 10,000 rows per `RETRIEVE` and waited for each whole batch. The question was how much of the gap to gpfdist was that client.

What changed in the client:

- Rows are read from the socket as they arrive, not one whole batch at a time.
- The batch size is a setting (`BATCH`, default 100000, or `all`).
- When a stream stops early its socket is closed at once.

Effect, 100M rows, discard, best of 3:

| Path | First client | Tuned client | Change |
|---|---|---|---|
| Coordinator fetch | 51.71 | 34.59 | 33 percent less time |
| Parallel retrieve | 9.56 | 8.98 | 6 percent less time |
| gpfdist (no client change) | 5.64 | 5.44 | run to run variation |

The coordinator path gained far more than the parallel path. So the tuning made the race closer (5.4 times before, 3.9 times after) and left the gap to gpfdist where it was (1.7 times).

What the tuning showed:

- **Batch size barely matters.** One 100M parallel run each: 10,000 rows per `RETRIEVE` 8.97 s, 100,000 8.77 s, 1,000,000 9.22 s, `all` 8.54 s and 8.40 s.
- **One stream alone is no faster than eight together.** With `MAX_OPEN=1` an endpoint still takes 6.5 to 7.3 s for its 12.5M rows, about 1.8M rows per second. Running all eight at once does not slow any of them. The limit is per stream.
- **The client is not the busiest part of a stream.** One stream alone, sampled over 2 seconds with the first client: the two server processes for that endpoint used 81 and 50 percent of a core, and the busiest client thread used 38 percent.
- **`BATCH=all` has a cost.** It is the fastest setting by a few percent, but the four streams on the client's own host show almost no rows for the first 3 seconds and then catch up. A failure injected at 2M rows on one of those streams took 6.5 s to show, against about 3 s with batches. The cause was not found. The default stays at batches of 100,000.

Taken together: with this client the per stream rate looks limited on the server side of a retrieve session, where two processes hand rows to each other, and not by the client. That is a reading of the samples above, not a proof.

Not tried: the binary protocol, a client that does not go through tokio-postgres, any server setting.

## Cluster

- Apache Cloudberry 2.2.0-incubating (PostgreSQL 14.10), build 13d3c58eaa0. gpfdist 2.2.0-incubating from the same build.
- Three AWS c7i.4xlarge instances (16 vCPU, 30 GB each), Rocky Linux 9, one 100 GB root volume each, same subnet.
- Host `cdw`: coordinator and 4 primary segments. Host `sdw1`: 4 primary segments. 8 primary segments in total. No mirrors, no standby.
- `gp_interconnect_type = tcp`. UDP between the two hosts is blocked by the network rules, so the default UDP interconnect could not be used.
- Passes 1 and 2: the client (demo binary, gpfdist, psql) ran on `cdw`. Half the segments are local to it and half are one network hop away.
- Pass 3: the client ran on a third host, `sdw2`, which holds no segment. Every segment and the coordinator are one network hop away. The coordinator accepts that host by a `trust` line for its address in `pg_hba.conf`. Retrieve sessions needed no such line on the segments; the endpoint token was enough.
- Nothing else was running on any host.

## Tables

- `retrieve_demo`: 10,000,000 rows. `retrieve_demo_100m`: 100,000,000 rows.
- Columns: `id` integer, `payload` text (an md5 string, 32 characters), `ts` timestamp with time zone. Roughly 70 to 80 bytes per row as text, estimated from the column widths and not measured.
- `DISTRIBUTED BY (id)`. Rows per segment are within 0.1 percent of each other.

## Method

- Order within one run: the four paths with the discard sink, then the four paths with the disk sink. Three runs per table, 10M first.
- Output files are deleted after each disk case.
- Demo binary times are its own: from `BEGIN` to the last row, after the connection and a `count(*)` check. The binary fails the run if its row total differs from `count(*)`.
- gpfdist and COPY times are psql `\timing` for the one statement. Row counts are from the command tag.
- No path includes connection time.
- Tools: demo binary built with `cargo build --release`, cargo 1.100.0. Asyncband 0.8.1 release candidate source. Pass 1: commit f03105c. Passes 2 and 3: commit 8548a4e. The binary for pass 3 was built on `cdw` and copied to `sdw2`.

## All runs, pass 3: tuned client on a third machine

Seconds, runs 1 to 3.

| Table | Path | Sink | Run 1 | Run 2 | Run 3 |
|---|---|---|---|---|---|
| 10M | coordinator | discard | 3.77 | 3.88 | 3.96 |
| 10M | parallel | discard | 1.26 | 1.30 | 1.03 |
| 10M | gpfdist | discard | 0.84 | 0.93 | 1.10 |
| 10M | copy-on-seg | discard | 0.82 | 0.91 | 1.01 |
| 10M | coordinator | disk | 9.83 | 9.99 | 8.54 |
| 10M | parallel | disk | 1.50 | 1.57 | 1.52 |
| 10M | gpfdist | disk | 0.99 | 1.11 | 1.07 |
| 10M | copy-on-seg | disk | 0.91 | 1.05 | 1.05 |
| 100M | coordinator | discard | 39.35 | 38.26 | 39.80 |
| 100M | parallel | discard | 7.80 | 7.64 | 7.78 |
| 100M | gpfdist | discard | 6.13 | 6.27 | 6.14 |
| 100M | copy-on-seg | discard | 3.87 | 3.88 | 3.93 |
| 100M | coordinator | disk | 92.59 | 92.49 | 96.60 |
| 100M | parallel | disk | 12.44 | 12.56 | 12.26 |
| 100M | gpfdist | disk | 6.15 | 6.32 | 6.04 |
| 100M | copy-on-seg | disk | 3.77 | 3.84 | 3.98 |

## All runs, pass 2: tuned client on the coordinator host

Seconds, runs 1 to 3.

| Table | Path | Sink | Run 1 | Run 2 | Run 3 |
|---|---|---|---|---|---|
| 10M | coordinator | discard | 3.52 | 3.51 | 3.52 |
| 10M | parallel | discard | 1.24 | 1.36 | 1.35 |
| 10M | gpfdist | discard | 1.12 | 0.79 | 0.78 |
| 10M | copy-on-seg | discard | 0.98 | 0.94 | 0.93 |
| 10M | coordinator | disk | 6.83 | 7.14 | 7.04 |
| 10M | parallel | disk | 1.48 | 1.53 | 1.55 |
| 10M | gpfdist | disk | 0.91 | 0.99 | 1.06 |
| 10M | copy-on-seg | disk | 1.05 | 1.05 | 1.05 |
| 100M | coordinator | discard | 34.59 | 34.69 | 34.66 |
| 100M | parallel | discard | 9.01 | 9.37 | 8.98 |
| 100M | gpfdist | discard | 5.44 | 5.44 | 5.44 |
| 100M | copy-on-seg | discard | 4.21 | 4.08 | 4.33 |
| 100M | coordinator | disk | 72.90 | 70.99 | 72.37 |
| 100M | parallel | disk | 12.56 | 12.64 | 12.49 |
| 100M | gpfdist | disk | 5.61 | 5.65 | 5.56 |
| 100M | copy-on-seg | disk | 3.75 | 3.79 | 3.73 |

## All runs, pass 1: first client on the coordinator host

Seconds, runs 1 to 3. Same cluster, same script, demo binary at commit f03105c.

| Table | Path | Sink | Run 1 | Run 2 | Run 3 |
|---|---|---|---|---|---|
| 10M | coordinator | discard | 4.56 | 4.77 | 4.44 |
| 10M | parallel | discard | 1.19 | 1.24 | 1.31 |
| 10M | gpfdist | discard | 0.85 | 1.11 | 0.93 |
| 10M | copy-on-seg | discard | 0.83 | 0.97 | 0.89 |
| 10M | coordinator | disk | 8.61 | 9.03 | 8.51 |
| 10M | parallel | disk | 1.52 | 1.52 | 1.54 |
| 10M | gpfdist | disk | 1.08 | 1.17 | 1.07 |
| 10M | copy-on-seg | disk | 1.06 | 1.05 | 1.05 |
| 100M | coordinator | discard | 53.67 | 51.71 | 51.83 |
| 100M | parallel | discard | 9.56 | 9.80 | 9.79 |
| 100M | gpfdist | discard | 6.18 | 5.64 | 5.67 |
| 100M | copy-on-seg | discard | 4.50 | 3.86 | 4.10 |
| 100M | coordinator | disk | 90.93 | 90.50 | 94.67 |
| 100M | parallel | disk | 13.80 | 13.84 | 13.81 |
| 100M | gpfdist | disk | 5.88 | 5.65 | 5.63 |
| 100M | copy-on-seg | disk | 3.99 | 3.91 | 3.95 |

## Anything odd

- **gpfdist to disk is as fast as gpfdist to discard.** It wrote the whole 100M row text file in 5.6 s. That is faster than the volume can take, so the data was still in memory when the statement returned. The disk numbers for gpfdist and COPY ON SEGMENT measure writing to the page cache, not to storage. The same is true of the Parquet files, which are smaller.
- **The coordinator fetch more than doubles with Parquet.** One task builds every Parquet batch. In parallel mode 8 tasks do it at once, so Parquet adds about 4 s, not 36 or more.
- **In passes 1 and 2, parallel retrieve is slower on the client's own host.** In the 100M runs the four endpoints on `cdw` finish a second or two after the four on `sdw1`. The client, the coordinator, and four segments share the 16 vCPUs on `cdw`. Pass 3 removes this.
- **At 10M rows the tuned parallel path is not faster than the first one** (1.24 s against 1.19 s). The difference is inside run to run variation.
- **Only one cluster shape.** 8 segments on 2 hosts, with the client in the same subnet, so the network between client and segments is fast. More hosts, or a client across a slower network, would change every number here.
- **Asyncband adds no measurable time.** With `MAX_OPEN` at 64 and 8 endpoints no task waits for a permit. Its work is in the failure path, which this benchmark does not measure.

## Reproduce

On a cluster host, as the database owner, with the Cloudberry environment sourced:

```
psql -d postgres -f trial/setup.sql      # 10M rows; make the 100M table the same way
(cd demo && cargo build --release)
export CB_HOST=localhost CB_PORT=5432 CB_USER=rocky CB_DB=postgres
bench/bench.sh > results.tsv
```

From a separate client machine, set `CB_HOST` to the coordinator and `GPFDIST_HOST` to a name the segment hosts can resolve for the client. That machine needs the Cloudberry client tools (`psql`, `gpfdist`), the demo binary, SSH to the segment hosts for cleanup, and a `pg_hba.conf` line on the coordinator.

`bench/bench.sh` starts its own gpfdist on port 8081, creates and drops its external tables, and removes its output files. Segment hosts must be able to reach port 8081 on the host that runs it. Settings are listed at the top of the script.
