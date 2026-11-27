# T6 benchmark

Run on 2026-11-27 with `bench/bench.sh`. Every one of the 48 runs exported the full row count.

## Result

Parallel retrieve is about 5 times faster than a fetch through the coordinator. It is slower than gpfdist. On 100M rows gpfdist takes 5.64 s and parallel retrieve takes 9.56 s.

Against the kill criterion in `docs/BACKGROUND.md` ("stop if parallel retrieve does not clearly beat the existing parallel export path"): on this cluster it does not beat gpfdist.

Best of 3 runs, seconds:

| Path | 10M discard | 10M to disk | 100M discard | 100M to disk |
|---|---|---|---|---|
| 1. Coordinator fetch (demo binary) | 4.44 | 8.51 | 51.71 | 90.50 |
| 2. Parallel retrieve (demo binary) | 1.19 | 1.52 | 9.56 | 13.80 |
| 3. gpfdist writable external table | 0.85 | 1.07 | 5.64 | 5.63 |
| 4. COPY ON SEGMENT | 0.83 | 1.05 | 3.86 | 3.91 |

Rows per second on 100M rows, discard: coordinator 1.9M, parallel retrieve 10.5M, gpfdist 17.7M, COPY ON SEGMENT 25.9M.

Ratios on 100M rows, discard:

- Parallel retrieve against coordinator fetch: 5.4 times faster.
- gpfdist against parallel retrieve: 1.7 times faster.
- COPY ON SEGMENT against parallel retrieve: 2.5 times faster.

## What each path does

1. **Coordinator fetch.** `demo --mode coordinator`. One session, `DECLARE CURSOR`, `FETCH 10000` until empty. Every row passes through the coordinator.
2. **Parallel retrieve.** `demo --mode parallel`. `DECLARE PARALLEL RETRIEVE CURSOR`, then one retrieve session per segment, `RETRIEVE 10000` until empty, all 8 at once. Asyncband coordinates the streams.
3. **gpfdist.** A writable external table with `LOCATION ('gpfdist://cdw:8081/...')`, `FORMAT 'TEXT'`, distributed by the same key as the source. `INSERT INTO ... SELECT * FROM` the table. Each segment pushes its rows over HTTP to one gpfdist process on the client host. The plan has no redistribute step.
4. **COPY ON SEGMENT.** `COPY <table> TO ... ON SEGMENT`. Each segment writes on its own host. Nothing is delivered to the client. It is here as a ceiling: how fast the segments can produce the rows when no client is involved.

Paths 1 to 3 deliver the data to the client host. Path 4 does not.

## Sinks

- **Discard.** Nothing reaches a disk. The demo binary reads and counts rows. gpfdist writes to a file that is a link to `/dev/null`. COPY ON SEGMENT uses `TO PROGRAM 'cat > /dev/null'`.
- **To disk.** The demo binary writes Parquet with snappy compression (one file in coordinator mode, one per segment in parallel mode). gpfdist writes one text file. COPY ON SEGMENT writes one text file per segment.

The disk outputs are not the same thing. Parquet is typed and compressed and costs CPU to build. Text is what the server already produces.

## Cluster

- Apache Cloudberry 2.2.0-incubating (PostgreSQL 14.10), build 13d3c58eaa0. gpfdist 2.2.0-incubating from the same build.
- Two AWS c7i.4xlarge instances (16 vCPU, 30 GB each), Rocky Linux 9, one 100 GB root volume each, same subnet.
- Host `cdw`: coordinator and 4 primary segments. Host `sdw1`: 4 primary segments. 8 primary segments in total. No mirrors, no standby.
- `gp_interconnect_type = tcp`. UDP between the two hosts is blocked by the network rules, so the default UDP interconnect could not be used.
- The client (demo binary, gpfdist, psql) ran on `cdw`. Half the segments are local to it and half are one network hop away.
- Nothing else was running on either host.

## Tables

- `retrieve_demo`: 10,000,000 rows. `retrieve_demo_100m`: 100,000,000 rows.
- Columns: `id` integer, `payload` text (an md5 string, 32 characters), `ts` timestamp with time zone. About 80 bytes per row as text.
- `DISTRIBUTED BY (id)`. Rows per segment are within 0.1 percent of each other.

## Method

- Order within one run: the four paths with the discard sink, then the four paths with the disk sink. Three runs per table, 10M first.
- Output files are deleted after each disk case.
- Demo binary times are its own: from `BEGIN` to the last row, after the connection and a `count(*)` check. The binary fails the run if its row total differs from `count(*)`.
- gpfdist and COPY times are psql `\timing` for the one statement. Row counts are from the command tag.
- No path includes connection time.
- Tools: demo binary at commit f03105c, built with `cargo build --release`, cargo 1.100.0. Asyncband 0.8.1 release candidate source.

## All runs

Seconds, runs 1 to 3.

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

- **gpfdist to disk is as fast as gpfdist to discard.** It wrote about 8 GB of text in 5.6 s. That is faster than the volume can take, so the data was still in memory when the statement returned. The disk numbers for gpfdist and COPY ON SEGMENT measure writing to the page cache, not to storage. The same is true of the Parquet files, which are smaller.
- **The coordinator fetch almost doubles with Parquet.** One task builds every Parquet batch. In parallel mode 8 tasks do it at once, so Parquet adds 4 s, not 40.
- **Parallel retrieve is slower on the client's own host.** In the 100M runs the four endpoints on `cdw` took about 9.5 s and the four on `sdw1` about 8 s. The client, the coordinator, and four segments share the 16 vCPUs on `cdw`.
- **The demo client is not tuned.** It asks for 10,000 rows per `RETRIEVE`, waits for the reply, and holds each batch in memory as text before counting it. gpfdist receives a continuous stream. Larger batches, a streaming read, or the binary protocol were not tried. The gap to gpfdist may be partly the client and partly the feature; this benchmark does not separate the two.
- **Only one cluster shape.** 8 segments on 2 hosts, with the client on one of them. A client on a third machine, more hosts, or a slower network would change every number here.
- **Asyncband adds no measurable time.** With `MAX_OPEN` at 64 and 8 endpoints no task waits for a permit. Its work is in the failure path, which this benchmark does not measure.

## Reproduce

On a cluster host, as the database owner, with the Cloudberry environment sourced:

```
psql -d postgres -f trial/setup.sql      # 10M rows; make the 100M table the same way
(cd demo && cargo build --release)
export CB_HOST=localhost CB_PORT=5432 CB_USER=rocky CB_DB=postgres
bench/bench.sh > results.tsv
```

`bench/bench.sh` starts its own gpfdist on port 8081, creates and drops its external tables, and removes its output files. Segment hosts must be able to reach port 8081 on the host that runs it. Settings are listed at the top of the script.
