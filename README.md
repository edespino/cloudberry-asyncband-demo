# Cloudberry parallel retrieve demo with Apache Asyncband

A fast, throwaway demo: export query results straight from Apache Cloudberry / SynxDB segments with parallel retrieve cursors, with Apache Asyncband coordinating the concurrent streams.

License: Apache License 2.0. See `LICENSE`. Copyright 2026 Ed Espino.

## Where to look

- [`demo/README.md`](demo/README.md): what the demo shows, how to run it, what it needs, and its limits.
- [`bench/BENCH.md`](bench/BENCH.md): the benchmark against a coordinator fetch, gpfdist, and COPY ON SEGMENT.
- [`demo/FAILURE.md`](demo/FAILURE.md): the failure act.
- [`docs/BACKGROUND.md`](docs/BACKGROUND.md): why this exists and what was decided.
- [`docs/UPSTREAM.md`](docs/UPSTREAM.md): notes from building against the Asyncband 0.8.1 release candidate.

## Result in one paragraph

On 100M rows across 8 segments, parallel retrieve is about 5 times faster than reading through the coordinator and about 25 percent slower than gpfdist. It needs nothing set up on the server, but the client must be able to reach every segment host.

## Layout

```
demo/     the demo CLI, scripts, and terminal recordings
bench/    benchmark script and results
spike/    first Python spike (T0), on an earlier 3 segment cluster
trial/    first Rust trial (T1), on that same earlier cluster
docs/     background and upstream notes
archive/  superseded SDK plan, for reference only
vendor/   extracted Asyncband source (not committed)
```

## Working on it

`CLAUDE.md` and `TASKS.md` hold the ground rules and the task list this was built from. Before building, extract the Asyncband 0.8.1 source into `vendor/` and set the `CB_*` environment variables for the cluster. Copy `.env.example` to a file the shell exports. Do not commit that file. `vendor/`, `.env`, keys, and state files are ignored.
