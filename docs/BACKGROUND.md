# Background: everything decided so far

Context, research, critique and decisions from Ed's planning conversation, as of November 27, 2026. Read top to bottom once. Facts marked "unverified" were stated from memory and not checked against a source.

## 1. Goal and how the project evolved

Ed wants to use Apache Asyncband in a real workload and leverage the Apache cross-project angle: Synx Data Labs (SDL) builds SynxDB on the Apache Cloudberry kernel, and both Cloudberry and Asyncband are Apache incubating projects.

How the idea narrowed, in order:

1. Possible Asyncband uses near Cloudberry: SDK, loader, admission gateway, pooler, AI query front end.
2. Chosen: a Rust client SDK built around Cloudberry's parallel retrieve cursors. A full plan was written (archived in `archive/`, plus a separate plan doc).
3. ConnectorX found as an existing Rust tool with real users. Ed liked it as a distribution channel.
4. Ed insisted Asyncband stay central. Resolved with a two-layer design: an Asyncband-based retrieve core, with a thin ConnectorX source on top (later, not now).
5. Critical review: real demand is unproven. Decision: build a fast, throwaway demo first, built with AI assistance (Claude Code).
6. Ed is reviewing the Asyncband 0.8.1 release vote and wants to try it on real Cloudberry, not a simulation. `trial/` is that first real program.

## 2. Apache Asyncband

- Apache Incubator podling, entered August 18, 2026. Formerly published as the `mea` crate (deprecated). Mentors include Xuanwo and tison.
- Graduation readiness discussion started November 1, 2026. A real adopter is most useful to them now.
- 0.8.0 released November 15 to 16, 2026. 0.8.1 is under release vote.
- What it is: composable, runtime-agnostic concurrency building blocks for async Rust. Uses standard futures and wakers; runs on Tokio, async-std, smol, or a custom executor.
- What it is not: it does no I/O, starts no threads, spawns no tasks, installs no timers. Networking stays with the caller's runtime.
- All features are opt-in Cargo features; none enabled by default.
- Primitives (feature name in parentheses): Mutex (mutex), RwLock and UpgradableRwLock (rwlock), Condvar (condvar), Semaphore (semaphore), Barrier (barrier), ManualResetEvent and AutoResetEvent (event), Latch (latch), Phaser (phaser), WaitGroup (waitgroup), Shutdown (shutdown), Once (once), OnceCell (once-cell), LazyCell (lazy-cell), OnceMap (once-map), singleflight Group (singleflight), Completion (completion), channels oneshot, mpmc, mpsc, spmc, broadcast, watch, object pool (pool), and a blocking adapter FutureExt with `block_on` and `wait_timeout` (blocking).
- APIs verified against 0.8.0 docs and used in `trial/`:
    - `Semaphore::new(permits: usize)`, `acquire(n).await`, `acquire_owned(self: Arc<Self>, n).await`, `available_permits()`, `try_acquire(n)`.
    - `shutdown::new()` returns `(Shutdown, ShutdownGuard)`. Both are Clone. `ShutdownGuard::shutdown_requested().await`, `is_shutdown_requested()`. `Shutdown::request_shutdown()` is sticky and idempotent.
    - Awaiting a `Shutdown` requests shutdown on first poll, then waits until every guard is dropped. So await it only when you mean to stop everything, or at the end as a leak check.
- Semaphore and RwLock were derived from Tokio's versions and changed (no close method, so no max permit cap; any positive reader count).

### Why Asyncband if Tokio exists

An earlier claim in the conversation, that Tokio's coordination tools only work inside Tokio, was too strong. Unverified, but Tokio's docs describe `tokio::sync` as runtime-agnostic. Tokio's timers and networking are what tie code to Tokio. The honest case for Asyncband:

1. Primitives Tokio lacks: WaitGroup, Latch, Shutdown, Phaser, OnceMap, singleflight, object pools, and (in later `mea` versions) a fair-share admission policy.
2. Deliberate redesigns of Tokio-derived primitives.
3. A neutral dependency with no runtime attached.
4. Strategic: Apache cross-project value. Be careful this does not look engineered for marketing.

## 3. Cloudberry parallel retrieve cursors

The protocol is in CLAUDE.md. Additional facts:

- Results come directly from segments, bypassing the coordinator.
- Present across the Greenplum lineage: Greenplum 6 and 7 docs, EDB's WarehousePG (as the `gp_parallel_retrieve_cursor` extension, registered per database), and Cloudberry.
- Endpoint placement depends on the query. Queries with a gather motion yield one coordinator endpoint; the docs advise checking EXPLAIN for motion. The speedup applies to bulk export of distributed rows: scans and filters.
- Retrieve sessions authenticate with a generated token, not pg_hba.conf.
- The retrieve user must be the user that declared the cursor.

## 4. Existing tools and gaps

- **No Rust client using parallel retrieve was found.** All examples are SQL or C.
- **ConnectorX** (github.com/sfu-db/connector-x): Rust core plus Python library, loads databases into pandas, Arrow, Polars. About 2.6k GitHub stars. Parallelism splits a query into ranges on a numeric partition column (min/max, then range queries), one thread per partition. On Cloudberry every range query still passes through the coordinator. Sources are pluggable. Untested on Cloudberry (unverified that it works).
- **Greenplum Connector for Apache Spark**: built by Pivotal because Spark JDBC is limited by the master node; segments transfer data in parallel via gpfdist directly to Spark workers. Scala API, commercial, tied to Tanzu Greenplum, needs explicit configuration on Kubernetes.
- **Greenplum status**: Broadcom moved Greenplum to closed source in May 2024; public repos archived. Still released commercially (Tanzu Greenplum 7.8.2, June 22, 2026).
- **Bulk export alternatives that the demo must beat** (from memory, unverified details): writable external tables with gpfdist, `COPY ... ON SEGMENT`, PXF writing Parquet to object storage.

## 5. Evidence of demand

Real but unsized:

1. Pivotal built and Broadcom still sells the Spark connector specifically to bypass the master bottleneck.
2. A Dataiku community user on Greenplum: reading into pandas through the master node becomes the bottleneck and removes MPP benefits; asked for something like the Spark connector.
3. A Broadcom knowledge base article on slow `SELECT *` names result-set fetch rate as a common factor even on large clusters.

The gap: data scientists using pandas or Polars without Spark have no parallel path. That is the user a ConnectorX source would serve.

Not known: how many such users exist, and whether they are in SynxDB's market.

## 6. Critical review (keep in mind while building)

1. Narrow query class: only scan and filter style exports benefit.
2. Segment reachability: in Kubernetes deployments like SynxDB BYOC, segment pods are usually not reachable from outside. The demo should run where segments are reachable and say so openly.
3. Existing parallel file export paths may already serve the need. The benchmark must include one.
4. Asyncband's role is modest for a few dozen connections; the failure act is what makes it visible.
5. Opportunity cost for an early-stage company.
6. The speedup belongs to the database feature; the tool is glue. Pitch it as "open MPP data, fast in your notebook," not as novel technology.

Verdict reached: not justified as a product yet; reasonable as a short, honest demo and a Cloudberry community story.

## 7. Decisions

- Demo first, throwaway, built with Claude Code. Python spike before any Rust.
- Tokio for networking; Asyncband for coordination. No runtime-agnostic I/O work.
- Two acts: the race (per-segment progress bars, Parquet, Polars) and the failure (prompt cancellation, rollback, leak check).
- Record the demo; never depend on a live run.
- Kill criteria: stop if parallel retrieve does not clearly beat the existing parallel export path, or cannot reach segments in the target environment. Publish the results either way.
- If the demo succeeds: rewrite properly, do not polish demo code. Likely path is the two-layer design with ConnectorX as the user-facing channel, Python first.
- Naming: no "Apache" in product or crate names; use forms like "for Apache Cloudberry" and confirm with the Cloudberry PPMC.

## 8. Apache engagement (for Ed)

- Cloudberry is still incubating (per its August 2026 report). It has cross-project precedent (native support work in Apache MADlib).
- Sequence: ask Asyncband mentors and Cloudberry Slack informally first; after results, post a DISCUSS thread on dev@cloudberry.apache.org with the benchmark; file Asyncband issues and patches from real findings; offer an adopter statement for Asyncband graduation; propose a joint talk (Cloudberry ran seven talks at Community Over Code Asia 2026).
- Strongest open source artifact if the project continues: a Cloudberry source merged into ConnectorX.

## 9. Customer discovery (for Ed, in parallel with T0)

- Decide whether this is a community demo or a customer feature; they justify different effort.
- Five conversations with SynxDB users, prospects, or Greenplum contacts. Do not pitch. Ask: "When you pull large result sets out of the warehouse, how do you do it? How long does it take? What breaks?" Sharper follow-up: "Do you pull large tables into pandas or Polars without Spark?"
- Search Cloudberry and Greenplum issues, mailing lists, Slack, Stack Overflow for export and coordinator bottleneck complaints.
- Bar set in advance: if at least two of five describe painful exports unprompted, treat it as a customer feature; otherwise only the community demo value applies.

## 10. Open questions

- Does stock ConnectorX work against Cloudberry through its Postgres source?
- Would ConnectorX maintainers accept a Cloudberry source upstream? ConnectorX license and activity level (unverified).
- Which parallel export path to benchmark against on Ed's cluster.
- `gp_endpoints` versus `gp_get_endpoints()` differences across Cloudberry, SynxDB, Greenplum, WarehousePG.
- Whether a Rust client is the right first language versus Python for customers.

## 11. Superseded material

- `archive/sdk-plan-CLAUDE.md` and `archive/sdk-plan-TASKS.md`: full runtime-agnostic SDK plan. Reference only.
- An earlier SDK plan doc was superseded by this demo plan. It is not in this tree.
- An early simulated-segment Asyncband example was dropped; Ed wants real Cloudberry only.

## 12. Sources

- Asyncband incubator status: https://incubator.apache.org/clutch/asyncband.html
- Asyncband docs (0.8.0): https://docs.rs/asyncband/latest/asyncband/index.html
- Asyncband repository: https://github.com/apache/asyncband
- Cloudberry DECLARE: https://cloudberry.apache.org/docs/sql-stmts/declare.md
- Cloudberry RETRIEVE: https://cloudberry.apache.org/docs/next/sql-stmts/retrieve
- Cloudberry gp_endpoints: https://cloudberry.apache.org/docs/sys-catalogs/sys-views/gp-endpoints.md
- Cloudberry C example: https://apache.googlesource.com/cloudberry/+/HEAD/src/test/examples/test_parallel_retrieve_cursor_wait.c
- Greenplum 7 parallel retrieve guide: https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-parallel_retrieve_cursor.html
- WarehousePG parallel retrieve (wait function): https://www.enterprisedb.com/docs/warehousepg/7.3/admin_guide/parallel_retrieve_cursor/
- WarehousePG extension: https://www.enterprisedb.com/docs/supported-open-source/warehousepg/extensions/bundled/gp_parallel_retrieve_cursor/
- ConnectorX: https://github.com/sfu-db/connector-x/wiki and https://docs.rs/crate/connectorx/0.2.0
- SQLx runtime support: https://github.com/transact-rs/sqlx
- Greenplum Spark connector launch: https://blogs.vmware.com/tanzu/introducing-pivotal-greenplum-spark-connector-integrating-apache-spark/
- Spark connector on Kubernetes: https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/configure.html
- Dataiku user report: https://community.dataiku.com/discussion/38025/greenplum-interoperability-enhancements
- Broadcom KB on fetch rate: https://knowledge.broadcom.com/external/article/377508/basic-select-with-order-by-clause-runs.html
- Greenplum closed source: https://en.wikipedia.org/wiki/Greenplum
- Tanzu Greenplum 7.8.2: https://support.broadcom.com/web/ecx/support-content-notification/-/external/content/SecurityAdvisories/0/37753
- Cloudberry August 2026 report: https://cloudberry.apache.org/blog/apache-cloudberry-incubation-report-202608
- Cloudberry 2025 review: https://cloudberry.apache.org/blog/apache-cloudberry-2025-review.md

## 13. After the benchmark (November 27, 2026)

Full numbers are in `bench/BENCH.md`.

What the benchmark found, 100M rows, 8 segments on two hosts, client on a third machine:

- Coordinator fetch 38.26 s. Parallel retrieve 7.64 s. gpfdist 6.13 s. COPY ON SEGMENT 3.87 s.
- Parallel retrieve is 5 times faster than the coordinator fetch and 25 percent slower than gpfdist.
- The kill criterion in section 7 ("stop if parallel retrieve does not clearly beat the existing parallel export path") is not met. Tuning the client and moving it to its own machine narrowed the gap from 70 percent to 25 percent and did not close it.
- One retrieve stream runs at about 1.8M rows per second however many run beside it, and the client is not the busiest process in that stream. The limit looks to be on the server side of a retrieve session. Not proven.

Decisions Ed made after seeing the numbers:

- **Reframe the pitch.** Do not claim to beat the existing parallel export. The claim is: about 5 times faster than reading through the coordinator, which is what pandas, Polars, JDBC, and ConnectorX users get today, and within 25 percent of gpfdist with nothing to set up on the server. gpfdist needs a process on the client host, a writable external table, and segments that can connect back to the client. Parallel retrieve needs an ordinary login and outbound connections from the client to the segments. This replaces the pitch in section 6, item 6, and it is the framing for T7.
- **Hold the Cloudberry mailing list question** about the per stream limit. Not now.
- **No Asyncband follow-up** from this work. The notes in `docs/UPSTREAM.md` stay local.
- **Do not start the ConnectorX source.** Unchanged from section 7.
