// Copyright 2026 Ed Espino
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Export one query from a real Cloudberry / SynxDB cluster, two ways.
//
//   --mode coordinator   regular cursor, batched FETCH through the coordinator
//   --mode parallel      parallel retrieve cursor, one session per segment
//   --to out.parquet     optional: write the rows as Parquet
//                        coordinator mode writes that one file
//                        parallel mode writes one file per segment:
//                        out.0.parquet, out.1.parquet, ...
//
// Tokio does the I/O. Asyncband does the coordination in parallel mode:
//   Semaphore  caps how many retrieve sessions are open at once (MAX_OPEN)
//   shutdown   one failing endpoint cancels every other stream; awaiting
//              Shutdown at the end checks every retrieve connection closed
//
// Env: CB_HOST CB_PORT CB_USER CB_PASSWORD CB_DB CB_QUERY MAX_OPEN
//      FAIL_ENDPOINT=<index>  optional: drop that stream after its first batch

use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use asyncband::semaphore::Semaphore;
use asyncband::shutdown::{self, ShutdownGuard};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::time::timeout;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

const BATCH: usize = 10_000;
const CURSOR: &str = "asyncband_demo";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// Longest wait for one statement on the coordinator or one batch from a segment.
const STEP_TIMEOUT: Duration = Duration::from_secs(120);

type AnyError = Box<dyn std::error::Error>;

struct Endpoint {
    host: String,
    port: u16,
    token: String,
    name: String,
    user: String,
}

/// How one result column is stored in Parquet. Anything that is not an
/// integer, a float, or a boolean is kept as the text the server sent.
#[derive(Clone, Copy)]
enum Kind {
    Int,
    Float,
    Bool,
    Text,
}

/// Column layout of the query, shared by every Parquet file of a run.
#[derive(Clone)]
struct Layout {
    schema: SchemaRef,
    kinds: Vec<Kind>,
}

/// One Parquet file being written.
struct Sink {
    writer: ArrowWriter<File>,
    layout: Layout,
}

impl Sink {
    fn create(path: &Path, layout: &Layout) -> Result<Sink, String> {
        let file = File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, layout.schema.clone(), Some(props))
            .map_err(|e| e.to_string())?;
        Ok(Sink {
            writer,
            layout: layout.clone(),
        })
    }

    /// Turn one batch of text rows into typed columns and append it.
    fn write(&mut self, msgs: &[SimpleQueryMessage]) -> Result<(), String> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.layout.kinds.len());
        for (c, kind) in self.layout.kinds.iter().enumerate() {
            let cells = msgs.iter().filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(c)),
                _ => None,
            });
            let bad = |v: &str| format!("column {c}: cannot convert {v:?}");
            columns.push(match kind {
                Kind::Int => {
                    let mut b = Int64Builder::new();
                    for cell in cells {
                        b.append_option(match cell {
                            Some(v) => Some(v.parse::<i64>().map_err(|_| bad(v))?),
                            None => None,
                        });
                    }
                    Arc::new(b.finish())
                }
                Kind::Float => {
                    let mut b = Float64Builder::new();
                    for cell in cells {
                        b.append_option(match cell {
                            Some(v) => Some(v.parse::<f64>().map_err(|_| bad(v))?),
                            None => None,
                        });
                    }
                    Arc::new(b.finish())
                }
                Kind::Bool => {
                    let mut b = BooleanBuilder::new();
                    for cell in cells {
                        b.append_option(cell.map(|v| v == "t"));
                    }
                    Arc::new(b.finish())
                }
                Kind::Text => {
                    let mut b = StringBuilder::new();
                    for cell in cells {
                        b.append_option(cell);
                    }
                    Arc::new(b.finish())
                }
            });
        }
        let batch =
            RecordBatch::try_new(self.layout.schema.clone(), columns).map_err(|e| e.to_string())?;
        self.writer.write(&batch).map_err(|e| e.to_string())
    }

    fn close(self) -> Result<(), String> {
        self.writer.close().map(|_| ()).map_err(|e| e.to_string())
    }
}

/// Ask the coordinator for the column names and types of the query.
async fn layout_of(coord: &Client, query: &str) -> Result<Layout, AnyError> {
    let stmt = match timeout(STEP_TIMEOUT, coord.prepare(query)).await {
        Ok(r) => r?,
        Err(_) => return Err(format!("timed out after {STEP_TIMEOUT:?}: prepare").into()),
    };
    let mut fields = Vec::new();
    let mut kinds = Vec::new();
    for col in stmt.columns() {
        let ty = col.type_();
        let (kind, data_type) = if [Type::INT2, Type::INT4, Type::INT8].contains(ty) {
            (Kind::Int, DataType::Int64)
        } else if [Type::FLOAT4, Type::FLOAT8].contains(ty) {
            (Kind::Float, DataType::Float64)
        } else if *ty == Type::BOOL {
            (Kind::Bool, DataType::Boolean)
        } else {
            (Kind::Text, DataType::Utf8)
        };
        fields.push(Field::new(col.name(), data_type, true));
        kinds.push(kind);
    }
    Ok(Layout {
        schema: Arc::new(Schema::new(fields)),
        kinds,
    })
}

/// out.parquet -> out.3.parquet
fn part_path(to: &Path, i: usize) -> PathBuf {
    to.with_extension(format!("{i}.parquet"))
}

fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

fn usage() -> ! {
    eprintln!("usage: asyncband-cloudberry-demo --mode coordinator|parallel [--to out.parquet]");
    exit(2);
}

/// One statement on a session, bounded by STEP_TIMEOUT.
async fn step(client: &Client, sql: &str) -> Result<Vec<SimpleQueryMessage>, AnyError> {
    match timeout(STEP_TIMEOUT, client.simple_query(sql)).await {
        Ok(r) => Ok(r?),
        Err(_) => Err(format!("timed out after {STEP_TIMEOUT:?}: {sql}").into()),
    }
}

fn count_rows(msgs: &[SimpleQueryMessage]) -> u64 {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count() as u64
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let args: Vec<String> = env::args().skip(1).collect();
    let (mode, to) = match args.as_slice() {
        [m, mode] if m == "--mode" => (mode.as_str(), None),
        [m, mode, t, to] if m == "--mode" && t == "--to" => {
            (mode.as_str(), Some(PathBuf::from(to)))
        }
        _ => usage(),
    };
    if mode != "coordinator" && mode != "parallel" {
        usage();
    }

    let host = env_or("CB_HOST", "localhost");
    let port: u16 = env_or("CB_PORT", "5432").parse()?;
    let user = env_or("CB_USER", "gpadmin");
    let password = env::var("CB_PASSWORD").ok().filter(|p| !p.is_empty());
    let db = env_or("CB_DB", "postgres");
    let query = env_or("CB_QUERY", "SELECT * FROM retrieve_demo");

    // Coordinator session. In parallel mode it holds the transaction for the run.
    let mut cfg = Config::new();
    cfg.host(&host)
        .port(port)
        .user(&user)
        .dbname(&db)
        .connect_timeout(CONNECT_TIMEOUT);
    if let Some(p) = &password {
        cfg.password(p);
    }
    let (coord, conn) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("coordinator connection: {e}");
        }
    });

    // Same check for both modes: what the database says the query returns.
    let mut expected = 0u64;
    for msg in step(&coord, &format!("SELECT count(*) FROM ({query}) q")).await? {
        if let SimpleQueryMessage::Row(r) = msg {
            expected = r.get(0).unwrap_or("0").parse()?;
        }
    }
    let out = match &to {
        Some(path) => Some((path.clone(), layout_of(&coord, &query).await?)),
        None => None,
    };
    println!("mode {mode}: {query}");

    let start = Instant::now();
    let rows = if mode == "coordinator" {
        run_coordinator(&coord, &query, out).await?
    } else {
        run_parallel(&coord, &query, &db, out).await?
    };
    let secs = start.elapsed().as_secs_f64();

    let Some(rows) = rows else {
        println!("export failed, nothing committed");
        exit(1);
    };
    println!(
        "{rows} rows in {secs:.2}s, {:.0} rows/s",
        rows as f64 / secs
    );
    if rows == expected {
        println!("row count matches count(*) = {expected}");
        Ok(())
    } else {
        println!("ROW COUNT MISMATCH: count(*) = {expected}");
        exit(1);
    }
}

/// Everything flows through the one coordinator session.
async fn run_coordinator(
    coord: &Client,
    query: &str,
    out: Option<(PathBuf, Layout)>,
) -> Result<Option<u64>, AnyError> {
    let mut sink = match &out {
        Some((path, layout)) => Some(Sink::create(path, layout)?),
        None => None,
    };
    step(coord, "BEGIN").await?;
    step(
        coord,
        &format!("DECLARE {CURSOR} NO SCROLL CURSOR FOR {query}"),
    )
    .await?;
    let fetch = format!("FETCH {BATCH} FROM {CURSOR}");
    let mut total = 0u64;
    loop {
        let msgs = step(coord, &fetch).await?;
        let n = count_rows(&msgs);
        if n == 0 {
            break;
        }
        if let Some(s) = sink.as_mut() {
            s.write(&msgs)?;
        }
        total += n;
    }
    step(coord, &format!("CLOSE {CURSOR}")).await?;
    step(coord, "COMMIT").await?;
    if let (Some(s), Some((path, _))) = (sink, &out) {
        s.close()?;
        println!("wrote {}", path.display());
    }
    Ok(Some(total))
}

/// One retrieve session per segment endpoint, all concurrent.
/// Returns None when a stream failed and the transaction was rolled back.
async fn run_parallel(
    coord: &Client,
    query: &str,
    db: &str,
    out: Option<(PathBuf, Layout)>,
) -> Result<Option<u64>, AnyError> {
    let max_open: usize = env_or("MAX_OPEN", "64").parse()?;
    let fail_at: Option<usize> = env::var("FAIL_ENDPOINT").ok().and_then(|v| v.parse().ok());

    step(coord, "BEGIN").await?;
    step(
        coord,
        &format!("DECLARE {CURSOR} PARALLEL RETRIEVE CURSOR FOR {query}"),
    )
    .await?;

    let mut endpoints = Vec::new();
    let sql = format!(
        "SELECT hostname, port, auth_token, endpointname, username \
         FROM gp_endpoints WHERE cursorname = '{CURSOR}'"
    );
    for msg in step(coord, &sql).await? {
        if let SimpleQueryMessage::Row(r) = msg {
            endpoints.push(Endpoint {
                host: r.get(0).unwrap_or_default().to_string(),
                port: r.get(1).unwrap_or("0").parse()?,
                token: r.get(2).unwrap_or_default().to_string(),
                name: r.get(3).unwrap_or_default().to_string(),
                user: r.get(4).unwrap_or_default().to_string(),
            });
        }
    }
    println!(
        "{} endpoints, at most {max_open} open at once",
        endpoints.len()
    );
    let parts: Vec<PathBuf> = match &out {
        Some((to, _)) => (0..endpoints.len()).map(|i| part_path(to, i)).collect(),
        None => Vec::new(),
    };

    let sem = Arc::new(Semaphore::new(max_open));
    let (stop, guard) = shutdown::new();
    let mut tasks = Vec::new();

    for (i, ep) in endpoints.into_iter().enumerate() {
        let (sem, guard, stop, db) = (sem.clone(), guard.clone(), stop.clone(), db.to_string());
        let part = out
            .as_ref()
            .map(|(_, layout)| (parts[i].clone(), layout.clone()));
        tasks.push(tokio::spawn(async move {
            let queued = Instant::now();
            let _permit = sem.acquire_owned(1).await;
            let waited = queued.elapsed();
            if guard.is_shutdown_requested() {
                println!("endpoint {i}  never started");
                return Err("never started".to_string());
            }
            let t0 = Instant::now();
            let result = retrieve(&ep, &db, &guard, fail_at == Some(i), part).await;
            match &result {
                Ok(n) => println!(
                    "endpoint {i} {}:{}  waited {:>5.2}s for a permit, then {n} rows in {:.2}s",
                    ep.host,
                    ep.port,
                    waited.as_secs_f64(),
                    t0.elapsed().as_secs_f64()
                ),
                Err(e) if e == "cancelled" => println!("endpoint {i}  cancelled, session closed"),
                Err(e) => {
                    eprintln!(
                        "endpoint {i} {}:{}  FAILED: {e}; requesting shutdown",
                        ep.host, ep.port
                    );
                    stop.request_shutdown();
                }
            }
            result
        }));
    }
    drop(guard); // main holds no guard, so the leak check below can complete

    let mut total = 0u64;
    let mut failed = false;
    for t in tasks {
        match t.await? {
            Ok(n) => total += n,
            Err(_) => failed = true,
        }
    }

    if failed {
        // When a retrieve session quits early, Cloudberry cancels the cursor on
        // the coordinator session. That error arrives as the reply to the next
        // statement, so the first ROLLBACK can fail and a second one is needed.
        if let Err(e) = coord.simple_query("ROLLBACK").await {
            let msg = e.as_db_error().map(|d| d.message().to_string());
            println!("coordinator: {}", msg.unwrap_or_else(|| e.to_string()));
            step(coord, "ROLLBACK").await?;
        }
        println!("rolled back after failure");
        if !parts.is_empty() {
            for p in &parts {
                let _ = fs::remove_file(p);
            }
            println!("removed partial Parquet files");
        }
    } else {
        step(coord, &format!("CLOSE {CURSOR}")).await?;
        step(coord, "COMMIT").await?;
        if let (Some(first), Some(last)) = (parts.first(), parts.last()) {
            println!(
                "wrote {} files, one per segment: {} to {}",
                parts.len(),
                first.display(),
                last.display()
            );
        }
    }

    match timeout(Duration::from_secs(5), stop).await {
        Ok(()) => println!("all guards released, no leaked retrieve connections"),
        Err(_) => println!("LEAK: a retrieve connection is still open after 5s"),
    }
    Ok(if failed { None } else { Some(total) })
}

async fn retrieve(
    ep: &Endpoint,
    db: &str,
    guard: &ShutdownGuard,
    inject_fail: bool,
    part: Option<(PathBuf, Layout)>,
) -> Result<u64, String> {
    let mut cfg = Config::new();
    cfg.host(&ep.host)
        .port(ep.port)
        .user(&ep.user)
        .password(&ep.token)
        .dbname(db)
        .options("-c gp_retrieve_conn=true")
        .connect_timeout(CONNECT_TIMEOUT);
    let (client, conn) = cfg.connect(NoTls).await.map_err(|e| e.to_string())?;
    // The connection task holds its own guard until the socket is closed, so
    // the final leak check covers retrieve connections, not only the tasks.
    let conn_guard = guard.clone();
    tokio::spawn(async move {
        let _conn_guard = conn_guard;
        let _ = conn.await;
    });
    let mut sink = match &part {
        Some((path, layout)) => Some(Sink::create(path, layout)?),
        None => None,
    };

    let sql = format!("RETRIEVE {BATCH} FROM ENDPOINT {}", ep.name);
    let mut total = 0u64;
    loop {
        let msgs = tokio::select! {
            _ = guard.shutdown_requested() => return Err("cancelled".to_string()),
            r = timeout(STEP_TIMEOUT, client.simple_query(&sql)) => match r {
                Ok(r) => r.map_err(|e| e.to_string())?,
                Err(_) => return Err(format!("no batch within {STEP_TIMEOUT:?}")),
            },
        };
        let n = count_rows(&msgs);
        if n == 0 {
            break;
        }
        if let Some(s) = sink.as_mut() {
            s.write(&msgs)?;
        }
        total += n;
        if inject_fail {
            return Err("injected failure after first batch".to_string());
        }
    }
    if let Some(s) = sink {
        s.close()?;
    }
    Ok(total)
}
