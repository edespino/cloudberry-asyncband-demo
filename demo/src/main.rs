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
//
// Tokio does the I/O. Asyncband does the coordination in parallel mode:
//   Semaphore  caps how many retrieve sessions are open at once (MAX_OPEN)
//   shutdown   one failing endpoint cancels every other stream; awaiting
//              Shutdown at the end checks every retrieve connection closed
//
// Env: CB_HOST CB_PORT CB_USER CB_PASSWORD CB_DB CB_QUERY MAX_OPEN
//      FAIL_ENDPOINT=<index>  optional: drop that stream after its first batch

use std::env;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use asyncband::semaphore::Semaphore;
use asyncband::shutdown::{self, ShutdownGuard};
use tokio::time::timeout;
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

fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

fn usage() -> ! {
    eprintln!("usage: asyncband-cloudberry-demo --mode coordinator|parallel");
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
    let mode = match args.as_slice() {
        [flag, value] if flag == "--mode" => value.as_str(),
        _ => usage(),
    };

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
    println!("mode {mode}: {query}");

    let start = Instant::now();
    let rows = match mode {
        "coordinator" => run_coordinator(&coord, &query).await?,
        "parallel" => run_parallel(&coord, &query, &db).await?,
        _ => usage(),
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
async fn run_coordinator(coord: &Client, query: &str) -> Result<Option<u64>, AnyError> {
    step(coord, "BEGIN").await?;
    step(
        coord,
        &format!("DECLARE {CURSOR} NO SCROLL CURSOR FOR {query}"),
    )
    .await?;
    let fetch = format!("FETCH {BATCH} FROM {CURSOR}");
    let mut total = 0u64;
    loop {
        let n = count_rows(&step(coord, &fetch).await?);
        if n == 0 {
            break;
        }
        total += n;
    }
    step(coord, &format!("CLOSE {CURSOR}")).await?;
    step(coord, "COMMIT").await?;
    Ok(Some(total))
}

/// One retrieve session per segment endpoint, all concurrent.
/// Returns None when a stream failed and the transaction was rolled back.
async fn run_parallel(coord: &Client, query: &str, db: &str) -> Result<Option<u64>, AnyError> {
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

    let sem = Arc::new(Semaphore::new(max_open));
    let (stop, guard) = shutdown::new();
    let mut tasks = Vec::new();

    for (i, ep) in endpoints.into_iter().enumerate() {
        let (sem, guard, stop, db) = (sem.clone(), guard.clone(), stop.clone(), db.to_string());
        tasks.push(tokio::spawn(async move {
            let queued = Instant::now();
            let _permit = sem.acquire_owned(1).await;
            let waited = queued.elapsed();
            if guard.is_shutdown_requested() {
                println!("endpoint {i}  never started");
                return Err("never started".to_string());
            }
            let t0 = Instant::now();
            let result = retrieve(&ep, &db, &guard, fail_at == Some(i)).await;
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
    } else {
        step(coord, &format!("CLOSE {CURSOR}")).await?;
        step(coord, "COMMIT").await?;
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
            return Ok(total);
        }
        total += n;
        if inject_fail {
            return Err("injected failure after first batch".to_string());
        }
    }
}
