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

// Parallel retrieve from a real Cloudberry / SynxDB cluster.
//
// Coordinator: BEGIN, DECLARE ... PARALLEL RETRIEVE CURSOR, read gp_endpoints.
// Then one retrieve session per segment endpoint, all concurrent.
//
// Asyncband pieces exercised:
//   Semaphore  caps how many retrieve sessions are open at once (MAX_OPEN)
//   shutdown   any failing endpoint cancels all other streams; then ROLLBACK
//              awaiting Shutdown at the end checks that every retrieve
//              connection has closed (each connection task holds a guard)
//
// Env: CB_HOST CB_PORT CB_USER CB_PASSWORD CB_DB CB_QUERY MAX_OPEN
//      FAIL_ENDPOINT=<index>  optional: drop that stream after its first batch

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use asyncband::semaphore::Semaphore;
use asyncband::shutdown::{self, ShutdownGuard};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

const BATCH: usize = 10_000;
const CURSOR: &str = "asyncband_demo";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

struct Endpoint {
    host: String,
    port: u16,
    token: String,
    name: String,
    user: String,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = env_or("CB_HOST", "localhost");
    let port: u16 = env_or("CB_PORT", "5432").parse()?;
    let user = env_or("CB_USER", "gpadmin");
    let password = env::var("CB_PASSWORD").ok();
    let db = env_or("CB_DB", "postgres");
    let query = env_or("CB_QUERY", "SELECT * FROM retrieve_demo");
    let max_open: usize = env_or("MAX_OPEN", "64").parse()?;
    let fail_at: Option<usize> = env::var("FAIL_ENDPOINT").ok().and_then(|v| v.parse().ok());

    // Coordinator session: holds the transaction for the whole run.
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

    let start = Instant::now();
    coord.simple_query("BEGIN").await?;
    coord
        .simple_query(&format!(
            "DECLARE {CURSOR} PARALLEL RETRIEVE CURSOR FOR {query}"
        ))
        .await?;

    let mut endpoints = Vec::new();
    let sql = format!(
        "SELECT hostname, port, auth_token, endpointname, username \
         FROM gp_endpoints WHERE cursorname = '{CURSOR}'"
    );
    for msg in coord.simple_query(&sql).await? {
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
        let (sem, guard, stop, db) = (sem.clone(), guard.clone(), stop.clone(), db.clone());
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned(1).await;
            if guard.is_shutdown_requested() {
                return Err("never started".to_string());
            }
            let t0 = Instant::now();
            let result = retrieve(&ep, &db, &guard, fail_at == Some(i)).await;
            match &result {
                Ok(n) => println!(
                    "endpoint {i} {}:{}  {n} rows in {:.2?}",
                    ep.host,
                    ep.port,
                    t0.elapsed()
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
            coord.simple_query("ROLLBACK").await?;
        }
        println!("rolled back after failure");
    } else {
        coord.simple_query(&format!("CLOSE {CURSOR}")).await?;
        coord.simple_query("COMMIT").await?;
        println!("{total} rows in {:.2?}", start.elapsed());
    }

    match tokio::time::timeout(Duration::from_secs(5), stop).await {
        Ok(()) => println!("all guards released, no leaked retrieve connections"),
        Err(_) => println!("LEAK: a retrieve connection is still open after 5s"),
    }
    Ok(())
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
            r = client.simple_query(&sql) => r.map_err(|e| e.to_string())?,
        };
        let n = msgs
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count() as u64;
        if n == 0 {
            return Ok(total);
        }
        total += n;
        if inject_fail {
            return Err("injected failure after first batch".to_string());
        }
    }
}
