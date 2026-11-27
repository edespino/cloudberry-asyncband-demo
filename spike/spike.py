# Copyright 2026 Ed Espino
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""T0 spike: parallel retrieve versus a coordinator fetch, same query.

Parallel path:    BEGIN, DECLARE ... PARALLEL RETRIEVE CURSOR, read gp_endpoints,
                  then one worker per endpoint running RETRIEVE in batches.
Coordinator path: BEGIN, DECLARE ... CURSOR, FETCH in batches on one session.

Both paths pull every row into Python as text and count it. Nothing is written.

Env: CB_HOST CB_PORT CB_USER CB_PASSWORD CB_DB CB_QUERY
     SPIKE_RUNS     runs per path (default 3)
     SPIKE_BATCH    rows per RETRIEVE or FETCH (default 10000)
     SPIKE_WORKERS  "thread" (default) or "process", one worker per endpoint
     SPIKE_TIMEOUT  seconds allowed for one run before the spike aborts (default 600)
"""

import multiprocessing
import os
import sys
import threading
import time

import psycopg

HOST = os.environ.get("CB_HOST", "localhost")
PORT = int(os.environ.get("CB_PORT", "5432"))
USER = os.environ.get("CB_USER", "gpadmin")
PASSWORD = os.environ.get("CB_PASSWORD") or None
DB = os.environ.get("CB_DB", "postgres")
QUERY = os.environ.get("CB_QUERY") or "SELECT * FROM retrieve_demo"
RUNS = int(os.environ.get("SPIKE_RUNS", "3"))
BATCH = int(os.environ.get("SPIKE_BATCH", "10000"))
WORKERS = os.environ.get("SPIKE_WORKERS", "thread")
TIMEOUT = float(os.environ.get("SPIKE_TIMEOUT", "600"))
CURSOR = "spike_cur"
CONNECT_TIMEOUT = 10


def coordinator_conn():
    return psycopg.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DB,
        connect_timeout=CONNECT_TIMEOUT,
    )


def retrieve_endpoint(ep, out, idx):
    """One retrieve session. ep = (hostname, port, auth_token, endpointname, username)."""
    host, port, token, name, user = ep
    try:
        # The retrieve session authenticates with the endpoint token and must
        # set gp_retrieve_conn at startup.
        with psycopg.connect(
            host=host, port=port, user=user, password=token, dbname=DB,
            options="-c gp_retrieve_conn=true",
            connect_timeout=CONNECT_TIMEOUT, autocommit=True,
        ) as conn:
            cur = conn.cursor()
            sql = f"RETRIEVE {BATCH} FROM ENDPOINT {name}"
            total = 0
            while True:
                cur.execute(sql)
                n = len(cur.fetchall())
                if n == 0:
                    break
                total += n
        out[idx] = total
    except Exception as e:  # print and let the count check fail the run
        out[idx] = f"ERROR {type(e).__name__}: {e}"


def run_parallel():
    with coordinator_conn() as coord:
        t0 = time.perf_counter()
        cur = coord.cursor()
        cur.execute(f"DECLARE {CURSOR} PARALLEL RETRIEVE CURSOR FOR {QUERY}")
        cur.execute(
            "SELECT hostname, port, auth_token, endpointname, username "
            "FROM gp_endpoints WHERE cursorname = %s",
            (CURSOR,),
        )
        endpoints = cur.fetchall()

        if WORKERS == "process":
            mgr = multiprocessing.Manager()
            out = mgr.list([None] * len(endpoints))
            make = multiprocessing.Process
        else:
            out = [None] * len(endpoints)
            make = threading.Thread
        workers = [
            make(target=retrieve_endpoint, args=(ep, out, i), daemon=True)
            for i, ep in enumerate(endpoints)
        ]
        for w in workers:
            w.start()
        deadline = time.monotonic() + TIMEOUT
        for w in workers:
            w.join(max(0.0, deadline - time.monotonic()))
            if w.is_alive():
                print("TIMEOUT: a retrieve worker is still running; aborting", flush=True)
                os._exit(2)
        elapsed = time.perf_counter() - t0

        per_endpoint = list(out)
        # Returns true only when every endpoint was fully retrieved.
        cur.execute("SELECT gp_wait_parallel_retrieve_cursor(%s, 5)", (CURSOR,))
        finished = cur.fetchone()[0]
        cur.execute(f"CLOSE {CURSOR}")
        coord.commit()

    errors = [x for x in per_endpoint if not isinstance(x, int)]
    if errors:
        print("retrieve errors:", errors, flush=True)
        sys.exit(1)
    return sum(per_endpoint), elapsed, per_endpoint, finished


def run_coordinator():
    with coordinator_conn() as coord:
        t0 = time.perf_counter()
        cur = coord.cursor()
        cur.execute(f"DECLARE {CURSOR} NO SCROLL CURSOR FOR {QUERY}")
        sql = f"FETCH {BATCH} FROM {CURSOR}"
        total = 0
        while True:
            cur.execute(sql)
            n = len(cur.fetchall())
            if n == 0:
                break
            total += n
        cur.execute(f"CLOSE {CURSOR}")
        coord.commit()
        elapsed = time.perf_counter() - t0
    return total, elapsed


def main():
    with coordinator_conn() as c:
        cur = c.cursor()
        cur.execute("SELECT version()")
        print(cur.fetchone()[0].split(" on ")[0])
        cur.execute(
            "SELECT count(*) FROM gp_segment_configuration "
            "WHERE content >= 0 AND role = 'p'"
        )
        print("primary segments:", cur.fetchone()[0])
        cur.execute(f"SELECT count(*) FROM ({QUERY}) q")
        expected = cur.fetchone()[0]
    print("query:", QUERY)
    print("expected rows (count(*)):", expected)
    print(f"batch {BATCH}, workers {WORKERS}, psycopg {psycopg.__version__}, runs {RUNS}")
    print()

    best = {}
    ok = True
    for run in range(1, RUNS + 1):
        rows, secs = run_coordinator()
        ok &= rows == expected
        best["coordinator"] = min(best.get("coordinator", secs), secs)
        print(f"run {run} coordinator  rows={rows}  {secs:7.2f}s  {rows / secs:12,.0f} rows/s", flush=True)

        rows, secs, per_ep, finished = run_parallel()
        ok &= rows == expected and finished
        best["parallel"] = min(best.get("parallel", secs), secs)
        print(
            f"run {run} parallel     rows={rows}  {secs:7.2f}s  {rows / secs:12,.0f} rows/s"
            f"  endpoints={per_ep}  all_finished={finished}",
            flush=True,
        )

    print()
    print(f"best coordinator {best['coordinator']:.2f}s, best parallel {best['parallel']:.2f}s, "
          f"speedup {best['coordinator'] / best['parallel']:.2f}x")
    print("row counts match" if ok else "ROW COUNT MISMATCH")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
