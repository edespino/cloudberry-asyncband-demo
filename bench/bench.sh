#!/usr/bin/env bash
# Copyright 2026 Ed Espino
# Licensed under the Apache License, Version 2.0. See the LICENSE file.
#
# T6 benchmark: four ways to export one table, two sinks each.
#
#   coordinator   demo binary, regular cursor through the coordinator
#   parallel      demo binary, parallel retrieve cursor, one session per segment
#   gpfdist       writable external table, segments push to gpfdist on this host
#   copy-on-seg   COPY ... ON SEGMENT, each segment writes on its own host
#
#   sink "discard"  rows are read and dropped (nothing reaches a disk)
#   sink "disk"     rows are written: Parquet for the demo binary, text for the
#                   other two
#
# Run on a cluster host as the database owner with the Cloudberry environment
# sourced and the demo binary built. Prints one tab separated line per run:
#   table  path  sink  run  seconds  rows
#
# Env: CB_HOST CB_PORT CB_USER CB_DB (as for the demo)
#      TABLES   tables to export (default "retrieve_demo retrieve_demo_100m")
#      RUNS     runs per case (default 3)
#      DIST_KEY distribution key of the tables (default id)
#      BIN      demo binary
#      OUT      scratch directory on this host (default ./bench-out)
#      SEG_DIR  scratch directory created on every segment host
#      GPFDIST_HOST GPFDIST_PORT  where segments reach gpfdist (default this host, 8081)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
export PGHOST=${CB_HOST:-localhost} PGPORT=${CB_PORT:-5432} PGUSER=${CB_USER:-$USER} PGDATABASE=${CB_DB:-postgres}
export PGCONNECT_TIMEOUT=10
TABLES=${TABLES:-"retrieve_demo retrieve_demo_100m"}
RUNS=${RUNS:-3}
DIST_KEY=${DIST_KEY:-id}
BIN=${BIN:-"$here/../demo/target/release/asyncband-cloudberry-demo"}
OUT=${OUT:-"$PWD/bench-out"}
SEG_DIR=${SEG_DIR:-/tmp/asyncband-bench}
GPFDIST_HOST=${GPFDIST_HOST:-$(hostname)}
GPFDIST_PORT=${GPFDIST_PORT:-8081}
LIMIT=${BENCH_TIMEOUT:-1800}

hosts=$(psql -X -At -c "SELECT DISTINCT hostname FROM gp_segment_configuration WHERE content >= 0 AND role = 'p'")
clean() {
    rm -f "$OUT"/*.parquet "$OUT"/*.txt
    for h in $hosts; do ssh "$h" "rm -f $SEG_DIR/*.txt"; done
}
mkdir -p "$OUT"
for h in $hosts; do ssh "$h" "mkdir -p $SEG_DIR"; done
clean
ln -sf /dev/null "$OUT/discard.txt"

gpfdist -d "$OUT" -p "$GPFDIST_PORT" -l "$OUT/gpfdist.log" >/dev/null 2>&1 &
gpfdist_pid=$!
trap 'kill $gpfdist_pid 2>/dev/null; clean; rm -f "$OUT/discard.txt"' EXIT
sleep 2
kill -0 $gpfdist_pid

# Run one SQL statement; print "<seconds> <rows>" from psql's timing and command tag.
sql() {
    timeout "$LIMIT" psql -X -v ON_ERROR_STOP=1 -c '\timing on' -c "$1" |
        awk '/^(INSERT|COPY)/ { rows = $NF } /^Time:/ { secs = $2 / 1000 } END { printf "%.2f %d\n", secs, rows }'
}
# Run the demo binary; print "<seconds> <rows>" from its own summary line.
demo() {
    timeout "$LIMIT" "$BIN" "$@" 2>/dev/null |
        awk '/ rows in / { rows = $1; sub(/s,$/, "", $4); secs = $4 } /MISMATCH|export failed/ { bad = 1 }
             END { if (bad) exit 1; printf "%.2f %d\n", secs, rows }'
}
report() {
    set -- "$1" "$2" "$3" "$4" $5
    if [ "${6:-0}" -eq 0 ]; then
        echo "FAILED: $1 $2 $3 run $4 exported no rows" >&2
        exit 1
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$@"
}

for t in $TABLES; do
    export CB_QUERY="SELECT * FROM $t"
    psql -X -q -v ON_ERROR_STOP=1 <<SQL
DROP EXTERNAL TABLE IF EXISTS bench_wet_discard;
DROP EXTERNAL TABLE IF EXISTS bench_wet_disk;
CREATE WRITABLE EXTERNAL TABLE bench_wet_discard (LIKE $t)
    LOCATION ('gpfdist://$GPFDIST_HOST:$GPFDIST_PORT/discard.txt') FORMAT 'TEXT' DISTRIBUTED BY ($DIST_KEY);
CREATE WRITABLE EXTERNAL TABLE bench_wet_disk (LIKE $t)
    LOCATION ('gpfdist://$GPFDIST_HOST:$GPFDIST_PORT/gpfdist.txt') FORMAT 'TEXT' DISTRIBUTED BY ($DIST_KEY);
SQL
    for run in $(seq 1 "$RUNS"); do
        report "$t" coordinator discard "$run" "$(demo --mode coordinator)"
        report "$t" parallel discard "$run" "$(demo --mode parallel)"
        report "$t" gpfdist discard "$run" "$(sql "INSERT INTO bench_wet_discard SELECT * FROM $t")"
        report "$t" copy-on-seg discard "$run" "$(sql "COPY $t TO PROGRAM 'cat > /dev/null && : <SEGID>' ON SEGMENT")"

        report "$t" coordinator disk "$run" "$(demo --mode coordinator --to "$OUT/coord.parquet")"
        clean
        report "$t" parallel disk "$run" "$(demo --mode parallel --to "$OUT/par.parquet")"
        clean
        report "$t" gpfdist disk "$run" "$(sql "INSERT INTO bench_wet_disk SELECT * FROM $t")"
        clean
        report "$t" copy-on-seg disk "$run" "$(sql "COPY $t TO '$SEG_DIR/out_<SEGID>.txt' ON SEGMENT")"
        clean
    done
done
psql -X -q -c 'DROP EXTERNAL TABLE IF EXISTS bench_wet_discard' -c 'DROP EXTERNAL TABLE IF EXISTS bench_wet_disk'
