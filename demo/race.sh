#!/usr/bin/env bash
# Copyright 2026 Ed Espino
# Licensed under the Apache License, Version 2.0. See the LICENSE file.
#
# The race: the same query exported through the coordinator, then straight
# from the segments. Runs back to back and ends with one comparison line.
#
# Needs the CB_* variables. Optional: CB_QUERY, MAX_OPEN, RACE_TIMEOUT (seconds).
set -euo pipefail

BIN=${BIN:-"$(dirname "$0")/target/release/asyncband-cloudberry-demo"}
LIMIT=${RACE_TIMEOUT:-900}
log=$(mktemp)
trap 'rm -f "$log"' EXIT

run() {
    echo
    timeout "$LIMIT" "$BIN" --mode "$1" | tee "$log"
}
seconds() { sed -n 's/^[0-9]* rows in \([0-9.]*\)s.*/\1/p' "$log"; }

run coordinator
coord=$(seconds)
run parallel
par=$(seconds)
segments=$(sed -n 's/^\([0-9]*\) endpoints.*/\1/p' "$log")

echo
awk -v c="$coord" -v p="$par" -v n="$segments" 'BEGIN {
    printf "coordinator %.2fs, parallel %.2fs across %d segments: %.1fx faster\n", c, p, n, c / p
}'
