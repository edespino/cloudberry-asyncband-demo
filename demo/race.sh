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
summary=$(mktemp)
trap 'rm -f "$summary"' EXIT
export SUMMARY_FILE=$summary

# The binary writes straight to the terminal so its bars and lines stay in
# order. The times for the last line come from the summary file it appends to.
for mode in coordinator parallel; do
    echo
    timeout "$LIMIT" "$BIN" --mode "$mode"
done

echo
awk 'NR == 1 { c = $2 } NR == 2 { p = $2; n = $4 } END {
    printf "coordinator %.2fs, parallel %.2fs across %d segments: %.1fx faster\n", c, p, n, c / p
}' "$summary"
