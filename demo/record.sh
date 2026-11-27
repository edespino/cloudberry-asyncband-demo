#!/usr/bin/env bash
# Copyright 2026 Ed Espino
# Licensed under the Apache License, Version 2.0. See the LICENSE file.
#
# Record the demo as asciinema casts, 80 columns by 24 rows, so a talk never
# depends on a live run. Needs asciinema, the CB_* variables, and for the
# failure act the settings failure.sh describes.
#
# usage: record.sh [output directory]      (default: recordings next to this script)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
out=${1:-"$here/recordings"}
mkdir -p "$out"
export CB_QUERY=${CB_QUERY:-"SELECT * FROM retrieve_demo_100m"}

rec() {  # name, title, command line shown and run
    asciinema rec -q --overwrite --cols 80 --rows 24 -t "$2" \
        -c "echo '\$ $3'; $3; sleep 2" "$out/$1.cast"
}

cd "$here/.."
rec race "Coordinator fetch against parallel retrieve" "demo/race.sh"
rec permits "Asyncband semaphore: four retrieve sessions at a time" \
    "MAX_OPEN=4 demo/target/release/asyncband-cloudberry-demo --mode parallel"
rec failure "One retrieve backend is terminated mid export" \
    "demo/failure.sh ${FAIL_HOST:-sdw1} ${FAIL_PORT:-6001} ${FAIL_DELAY:-4}"
ls -l "$out"
