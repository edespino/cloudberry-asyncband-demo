#!/usr/bin/env bash
# Copyright 2026 Ed Espino
# Licensed under the Apache License, Version 2.0. See the LICENSE file.
#
# The failure act with a real server side failure: start a parallel export,
# and while it runs terminate the backend behind one retrieve session.
# See FAILURE.md.
#
# usage: failure.sh [segment host] [segment port] [seconds before the kill]
#
# kill-one.sh has to run as the database owner on a cluster host. When this
# script runs somewhere else, set KILL_FROM to a cluster host reachable over
# ssh. KILL_DIR is the demo directory there and KILL_ENV the file that sets up
# the Cloudberry environment there.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-"$here/target/release/asyncband-cloudberry-demo"}
host=${1:-sdw1}
port=${2:-6001}
delay=${3:-4}
log=$(mktemp)
trap 'rm -f "$log"' EXIT

if [ -n "${KILL_FROM:-}" ]; then
    ssh "$KILL_FROM" "source ${KILL_ENV:-/usr/local/cloudberry/cloudberry-env.sh} && ${KILL_DIR:-asyncband-demo/demo}/kill-one.sh $host $port $delay" >"$log" 2>&1 &
else
    "$here/kill-one.sh" "$host" "$port" "$delay" >"$log" 2>&1 &
fi

status=0
timeout "${FAILURE_TIMEOUT:-300}" "$BIN" --mode parallel || status=$?
wait
echo
echo "second session: $(cat "$log")"
echo "client exit code: $status"
