#!/usr/bin/env bash
# Copyright 2026 Ed Espino
# Licensed under the Apache License, Version 2.0. See the LICENSE file.
#
# A real server side failure for the failure act: wait until a retrieve
# session shows up on one segment, let it run for a moment, then terminate
# its backend from a second session. See FAILURE.md.
#
# usage: kill-one.sh <segment host> <segment port> [seconds to let it run]
# Run as the database owner on a cluster host, with the Cloudberry
# environment sourced. CB_DB names the database (default postgres).
set -euo pipefail

host=$1
port=$2
delay=${3:-2}
# A direct session to a segment has to be in utility mode.
export PGOPTIONS='-c gp_role=utility' PGCONNECT_TIMEOUT=5
q() { psql -X -At -h "$host" -p "$port" -d "${CB_DB:-postgres}" -c "$1"; }

for _ in $(seq 1 300); do
    pid=$(q "SELECT pid FROM pg_stat_activity
             WHERE query ILIKE 'retrieve%' AND pid <> pg_backend_pid() LIMIT 1")
    if [ -n "$pid" ]; then
        sleep "$delay"
        q "SELECT 'terminated backend $pid on $host:$port', pg_terminate_backend($pid)"
        exit 0
    fi
    sleep 0.1
done
echo "no retrieve session seen on $host:$port within 30 seconds"
exit 1
