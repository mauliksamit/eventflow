#!/usr/bin/env bash
# End-to-end check on one real archive day: the happy path, a blocked publish and a reload.
# Expects EVENTFLOW_PG_DSN and EVENTFLOW_LAKE; run it against an empty warehouse.
set -euo pipefail

DAY="${E2E_DAY:-2026-08-20}"
PY="${PYTHON:-python}"
cli() { "$PY" -m eventflow.cli "$@"; }
sql() { "$PY" -c 'import os, sys, psycopg
with psycopg.connect(os.environ["EVENTFLOW_PG_DSN"]) as c:
    print(c.execute(sys.argv[1]).fetchone()[0])' "$1"; }
counts() {
  for t in fact_repo_daily fact_star fact_type_hourly fact_feed_hourly fact_feed_minute dim_repo dim_actor; do
    printf '%s=%s ' "$t" "$(sql "SELECT count(*) FROM dw.$t")"
  done
}

cli init
cli status

echo "== pipeline run"
cli run --start "$DAY"
first=$(counts)
published=$(sql "SELECT run_id FROM ops.published_partitions WHERE dt = '$DAY'")
echo "$first"
[ "$(sql 'SELECT count(*) FROM dw.fact_repo_daily')" -gt 0 ]

echo "== audit failure must not reach the warehouse"
if EVENTFLOW_MIN_HOURS=25 cli transform --start "$DAY"; then
  echo "expected the audit to block this run" >&2
  exit 1
fi
[ "$(sql "SELECT run_id FROM ops.published_partitions WHERE dt = '$DAY'")" = "$published" ]
cli load --start "$DAY"
[ "$(counts)" = "$first" ]

echo "== reprocessing the same day changes nothing"
cli run --start "$DAY"
[ "$(counts)" = "$first" ]

cli marts
cli analyze
echo "e2e passed"
