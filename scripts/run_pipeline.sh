#!/usr/bin/env bash
# Orchestrates one end-to-end pipeline run with logging, timing and retries.
#
#   scripts/run_pipeline.sh            # full run (generate synthetic sources first)
#   SKIP_GENERATE=1 scripts/run_pipeline.sh   # re-process existing raw data (idempotent)
#
# Exit code is non-zero if any step fails or a critical data quality check fails,
# so a scheduler (cron, Airflow, Step Functions) can alert on it.
set -euo pipefail

cd "$(dirname "$0")/.."
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG_DIR="${EVENTFLOW_DATA_DIR:-data}/runs/${RUN_ID}"
mkdir -p "$LOG_DIR"
PY="${PYTHON:-python3}"

log() { printf '%s [%s] %s\n' "$(date -u +%H:%M:%S)" "$RUN_ID" "$*" | tee -a "$LOG_DIR/pipeline.log"; }

# retry <attempts> <command...>: re-run transient failures (e.g. database briefly unavailable)
retry() {
  local attempts=$1; shift
  local n=1
  until "$@" >>"$LOG_DIR/pipeline.log" 2>&1; do
    if (( n >= attempts )); then
      log "FAILED after $n attempts: $*"
      return 1
    fi
    log "attempt $n failed, retrying in $((n * 5))s: $*"
    sleep $((n * 5))
    n=$((n + 1))
  done
}

step() {
  local name=$1; shift
  local start=$SECONDS
  log "start $name"
  retry "${RETRIES:-2}" "$PY" -m eventflow.cli "$name" "$@"
  log "done  $name ($((SECONDS - start))s)"
}

log "pipeline run starting (data dir: ${EVENTFLOW_DATA_DIR:-data})"
if [[ -z "${SKIP_GENERATE:-}" ]]; then
  step generate
fi
step extract
step land
step transform
step load
step marts
# Quality checks are not retried: a failure here is a data problem, not a transient one.
log "start check"
if ! "$PY" -m eventflow.cli check "$RUN_ID" 2>&1 | tee -a "$LOG_DIR/pipeline.log"; then
  log "critical data quality check failed - see ops.dq_results for run $RUN_ID"
  exit 1
fi
step report
log "pipeline run finished in ${SECONDS}s"
