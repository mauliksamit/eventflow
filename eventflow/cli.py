"""Command line entry point: python -m eventflow.cli <command> [--start DAY] [--end DAY]

    init       create warehouse tables
    status     snapshot GitHub's published incidents
    ingest     land raw hours for each day
    transform  transform, audit and publish each day (Write-Audit-Publish)
    load       load each published day into the warehouse
    run        ingest + transform + load for each day, oldest first
    marts      build or refresh the reporting layer
    analyze    score feed anomalies against published incidents
    report     write the dashboard and charts
    tune       run the query tuning benchmark

Exit code is non-zero if any step fails, so a scheduler can alert on it. An audit failure stops
the run: that day is not published and the warehouse keeps its last good data.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg

from .config import Config, get_config

log = logging.getLogger("eventflow")
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def days(start: str, end: str | None) -> list[date]:
    a, b = date.fromisoformat(start), date.fromisoformat(end or start)
    return [a + timedelta(n) for n in range((b - a).days + 1)]


def transform_days(cfg: Config, targets: list[date]) -> bool:
    from .quality import audit_and_publish, blocking
    from .transform import run_day, spark_session
    spark = spark_session(cfg)
    for day in targets:
        run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        stats = run_day(cfg, spark, day, run_id)
        ok, checks = audit_and_publish(cfg, day, run_id, stats)
        warned = [c.name for c in checks if not c.passed and c.severity == "warn"]
        log.info("%s %s rows=%s unique=%s %ss warn=%s", day, "published" if ok else "BLOCKED", stats["parsed_rows"],
                 stats["valid_unique"], stats["transform_seconds"], warned)
        if not ok:
            for c in blocking(checks):
                log.error("%s audit failed: %s observed=%s threshold=%s %s", day, c.name, c.observed, c.threshold, c.details)
            return False
    return True


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser(prog="eventflow", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command")
    ap.add_argument("--start")
    ap.add_argument("--end")
    a = ap.parse_args(argv)
    cfg = get_config()

    if a.command in ("ingest", "transform", "load", "run") and not a.start:
        ap.error(f"{a.command} needs --start")
    targets = days(a.start, a.end) if a.start else []

    if a.command == "init":
        from .load import init_warehouse
        init_warehouse(cfg)
    elif a.command == "status":
        from .ingest import ingest_status
        log.info("status %s", json.dumps(ingest_status(cfg)))
    elif a.command == "ingest":
        from .ingest import ingest_day
        for day in targets:
            log.info("ingest %s", json.dumps(ingest_day(cfg, day)))
    elif a.command == "transform":
        return 0 if transform_days(cfg, targets) else 1
    elif a.command == "load":
        from .load import load_day
        for day in targets:
            log.info("load %s", json.dumps(load_day(cfg, day)))
    elif a.command == "run":
        from .ingest import ingest_day
        from .load import load_day
        for day in targets:
            log.info("ingest %s", json.dumps(ingest_day(cfg, day)))
            if not transform_days(cfg, [day]):
                return 1
            log.info("load %s", json.dumps(load_day(cfg, day)))
    elif a.command == "marts":
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            conn.execute((SQL_DIR / "marts.sql").read_text())
    elif a.command == "analyze":
        from .analysis import analyze
        log.info("analyze %s", json.dumps(analyze(cfg)))
    elif a.command == "report":
        from .report import build
        log.info("report written to %s", build(cfg))
    elif a.command == "tune":
        from .tune import benchmark
        for r in benchmark(cfg):
            log.info("%-26s %9.2f ms -> %8.2f ms (%sx)", r["query"], r["before_ms"], r["after_ms"], r["speedup"])
    else:
        ap.error(f"unknown command {a.command!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
