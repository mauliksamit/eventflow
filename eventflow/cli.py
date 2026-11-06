"""Command line entry point: `python -m eventflow.cli <step>`.

Steps: generate | extract | land | transform | load | marts | check | report | tune
scripts/run_pipeline.sh runs them in order with logging and retries.
"""
from __future__ import annotations

import json
import logging
import sys
import uuid
from pathlib import Path

import psycopg

from .config import get_config

log = logging.getLogger("eventflow")


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not argv:
        print(__doc__)
        return 2
    step, cfg = argv[0], get_config()
    if step == "generate":
        from .generate import generate
        from .load import init_warehouse
        init_warehouse(cfg)
        log.info("generated %s", json.dumps(generate(cfg)))
    elif step == "extract":
        from .extract import extract_orders
        log.info("extracted %s", json.dumps(extract_orders(cfg)))
    elif step == "land":
        if cfg.storage == "s3":
            from .storage import S3RawStore
            store = S3RawStore(cfg.s3_bucket)
            store.ensure_bucket()
            log.info("uploaded %d raw files to s3://%s/raw", store.upload_tree(cfg.raw), cfg.s3_bucket)
        else:
            log.info("storage=local, raw zone stays at %s", cfg.raw)
    elif step == "transform":
        from .transform import run
        log.info("transform %s", json.dumps(run(cfg)))
    elif step == "load":
        from .load import init_warehouse, load
        init_warehouse(cfg)
        log.info("load %s", json.dumps(load(cfg)))
    elif step == "marts":
        sql = (Path(__file__).resolve().parent.parent / "sql" / "marts.sql").read_text()
        with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
            conn.execute(sql)
        log.info("marts refreshed")
    elif step == "check":
        from .quality import run_checks
        run_id = argv[1] if len(argv) > 1 else uuid.uuid4().hex[:12]
        ok, checks = run_checks(run_id, cfg)
        for c in checks:
            level = logging.INFO if c.passed else (logging.ERROR if c.severity == "critical" else logging.WARNING)
            log.log(level, "dq %-32s %-8s passed=%s observed=%s threshold=%s %s",
                    c.name, c.severity, c.passed, c.observed, c.threshold, c.details)
        if not ok:
            log.error("critical data quality check failed for run %s", run_id)
            return 1
    elif step == "report":
        from .report import build_dashboard
        log.info("dashboard written to %s", build_dashboard(cfg=cfg))
    elif step == "tune":
        from .tune import benchmark
        for r in benchmark(cfg):
            log.info("%-26s %9.2f ms -> %8.2f ms (%sx)", r["query"], r["before_ms"], r["after_ms"], r["speedup"])
    else:
        print(f"unknown step {step!r}\n{__doc__}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
