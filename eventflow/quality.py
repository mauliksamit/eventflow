"""Data quality checks and pipeline monitoring.

Every run records its stats in ops.pipeline_runs and each check result in ops.dq_results, so
problems can be traced back to the run (and input) that caused them. A failed *critical* check
fails the run (non-zero exit), a failed *warn* check only raises an alert in the log.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta

import psycopg

from .config import Config, get_config


@dataclass
class Check:
    name: str
    severity: str  # "critical" | "warn"
    passed: bool
    observed: float | None = None
    threshold: float | None = None
    details: str = ""


def reconciliation_checks(t: dict, cfg: Config) -> list[Check]:
    """Checks on transform stats: nothing may be silently dropped between raw and curated."""
    checks = []
    accounted = t["clickstream_valid"] + t["clickstream_quarantined"]
    checks.append(Check("clickstream_rows_reconcile", "critical", accounted == t["clickstream_raw_lines"],
                        accounted, t["clickstream_raw_lines"], "valid + quarantined must equal raw lines"))
    qrate = t["clickstream_quarantined"] / max(t["clickstream_raw_lines"], 1)
    checks.append(Check("clickstream_quarantine_rate", "critical", qrate <= cfg.max_quarantine_rate,
                        round(qrate, 5), cfg.max_quarantine_rate))
    drate = t["duplicates_removed"] / max(t["clickstream_valid"], 1)
    checks.append(Check("clickstream_duplicate_rate", "warn", drate <= cfg.max_duplicate_rate,
                        round(drate, 5), cfg.max_duplicate_rate))
    lrate = t["access_quarantined"] / max(t["access_raw_lines"], 1)
    checks.append(Check("access_log_quarantine_rate", "critical", lrate <= cfg.max_quarantine_rate,
                        round(lrate, 5), cfg.max_quarantine_rate))
    return checks


def warehouse_checks(conn, t: dict, cfg: Config) -> list[Check]:
    q = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
    checks = []
    n = q("SELECT count(*) FROM dw.fact_events")
    checks.append(Check("fact_events_matches_curated", "critical", n == t["clickstream_deduped"],
                        n, t["clickstream_deduped"]))
    nulls = q("SELECT count(*) FROM dw.fact_events WHERE session_id IS NULL OR customer_id IS NULL")
    checks.append(Check("fact_events_required_not_null", "critical", nulls == 0, nulls, 0))
    expected_last = cfg.start_date + timedelta(days=cfg.days - 1)
    last = q("SELECT max(full_date) FROM dw.fact_events f JOIN dw.dim_date d USING (date_key)")
    checks.append(Check("freshness_latest_event_date", "critical", last is not None and last >= expected_last,
                        None, None, f"latest={last} expected>={expected_last}"))
    orphan = q("""SELECT count(*) FROM dw.fact_order_items f
                  LEFT JOIN dw.dim_product p USING (product_id) WHERE p.product_id IS NULL""")
    checks.append(Check("order_items_product_integrity", "critical", orphan == 0, orphan, 0))
    neg = q("SELECT count(*) FROM dw.fact_order_items WHERE revenue < 0 OR quantity <= 0")
    checks.append(Check("order_items_valid_amounts", "critical", neg == 0, neg, 0))
    # Operational anomaly: the worst hour for server errors.
    row = conn.execute("""SELECT date_trunc('hour', request_ts) AS h,
                                 avg((status_code >= 500)::int) AS err
                          FROM dw.fact_requests GROUP BY 1 ORDER BY 2 DESC LIMIT 1""").fetchone()
    if row:
        checks.append(Check("max_hourly_5xx_rate", "warn", row[1] <= cfg.max_5xx_rate_warn,
                            round(float(row[1]), 4), cfg.max_5xx_rate_warn, f"worst hour {row[0]:%Y-%m-%d %H:00} UTC"))
    return checks


def record_run(cfg: Config, run_id: str, status: str, stats: dict, checks: list[Check]) -> None:
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        conn.execute("""INSERT INTO ops.pipeline_runs (run_id, finished_at, status, stats)
                        VALUES (%s, now(), %s, %s)
                        ON CONFLICT (run_id) DO UPDATE SET finished_at = now(), status = EXCLUDED.status,
                            stats = EXCLUDED.stats""", (run_id, status, json.dumps(stats)))
        for c in checks:
            conn.execute("""INSERT INTO ops.dq_results (run_id, check_name, severity, passed, observed, threshold, details)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (run_id, check_name) DO UPDATE SET passed = EXCLUDED.passed,
                                observed = EXCLUDED.observed, details = EXCLUDED.details, checked_at = now()""",
                         (run_id, c.name, c.severity, c.passed, c.observed, c.threshold, c.details))


def run_checks(run_id: str, cfg: Config | None = None) -> tuple[bool, list[Check]]:
    cfg = cfg or get_config()
    t = json.loads((cfg.runs / "transform_stats.json").read_text())
    checks = reconciliation_checks(t, cfg)
    with psycopg.connect(cfg.pg_dsn) as conn:
        checks += warehouse_checks(conn, t, cfg)
    ok = all(c.passed for c in checks if c.severity == "critical")
    record_run(cfg, run_id, "succeeded" if ok else "failed", t, checks)
    return ok, checks
