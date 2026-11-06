"""Write-Audit-Publish: a day's output is audited before anything downstream can see it.

A critical failure leaves ops.published_partitions unchanged, so the warehouse keeps serving the
last good data. Warnings are recorded but do not block; several of them describe conditions in
the source that are real and persistent (see docs/findings.md), not defects in this pipeline.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import psycopg

from .config import Config
from .transform import TOP_LEVEL_FIELDS


@dataclass
class Check:
    name: str
    severity: str  # "critical" | "warn"
    passed: bool
    observed: float | None = None
    threshold: float | None = None
    details: str = ""


def audit(stats: dict, source_lines: int, cfg: Config, known_fields: set[tuple[str, str]] | None = None) -> list[Check]:
    """source_lines comes from the ingest manifest, counted outside Spark, so reconciliation is independent."""
    rows = stats["parsed_rows"]
    qrate = stats["rejected"] / max(rows, 1)
    cap_ref, cap_collab = stats["capture_ref"], stats["capture_collab"]
    seen = {(f["event_type"], f["field"]) for f in stats["observed_fields"]}
    extra_top = sorted(f for t, f in seen if t == "*" and f not in TOP_LEVEL_FIELDS)
    new_fields = sorted(seen - known_fields) if known_fields else []
    return [
        Check("source_hours_complete", "critical", stats["hours_present"] >= cfg.min_hours,
              stats["hours_present"], cfg.min_hours),
        Check("rows_reconcile_to_manifest", "critical", rows == source_lines, rows, source_lines,
              "rows parsed by Spark must equal lines counted at ingest"),
        Check("quarantine_rate", "critical", qrate <= cfg.max_quarantine_rate, round(qrate, 6),
              cfg.max_quarantine_rate, json.dumps(stats["reject_reasons"])),
        Check("has_valid_events", "critical", stats["valid_unique"] > 0, stats["valid_unique"], 1),
        Check("ref_stream_present", "warn", stats["events_by_type"].get("PushEvent", 0) > 0,
              stats["events_by_type"].get("PushEvent", 0), 1, "push/create/delete events absent from the feed"),
        Check("ref_capture_floor", "warn", cap_ref >= cfg.min_capture_warn, round(cap_ref, 6), cfg.min_capture_warn),
        Check("collab_capture_floor", "warn", cap_collab >= cfg.min_capture_warn, round(cap_collab, 6), cfg.min_capture_warn),
        Check("no_unknown_event_types", "warn", not stats["unknown_event_types"], len(stats["unknown_event_types"]), 0,
              ",".join(stats["unknown_event_types"])),
        Check("no_unexpected_top_level_fields", "warn", not extra_top, len(extra_top), 0, ",".join(extra_top)),
        Check("no_new_payload_fields", "warn", not new_fields, len(new_fields), 0,
              ",".join(f"{t}.{f}" for t, f in new_fields[:20])),
    ]


def blocking(checks: list[Check]) -> list[Check]:
    return [c for c in checks if c.severity == "critical" and not c.passed]


def source_line_count(conn, day: date) -> int:
    return conn.execute("SELECT coalesce(sum(line_count), 0) FROM ops.ingest_files WHERE dt = %s", (day,)).fetchone()[0]


def known_fields(conn) -> set[tuple[str, str]]:
    return {(t, f) for t, f in conn.execute("SELECT event_type, field FROM ops.schema_fields").fetchall()}


def record(conn, day: date, run_id: str, status: str, stats: dict, checks: list[Check]) -> None:
    summary = {k: v for k, v in stats.items() if k != "observed_fields"}
    conn.execute("""INSERT INTO ops.pipeline_runs (run_id, dt, finished_at, status, stats) VALUES (%s, %s, now(), %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET finished_at = now(), status = EXCLUDED.status,
                        stats = EXCLUDED.stats""", (run_id, day, status, json.dumps(summary)))
    for c in checks:
        conn.execute("""INSERT INTO ops.dq_results (run_id, dt, check_name, severity, passed, observed, threshold, details)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, check_name) DO UPDATE SET passed = EXCLUDED.passed,
                            observed = EXCLUDED.observed, details = EXCLUDED.details""",
                     (run_id, day, c.name, c.severity, c.passed, c.observed, c.threshold, c.details))
    for f in stats["observed_fields"]:
        conn.execute("""INSERT INTO ops.schema_fields (event_type, field, first_seen, last_seen) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (event_type, field) DO UPDATE SET
                            first_seen = least(ops.schema_fields.first_seen, EXCLUDED.first_seen),
                            last_seen = greatest(ops.schema_fields.last_seen, EXCLUDED.last_seen)""",
                     (f["event_type"], f["field"], day, day))


def publish(conn, day: date, run_id: str) -> None:
    conn.execute("""INSERT INTO ops.published_partitions (dt, run_id) VALUES (%s, %s)
                    ON CONFLICT (dt) DO UPDATE SET run_id = EXCLUDED.run_id, published_at = now()""", (day, run_id))


def published_run(conn, day: date) -> str | None:
    row = conn.execute("SELECT run_id FROM ops.published_partitions WHERE dt = %s", (day,)).fetchone()
    return row[0] if row else None


def audit_and_publish(cfg: Config, day: date, run_id: str, stats: dict) -> tuple[bool, list[Check]]:
    with psycopg.connect(cfg.pg_dsn) as conn:
        checks = audit(stats, source_line_count(conn, day), cfg, known_fields(conn))
        ok = not blocking(checks)
        record(conn, day, run_id, "published" if ok else "failed_audit", stats, checks)
        if ok:
            publish(conn, day, run_id)
    return ok, checks
