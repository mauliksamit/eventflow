"""Query tuning benchmark: measure real report queries before and after tuning.

Baseline = the warehouse as loaded (primary keys only). Tuning steps:
  1. B-tree index on fact_events (customer_id, event_ts) for per-customer history lookups
  2. B-tree index on fact_order_items (date_key) and BRIN on fact_requests (request_ts)
  3. Query rewrite: read pre-aggregated materialized views instead of re-joining facts
Each query runs 5 times; we report the median EXPLAIN ANALYZE execution time.
Results are written to docs/query_tuning.md.
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

import psycopg

from .config import Config, get_config

QUERIES = {
    "customer_event_history": (
        "Last 50 events for one customer (support/debugging lookup)",
        """SELECT event_ts, event_type, product_id FROM dw.fact_events
           WHERE customer_id = {customer} ORDER BY event_ts DESC LIMIT 50""",
        None),
    "category_revenue_last_7d": (
        "Revenue by category for the last 7 days",
        """SELECT p.category_name, sum(f.revenue) FROM dw.fact_order_items f
           JOIN dw.dim_product p USING (product_id)
           WHERE f.date_key >= 20261008 AND f.status <> 'cancelled' GROUP BY 1 ORDER BY 2 DESC""",
        """SELECT category_name, sum(revenue) FROM mart.category_revenue_daily
           WHERE full_date >= '2026-10-08' GROUP BY 1 ORDER BY 2 DESC"""),
    "incident_window_errors": (
        "5xx errors by endpoint during a 2-hour incident window",
        """SELECT path_group, count(*) FILTER (WHERE status_code >= 500), count(*) FROM dw.fact_requests
           WHERE request_ts >= '2026-10-10 15:00+00' AND request_ts < '2026-10-10 17:00+00' GROUP BY 1""",
        None),
    "daily_funnel": (
        "Daily funnel: sessions, active users, conversion",
        """SELECT d.full_date, count(*), count(DISTINCT s.customer_id), avg(s.converted::int)
           FROM dw.fact_sessions s JOIN dw.dim_date d USING (date_key) GROUP BY 1 ORDER BY 1""",
        """SELECT full_date, sessions, active_users, conversion_rate FROM mart.daily_kpis ORDER BY 1"""),
}

CHANGES = {
    "customer_event_history": "B-tree index on (customer_id, event_ts DESC)",
    "category_revenue_last_7d": "read pre-aggregated materialized view",
    "incident_window_errors": "BRIN index on request_ts",
    "daily_funnel": "read pre-aggregated materialized view",
}

TUNING_DDL = [
    "CREATE INDEX IF NOT EXISTS ix_events_customer_ts ON dw.fact_events (customer_id, event_ts DESC)",
    "CREATE INDEX IF NOT EXISTS ix_order_items_date ON dw.fact_order_items (date_key)",
    "CREATE INDEX IF NOT EXISTS brin_requests_ts ON dw.fact_requests USING brin (request_ts)",
    "ANALYZE dw.fact_events", "ANALYZE dw.fact_order_items", "ANALYZE dw.fact_requests",
]
DROP_DDL = ["DROP INDEX IF EXISTS dw.ix_events_customer_ts", "DROP INDEX IF EXISTS dw.ix_order_items_date",
            "DROP INDEX IF EXISTS dw.brin_requests_ts"]


def _time(conn, sql: str, runs: int = 5) -> tuple[float, str]:
    times, plan_node = [], ""
    for _ in range(runs):
        plan = conn.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql).fetchone()[0][0]
        times.append(plan["Execution Time"])
        plan_node = _scan_types(plan["Plan"])
    return statistics.median(times), plan_node


def _scan_types(node: dict) -> str:
    found = []
    def walk(n):
        if "Scan" in n["Node Type"]:
            found.append(f'{n["Node Type"]} on {n.get("Relation Name", n.get("Index Name", "?"))}')
        for c in n.get("Plans", []):
            walk(c)
    walk(node)
    return "; ".join(dict.fromkeys(found))


def benchmark(cfg: Config | None = None, out: Path = Path("docs/query_tuning.md")) -> list[dict]:
    cfg = cfg or get_config()
    rows = []
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        conn.execute("SET max_parallel_workers_per_gather = 0")  # stable, comparable timings
        for ddl in DROP_DDL:
            conn.execute(ddl)
        sizes = {t: conn.execute(f"SELECT count(*) FROM dw.{t}").fetchone()[0]
                 for t in ["fact_events", "fact_order_items", "fact_requests", "fact_sessions"]}
        # Benchmark the lookup for a realistic busy customer (the one with the most events).
        customer = conn.execute("""SELECT customer_id FROM dw.fact_events GROUP BY 1
                                   ORDER BY count(*) DESC, customer_id LIMIT 1""").fetchone()[0]
        queries = {k: (d, q.format(customer=customer), r) for k, (d, q, r) in QUERIES.items()}
        base = {k: _time(conn, q[1]) for k, q in queries.items()}
        for ddl in TUNING_DDL:
            conn.execute(ddl)
        for k, (desc, sql, rewrite) in queries.items():
            after = _time(conn, rewrite or sql)
            rows.append({"query": k, "description": desc, "before_ms": round(base[k][0], 2),
                         "after_ms": round(after[0], 2), "speedup": round(base[k][0] / max(after[0], 1e-3), 1),
                         "before_plan": base[k][1], "after_plan": after[1],
                         "change": CHANGES[k]})
    lines = ["# Query tuning results", "",
             "Generated by `python -m eventflow.cli tune` on the full dataset "
             f"({sizes['fact_events']:,} events, {sizes['fact_requests']:,} requests, "
             f"{sizes['fact_order_items']:,} order lines). Median of 5 runs of `EXPLAIN ANALYZE`, "
             "parallel workers disabled for stable timings.", "",
             "| Query | Before (ms) | After (ms) | Speed-up | Change |", "|---|---:|---:|---:|---|"]
    for r in rows:
        lines.append(f"| {r['description']} | {r['before_ms']} | {r['after_ms']} | {r['speedup']}x | {r['change']} |")
    lines += ["", "## Plans", ""]
    for r in rows:
        lines += [f"**{r['query']}**", f"- before: {r['before_plan']}", f"- after: {r['after_plan']}", ""]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    (cfg.runs / "tuning.json").write_text(json.dumps(rows, indent=2))
    return rows
