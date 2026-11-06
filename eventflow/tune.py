"""Query tuning on the real warehouse: each query is timed before and after its index.

Timings are the median of several warm runs, so they compare plans, not disk caches. An index
stays only if it makes its query at least MIN_SPEEDUP times faster; anything less is not worth
what it adds to every load. Indexes are not in the base schema for the same reason.
"""
from __future__ import annotations

import statistics
import time

import psycopg

from .config import Config

RUNS = 5
MIN_SPEEDUP = 2.0

# (name, what it answers, the query, the index that serves it)
CASES = [
    ("repo_history", "daily activity for one repo across every name it has had",
     """SELECT d.full_date, sum(f.events), sum(f.stars)
        FROM dw.fact_repo_daily f JOIN dw.dim_date d USING (date_key)
        WHERE f.repo_key IN (SELECT repo_key FROM dw.dim_repo WHERE repo_id =
              (SELECT repo_id FROM dw.dim_repo WHERE repo_name = %(repo)s ORDER BY valid_from DESC LIMIT 1))
        GROUP BY 1 ORDER BY 1""",
     ["CREATE INDEX ix_dim_repo_name ON dw.dim_repo (repo_name)",
      "CREATE INDEX ix_dim_repo_id ON dw.dim_repo (repo_id)",
      "CREATE INDEX ix_fact_repo_daily_repo ON dw.fact_repo_daily (repo_key, date_key)"]),
    ("stargazers_of_repo", "who starred a repo, newest first",
     """SELECT s.actor_id, s.starred_at FROM dw.fact_star s
        WHERE s.repo_key IN (SELECT repo_key FROM dw.dim_repo WHERE repo_id =
              (SELECT repo_id FROM dw.dim_repo WHERE repo_name = %(repo)s ORDER BY valid_from DESC LIMIT 1))
        ORDER BY s.starred_at DESC LIMIT 100""",
     ["CREATE INDEX ix_fact_star_repo ON dw.fact_star (repo_key, starred_at DESC)"]),
    ("stars_by_actor", "everything one account starred",
     "SELECT repo_key, starred_at FROM dw.fact_star WHERE actor_id = %(actor)s ORDER BY starred_at",
     ["CREATE INDEX ix_fact_star_actor ON dw.fact_star (actor_id, starred_at)"]),
    ("stars_in_incident", "stars per minute around an incident",
     """SELECT date_trunc('minute', starred_at), count(*) FROM dw.fact_star
        WHERE starred_at >= '2026-09-13 08:00+00' AND starred_at < '2026-09-13 11:00+00' GROUP BY 1 ORDER BY 1""",
     ["CREATE INDEX ix_fact_star_time ON dw.fact_star USING brin (starred_at)"]),
    ("day_by_event_type", "one day of hourly counts by event type",
     """SELECT event_type, sum(events) FROM dw.fact_type_hourly
        WHERE hour_ts >= '2026-10-07 00:00+00' AND hour_ts < '2026-10-08 00:00+00' GROUP BY 1 ORDER BY 2 DESC""",
     ["CREATE INDEX ix_fact_type_hour ON dw.fact_type_hourly (hour_ts) INCLUDE (event_type, events)"]),
]


def _index_name(ddl: str) -> str:
    return ddl.split()[2]


def _timed(conn, sql: str, params: dict) -> float:
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        conn.execute(sql, params).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times)


def _plan(conn, sql: str, params: dict) -> list[str]:
    rows = conn.execute("EXPLAIN " + sql, params).fetchall()
    return [r[0].strip().lstrip("-> ").split("  (")[0] for r in rows if "Scan" in r[0]]


def _index_bytes(conn, ddl: str) -> int:
    size = conn.execute("SELECT coalesce(sum(pg_relation_size(c.oid)), 0) FROM pg_class c WHERE relname LIKE %s",
                        (_index_name(ddl) + "%",)).fetchone()[0]
    return int(size)


def benchmark(cfg: Config) -> list[dict]:
    results = []
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        repo = conn.execute("SELECT repo_name FROM mart.repo_stars ORDER BY stars_captured DESC LIMIT 1").fetchone()[0]
        actor = conn.execute("""SELECT actor_id FROM dw.fact_star WHERE date_key = 20261001 GROUP BY 1
                                ORDER BY count(*) DESC LIMIT 1""").fetchone()[0]
        params = {"repo": repo, "actor": actor}
        for _, _, _, indexes in CASES:
            for ddl in indexes:
                conn.execute(f"DROP INDEX IF EXISTS dw.{_index_name(ddl)}")
        conn.execute("SET max_parallel_workers_per_gather = 2")
        for name, question, sql, indexes in CASES:
            before, plan_before = _timed(conn, sql, params), _plan(conn, sql, params)
            t0 = time.perf_counter()
            for ddl in indexes:
                conn.execute(ddl)
            build = time.perf_counter() - t0
            conn.execute("ANALYZE dw.dim_repo")
            after, plan_after = _timed(conn, sql, params), _plan(conn, sql, params)
            size = round(sum(_index_bytes(conn, d) for d in indexes) / 1e6, 1)
            kept = before / after >= MIN_SPEEDUP
            if not kept:
                for ddl in indexes:
                    conn.execute(f"DROP INDEX dw.{_index_name(ddl)}")
            results.append({"query": name, "question": question, "before_ms": round(before, 2), "after_ms": round(after, 2),
                            "speedup": round(before / after, 1), "index_build_s": round(build, 1), "index_mb": size,
                            "indexes": [_index_name(d) for d in indexes], "kept": kept,
                            "plan_before": plan_before, "plan_after": plan_after})
    return results
