"""Load one published day into the warehouse, in a single transaction.

The day's curated Parquet is bulk COPY'd into staging, then merged with set-based SQL. Dimensions
are upserted and facts are delete-then-insert by day, so reloading a day leaves the warehouse
unchanged. Only the run recorded in ops.published_partitions is ever read.
"""
from __future__ import annotations

import io
import time
from datetime import date
from pathlib import Path

import psycopg
import pyarrow.csv as pacsv
import pyarrow.dataset as ds

from .config import Config
from .lake import Lake
from .quality import published_run
from .transform import REF_EVENTS

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

STAGING = {
    "repo_daily": ("(repo_id BIGINT, repo_name TEXT, events INT, stars INT, forks INT, pushes INT, prs_opened INT, "
                   "issues_opened INT, releases INT, actors INT)"),
    "actor_daily": "(actor_id BIGINT, actor_login TEXT, actor_class TEXT, first_seen TIMESTAMP, last_seen TIMESTAMP, events INT)",
    "stars": "(event_id BIGINT, repo_id BIGINT, actor_id BIGINT, created_ts TIMESTAMP, hour_ts TIMESTAMP)",
    "feed_hourly": ("(hour_ts TIMESTAMP, id_series TEXT, events BIGINT, est_total DOUBLE PRECISION, "
                    "capture_rate DOUBLE PRECISION)"),
    "feed_minute": "(minute_ts TIMESTAMP, id_series TEXT, events INT, id_median BIGINT)",
    "type_hourly": "(hour_ts TIMESTAMP, event_type TEXT, actor_class TEXT, events BIGINT)",
}

# Each statement runs with %(d)s = the day and %(k)s = its yyyymmdd key.
MERGE = [
    """INSERT INTO dw.dim_date
       SELECT %(k)s, d, extract(isodow FROM d), trim(to_char(d, 'Day')), extract(week FROM d), extract(month FROM d),
              extract(year FROM d), extract(isodow FROM d) >= 6
       FROM (SELECT %(d)s::date AS d) x ON CONFLICT (date_key) DO NOTHING""",
    """INSERT INTO dw.dim_event_type
       SELECT DISTINCT event_type, CASE WHEN event_type = ANY(%(ref)s) THEN 'ref' ELSE 'collab' END
       FROM staging.type_hourly ON CONFLICT (event_type) DO NOTHING""",
    """INSERT INTO dw.dim_actor (actor_id, login, actor_class, first_seen, last_seen)
       SELECT actor_id, actor_login, actor_class, first_seen, last_seen FROM staging.actor_daily
       ON CONFLICT (actor_id) DO UPDATE SET login = EXCLUDED.login, actor_class = EXCLUDED.actor_class,
           first_seen = least(dw.dim_actor.first_seen, EXCLUDED.first_seen),
           last_seen = greatest(dw.dim_actor.last_seen, EXCLUDED.last_seen)""",
    # Type 2. Days can be reloaded, so a difference only counts against a version older than this day.
    """UPDATE dw.dim_repo r SET valid_to = %(d)s, is_current = false
       FROM staging.repo_daily s
       WHERE r.repo_id = s.repo_id AND r.is_current AND r.repo_name <> s.repo_name AND r.valid_from < %(d)s""",
    """INSERT INTO dw.dim_repo (repo_id, repo_name, owner, valid_from)
       SELECT s.repo_id, s.repo_name, split_part(s.repo_name, '/', 1), %(d)s FROM staging.repo_daily s
       WHERE NOT EXISTS (SELECT 1 FROM dw.dim_repo r WHERE r.repo_id = s.repo_id AND r.is_current)""",
    # A day older than a repo's first version extends that version back instead of adding a gap.
    """UPDATE dw.dim_repo r SET valid_from = %(d)s
       FROM staging.repo_daily s
       WHERE r.repo_id = s.repo_id AND r.valid_from > %(d)s
         AND NOT EXISTS (SELECT 1 FROM dw.dim_repo o WHERE o.repo_id = r.repo_id AND o.valid_from < r.valid_from)""",
    "DELETE FROM dw.fact_repo_daily WHERE date_key = %(k)s",
    "DELETE FROM dw.fact_star WHERE date_key = %(k)s",
    "DELETE FROM dw.fact_type_hourly WHERE hour_ts >= %(d)s::date AND hour_ts < %(d)s::date + 1",
    "DELETE FROM dw.fact_feed_hourly WHERE hour_ts >= %(d)s::date AND hour_ts < %(d)s::date + 1",
    "DELETE FROM dw.fact_feed_minute WHERE minute_ts >= %(d)s::date AND minute_ts < %(d)s::date + 1",
    """INSERT INTO dw.fact_repo_daily
       SELECT %(k)s, r.repo_key, s.events, s.stars, s.forks, s.pushes, s.prs_opened, s.issues_opened, s.releases, s.actors
       FROM staging.repo_daily s
       JOIN dw.dim_repo r ON r.repo_id = s.repo_id AND %(d)s >= r.valid_from AND %(d)s < r.valid_to""",
    """INSERT INTO dw.fact_star
       SELECT s.event_id, %(k)s, r.repo_key, s.actor_id, s.created_ts FROM staging.stars s
       JOIN dw.dim_repo r ON r.repo_id = s.repo_id AND %(d)s >= r.valid_from AND %(d)s < r.valid_to""",
    "INSERT INTO dw.fact_type_hourly SELECT hour_ts, event_type, actor_class, events FROM staging.type_hourly",
    "INSERT INTO dw.fact_feed_hourly SELECT hour_ts, id_series, events, est_total, capture_rate FROM staging.feed_hourly",
    "INSERT INTO dw.fact_feed_minute SELECT minute_ts, id_series, events, id_median FROM staging.feed_minute",
]


def init_warehouse(cfg: Config) -> None:
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        conn.execute((SQL_DIR / "dw_schema.sql").read_text())


def _month_partition(cur, day: date) -> None:
    lo = day.year * 10000 + day.month * 100
    nxt = lo + 10000 - 1100 if day.month == 12 else lo + 100
    cur.execute(f"""CREATE TABLE IF NOT EXISTS dw.fact_repo_daily_{lo // 100} PARTITION OF dw.fact_repo_daily
                    FOR VALUES FROM ({lo}) TO ({nxt})""")


def _copy(cur, lake: Lake, name: str, key: str, columns: list[str]) -> int:
    fs, path = lake.arrow(key)
    n = 0
    with cur.copy(f"COPY staging.{name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)") as cp:
        for batch in ds.dataset(path, filesystem=fs, format="parquet").to_batches(columns=columns, batch_size=100_000):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, pacsv.WriteOptions(include_header=False))
            cp.write(buf.getvalue())
            n += batch.num_rows
    return n


def load_day(cfg: Config, day: date) -> dict:
    lake, t0 = Lake(cfg), time.time()
    key = day.year * 10000 + day.month * 100 + day.day
    with psycopg.connect(cfg.pg_dsn) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        run_id = published_run(conn, day)
        if run_id is None:
            raise RuntimeError(f"{day} has no published run: its audit failed or it was never transformed")
        stats = {"dt": str(day), "run_id": run_id}
        with conn.cursor() as cur:
            _month_partition(cur, day)
            for name, ddl in STAGING.items():
                cur.execute(f"DROP TABLE IF EXISTS staging.{name}; CREATE UNLOGGED TABLE staging.{name} {ddl}")
                cols = [c.split()[0] for c in ddl.strip("()").split(", ")]
                stats[f"staged_{name}"] = _copy(cur, lake, name, f"curated/{name}/dt={day}/run={run_id}", cols)
            cur.execute("ANALYZE staging.repo_daily")
            for sql in MERGE:
                cur.execute(sql, {"d": day, "k": key, "ref": REF_EVENTS})
    stats["load_seconds"] = round(time.time() - t0, 1)
    return stats
