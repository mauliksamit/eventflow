"""Load curated Parquet into the PostgreSQL star schema.

Pattern: bulk COPY into staging tables (fast, no constraints), then set-based SQL upserts into
dimensions and facts (ON CONFLICT ...), all inside one transaction. Re-running a load with the
same data leaves the warehouse unchanged (idempotent).
"""
from __future__ import annotations

import io
import json
import time
from pathlib import Path

import psycopg
import pyarrow.csv as pacsv
import pyarrow.dataset as ds

from .config import Config, get_config

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

STAGING_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.events, staging.sessions, staging.requests, staging.orders;
CREATE UNLOGGED TABLE staging.events (event_id TEXT, user_id BIGINT, event_type TEXT, product_id BIGINT,
    device TEXT, app_version TEXT, event_ts TIMESTAMPTZ, session_id TEXT, event_date DATE);
CREATE UNLOGGED TABLE staging.sessions (session_id TEXT, user_id BIGINT, start_ts TIMESTAMPTZ,
    end_ts TIMESTAMPTZ, n_events INT, converted BOOLEAN, event_date DATE);
CREATE UNLOGGED TABLE staging.requests (request_ts TIMESTAMPTZ, method TEXT, status_code SMALLINT,
    bytes_sent INT, latency_ms REAL, path_group TEXT, event_date DATE);
CREATE UNLOGGED TABLE staging.orders (order_id BIGINT, customer_id BIGINT, status TEXT,
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, line_no INT, product_id BIGINT, quantity INT,
    unit_price NUMERIC(10,2), order_date DATE);
"""

UPSERTS = [
    # Date dimension covering every date seen in any fact.
    """INSERT INTO dw.dim_date
       SELECT to_char(d, 'YYYYMMDD')::int, d, extract(isodow FROM d), trim(to_char(d, 'Day')),
              extract(week FROM d), extract(month FROM d), extract(year FROM d), extract(isodow FROM d) >= 6
       FROM generate_series(
            (SELECT least(min(event_date), (SELECT min(order_date) FROM staging.orders)) FROM staging.events),
            (SELECT greatest(max(event_date), (SELECT max(order_date) FROM staging.orders)) FROM staging.events),
            interval '1 day') AS g(d)
       ON CONFLICT (date_key) DO NOTHING""",
    # Product dimension: denormalize category name onto each product.
    """INSERT INTO dw.dim_product (product_id, product_name, category_name, list_price)
       SELECT p.product_id, p.name, c.name, p.unit_price
       FROM oltp.products p JOIN oltp.categories c USING (category_id)
       ON CONFLICT (product_id) DO UPDATE SET product_name = EXCLUDED.product_name,
           category_name = EXCLUDED.category_name, list_price = EXCLUDED.list_price""",
    # Customer dimension: OLTP attributes + behaviour derived from the clickstream.
    """INSERT INTO dw.dim_customer (customer_id, city, signup_date, first_seen_ts, primary_device)
       SELECT c.customer_id, c.city, c.signup_date, e.first_seen, e.device
       FROM oltp.customers c
       LEFT JOIN (SELECT user_id, min(event_ts) AS first_seen, mode() WITHIN GROUP (ORDER BY device) AS device
                  FROM staging.events GROUP BY user_id) e ON e.user_id = c.customer_id
       ON CONFLICT (customer_id) DO UPDATE SET city = EXCLUDED.city,
           first_seen_ts = least(dw.dim_customer.first_seen_ts, EXCLUDED.first_seen_ts),
           primary_device = coalesce(EXCLUDED.primary_device, dw.dim_customer.primary_device)""",
    """INSERT INTO dw.fact_events (event_id, date_key, customer_id, product_id, session_id, event_type, device, event_ts)
       SELECT event_id, to_char(event_date, 'YYYYMMDD')::int, user_id, product_id, session_id, event_type, device, event_ts
       FROM staging.events
       ON CONFLICT (event_id) DO UPDATE SET session_id = EXCLUDED.session_id""",
    """INSERT INTO dw.fact_sessions
       SELECT session_id, to_char(event_date, 'YYYYMMDD')::int, user_id, start_ts, end_ts, n_events, converted
       FROM staging.sessions
       ON CONFLICT (session_id) DO UPDATE SET end_ts = EXCLUDED.end_ts, n_events = EXCLUDED.n_events,
           converted = EXCLUDED.converted""",
    """INSERT INTO dw.fact_order_items
       SELECT order_id, line_no, to_char(order_date, 'YYYYMMDD')::int, customer_id, product_id, status,
              quantity, round(quantity * unit_price, 2)
       FROM staging.orders
       ON CONFLICT (order_id, line_no) DO UPDATE SET status = EXCLUDED.status""",
    # No natural key for requests: replace the affected days (delete + insert) to stay idempotent.
    """DELETE FROM dw.fact_requests WHERE date_key IN
       (SELECT DISTINCT to_char(event_date, 'YYYYMMDD')::int FROM staging.requests)""",
    """INSERT INTO dw.fact_requests
       SELECT to_char(event_date, 'YYYYMMDD')::int, request_ts, method, path_group, status_code, bytes_sent, latency_ms
       FROM staging.requests""",
]


def init_warehouse(cfg: Config | None = None) -> None:
    cfg = cfg or get_config()
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        conn.execute((SQL_DIR / "dw_schema.sql").read_text())


def _copy_parquet(cur, table: str, path: Path, columns: list[str]) -> int:
    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")
    n = 0
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)") as cp:
        for batch in dataset.to_batches(columns=columns, batch_size=200_000):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, pacsv.WriteOptions(include_header=False))
            cp.write(buf.getvalue())
            n += batch.num_rows
    return n


def load(cfg: Config | None = None) -> dict:
    cfg = cfg or get_config()
    t0 = time.time()
    stats = {}
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(STAGING_DDL)
            stats["staged_events"] = _copy_parquet(cur, "staging.events", cfg.curated / "events",
                ["event_id", "user_id", "event_type", "product_id", "device", "app_version", "event_ts",
                 "session_id", "event_date"])
            stats["staged_sessions"] = _copy_parquet(cur, "staging.sessions", cfg.curated / "sessions",
                ["session_id", "user_id", "start_ts", "end_ts", "n_events", "converted", "event_date"])
            stats["staged_requests"] = _copy_parquet(cur, "staging.requests", cfg.curated / "requests",
                ["request_ts", "method", "status_code", "bytes_sent", "latency_ms", "path_group", "event_date"])
            stats["staged_order_lines"] = _copy_parquet(cur, "staging.orders", cfg.curated / "orders",
                ["order_id", "customer_id", "status", "created_at", "updated_at", "line_no", "product_id",
                 "quantity", "unit_price", "order_date"])
            for sql in UPSERTS:
                cur.execute(sql)
        conn.commit()
        conn.autocommit = True
        conn.execute("ANALYZE dw.fact_events; ANALYZE dw.fact_sessions; ANALYZE dw.fact_order_items; ANALYZE dw.fact_requests")
    stats["load_seconds"] = round(time.time() - t0, 1)
    cfg.runs.mkdir(parents=True, exist_ok=True)
    (cfg.runs / "load_stats.json").write_text(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    init_warehouse()
    print(json.dumps(load(), indent=2))
