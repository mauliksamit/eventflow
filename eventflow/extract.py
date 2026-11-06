"""Incremental extract from the OLTP database into the raw zone.

Uses a high-water mark on orders.updated_at stored in ops.watermarks, so each run only pulls
orders created or changed since the last successful run (change data capture, the simple way).
Rows are written as gzip CSV, partitioned by the date they changed.
"""
from __future__ import annotations

import csv
import gzip
from collections import defaultdict
from datetime import datetime, timezone

import psycopg

from .config import Config, get_config

EXTRACT_SQL = """
SELECT o.order_id, o.customer_id, o.status, o.created_at, o.updated_at,
       i.line_no, i.product_id, i.quantity, i.unit_price
FROM oltp.orders o
JOIN oltp.order_items i USING (order_id)
WHERE o.updated_at > %(since)s
ORDER BY o.updated_at
"""
COLUMNS = ["order_id", "customer_id", "status", "created_at", "updated_at",
           "line_no", "product_id", "quantity", "unit_price"]


def extract_orders(cfg: Config | None = None) -> dict:
    cfg = cfg or get_config()
    with psycopg.connect(cfg.pg_dsn) as conn:
        row = conn.execute("SELECT high_water FROM ops.watermarks WHERE source = 'orders'").fetchone()
        since = row[0] if row else datetime(1970, 1, 1, tzinfo=timezone.utc)
        by_day: dict[str, list] = defaultdict(list)
        high = since
        with conn.cursor(name="orders_extract") as cur:  # server-side cursor: constant memory
            cur.itersize = 50_000
            cur.execute(EXTRACT_SQL, {"since": since})
            for r in cur:
                by_day[str(r[4].astimezone(timezone.utc).date())].append(r)
                high = max(high, r[4])
        for day, rows in by_day.items():
            out = cfg.raw / "orders" / f"dt={day}" / f"orders-{high:%Y%m%dT%H%M%S}.csv.gz"
            out.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(out, "wt", newline="") as f:
                w = csv.writer(f)
                w.writerow(COLUMNS)
                for r in rows:
                    w.writerow([v.isoformat() if isinstance(v, datetime) else v for v in r])
        conn.execute(
            "INSERT INTO ops.watermarks (source, high_water) VALUES ('orders', %s) "
            "ON CONFLICT (source) DO UPDATE SET high_water = EXCLUDED.high_water", (high,))
        conn.commit()
    return {"order_rows_extracted": sum(len(v) for v in by_day.values()), "watermark": high.isoformat()}
