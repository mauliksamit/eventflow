"""Generate realistic synthetic source data for three sources:

1. Clickstream events (JSON lines, one file per arrival day) from web/iOS/Android clients.
2. Web server access logs (Apache/Nginx "combined"-style text lines with latency).
3. An OLTP order database in PostgreSQL (normalized schema, see sql/oltp_schema.sql).

Real-world problems are injected on purpose so the pipeline has something to handle:
  * malformed records (truncated JSON, missing fields, unknown event types, garbage log lines)
  * duplicate deliveries (same event_id sent twice, sometimes in the next day's file)
  * late-arriving events (event happened yesterday, file arrives today)
  * mixed timezone offsets on client timestamps
  * an incident: a 2-hour spike of 5xx errors and slow /checkout requests on one day
"""
from __future__ import annotations

import gzip
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import psycopg

from .config import Config, get_config

CITIES = ["Edmonton", "Calgary", "Toronto", "Vancouver", "Montreal", "Ottawa", "Winnipeg", "Halifax"]
CATEGORIES = ["Electronics", "Books", "Home", "Garden", "Toys", "Sports", "Beauty", "Grocery",
              "Clothing", "Automotive", "Office", "Pets"]
DEVICES = ["web", "ios", "android"]
TZ_OFFSETS = ["+00:00", "-06:00", "-05:00", "-07:00", "-04:00"]
INCIDENT_DAY = 9          # day index with the injected outage
INCIDENT_HOURS = (15, 17)  # UTC


def _iso(ts: datetime, offset: str) -> str:
    sign = -1 if offset[0] == "-" else 1
    hours = int(offset[1:3])
    local = ts + timedelta(hours=sign * hours)
    return local.strftime("%Y-%m-%dT%H:%M:%S.") + f"{local.microsecond // 1000:03d}" + offset


def generate(cfg: Config | None = None) -> dict:
    cfg = cfg or get_config()
    rng = np.random.default_rng(cfg.seed)
    random.seed(cfg.seed)
    n_customers = max(500, int(50_000 * cfg.scale))
    n_products = 2_000
    events_per_day = int(143_000 * cfg.scale)
    requests_per_day = int(71_000 * cfg.scale)
    start = datetime(cfg.start_date.year, cfg.start_date.month, cfg.start_date.day, tzinfo=timezone.utc)

    prices = np.round(rng.lognormal(3.2, 0.8, n_products), 2) + 1.0
    product_cat = rng.integers(0, len(CATEGORIES), n_products)
    device_of = rng.choice(DEVICES, n_customers, p=[0.5, 0.3, 0.2])
    # A few heavy users generate most traffic (Zipf-like), like real products.
    user_weights = rng.pareto(1.2, n_customers) + 1
    user_weights /= user_weights.sum()

    clickstream_files: dict[str, list[str]] = {}
    orders: list[tuple] = []
    order_items: list[tuple] = []
    order_id = 1
    stats = {"events_generated": 0, "malformed_injected": 0, "duplicates_injected": 0, "late_injected": 0}

    for d in range(cfg.days):
        day_start = start + timedelta(days=d)
        n_sessions = events_per_day // 8
        users = rng.choice(n_customers, n_sessions, p=user_weights) + 1
        starts = rng.uniform(0, 86_400, n_sessions)
        lengths = rng.geometric(1 / 8, n_sessions)
        for u, st, ln in zip(users, starts, lengths):
            dev = device_of[u - 1]
            offset = random.choice(TZ_OFFSETS)
            t = day_start + timedelta(seconds=float(st))
            converted = rng.random() < 0.06
            carted: list[int] = []
            for i in range(int(ln)):
                if i == ln - 1 and converted and carted:
                    etype = "checkout"
                else:
                    etype = random.choices(["page_view", "search", "add_to_cart"], [0.75, 0.15, 0.10])[0]
                pid = int(rng.integers(1, n_products + 1)) if etype != "search" else None
                if etype == "add_to_cart":
                    carted.append(pid)
                if etype == "checkout":
                    pid = carted[-1]
                ev = {
                    "event_id": f"e{d:02d}-{stats['events_generated']:09d}",
                    "user_id": int(u),
                    "event_type": etype,
                    "product_id": pid,
                    "device": dev,
                    "app_version": random.choice(["5.2.0", "5.3.1", "5.4.0"]),
                    "ts": _iso(t, offset),
                }
                stats["events_generated"] += 1
                arrival = t.date()
                # late arrival: the client was offline and sent it the next day
                if rng.random() < 0.005:
                    arrival = (t + timedelta(days=1)).date()
                    stats["late_injected"] += 1
                line = json.dumps(ev, separators=(",", ":"))
                r = rng.random()
                if r < 0.002:
                    line = line[: len(line) // 2]  # truncated payload
                    stats["malformed_injected"] += 1
                elif r < 0.003:
                    bad = dict(ev)
                    bad.pop("user_id")
                    line = json.dumps(bad)
                    stats["malformed_injected"] += 1
                elif r < 0.004:
                    bad = dict(ev, event_type="pageview_v2")
                    line = json.dumps(bad)
                    stats["malformed_injected"] += 1
                clickstream_files.setdefault(str(arrival), []).append(line)
                if rng.random() < 0.01:  # at-least-once delivery: resend
                    resend_day = arrival + timedelta(days=int(rng.random() < 0.3))
                    clickstream_files.setdefault(str(resend_day), []).append(line)
                    stats["duplicates_injected"] += 1
                if etype == "checkout":
                    status = random.choices(["placed", "shipped", "cancelled"], [0.3, 0.6, 0.1])[0]
                    upd = t + timedelta(hours=float(rng.uniform(0, 48))) if status != "placed" else t
                    orders.append((order_id, int(u), status, t, upd))
                    items = list(dict.fromkeys(carted))[:4]
                    for ln_no, p in enumerate(items, 1):
                        order_items.append((order_id, ln_no, p, int(rng.integers(1, 4)),
                                            float(prices[p - 1] * rng.uniform(0.9, 1.0))))
                    order_id += 1
                t += timedelta(seconds=float(rng.exponential(45)))

    raw_click = cfg.raw / "clickstream"
    last_day = str((start + timedelta(days=cfg.days - 1)).date())
    for day, lines in clickstream_files.items():
        if day > last_day:
            continue  # arrives after the generated window
        random.shuffle(lines)
        out = raw_click / f"dt={day}" / "events-000.jsonl.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(out, "wt") as f:
            f.write("\n".join(lines) + "\n")

    stats["access_log_lines"] = _generate_access_logs(cfg, rng, start, requests_per_day, n_products)
    _load_oltp(cfg, rng, n_customers, n_products, prices, product_cat, orders, order_items)
    stats["orders"] = len(orders)
    stats["order_items"] = len(order_items)
    stats["customers"] = n_customers
    return stats


def _generate_access_logs(cfg, rng, start, per_day, n_products) -> int:
    paths = ["/", "/search", "/p/{}", "/cart", "/checkout", "/api/recommendations", "/static/app.js"]
    weights = [0.12, 0.15, 0.40, 0.08, 0.03, 0.12, 0.10]
    agents = ["Mozilla/5.0 (Windows NT 10.0)", "Mozilla/5.0 (iPhone)", "okhttp/4.12", "curl/8.5"]
    total = 0
    for d in range(cfg.days):
        day_start = start + timedelta(days=d)
        secs = np.sort(rng.uniform(0, 86_400, per_day))
        out = cfg.raw / "access_logs" / f"dt={day_start.date()}" / "access-000.log.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(out, "wt") as f:
            for s in secs:
                ts = day_start + timedelta(seconds=float(s))
                p = random.choices(paths, weights)[0]
                path = p.format(int(rng.integers(1, n_products + 1))) if "{}" in p else p
                method = "POST" if p in ("/cart", "/checkout") and rng.random() < 0.7 else "GET"
                latency = float(rng.lognormal(-2.5, 0.6))
                status = 200
                r = rng.random()
                if r < 0.01:
                    status = 404
                elif r < 0.015:
                    status = 500
                in_incident = d == INCIDENT_DAY and INCIDENT_HOURS[0] <= ts.hour < INCIDENT_HOURS[1]
                if in_incident and p in ("/checkout", "/cart", "/api/recommendations"):
                    latency *= 4
                    if rng.random() < 0.25:
                        status = random.choice([500, 502, 503])
                bytes_sent = int(rng.integers(200, 60_000)) if status == 200 else int(rng.integers(100, 600))
                ip = f"10.{rng.integers(0, 255)}.{rng.integers(0, 255)}.{rng.integers(1, 255)}"
                line = (f'{ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S")} +0000] "{method} {path} HTTP/1.1" '
                        f'{status} {bytes_sent} {latency:.3f} "{random.choice(agents)}"')
                if rng.random() < 0.002:
                    line = "\x00\x17 garbled " + line[:20]
                f.write(line + "\n")
                total += 1
    return total


def _load_oltp(cfg, rng, n_customers, n_products, prices, product_cat, orders, order_items) -> None:
    schema = (Path(__file__).resolve().parent.parent / "sql" / "oltp_schema.sql").read_text()
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as conn:
        conn.execute(schema)
        with conn.cursor() as cur:
            with cur.copy("COPY oltp.customers (customer_id, email, city, signup_date) FROM STDIN") as cp:
                for c in range(1, n_customers + 1):
                    signup = cfg.start_date - timedelta(days=int(rng.integers(1, 900)))
                    cp.write_row((c, f"user{c}@example.com", CITIES[c % len(CITIES)], signup))
            with cur.copy("COPY oltp.categories (category_id, name) FROM STDIN") as cp:
                for i, name in enumerate(CATEGORIES):
                    cp.write_row((i, name))
            with cur.copy("COPY oltp.products (product_id, category_id, name, unit_price) FROM STDIN") as cp:
                for p in range(1, n_products + 1):
                    cp.write_row((p, int(product_cat[p - 1]), f"{CATEGORIES[product_cat[p - 1]]} item {p}",
                                  float(prices[p - 1])))
            with cur.copy("COPY oltp.orders (order_id, customer_id, status, created_at, updated_at) FROM STDIN") as cp:
                for row in orders:
                    cp.write_row(row)
            with cur.copy("COPY oltp.order_items (order_id, line_no, product_id, quantity, unit_price) FROM STDIN") as cp:
                for row in order_items:
                    cp.write_row(row)
        conn.execute("ANALYZE")


if __name__ == "__main__":
    print(json.dumps(generate(), indent=2))
