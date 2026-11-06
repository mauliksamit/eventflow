"""Landing zone: copy GH Archive hours and GitHub status history into the lake, unchanged.

Raw objects are immutable, so any day can be reprocessed. Each hour is verified (declared size,
gzip integrity, line count) and recorded in ops.ingest_files. That line count is taken here,
independently of Spark, so the audit can reconcile against it later.
"""
from __future__ import annotations

import gzip
import hashlib
import logging
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone

import psycopg
import requests

from .config import Config
from .lake import Lake

log = logging.getLogger("eventflow.ingest")
ARCHIVE_URL = "https://data.gharchive.org/{day}-{hour}.json.gz"  # hours are not zero-padded
STATUS_URL = "https://www.githubstatus.com/api/v2/incidents.json"
TRANSIENT = (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError, OSError, EOFError)


class SourceNotReady(RuntimeError):
    """The hour is not published upstream yet. Worth retrying later, unlike corrupt data."""


@dataclass
class HourFile:
    day: date
    hour: int
    key: str
    bytes: int
    lines: int
    sha256: str


def raw_key(day: date, hour: int) -> str:
    return f"raw/gharchive/dt={day}/hr={hour:02d}/{day}-{hour}.json.gz"


def scan_gzip(path: str) -> tuple[int, str]:
    """Return (line count, sha256 of the compressed bytes). Raises on a truncated or corrupt file."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    lines, last = 0, b"\n"
    with gzip.open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            lines += chunk.count(b"\n")
            last = chunk[-1:]
    # Some archive hours have no newline after their final record.
    return lines + (last != b"\n"), sha.hexdigest()


def _fetch_hour(lake: Lake, day: date, hour: int, attempts: int = 6) -> HourFile:
    url = ARCHIVE_URL.format(day=day, hour=hour)
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=60) as r, tempfile.NamedTemporaryFile(suffix=".gz") as tmp:
                if r.status_code == 404:
                    raise SourceNotReady(url)
                r.raise_for_status()
                declared = int(r.headers["Content-Length"])
                got = 0
                for chunk in r.iter_content(1 << 20):
                    tmp.write(chunk)
                    got += len(chunk)
                tmp.flush()
                if got != declared:
                    raise EOFError(f"{url}: got {got} of {declared} bytes")
                lines, sha = scan_gzip(tmp.name)
                key = raw_key(day, hour)
                lake.put_file(tmp.name, key)
                return HourFile(day, hour, key, got, lines, sha)
        except TRANSIENT as e:
            if attempt == attempts:
                raise
            log.warning("%s attempt %d failed (%s)", url, attempt, e)
            time.sleep(2 ** attempt)
    raise AssertionError("unreachable")


def ingest_day(cfg: Config, day: date) -> dict:
    """Land all 24 hours of `day`. Hours already recorded and present in the lake are skipped."""
    lake = Lake(cfg)
    lake.ensure()
    with psycopg.connect(cfg.pg_dsn) as conn:
        known = dict(conn.execute("SELECT hour, bytes FROM ops.ingest_files WHERE dt = %s", (day,)).fetchall())
        todo = [h for h in range(24) if lake.size(raw_key(day, h)) != known.get(h, -1)]
        with ThreadPoolExecutor(cfg.download_workers) as pool:
            files = list(pool.map(lambda h: _fetch_hour(lake, day, h), todo))
        for f in files:
            conn.execute("""INSERT INTO ops.ingest_files (dt, hour, object_key, bytes, line_count, sha256)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (dt, hour) DO UPDATE SET bytes = EXCLUDED.bytes, sha256 = EXCLUDED.sha256,
                                line_count = EXCLUDED.line_count, ingested_at = now()""",
                         (f.day, f.hour, f.key, f.bytes, f.lines, f.sha256))
    return {"dt": str(day), "hours_landed": len(files), "hours_skipped": 24 - len(todo),
            "bytes": sum(f.bytes for f in files)}


def parse_incidents(doc: dict) -> list[tuple]:
    return [(i["id"], i["name"], i["impact"], i["status"], i["created_at"], i.get("resolved_at"),
             [c["name"] for c in i.get("components", [])]) for i in doc["incidents"]]


def ingest_status(cfg: Config) -> dict:
    """Snapshot GitHub's published incidents. The API only serves the latest 50, so snapshots accumulate."""
    r = requests.get(STATUS_URL, timeout=30)
    r.raise_for_status()
    lake = Lake(cfg)
    lake.ensure()
    lake.put_bytes(r.content, f"raw/status/fetched={datetime.now(timezone.utc):%Y-%m-%dT%H%M%S}.json")
    rows = parse_incidents(r.json())
    with psycopg.connect(cfg.pg_dsn) as conn:
        for row in rows:
            conn.execute("""INSERT INTO dw.dim_incident (incident_id, name, impact, status, started_at, resolved_at, components)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (incident_id) DO UPDATE SET status = EXCLUDED.status,
                                impact = EXCLUDED.impact, resolved_at = EXCLUDED.resolved_at""", row)
    return {"incidents": len(rows)}
