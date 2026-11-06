"""Central configuration, read from environment variables so the same code runs
locally, in CI and against real infrastructure."""
import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Config:
    # Root of the local data lake: raw/ (landing), curated/ (Parquet), quarantine/ (bad records)
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("EVENTFLOW_DATA_DIR", "data")))
    # PostgreSQL connection string. Two schemas live here:
    #   oltp  -> normalized (3NF) source system for orders
    #   dw    -> dimensional (star schema) warehouse, plus ops (monitoring) and mart (reporting)
    pg_dsn: str = field(default_factory=lambda: os.getenv(
        "EVENTFLOW_PG_DSN", "postgresql://postgres@localhost:5432/eventflow"))
    # Where raw files are landed: "local" (filesystem) or "s3" (AWS S3 via boto3)
    storage: str = field(default_factory=lambda: os.getenv("EVENTFLOW_STORAGE", "local"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("EVENTFLOW_S3_BUCKET", "eventflow-raw"))

    # Synthetic data size. SCALE=1.0 produces ~2M clickstream events and ~1M access-log lines.
    scale: float = field(default_factory=lambda: float(os.getenv("EVENTFLOW_SCALE", "1.0")))
    start_date: date = date(2026, 10, 1)
    days: int = field(default_factory=lambda: int(os.getenv("EVENTFLOW_DAYS", "14")))
    seed: int = 42

    # Data quality thresholds (fraction of records). Breaching a "critical" check fails the run.
    max_quarantine_rate: float = 0.02
    max_duplicate_rate: float = 0.03
    max_5xx_rate_warn: float = 0.02

    @property
    def raw(self) -> Path:
        return self.data_dir / "raw"

    @property
    def curated(self) -> Path:
        return self.data_dir / "curated"

    @property
    def quarantine(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def runs(self) -> Path:
        return self.data_dir / "runs"


def get_config() -> Config:
    return Config()
