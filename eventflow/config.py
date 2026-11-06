"""Runtime configuration, read from environment variables so the same code runs locally,
in CI and against real infrastructure."""
import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(f"EVENTFLOW_{name}", default)


@dataclass(frozen=True)
class Config:
    # Lake root: a local path, or s3://bucket[/prefix] for AWS S3 or any S3-compatible store.
    lake: str = field(default_factory=lambda: _env("LAKE", "data/lake"))
    # Leave unset for AWS; set it for SeaweedFS, MinIO and friends.
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", ""))
    pg_dsn: str = field(default_factory=lambda: _env("PG_DSN", "postgresql://postgres@localhost:5432/eventflow"))

    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "4g"))
    shuffle_partitions: int = field(default_factory=lambda: int(_env("SHUFFLE_PARTITIONS", "32")))
    download_workers: int = field(default_factory=lambda: int(_env("DOWNLOAD_WORKERS", "6")))

    # Audit thresholds. Critical checks block publication; warnings are recorded and surfaced.
    min_hours: int = field(default_factory=lambda: int(_env("MIN_HOURS", "24")))
    max_quarantine_rate: float = field(default_factory=lambda: float(_env("MAX_QUARANTINE_RATE", "0.001")))
    min_capture_warn: float = field(default_factory=lambda: float(_env("MIN_CAPTURE_WARN", "0.001")))


def get_config() -> Config:
    return Config()
