from datetime import datetime, timezone

import boto3
from moto import mock_aws

from eventflow.config import Config
from eventflow.generate import _iso
from eventflow.quality import reconciliation_checks
from eventflow.storage import S3RawStore

STATS = {"clickstream_raw_lines": 1000, "clickstream_valid": 990, "clickstream_quarantined": 10,
         "duplicates_removed": 9, "access_raw_lines": 500, "access_quarantined": 1}


def test_reconciliation_passes_when_every_row_is_accounted_for():
    checks = {c.name: c for c in reconciliation_checks(STATS, Config())}
    assert all(c.passed for c in checks.values())


def test_reconciliation_fails_when_rows_go_missing():
    stats = dict(STATS, clickstream_valid=980)  # 10 rows silently dropped
    checks = {c.name: c for c in reconciliation_checks(stats, Config())}
    assert not checks["clickstream_rows_reconcile"].passed
    assert checks["clickstream_rows_reconcile"].severity == "critical"


def test_quarantine_rate_threshold():
    stats = dict(STATS, clickstream_valid=900, clickstream_quarantined=100)
    checks = {c.name: c for c in reconciliation_checks(stats, Config())}
    assert not checks["clickstream_quarantine_rate"].passed


def test_iso_offsets_render_local_time():
    ts = datetime(2026, 10, 1, 5, 30, tzinfo=timezone.utc)
    assert _iso(ts, "-06:00") == "2026-09-30T23:30:00.000-06:00"


@mock_aws
def test_s3_store_round_trip(tmp_path):
    (tmp_path / "raw" / "clickstream" / "dt=2026-10-01").mkdir(parents=True)
    (tmp_path / "raw" / "clickstream" / "dt=2026-10-01" / "events-000.jsonl.gz").write_bytes(b"x")
    (tmp_path / "raw" / "clickstream" / "dt=2026-10-02").mkdir(parents=True)
    (tmp_path / "raw" / "clickstream" / "dt=2026-10-02" / "events-000.jsonl.gz").write_bytes(b"y")
    store = S3RawStore("test-bucket", boto3.client("s3", region_name="us-east-1"))
    store.ensure_bucket()
    assert store.upload_tree(tmp_path / "raw") == 2
    assert store.list_partitions("clickstream") == ["dt=2026-10-01", "dt=2026-10-02"]
    dest = tmp_path / "restore"
    assert store.download_partition("clickstream", "dt=2026-10-02", dest) == 1
    assert (dest / "clickstream" / "dt=2026-10-02" / "events-000.jsonl.gz").read_bytes() == b"y"
