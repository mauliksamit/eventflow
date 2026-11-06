import gzip

import boto3
import pytest
from moto import mock_aws

from eventflow.config import Config
from eventflow.ingest import parse_incidents, raw_key, scan_gzip
from eventflow.lake import Lake


def test_hours_are_zero_padded_in_the_lake_but_not_upstream():
    from datetime import date
    assert raw_key(date(2026, 10, 1), 3) == "raw/gharchive/dt=2026-10-01/hr=03/2026-10-01-3.json.gz"


def test_scan_counts_lines(tmp_path):
    p = tmp_path / "x.gz"
    p.write_bytes(gzip.compress(b"a\nb\nc\n"))
    assert scan_gzip(str(p))[0] == 3


def test_scan_counts_a_final_line_with_no_newline(tmp_path):
    p = tmp_path / "x.gz"
    p.write_bytes(gzip.compress(b"a\nb\nc"))
    assert scan_gzip(str(p))[0] == 3


def test_scan_rejects_truncated_file(tmp_path):
    p = tmp_path / "x.gz"
    p.write_bytes(gzip.compress(b"a\n" * 1000)[:-10])
    with pytest.raises(EOFError):
        scan_gzip(str(p))


def test_parse_incidents_handles_unresolved():
    doc = {"incidents": [{"id": "x", "name": "n", "impact": "minor", "status": "investigating",
                          "created_at": "2026-11-05T00:00:00Z", "components": [{"name": "Actions"}]}]}
    assert parse_incidents(doc) == [("x", "n", "minor", "investigating", "2026-11-05T00:00:00Z", None, ["Actions"])]


def test_local_lake_round_trip(tmp_path):
    lake = Lake(Config(lake=str(tmp_path)))
    lake.put_bytes(b"hello", "raw/a/b.txt")
    assert lake.size("raw/a/b.txt") == 5 and lake.size("raw/a/c.txt") is None
    assert lake.list("raw") == ["raw/a/b.txt"]
    lake.delete_prefix("raw")
    assert lake.list("raw") == []


@mock_aws
def test_s3_lake_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="lake")
    lake = Lake(Config(lake="s3://lake/prod", s3_endpoint=""))
    assert lake.uri("raw/a.gz") == "s3a://lake/prod/raw/a.gz"
    f = tmp_path / "a.gz"
    f.write_bytes(b"12345")
    lake.put_file(f, "raw/a.gz")
    assert lake.size("raw/a.gz") == 5 and lake.size("raw/b.gz") is None
    assert lake.list("raw") == ["raw/a.gz"]
    lake.delete_prefix("raw")
    assert lake.list("raw") == []
