"""Object storage for the raw landing zone.

The pipeline always processes from the local lake directory; with EVENTFLOW_STORAGE=s3 the raw
files are also landed in (and can be re-downloaded from) an S3 bucket, partitioned the same way:
    s3://<bucket>/raw/<source>/dt=YYYY-MM-DD/<file>
Keeping raw data immutable in object storage means any day can be re-processed (backfilled)
from source of truth.
"""
from __future__ import annotations

from pathlib import Path

import boto3


class S3RawStore:
    def __init__(self, bucket: str, client=None):
        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def ensure_bucket(self) -> None:
        existing = {b["Name"] for b in self.s3.list_buckets().get("Buckets", [])}
        if self.bucket not in existing:
            self.s3.create_bucket(Bucket=self.bucket)

    def upload_tree(self, local_root: Path, prefix: str = "raw") -> int:
        """Upload every file under local_root, keeping relative paths as keys. Returns file count."""
        n = 0
        for path in sorted(p for p in local_root.rglob("*") if p.is_file()):
            key = f"{prefix}/{path.relative_to(local_root).as_posix()}"
            self.s3.upload_file(str(path), self.bucket, key)
            n += 1
        return n

    def list_partitions(self, source: str, prefix: str = "raw") -> list[str]:
        """Return the dt=... partitions present for a source, e.g. ['dt=2026-10-01', ...]."""
        paginator = self.s3.get_paginator("list_objects_v2")
        parts = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{prefix}/{source}/"):
            for obj in page.get("Contents", []):
                segs = obj["Key"].split("/")
                if len(segs) > 2 and segs[2].startswith("dt="):
                    parts.add(segs[2])
        return sorted(parts)

    def download_partition(self, source: str, partition: str, local_root: Path, prefix: str = "raw") -> int:
        n = 0
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{prefix}/{source}/{partition}/"):
            for obj in page.get("Contents", []):
                dest = local_root / Path(obj["Key"]).relative_to(prefix)
                dest.parent.mkdir(parents=True, exist_ok=True)
                self.s3.download_file(self.bucket, obj["Key"], str(dest))
                n += 1
        return n
