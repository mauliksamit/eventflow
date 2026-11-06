"""Thin object-store layer so every step can run on a local directory or the S3 API."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from pyarrow import fs as pafs

from .config import Config


class Lake:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.is_s3 = cfg.lake.startswith("s3://")
        if self.is_s3:
            bucket, _, prefix = cfg.lake[5:].partition("/")
            self.bucket, self.prefix = bucket, prefix.strip("/")
            self.s3 = boto3.client("s3", endpoint_url=cfg.s3_endpoint or None,
                                   config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 8}))
        else:
            self.root = Path(cfg.lake).resolve()

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.is_s3 and self.prefix else key

    def ensure(self) -> None:
        if not self.is_s3:
            self.root.mkdir(parents=True, exist_ok=True)
        elif self.bucket not in {b["Name"] for b in self.s3.list_buckets().get("Buckets", [])}:
            self.s3.create_bucket(Bucket=self.bucket)

    def uri(self, key: str) -> str:
        """Location of `key` as Spark addresses it."""
        return f"s3a://{self.bucket}/{self._key(key)}" if self.is_s3 else str(self.root / key)

    def size(self, key: str) -> int | None:
        if self.is_s3:
            try:
                return self.s3.head_object(Bucket=self.bucket, Key=self._key(key))["ContentLength"]
            except ClientError:
                return None
        path = self.root / key
        return path.stat().st_size if path.is_file() else None

    def put_file(self, local: str | Path, key: str) -> None:
        if self.is_s3:
            self.s3.upload_file(str(local), self.bucket, self._key(key))
        else:
            dest = self.root / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local, dest)

    def put_bytes(self, data: bytes, key: str) -> None:
        if self.is_s3:
            self.s3.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)
        else:
            dest = self.root / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)

    def list(self, prefix: str) -> list[str]:
        if not self.is_s3:
            base = self.root / prefix
            return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())
        strip = len(self.prefix) + 1 if self.prefix else 0
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            keys += [o["Key"][strip:] for o in page.get("Contents", [])]
        return sorted(keys)

    def delete_prefix(self, prefix: str) -> None:
        if not self.is_s3:
            shutil.rmtree(self.root / prefix, ignore_errors=True)
            return
        for key in self.list(prefix):
            self.s3.delete_object(Bucket=self.bucket, Key=self._key(key))

    def arrow(self, key: str) -> tuple[pafs.FileSystem, str]:
        """A (filesystem, path) pair for reading `key` with pyarrow."""
        if not self.is_s3:
            return pafs.LocalFileSystem(), str(self.root / key)
        ep = self.cfg.s3_endpoint
        s3 = pafs.S3FileSystem(endpoint_override=ep.split("://")[-1] if ep else None,
                               scheme=ep.split("://")[0] if ep else "https",
                               region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        return s3, f"{self.bucket}/{self._key(key)}"
