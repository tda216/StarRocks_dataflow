#!/usr/bin/env python3
"""Upload generated incremental batch CSV files to the MinIO raw bucket."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from minio import Minio
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: minio. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc

from batch_storage import build_partitioned_batch_object_key


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"
DEFAULT_BATCH_DIR = ROOT_DIR / "data" / "input" / "incremental_batches"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def bool_env(name: str, default: str = "false") -> bool:
    return env(name, default).lower() in {"1", "true", "yes", "y"}


def upload_batches(batch_dir: Path = DEFAULT_BATCH_DIR) -> list[str]:
    load_env_file(ENV_PATH)

    if not batch_dir.exists():
        raise FileNotFoundError(f"Batch directory not found: {batch_dir}")

    batch_files = sorted(batch_dir.glob("batch_*.csv"))
    if not batch_files:
        raise FileNotFoundError(f"No batch_*.csv files found in {batch_dir}")

    endpoint = env("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
    access_key = env("MINIO_ROOT_USER", "minioadmin")
    secret_key = env("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket = env("MINIO_BUCKET", "hotel-booking-raw")
    prefix = env("MINIO_BATCH_PREFIX", "hotel_booking_demand/incremental_batches").strip("/")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=bool_env("MINIO_SECURE", "false"),
    )

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    uploaded: list[str] = []
    for batch_file in batch_files:
        object_key, metadata = build_partitioned_batch_object_key(prefix, batch_file)
        client.fput_object(bucket, object_key, str(batch_file), content_type="text/csv")
        object_uri = f"s3://{bucket}/{object_key}"
        uploaded.append(object_uri)
        print(
            f"Uploaded {batch_file} -> {object_uri} "
            f"(etl_date={metadata.watermark_date}, raw_batch_sequence={metadata.raw_batch_sequence})"
        )

    return uploaded


def main() -> int:
    try:
        upload_batches()
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
