#!/usr/bin/env python3
"""Upload the local Hotel Booking CSV to the MinIO raw bucket."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from minio import Minio
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: minio. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"
CSV_PATH = ROOT_DIR / "data" / "input" / "hotel_bookings.csv"


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


def upload_file() -> str:
    load_env_file(ENV_PATH)

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {CSV_PATH}")

    endpoint = env("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
    access_key = env("MINIO_ROOT_USER", "minioadmin")
    secret_key = env("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket = env("MINIO_BUCKET", "hotel-booking-raw")
    object_key = env("MINIO_RAW_OBJECT_KEY", "hotel_booking_demand/hotel_bookings.csv")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=bool_env("MINIO_SECURE", "false"),
    )

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.fput_object(
        bucket,
        object_key,
        str(CSV_PATH),
        content_type="text/csv",
    )

    object_uri = f"s3://{bucket}/{object_key}"
    print(f"Uploaded {CSV_PATH} -> {object_uri}")
    return object_uri


def main() -> int:
    try:
        upload_file()
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
