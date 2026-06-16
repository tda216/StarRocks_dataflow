#!/usr/bin/env python3
"""Check local service connectivity for the Phase 2/3 stack."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

try:
    from minio import Minio
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: minio. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc

try:
    import pymysql
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: PyMySQL. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"


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


def check_minio() -> None:
    client = Minio(
        env("MINIO_EXTERNAL_ENDPOINT", "localhost:9000"),
        access_key=env("MINIO_ROOT_USER", "minioadmin"),
        secret_key=env("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=bool_env("MINIO_SECURE", "false"),
    )
    bucket = env("MINIO_BUCKET", "hotel-booking-raw")
    exists = client.bucket_exists(bucket)
    print(f"MinIO: OK (bucket {bucket!r} exists={exists})")


def check_starrocks() -> None:
    connection = pymysql.connect(
        host=env("STARROCKS_EXTERNAL_HOST", "127.0.0.1"),
        port=int(env("STARROCKS_EXTERNAL_QUERY_PORT", env("STARROCKS_QUERY_PORT", "9030"))),
        user=env("STARROCKS_USER", "root"),
        password=env("STARROCKS_PASSWORD", ""),
        autocommit=True,
        charset="utf8mb4",
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()[0]
    print(f"StarRocks: OK (SELECT {result})")


def check_url(name: str, url: str) -> None:
    try:
        with urlopen(url, timeout=5) as response:
            print(f"{name}: OK (HTTP {response.status})")
    except URLError as exc:
        print(f"{name}: WARN ({exc})")


def main() -> int:
    load_env_file(ENV_PATH)
    failed = False

    for check in (check_minio, check_starrocks):
        try:
            check()
        except Exception as exc:
            failed = True
            print(f"{check.__name__}: ERROR ({exc})")

    check_url("Airflow UI", f"http://localhost:{env('AIRFLOW_WEB_PORT', '8080')}/health")
    check_url("Superset UI", f"http://localhost:{env('SUPERSET_PORT', '8088')}/health")
    check_url("Iceberg REST", f"http://localhost:{env('ICEBERG_REST_PORT', '8181')}/v1/config")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
