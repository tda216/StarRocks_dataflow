"""Shared helpers for deterministic raw batch object paths."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class BatchStorageMetadata:
    batch_id: str
    batch_sequence: int
    batch_effective_at: datetime
    etl_year: int
    etl_month: int
    etl_day: int
    watermark_date: str
    raw_batch_sequence: str


def _parse_batch_effective_at(value: str) -> datetime:
    clean_value = value.strip()
    for date_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(clean_value, date_format)
        except ValueError:
            continue
    raise ValueError(f"Unsupported batch_effective_at format: {value!r}")


def read_batch_storage_metadata(batch_file: Path) -> BatchStorageMetadata:
    with batch_file.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        first_row = next(reader, None)

    if not first_row:
        raise ValueError(f"Batch file has no data rows: {batch_file}")

    batch_id = (first_row.get("batch_id") or batch_file.stem).strip()
    raw_sequence = (first_row.get("batch_sequence") or "").strip()
    effective_at = _parse_batch_effective_at(first_row.get("batch_effective_at") or "")

    try:
        batch_sequence = int(raw_sequence)
    except ValueError as exc:
        raise ValueError(f"Invalid batch_sequence in {batch_file}: {raw_sequence!r}") from exc

    return BatchStorageMetadata(
        batch_id=batch_id,
        batch_sequence=batch_sequence,
        batch_effective_at=effective_at,
        etl_year=effective_at.year,
        etl_month=effective_at.month,
        etl_day=effective_at.day,
        watermark_date=effective_at.strftime("%Y%m%d"),
        raw_batch_sequence=f"{batch_sequence:03d}",
    )


def build_partitioned_batch_object_key(prefix: str, batch_file: Path) -> tuple[str, BatchStorageMetadata]:
    metadata = read_batch_storage_metadata(batch_file)
    clean_prefix = prefix.strip("/")
    object_key = (
        f"{clean_prefix}/"
        f"etl_year={metadata.etl_year}/"
        f"etl_month={metadata.etl_month:02d}/"
        f"etl_day={metadata.etl_day:02d}/"
        f"raw_batch_sequence={metadata.raw_batch_sequence}/"
        f"{batch_file.name}"
    )
    return object_key, metadata
