#!/usr/bin/env python3
"""Generate deterministic incremental CSV batches for dedup/current-state demos."""

from __future__ import annotations

import argparse
import csv
from copy import deepcopy
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT_DIR / "data" / "input" / "hotel_bookings.csv"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "data" / "input" / "incremental_batches"

SOURCE_DATASET = "hotel_booking_demand"
EFFECTIVE_DATES = {
    "batch_001_initial": "2026-01-01 00:00:00",
    "batch_002_updates": "2026-01-02 00:00:00",
    "batch_003_duplicate_replay": "2026-01-03 00:00:00",
    "batch_004_same_state": "2026-01-04 00:00:00",
    "batch_005_reverted_state": "2026-01-05 00:00:00",
}

METADATA_COLUMNS = [
    "source_dataset",
    "original_source_row_number",
    "booking_key",
    "batch_id",
    "batch_sequence",
    "batch_effective_at",
    "batch_row_number",
    "synthetic_operation",
]


def read_source_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        raise FileNotFoundError(f"Source CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames:
            raise ValueError(f"Source CSV has no header: {path}")

        rows: list[dict[str, str]] = []
        for row_number, row in enumerate(reader, start=1):
            clean_row = {key: (value or "") for key, value in row.items()}
            clean_row["source_dataset"] = SOURCE_DATASET
            clean_row["original_source_row_number"] = str(row_number)
            clean_row["booking_key"] = f"{SOURCE_DATASET}:{row_number}"
            rows.append(clean_row)

    return list(reader.fieldnames), rows


def with_batch_metadata(
    row: dict[str, str],
    *,
    batch_id: str,
    batch_sequence: int,
    batch_row_number: int,
    synthetic_operation: str,
) -> dict[str, str]:
    enriched = deepcopy(row)
    enriched["batch_id"] = batch_id
    enriched["batch_sequence"] = str(batch_sequence)
    enriched["batch_effective_at"] = EFFECTIVE_DATES[batch_id]
    enriched["batch_row_number"] = str(batch_row_number)
    enriched["synthetic_operation"] = synthetic_operation
    return enriched


def bump_decimal(value: str, delta: float) -> str:
    try:
        return f"{max(float(value or 0) + delta, 0):.2f}"
    except ValueError:
        return f"{delta:.2f}"


def flip_cancel_flag(value: str) -> str:
    return "0" if str(value).strip() == "1" else "1"


def next_room_type(value: str) -> str:
    room = (value or "A").strip().upper()[:1] or "A"
    if room >= "H":
        return "A"
    return chr(ord(room) + 1)


def make_update(row: dict[str, str], row_number: int) -> dict[str, str]:
    updated = deepcopy(row)
    if row_number == 2:
        updated["adr"] = bump_decimal(updated.get("adr", ""), 25)
        updated["reservation_status"] = "Check-Out"
    elif row_number in {3, 4, 5}:
        updated["adr"] = bump_decimal(updated.get("adr", ""), 15)
    elif row_number in {6, 7, 8}:
        updated["is_canceled"] = flip_cancel_flag(updated.get("is_canceled", "0"))
        updated["reservation_status"] = "Canceled" if updated["is_canceled"] == "1" else "Check-Out"
    else:
        updated["assigned_room_type"] = next_room_type(updated.get("assigned_room_type", "A"))
        updated["booking_changes"] = str(int(updated.get("booking_changes") or 0) + 1)
    return updated


def make_new_rows(source_rows: list[dict[str, str]], count: int = 5) -> list[dict[str, str]]:
    max_row_number = len(source_rows)
    new_rows: list[dict[str, str]] = []
    for offset in range(1, count + 1):
        row = deepcopy(source_rows[offset - 1])
        new_row_number = max_row_number + offset
        row["source_dataset"] = SOURCE_DATASET
        row["original_source_row_number"] = str(new_row_number)
        row["booking_key"] = f"{SOURCE_DATASET}:{new_row_number}"
        row["adr"] = bump_decimal(row.get("adr", ""), 10 + offset)
        row["reservation_status"] = "Check-Out"
        row["is_canceled"] = "0"
        new_rows.append(row)
    return new_rows


def write_batch(path: Path, source_columns: list[str], rows: list[dict[str, str]]) -> None:
    fieldnames = METADATA_COLUMNS + source_columns
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {path} ({len(rows):,} rows)")


def generate_batches(source_path: Path, output_dir: Path) -> None:
    source_columns, source_rows = read_source_rows(source_path)

    batch_001 = [
        with_batch_metadata(
            row,
            batch_id="batch_001_initial",
            batch_sequence=1,
            batch_row_number=row_number,
            synthetic_operation="initial",
        )
        for row_number, row in enumerate(source_rows, start=1)
    ]

    update_rows = [source_rows[0]]
    update_rows.extend(make_update(source_rows[row_number - 1], row_number) for row_number in range(2, 11))
    update_rows.extend(make_new_rows(source_rows))
    batch_002: list[dict[str, str]] = []
    for batch_row_number, row in enumerate(update_rows, start=1):
        operation = "same_state_fixture" if row["original_source_row_number"] == "1" else "update"
        if int(row["original_source_row_number"]) > len(source_rows):
            operation = "new"
        batch_002.append(
            with_batch_metadata(
                row,
                batch_id="batch_002_updates",
                batch_sequence=2,
                batch_row_number=batch_row_number,
                synthetic_operation=operation,
            )
        )

    # Intentional exact duplicates inside the same batch. dbt should collapse
    # them because booking_key + batch_id + record_hash is identical.
    for duplicate in (batch_002[1], batch_002[2]):
        duplicate_row = deepcopy(duplicate)
        duplicate_row["batch_row_number"] = str(len(batch_002) + 1)
        duplicate_row["synthetic_operation"] = "exact_duplicate"
        batch_002.append(duplicate_row)

    batch_003: list[dict[str, str]] = []
    replay_rows = [source_rows[0]]
    replay_rows.extend(update_rows[1:])
    for batch_row_number, row in enumerate(replay_rows, start=1):
        batch_003.append(
            with_batch_metadata(
                row,
                batch_id="batch_003_duplicate_replay",
                batch_sequence=3,
                batch_row_number=batch_row_number,
                synthetic_operation="duplicate_replay",
            )
        )

    batch_004 = [
        with_batch_metadata(
            source_rows[0],
            batch_id="batch_004_same_state",
            batch_sequence=4,
            batch_row_number=1,
            synthetic_operation="same_state_fixture",
        )
    ]

    batch_005 = [
        with_batch_metadata(
            source_rows[1],
            batch_id="batch_005_reverted_state",
            batch_sequence=5,
            batch_row_number=1,
            synthetic_operation="reverted_state_fixture",
        )
    ]

    batches = {
        "batch_001_initial.csv": batch_001,
        "batch_002_updates.csv": batch_002,
        "batch_003_duplicate_replay.csv": batch_003,
        "batch_004_same_state.csv": batch_004,
        "batch_005_reverted_state.csv": batch_005,
    }

    for filename, rows in batches.items():
        write_batch(output_dir / filename, source_columns, rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to original hotel_bookings.csv")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for generated batches")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        generate_batches(args.input, args.output_dir)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
