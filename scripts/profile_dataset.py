#!/usr/bin/env python3
"""Lightweight EDA cho CSV Hotel Booking Demand."""

from __future__ import annotations

import csv
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


ROOT_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT_DIR / "data" / "input" / "hotel_bookings.csv"
SUMMARY_PATH = ROOT_DIR / "docs" / "data_profile_summary.md"
NULL_LIKE_VALUES = {"", "null", "none", "nan", "na", "n/a"}


def is_missing(value: str | None) -> bool:
    return value is None or value.strip().lower() in NULL_LIKE_VALUES


def to_float(value: str | None) -> float | None:
    if is_missing(value):
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def infer_dtype(values: list[str]) -> str:
    non_missing = [value.strip() for value in values if not is_missing(value)]
    if not non_missing:
        return "empty"

    int_count = 0
    float_count = 0
    date_count = 0

    for value in non_missing:
        try:
            int(value)
            int_count += 1
            float_count += 1
            continue
        except ValueError:
            pass

        try:
            float(value)
            float_count += 1
            continue
        except ValueError:
            pass

        try:
            datetime.strptime(value, "%Y-%m-%d")
            date_count += 1
        except ValueError:
            pass

    total = len(non_missing)
    if int_count == total:
        return "integer"
    if float_count == total:
        return "decimal"
    if date_count == total:
        return "date"
    return "string"


def percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None

    position = (len(sorted_values) - 1) * pct
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[int(position)]

    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def money(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f}"


def number(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}"


def load_rows() -> tuple[list[str], list[dict[str, str]]]:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {CSV_PATH}")

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        columns = reader.fieldnames or []

    if not columns:
        raise ValueError(f"No header row found in: {CSV_PATH}")

    return columns, rows


def profile() -> dict[str, object]:
    columns, rows = load_rows()
    row_count = len(rows)

    missing_counts = Counter()
    sample_values: dict[str, list[str]] = {column: [] for column in columns}

    for row in rows:
        for column in columns:
            value = row.get(column)
            if is_missing(value):
                missing_counts[column] += 1
            elif len(sample_values[column]) < 1000:
                sample_values[column].append(value or "")

    inferred_dtypes = {
        column: infer_dtype(sample_values[column])
        for column in columns
    }

    adr_values = [value for row in rows if (value := to_float(row.get("adr"))) is not None]
    adr_values_sorted = sorted(adr_values)
    adr_min = min(adr_values) if adr_values else None
    adr_max = max(adr_values) if adr_values else None
    adr_mean = mean(adr_values) if adr_values else None
    adr_q1 = percentile(adr_values_sorted, 0.25)
    adr_q3 = percentile(adr_values_sorted, 0.75)
    adr_iqr = adr_q3 - adr_q1 if adr_q1 is not None and adr_q3 is not None else None
    adr_upper_bound = adr_q3 + (1.5 * adr_iqr) if adr_q3 is not None and adr_iqr is not None else None
    adr_lower_bound = adr_q1 - (1.5 * adr_iqr) if adr_q1 is not None and adr_iqr is not None else None
    adr_outlier_count = sum(
        1
        for value in adr_values
        if (adr_upper_bound is not None and value > adr_upper_bound)
        or (adr_lower_bound is not None and value < adr_lower_bound)
    )

    zero_night_count = 0
    zero_guest_count = 0
    invalid_metric_count = 0
    estimated_revenue_total = 0.0
    realized_revenue_total = 0.0

    for row in rows:
        weekend_nights = to_float(row.get("stays_in_weekend_nights"))
        week_nights = to_float(row.get("stays_in_week_nights"))
        adults = to_float(row.get("adults"))
        children = to_float(row.get("children")) or 0.0
        babies = to_float(row.get("babies"))
        adr = to_float(row.get("adr"))
        is_canceled = to_float(row.get("is_canceled"))

        required_values = [weekend_nights, week_nights, adults, babies, adr, is_canceled]
        if any(value is None for value in required_values):
            invalid_metric_count += 1
            continue

        total_nights = weekend_nights + week_nights
        total_guests = adults + children + babies
        estimated_revenue = adr * total_nights

        if total_nights == 0:
            zero_night_count += 1
        if total_guests == 0:
            zero_guest_count += 1

        estimated_revenue_total += estimated_revenue
        if int(is_canceled) == 0:
            realized_revenue_total += estimated_revenue

    return {
        "columns": columns,
        "row_count": row_count,
        "column_count": len(columns),
        "missing_counts": missing_counts,
        "inferred_dtypes": inferred_dtypes,
        "adr_min": adr_min,
        "adr_max": adr_max,
        "adr_mean": adr_mean,
        "adr_q1": adr_q1,
        "adr_q3": adr_q3,
        "adr_lower_bound": adr_lower_bound,
        "adr_upper_bound": adr_upper_bound,
        "adr_outlier_count": adr_outlier_count,
        "zero_night_count": zero_night_count,
        "zero_guest_count": zero_guest_count,
        "invalid_metric_count": invalid_metric_count,
        "estimated_revenue_total": estimated_revenue_total,
        "realized_revenue_total": realized_revenue_total,
    }


def print_profile(result: dict[str, object]) -> None:
    columns = result["columns"]
    missing_counts = result["missing_counts"]
    inferred_dtypes = result["inferred_dtypes"]

    print(f"Dataset: {CSV_PATH}")
    print(f"So dong (rows): {number(result['row_count'])}")
    print(f"So cot (columns): {number(result['column_count'])}")
    print()

    print("Columns va inferred dtypes:")
    for column in columns:
        print(f"- {column}: {inferred_dtypes[column]}")
    print()

    print("Missing values theo column:")
    for column in columns:
        print(f"- {column}: {number(missing_counts[column])}")
    print()

    print("ADR:")
    print(f"- min: {money(result['adr_min'])}")
    print(f"- max: {money(result['adr_max'])}")
    print(f"- mean: {money(result['adr_mean'])}")
    print(f"- IQR lower bound: {money(result['adr_lower_bound'])}")
    print(f"- IQR upper bound: {money(result['adr_upper_bound'])}")
    print(f"- possible outliers: {number(result['adr_outlier_count'])}")
    print()

    print("Kiem tra derived metrics:")
    print(f"- zero-night bookings: {number(result['zero_night_count'])}")
    print(f"- zero-guest bookings: {number(result['zero_guest_count'])}")
    print(f"- rows skipped do invalid metric fields: {number(result['invalid_metric_count'])}")
    print(f"- estimated_revenue total: {money(result['estimated_revenue_total'])}")
    print(f"- realized_revenue total: {money(result['realized_revenue_total'])}")


def write_summary(result: dict[str, object]) -> None:
    columns = result["columns"]
    missing_counts = result["missing_counts"]
    inferred_dtypes = result["inferred_dtypes"]
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        "# Data Profile Summary",
        "",
        f"Generated at: {generated_at}",
        "",
        f"- Dataset path: `{CSV_PATH.relative_to(ROOT_DIR)}`",
        f"- So dong (rows): {number(result['row_count'])}",
        f"- So cot (columns): {number(result['column_count'])}",
        "",
        "## Columns",
        "",
        "| Column | Inferred Type | Missing Values |",
        "| --- | --- | ---: |",
    ]

    for column in columns:
        lines.append(
            f"| `{column}` | {inferred_dtypes[column]} | {number(missing_counts[column])} |"
        )

    lines.extend(
        [
            "",
            "## ADR Checks",
            "",
            f"- Min ADR: {money(result['adr_min'])}",
            f"- Max ADR: {money(result['adr_max'])}",
            f"- Mean ADR: {money(result['adr_mean'])}",
            f"- IQR lower bound: {money(result['adr_lower_bound'])}",
            f"- IQR upper bound: {money(result['adr_upper_bound'])}",
            f"- Possible ADR outliers: {number(result['adr_outlier_count'])}",
            "",
            "## Derived Metric Checks",
            "",
            f"- Zero-night bookings: {number(result['zero_night_count'])}",
            f"- Zero-guest bookings: {number(result['zero_guest_count'])}",
            f"- Rows skipped due to invalid metric fields: {number(result['invalid_metric_count'])}",
            f"- Estimated revenue total: {money(result['estimated_revenue_total'])}",
            f"- Realized revenue total: {money(result['realized_revenue_total'])}",
            "",
            "## Metric Definitions",
            "",
            "- `total_nights = stays_in_weekend_nights + stays_in_week_nights`",
            "- `total_guests = adults + children + babies`",
            "- `estimated_revenue = adr * total_nights`",
            "- `realized_revenue = estimated_revenue` only when `is_canceled = 0`, otherwise `0`",
        ]
    )

    SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    try:
        result = profile()
    except (FileNotFoundError, ValueError) as error:
        print(f"ERROR: {error}")
        return 1

    print_profile(result)
    write_summary(result)
    print()
    print(f"Da ghi summary: {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
