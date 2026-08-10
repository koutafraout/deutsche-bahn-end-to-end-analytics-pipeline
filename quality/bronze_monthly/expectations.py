"""Expectation definitions for the Bronze monthly Parquet quality gate.

Column list, non-null set, and null-pattern evidence are all taken directly
from the executed Jan-Jul 2026 profiling pass
(`docs/data-profiling-2026-01-07.md` §3, §4) — not guessed.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd

# 17-column schema confirmed identical across Jan-Jul 2026, 0 field-level
# diffs (profiling doc §3).
EXPECTED_COLUMNS = [
    "station_name",
    "xml_station_name",
    "eva",
    "train_number",
    "line_number",
    "final_destination_station",
    "delay_in_min",
    "time",
    "is_canceled",
    "train_type",
    "train_line_ride_id",
    "train_line_station_num",
    "arrival_planned_time",
    "arrival_change_time",
    "departure_planned_time",
    "departure_change_time",
    "id",
]

# Fully non-null in every profiled month (profiling doc §4) — a blanket
# not_null test is warranted only for these; line_number/station_name have
# documented structural null patterns and must NOT be tested this way.
NOT_NULL_COLUMNS = [
    "id",
    "eva",
    "train_number",
    "train_type",
    "train_line_ride_id",
    "train_line_station_num",
    "time",
    "is_canceled",
    "xml_station_name",
    "delay_in_min",
]

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

NS_PER_SECOND = 1_000_000_000


def month_bounds_ns(month: str) -> tuple[int, int]:
    """Inclusive [start, end] nanosecond-epoch bounds for a YYYY-MM month.

    Used to check the `time` column falls inside its own source month
    (profiling doc §12 — service_date_outside_source_month_rows was 0 in
    every profiled month).
    """
    if not MONTH_RE.match(month):
        raise ValueError(f"month must be in YYYY-MM format, got {month!r}")

    year, month_num = (int(part) for part in month.split("-"))
    start = datetime(year, month_num, 1, tzinfo=timezone.utc)
    next_year, next_month = (year + 1, 1) if month_num == 12 else (year, month_num + 1)
    end_exclusive = datetime(next_year, next_month, 1, tzinfo=timezone.utc)

    start_ns = int(start.timestamp()) * NS_PER_SECOND
    end_ns = int(end_exclusive.timestamp()) * NS_PER_SECOND - 1
    return start_ns, end_ns


def month_bounds_timestamps(month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Same bounds as `month_bounds_ns`, as tz-naive pandas Timestamps.

    `pandas.read_parquet` decodes the `time` column's int64-nanosecond
    physical storage into a tz-naive `datetime64[ns]` Series, so
    expectations comparing against it must use the same type — comparing
    against raw integers raises `TypeError: Invalid comparison between
    dtype=datetime64[ns] and float` inside pandas.
    """
    start_ns, end_ns = month_bounds_ns(month)
    return pd.Timestamp(start_ns, unit="ns"), pd.Timestamp(end_ns, unit="ns")


def build_not_null_suite(month: str) -> gx.ExpectationSuite:
    """Suite validated against a dataframe pruned to NOT_NULL_COLUMNS only.

    Row-count and full 17-column schema checks are done separately via
    Parquet metadata (see validate.py) — no need to load the other seven
    columns (planned/change timestamps, station names, line_number) into
    memory just to check the ones that are always populated.
    """
    suite = gx.ExpectationSuite(name=f"bronze_monthly_not_null_{month}")

    for column in NOT_NULL_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

    start_ts, end_ts = month_bounds_timestamps(month)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="time", min_value=start_ts, max_value=end_ts
        )
    )

    return suite
