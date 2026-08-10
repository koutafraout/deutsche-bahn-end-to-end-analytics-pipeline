import pytest

import pandas as pd

from quality.bronze_monthly.expectations import (
    EXPECTED_COLUMNS,
    NOT_NULL_COLUMNS,
    month_bounds_ns,
    month_bounds_timestamps,
)


def test_expected_columns_has_17_columns():
    assert len(EXPECTED_COLUMNS) == 17
    assert len(set(EXPECTED_COLUMNS)) == 17


def test_not_null_columns_are_a_subset_of_expected_columns():
    assert set(NOT_NULL_COLUMNS) <= set(EXPECTED_COLUMNS)


def test_month_bounds_ns_rejects_invalid_month():
    with pytest.raises(ValueError):
        month_bounds_ns("2026-13")


def test_month_bounds_ns_january():
    start_ns, end_ns = month_bounds_ns("2026-01")
    assert start_ns == 1767225600_000000000
    assert end_ns == 1769904000_000000000 - 1


def test_month_bounds_ns_december_rolls_over_to_next_year():
    start_ns, end_ns = month_bounds_ns("2026-12")
    next_start_ns, _ = month_bounds_ns("2027-01")
    assert end_ns == next_start_ns - 1


def test_month_bounds_ns_start_before_end():
    start_ns, end_ns = month_bounds_ns("2026-07")
    assert start_ns < end_ns


def test_month_bounds_timestamps_matches_ns_bounds():
    start_ns, end_ns = month_bounds_ns("2026-07")
    start_ts, end_ts = month_bounds_timestamps("2026-07")
    assert start_ts == pd.Timestamp(start_ns, unit="ns")
    assert end_ts == pd.Timestamp(end_ns, unit="ns")
    assert start_ts.tzinfo is None
    assert end_ts.tzinfo is None


def test_month_bounds_timestamps_comparable_to_naive_datetime_series():
    start_ts, end_ts = month_bounds_timestamps("2026-07")
    series = pd.Series(pd.to_datetime(["2026-07-15T12:00:00", "2026-08-01T00:00:00"]))
    in_range = (series >= start_ts) & (series <= end_ts)
    assert in_range.tolist() == [True, False]
