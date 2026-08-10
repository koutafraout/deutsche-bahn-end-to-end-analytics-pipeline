import pytest

from ingestion.monthly_load.ingest_monthly import (
    build_s3_key,
    parse_month_spec,
    parse_month_specs,
    source_filename,
    validate_month,
)


def test_validate_month_accepts_valid_format():
    assert validate_month("2026-07") == "2026-07"


@pytest.mark.parametrize("month", ["2026-13", "26-07", "2026-7", "not-a-month", ""])
def test_validate_month_rejects_invalid_format(month):
    with pytest.raises(ValueError):
        validate_month(month)


def test_source_filename():
    assert source_filename("2026-07") == "data-2026-07.parquet"


def test_build_s3_key_default_prefix():
    assert (
        build_s3_key("2026-07")
        == "bronze/monthly-raw/year=2026/month=07/data-2026-07.parquet"
    )


def test_build_s3_key_custom_prefix():
    assert (
        build_s3_key("2026-07", prefix="bronze/monthly-raw")
        == "bronze/monthly-raw/year=2026/month=07/data-2026-07.parquet"
    )


def test_parse_month_spec_single_month():
    assert parse_month_spec("2026-07") == ["2026-07"]


def test_parse_month_spec_range():
    assert parse_month_spec("2026-01-07") == [
        "2026-01",
        "2026-02",
        "2026-03",
        "2026-04",
        "2026-05",
        "2026-06",
        "2026-07",
    ]


def test_parse_month_spec_range_single_month_start_equals_end():
    assert parse_month_spec("2026-05-05") == ["2026-05"]


def test_parse_month_spec_range_rejects_start_after_end():
    with pytest.raises(ValueError):
        parse_month_spec("2026-07-01")


def test_parse_month_spec_rejects_invalid():
    with pytest.raises(ValueError):
        parse_month_spec("not-a-spec")


def test_parse_month_specs_mixes_ranges_and_singles():
    assert parse_month_specs(["2026-01-03", "2026-07"]) == [
        "2026-01",
        "2026-02",
        "2026-03",
        "2026-07",
    ]


def test_parse_month_specs_dedupes_preserving_order():
    assert parse_month_specs(["2026-01-03", "2026-02"]) == [
        "2026-01",
        "2026-02",
        "2026-03",
    ]
