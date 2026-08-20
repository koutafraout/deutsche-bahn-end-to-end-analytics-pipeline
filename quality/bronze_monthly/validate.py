"""Bronze quality gate for landed monthly Parquet files.

Checks, in order:
1. The object is readable as valid Parquet (payload arrived, not corrupt).
2. Non-empty (row count > 0).
3. Schema matches the documented 17-column contract, in order.
4. The columns confirmed fully non-null in profiling are still non-null.
5. `time` values fall inside their own source month (freshness/coverage).

Steps 1-3 read only Parquet metadata (cheap). Steps 4-5 load just the
non-null column set via pandas, not the full 17 columns, to keep memory
bounded on multi-hundred-MB monthly files. Failures are logged, not
silently dropped, per CLAUDE.md §7 — a failing month should block staging,
not disappear.
"""

from __future__ import annotations

import argparse
import logging
import sys

import great_expectations as gx
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from ingestion.monthly_load.ingest_monthly import (
    S3_BRONZE_BUCKET,
    S3_BRONZE_PREFIX,
    build_s3_key,
)
from ingestion.monthly_load.ingest_monthly import validate_month as validate_month_format
from quality.bronze_monthly.expectations import (
    EXPECTED_COLUMNS,
    NOT_NULL_COLUMNS,
    build_not_null_suite,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def validate_month(month: str, *, bucket: str = S3_BRONZE_BUCKET) -> tuple[bool, int]:
    """Returns (passed, row_count). row_count is 0 if the object couldn't be read."""
    validate_month_format(month)
    key = build_s3_key(month)
    s3_path = f"{bucket}/{key}"
    fs = pafs.S3FileSystem()

    logger.info("checking s3://%s", s3_path)
    try:
        parquet_file = pq.ParquetFile(s3_path, filesystem=fs)
    except Exception:
        logger.exception("month=%s FAIL: object missing or not valid Parquet", month)
        return False, 0

    row_count = parquet_file.metadata.num_rows
    if row_count < 1:
        logger.error("month=%s FAIL: row count is %d, expected >= 1", month, row_count)
        return False, row_count
    logger.info("month=%s row_count=%d", month, row_count)

    actual_columns = parquet_file.schema_arrow.names
    if actual_columns != EXPECTED_COLUMNS:
        logger.error(
            "month=%s FAIL: schema mismatch. expected=%s actual=%s",
            month,
            EXPECTED_COLUMNS,
            actual_columns,
        )
        return False, row_count
    logger.info("month=%s schema OK (%d columns)", month, len(actual_columns))

    df = pd.read_parquet(s3_path, columns=NOT_NULL_COLUMNS, filesystem=fs)

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(f"bronze_monthly_{month}")
    asset = datasource.add_dataframe_asset("data")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = build_not_null_suite(month)
    result = batch.validate(suite)

    if not result.success:
        logger.error("month=%s FAIL: expectation suite failed\n%s", month, result)
        return False, row_count

    logger.info("month=%s PASS: all Bronze quality checks green", month)
    return True, row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month",
        action="append",
        dest="months",
        required=True,
        help="Month to validate, format YYYY-MM. Repeat --month for multiple.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    all_passed = True
    last_row_count = 0
    for month in args.months:
        passed, row_count = validate_month(month)
        all_passed = all_passed and passed
        last_row_count = row_count

    if not all_passed:
        sys.exit(1)

    # Last stdout line only, so BashOperator's default XCom push (which
    # captures the last line of stdout) carries the row count without
    # any of the logging noise above. Only meaningful for a single month.
    if len(args.months) == 1:
        print(f"ROW_COUNT={last_row_count}")


if __name__ == "__main__":
    main()
