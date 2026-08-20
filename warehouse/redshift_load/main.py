"""CLI entry point for loading one month's Bronze data into Redshift.

Delegates to `load_monthly.load_month_to_redshift`. Prints
`ROW_COUNT=<n>` as the last stdout line on success, matching the
convention used by `quality.bronze_monthly.validate` — so a caller (e.g.
an Airflow BashOperator's default XCom push) can capture the row count
without any of the logging noise above it.
"""

from __future__ import annotations

import argparse
import logging

from warehouse.redshift_load.load_monthly import load_month_to_redshift

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month",
        required=True,
        help="Month to load, format YYYY-MM.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reload even if this month is already loaded (deletes and re-copies).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    row_count = load_month_to_redshift(args.month, force=args.force)
    print(f"ROW_COUNT={row_count}")


if __name__ == "__main__":
    main()
