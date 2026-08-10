"""CLI entry point for landing monthly Bronze data.

Lands one or several months into Bronze on S3. Each --month value is either
a single month (2026-07) or an inclusive range within one year
(2026-01-07 -> months 01 through 07). Repeat --month to mix specific months
and/or ranges, e.g. --month 2026-01-03 --month 2026-07 for Jan-Mar plus Jul.
Delegates the actual download/upload to `ingest_monthly.load_month`.
"""

from __future__ import annotations

import argparse
import logging

from ingestion.monthly_load.ingest_monthly import load_month, parse_month_specs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month",
        action="append",
        dest="months",
        required=True,
        help=(
            "Month to land: a single YYYY-MM, or an inclusive range "
            "YYYY-MM-MM (e.g. 2026-01-07 for Jan-Jul 2026). "
            "Repeat --month for a non-successive collection."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and overwrite even if the S3 object already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    for month in parse_month_specs(args.months):
        load_month(month, force=args.force)


if __name__ == "__main__":
    main()
