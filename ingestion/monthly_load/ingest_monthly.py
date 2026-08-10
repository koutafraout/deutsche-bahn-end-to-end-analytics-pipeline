"""Land one month of the Hugging Face Deutsche Bahn delay release in Bronze.

Downloads a single `data-YYYY-MM.parquet` file from the
`piebro/deutsche-bahn-data` dataset's `monthly_processed_data/` folder and
uploads it unchanged to
`s3://<bucket>/bronze/monthly-raw/year=YYYY/month=MM/`. No transformation,
dedup, or schema logic here — Bronze is immutable and this loader's only job
is to move bytes. Library only — see `main.py` for the CLI.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import boto3
from huggingface_hub import hf_hub_download

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
MONTH_RANGE_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|1[0-2])$")

HF_REPO_ID = os.environ.get("HF_REPO_ID", "piebro/deutsche-bahn-data")
HF_MONTHLY_SUBFOLDER = os.environ.get("HF_MONTHLY_SUBFOLDER", "monthly_processed_data")
S3_BRONZE_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "deutsche-bahn-delay-data-lake")
S3_BRONZE_PREFIX = os.environ.get("S3_BRONZE_PREFIX", "bronze/monthly-raw")


def validate_month(month: str) -> str:
    if not MONTH_RE.match(month):
        raise ValueError(f"month must be in YYYY-MM format, got {month!r}")
    return month


def parse_month_spec(spec: str) -> list[str]:
    """Expand one --month value into a list of YYYY-MM months.

    Accepts either a single month (`2026-07`) or an inclusive range within
    one year (`2026-01-07` -> 2026-01, 2026-02, ..., 2026-07). Use a range
    only for successive months; repeat --month for non-successive ones.
    """
    range_match = MONTH_RANGE_RE.match(spec)
    if range_match:
        year, start, end = range_match.groups()
        start, end = int(start), int(end)
        if start > end:
            raise ValueError(
                f"range start month must be <= end month, got {spec!r}"
            )
        return [f"{year}-{month:02d}" for month in range(start, end + 1)]

    return [validate_month(spec)]


def parse_month_specs(specs: list[str]) -> list[str]:
    """Expand and flatten multiple --month values, deduped, order preserved."""
    months: dict[str, None] = {}
    for spec in specs:
        for month in parse_month_spec(spec):
            months[month] = None
    return list(months)


def source_filename(month: str) -> str:
    return f"data-{month}.parquet"


def build_s3_key(month: str, prefix: str = S3_BRONZE_PREFIX) -> str:
    validate_month(month)
    year, month_num = month.split("-")
    return f"{prefix}/year={year}/month={month_num}/{source_filename(month)}"


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def load_month(month: str, *, force: bool = False, s3_client=None) -> None:
    validate_month(month)
    s3_client = s3_client or boto3.client("s3")
    key = build_s3_key(month)

    if not force and object_exists(s3_client, S3_BRONZE_BUCKET, key):
        logger.info("skip %s: s3://%s/%s already exists", month, S3_BRONZE_BUCKET, key)
        return

    remote_path = f"{HF_MONTHLY_SUBFOLDER}/{source_filename(month)}"
    logger.info("downloading %s from %s", remote_path, HF_REPO_ID)
    local_path = hf_hub_download(
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        filename=remote_path,
    )

    logger.info("uploading %s to s3://%s/%s", local_path, S3_BRONZE_BUCKET, key)
    s3_client.upload_file(str(Path(local_path)), S3_BRONZE_BUCKET, key)
    logger.info("landed %s", month)
