"""Load one month of Bronze Parquet from S3 into Redshift's raw landing table.

Automates what was, until now, a manual SQL step (see docs/runbook.md
§2.3): COPY the month's Bronze object into `db_monthly.raw_observations`,
then derive `source_month` from the `"time"` column the same way it was
validated by hand. Skips a month already loaded, unless `force=True` —
same idempotency pattern as `ingestion.monthly_load.ingest_monthly.load_month`
— to avoid spending Redshift Serverless compute (and keeping it out of
auto-pause) reloading a month whose Bronze source hasn't changed. Library
only — see `main.py` for the CLI.

Needs a Redshift login with write access to `db_monthly`
(REDSHIFT_LOADER_USER/PASSWORD) — deliberately separate from dbt's
REDSHIFT_USER, which only has read-only SELECT there (docs/runbook.md
§1.4).
"""

from __future__ import annotations

import logging
import os

import redshift_connector

from ingestion.monthly_load.ingest_monthly import build_s3_key, validate_month

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DBNAME = os.environ.get("REDSHIFT_DBNAME", "dev")
REDSHIFT_LOADER_USER = os.environ.get("REDSHIFT_LOADER_USER")
REDSHIFT_LOADER_PASSWORD = os.environ.get("REDSHIFT_LOADER_PASSWORD")
REDSHIFT_S3_IAM_ROLE = os.environ.get("REDSHIFT_S3_IAM_ROLE")
S3_BRONZE_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "deutsche-bahn-delay-data-lake")

# Two separate statements, executed individually — redshift_connector uses
# server-side prepared statements per execute() call, which Redshift's
# protocol rejects if given more than one command at once ("cannot insert
# multiple commands into a prepared statement").
CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS db_monthly;"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS db_monthly.raw_observations (
    station_name               VARCHAR(256),
    xml_station_name           VARCHAR(256),
    eva                        VARCHAR(20),
    train_number                VARCHAR(50),
    line_number                 VARCHAR(100),
    final_destination_station  VARCHAR(256),
    delay_in_min                INTEGER,
    "time"                       BIGINT,
    is_canceled                  BOOLEAN,
    train_type                   VARCHAR(50),
    train_line_ride_id          VARCHAR(100),
    train_line_station_num     INTEGER,
    arrival_planned_time        BIGINT,
    arrival_change_time         BIGINT,
    departure_planned_time      BIGINT,
    departure_change_time       BIGINT,
    id                           VARCHAR(256),
    source_month                 VARCHAR(7)
);
"""

# Explicit column list, excluding source_month — the Parquet file only
# has these 17 columns; source_month is derived after COPY, below.
_COPY_COLUMNS = (
    "station_name, xml_station_name, eva, train_number, line_number, "
    "final_destination_station, delay_in_min, \"time\", is_canceled, "
    "train_type, train_line_ride_id, train_line_station_num, "
    "arrival_planned_time, arrival_change_time, departure_planned_time, "
    "departure_change_time, id"
)

# Only touches rows just landed by COPY (source_month is NULL until this
# runs) — ties source_month to the actual observation time rather than the
# intended partition, matching the profiling-validated manual process.
UPDATE_SOURCE_MONTH_SQL = """
UPDATE db_monthly.raw_observations
SET source_month = TO_CHAR(TIMESTAMP 'epoch' + ("time" / 1000000000.0) * INTERVAL '1 second', 'YYYY-MM')
WHERE source_month IS NULL;
"""


def build_delete_sql(month: str) -> str:
    validate_month(month)
    return f"DELETE FROM db_monthly.raw_observations WHERE source_month = '{month}'"


def build_copy_sql(month: str, *, bucket: str = S3_BRONZE_BUCKET, iam_role: str) -> str:
    validate_month(month)
    key = build_s3_key(month)
    return (
        f"COPY db_monthly.raw_observations ({_COPY_COLUMNS})\n"
        f"FROM 's3://{bucket}/{key}'\n"
        f"IAM_ROLE '{iam_role}'\n"
        f"FORMAT AS PARQUET"
    )


def load_month_to_redshift(month: str, *, connection=None, force: bool = False) -> int:
    """Loads one month into db_monthly.raw_observations. Returns its row count.

    Skips (no DELETE, no COPY, no Redshift compute spent) if this month
    already has rows and `force` is False. Pass `force=True` only when
    the Bronze source for this month was re-landed (e.g.
    `ingestion.monthly_load.main --month ... --force`) and Redshift needs
    to reflect that change. Runs as one transaction — a failure partway
    through rolls back rather than leaving the month half-loaded.
    """
    validate_month(month)
    if not REDSHIFT_S3_IAM_ROLE:
        raise ValueError("REDSHIFT_S3_IAM_ROLE is not set")

    own_connection = connection is None
    conn = connection or redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DBNAME,
        user=REDSHIFT_LOADER_USER,
        password=REDSHIFT_LOADER_PASSWORD,
    )

    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_SCHEMA_SQL)
        cursor.execute(CREATE_TABLE_SQL)

        cursor.execute(
            "SELECT COUNT(*) FROM db_monthly.raw_observations WHERE source_month = %s",
            (month,),
        )
        existing_row_count = cursor.fetchone()[0]

        if existing_row_count and not force:
            conn.commit()
            logger.info(
                "month=%s skip: already loaded (row_count=%d); pass force=True to reload",
                month,
                existing_row_count,
            )
            return existing_row_count

        if existing_row_count:
            delete_sql = build_delete_sql(month)
            logger.info("month=%s force reload: deleting existing rows first", month)
            cursor.execute(delete_sql)

        copy_sql = build_copy_sql(month, iam_role=REDSHIFT_S3_IAM_ROLE)
        logger.info("month=%s copying s3://%s/%s into db_monthly.raw_observations", month, S3_BRONZE_BUCKET, build_s3_key(month))
        cursor.execute(copy_sql)

        cursor.execute(UPDATE_SOURCE_MONTH_SQL)

        cursor.execute(
            "SELECT COUNT(*) FROM db_monthly.raw_observations WHERE source_month = %s",
            (month,),
        )
        row_count = cursor.fetchone()[0]

        conn.commit()
        logger.info("month=%s loaded row_count=%d", month, row_count)
        return row_count
    except Exception:
        conn.rollback()
        logger.exception("month=%s FAIL: rolled back", month)
        raise
    finally:
        if own_connection:
            conn.close()
