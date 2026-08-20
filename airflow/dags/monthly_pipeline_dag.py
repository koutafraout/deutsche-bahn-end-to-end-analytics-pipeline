"""Monthly pipeline: land HF Bronze data, validate it, build the warehouse.

resolve_month >> land_monthly_bronze >> great_expectation_validation >>
    copy_to_redshift >> dbt_staging >> dbt_intermediate >> dbt_marts >>
    pipeline_complete

The dbt build is split by layer (staging/intermediate/marts, matching the
`models.db_ops_analytics.*` groups in dbt_project.yml) into separate
Airflow tasks purely for visibility in the UI — `dbt build` alone would
already run them in this order via `ref()` resolution, but a failure
midway now shows which layer broke instead of just "dbt_build failed".

Runs automatically at 06:00 UTC on the 2nd of every month
(`schedule="0 6 2 * *"`), loading the month that just completed — the HF
monthly Parquet release for month M only exists once M is fully over, so
a scheduled run firing on the 2nd loads `data_interval_start`, which for
this schedule is the previous calendar month (e.g. a run firing 2026-03-02
loads 2026-02).

A manual trigger has no such schedule to infer from, so it must specify
the month explicitly — Trigger DAG w/ config `{"month": "YYYY-MM"}` — the
same way `python -m ingestion.monthly_load.main --month YYYY-MM` requires
it on the CLI. `resolve_month` enforces this: it raises (failing the run
immediately, before anything is loaded) if triggered manually without a
`month`, rather than silently guessing one.

`copy_to_redshift` loads that month's validated Bronze object into
`db_monthly.raw_observations` — dbt staging reads from there, not S3
directly. Skips the reload (no Redshift compute spent) if that month is
already loaded — pass `--force` on the CLI directly to reload after a
Bronze re-land. Uses a separate REDSHIFT_LOADER_USER with write access,
since dbt's REDSHIFT_USER is deliberately read-only on `db_monthly`
(docs/runbook.md §1.4). See warehouse/redshift_load/load_monthly.py.

`pipeline_complete` logs a one-glance run summary (month processed, Bronze
row count). The row count comes from `great_expectation_validation`, not
`land_monthly_bronze` — landing is an unchanged byte copy to S3 (CLAUDE.md
§7: Bronze is immutable), so it never parses the Parquet; validation is
the step that already opens the file and knows the row count.

Scoped to the monthly leg only — api_pull/spark_parse/monthly_reconcile
DAGs are added once ingestion/api_poller and batch/spark_parse_api exist.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.types import DagRunType

with DAG(
    dag_id="db_monthly_pipeline",
    description="Land monthly HF Bronze data, validate it, build the dbt warehouse.",
    schedule="0 6 2 * *",  # 06:00 UTC on the 2nd of every month
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"month": Param(default=None, type=["string", "null"], pattern=r"^\d{4}-\d{2}$")},
    tags=["monthly", "bronze", "dbt"],
) as dag:

    @task
    def resolve_month(**context) -> str:
        dag_run = context["dag_run"]
        month = (dag_run.conf or {}).get("month") if dag_run.conf else None

        if dag_run.run_type == DagRunType.MANUAL:
            if not month:
                raise ValueError(
                    "Manual trigger requires an explicit month — pass "
                    '{"month": "YYYY-MM"} via Trigger DAG w/ config, the '
                    "same way `python -m ingestion.monthly_load.main "
                    "--month YYYY-MM` requires it on the CLI."
                )
            return month

        # Scheduled (or backfill) run: default to the month that just
        # completed, unless a specific month was explicitly requested.
        return month or context["data_interval_start"].strftime("%Y-%m")

    month = resolve_month()

    land_monthly_bronze = BashOperator(
        task_id="land_monthly_bronze",
        bash_command="python -m ingestion.monthly_load.main --month {{ ti.xcom_pull(task_ids='resolve_month') }}",
        cwd="/opt/airflow/project",
        retries=1,
        retry_delay=pendulum.duration(minutes=5),
    )

    great_expectation_validation = BashOperator(
        task_id="great_expectation_validation",
        bash_command="python -m quality.bronze_monthly.validate --month {{ ti.xcom_pull(task_ids='resolve_month') }}",
        cwd="/opt/airflow/project",
        retries=1,
        retry_delay=pendulum.duration(minutes=5),
    )

    copy_to_redshift = BashOperator(
        task_id="copy_to_redshift",
        bash_command="python -m warehouse.redshift_load.main --month {{ ti.xcom_pull(task_ids='resolve_month') }}",
        cwd="/opt/airflow/project",
        retries=1,
        retry_delay=pendulum.duration(minutes=5),
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="dbt build --target redshift --select staging",
        cwd="/opt/airflow/project/warehouse/dbt",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command="dbt build --target redshift --select intermediate",
        cwd="/opt/airflow/project/warehouse/dbt",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="dbt build --target redshift --select marts",
        cwd="/opt/airflow/project/warehouse/dbt",
    )

    @task
    def pipeline_complete(month: str, bronze_row_count: str) -> None:
        print("=" * 60)
        print("MONTHLY PIPELINE COMPLETE")
        print(f"  Month processed:   {month}")
        print(f"  Bronze row count:  {bronze_row_count}")
        print("  dbt layers built:  staging -> intermediate -> marts")
        print("=" * 60)

    (
        month
        >> land_monthly_bronze
        >> great_expectation_validation
        >> copy_to_redshift
        >> dbt_staging
        >> dbt_intermediate
        >> dbt_marts
        >> pipeline_complete(
            month=month,
            bronze_row_count=great_expectation_validation.output,
        )
    )
