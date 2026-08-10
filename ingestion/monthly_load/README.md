# monthly_load

Lands the Hugging Face `piebro/deutsche-bahn-data` monthly Parquet release
(`monthly_processed_data/data-YYYY-MM.parquet`) unchanged into Bronze on S3
at `s3://<bucket>/bronze/monthly-raw/year=YYYY/month=MM/`. Idempotent — skips a month
that's already landed unless `--force` is passed. No transformation, dedup,
or business logic; that belongs downstream in dbt staging/intermediate per
`docs/PROJECT_PLAN.md` §4.

`ingest_monthly.py` holds the library logic (`load_month` and helpers) with
no CLI of its own. `main.py` is the CLI entry point. Each `--month` value is
either a single month (`YYYY-MM`) or an inclusive range within one year
(`YYYY-MM-MM`); repeat `--month` to mix specific months and/or ranges for a
non-successive collection.

```bash
# single month
python -m ingestion.monthly_load.main --month 2026-07

# successive range: Jan-Jul 2026
python -m ingestion.monthly_load.main --month 2026-01-07

# non-successive: Jan-Mar plus Jul
python -m ingestion.monthly_load.main --month 2026-01-03 --month 2026-07

python -m ingestion.monthly_load.main --month 2026-07 --force
```

Config via env vars (see `.env.example`): `HF_REPO_ID`, `HF_MONTHLY_SUBFOLDER`,
`S3_BRONZE_BUCKET`, `S3_BRONZE_PREFIX`, plus standard AWS credential env vars
consumed by `boto3`.
