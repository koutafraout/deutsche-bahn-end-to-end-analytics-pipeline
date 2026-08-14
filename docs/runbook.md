# Runbook

Operational manual for running the pipeline, handling failures, and
managing the environment. Reflects what is actually implemented today
(the monthly Bronze → Redshift → dbt staging path); sections for
not-yet-built components are marked accordingly.

## 1. Environment setup

### 1.1 Prerequisites

- Python 3.11
- [`uv`](https://docs.astral.sh/uv/) for dependency management and running
  commands
- Docker + Docker Compose (for local Spark/Jupyter profiling environment)
- AWS CLI configured, or an IAM role, with access to the project's S3
  bucket and Redshift Serverless workgroup
- `psql` (optional, for direct Redshift debugging)

### 1.2 Install dependencies

```bash
uv sync --extra ingestion --extra quality --extra warehouse --extra dev
```

This creates/updates `.venv` and installs the project in editable mode
with the requested optional dependency groups (see `pyproject.toml`).
Run any project command through `uv run ...` (shown below) so it uses
that environment without needing to activate it manually.

### 1.3 Configure environment variables

Copy the template and fill in real values — **never commit `.env`**:

```bash
cp .env.example .env
```

Required for ingestion:

```text
HF_REPO_ID=piebro/deutsche-bahn-data
HF_MONTHLY_SUBFOLDER=monthly_processed_data
S3_BRONZE_BUCKET=<your bucket>
S3_BRONZE_PREFIX=bronze/monthly-raw
AWS_ACCESS_KEY_ID=...        # or leave blank to use ~/.aws/credentials or an IAM role
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-central-1
```

Required for dbt:

```text
REDSHIFT_HOST=<workgroup endpoint>
REDSHIFT_PORT=5439
REDSHIFT_DBNAME=dev
REDSHIFT_USER=<dbt service user>
REDSHIFT_PASSWORD=...
REDSHIFT_SCHEMA=dbt_dev
```

### 1.4 AWS resources this project depends on

These are provisioned manually (console/CLI), not via IaC, per the
project's current scope:

- An S3 bucket for Bronze storage.
- A Redshift Serverless namespace + workgroup, publicly accessible, with
  inbound access on port 5439 restricted to your IP.
- A dedicated, non-superuser Redshift database user (e.g. `dbt_user`) with:
  - `USAGE` on the raw landing schema (`db_monthly`) and `SELECT` on
    `db_monthly.raw_observations`
  - ownership of the dbt schema (`dbt_dev`)
  - `TEMP` and `CREATE` grants at the **database** level (required by
    dbt-redshift's create-and-swap materialization, and by the
    `CREATE SCHEMA IF NOT EXISTS` dbt issues on every run)

If a grant seems to be missing, re-run the specific `GRANT` above rather
than widening the user's privileges.

## 2. Running the pipeline

### 2.1 Land monthly data in Bronze

```bash
# single month
uv run python -m ingestion.monthly_load.main --month 2026-07

# inclusive range within one year
uv run python -m ingestion.monthly_load.main --month 2026-01-07

# mixed: repeat --month for non-successive months/ranges
uv run python -m ingestion.monthly_load.main --month 2026-01-03 --month 2026-07

# force re-download and overwrite an existing Bronze object
uv run python -m ingestion.monthly_load.main --month 2026-07 --force
```

By default, a month already present in S3 is skipped (idempotent). Use
`--force` only when the source file itself changed or you suspect the
landed object is corrupt.

### 2.2 Validate Bronze (Great Expectations gate)

```bash
uv run python -m quality.bronze_monthly.validate --month 2026-07
```

Exits non-zero if any month fails. Checks, in order: valid/readable
Parquet, non-empty, exact 17-column schema, not-null on the columns
confirmed always-populated by profiling, and `time` values falling inside
their own source month.

**A failed validation does not delete or modify the Bronze object.** The
month is left in S3 for investigation and can be re-validated once
resolved.

### 2.3 Load Bronze into Redshift

Not yet automated — currently a manual step. From a Redshift SQL client
connected as an admin/owner of `db_monthly`:

```sql
COPY db_monthly.raw_observations
FROM 's3://<bucket>/bronze/monthly-raw/year=<YYYY>/month=<MM>/data-<YYYY>-<MM>.parquet'
IAM_ROLE '<redshift-s3-read-role-arn>'
FORMAT AS PARQUET;
```

Only run this **after** step 2.2 passes for that month. `raw_observations`
declares the five timestamp columns as `BIGINT` (not `TIMESTAMP`) so the
Parquet nanosecond-epoch values land unchanged — decoding happens in dbt
staging, not at `COPY` time.

### 2.4 Build the warehouse (dbt)

```bash
cd warehouse/dbt
uv run dbt debug            # verify the Redshift connection first
uv run dbt build            # run + test all models
uv run dbt build --select stg_monthly_observations   # a single model
```

Currently implemented: `stg_monthly_observations` (rename/cast only, plus
`not_null` and `unique` schema tests). Intermediate and mart models are
not yet built — see `architecture.md` §3 for the target shape.

### 2.5 Local Spark / profiling environment

For development-time profiling only — not part of the production
pipeline:

```bash
docker compose up -d
```

This brings up a Spark master/worker/client and a JupyterLab instance
(bound to `127.0.0.1:8888`, no token). Profiling notebooks live under
`notebooks/data-assessment/`.

## 3. Handling failures

| Failure | Where it surfaces | What to do |
|---|---|---|
| HF download fails / times out | `ingestion.monthly_load.main` raises | Re-run the same command — it's idempotent (skips already-landed months) once the download succeeds. Check `HF_REPO_ID`/`HF_MONTHLY_SUBFOLDER` if the file genuinely doesn't exist upstream yet. |
| S3 upload fails | `ingestion.monthly_load.main` raises (boto3 exception) | Check AWS credentials and bucket permissions; re-run — no partial state is left because the object is only considered "landed" once the upload completes. |
| Great Expectations validation fails | `quality.bronze_monthly.validate` logs `FAIL` and exits non-zero | **Do not COPY that month into Redshift.** Read the logged reason (missing/corrupt Parquet, schema mismatch, unexpected nulls, or `time` values outside the source month). Investigate the source file; if it's a genuine upstream schema change, this needs a code change to `EXPECTED_COLUMNS`/`NOT_NULL_COLUMNS`, not a bypass. The Bronze object is preserved either way, so reprocessing after a fix doesn't require re-downloading. |
| Redshift `COPY` fails | SQL client error | Common causes: wrong S3 path/partition, IAM role missing S3 read access, or a schema mismatch between the Parquet file and `raw_observations`'s DDL. Never widen the table's column types to "make it work" without confirming against the actual 17-column schema first. |
| `permission denied for schema db_monthly` | `dbt debug` / `dbt build` | `GRANT USAGE ON SCHEMA db_monthly TO dbt_user;` |
| `permission denied for relation raw_observations` | `dbt build` | `GRANT SELECT ON TABLE db_monthly.raw_observations TO dbt_user;` — note this grant does **not** survive a drop/recreate of the table; re-run it after any DDL change to `raw_observations`. |
| `permission denied for database dev` | `dbt build`/`dbt run` | Needs both `GRANT TEMP ON DATABASE dev TO dbt_user;` and `GRANT CREATE ON DATABASE dev TO dbt_user;` — `SELECT` and schema ownership alone are not sufficient. |
| dbt builds into the wrong schema | Silent — check `information_schema` | Confirm `REDSHIFT_SCHEMA=dbt_dev` in `.env` (not left pointing at the raw landing schema). dbt's schema-naming macro concatenates the profile schema with each model's `+schema` config, so a wrong base value silently creates a stray schema instead of erroring. |
| `nc`/connection to Redshift times out | Any dbt or `psql` command | Check, in order: workgroup `publiclyAccessible = true`, security group allows your current public IP on port 5439, the workgroup's subnets are public, and the route table has a route to an Internet Gateway. |
| dbt schema test fails (`not_null`/`unique`) | `dbt build` output | Treat as a real data-quality signal, not noise — investigate the specific row(s) before deciding whether to adjust the test or fix the model. Do not silently loosen a test to make the build pass. |

## 4. Managing the environment

- **Idempotency:** the monthly loader and Bronze validator are both
  safe to re-run — the loader skips existing S3 objects unless `--force`
  is passed, and validation is read-only.
- **Cost control:** Redshift Serverless auto-pauses when idle — avoid
  scripts that poll it continuously, which would keep it resumed. Watch
  the AWS Budget alert configured on the account.
- **Secrets:** only `.env.example` (placeholder values) is committed.
  Real credentials live in `.env` (git-ignored) or environment variables
  injected by the shell/orchestrator — never hardcoded.
- **Reprocessing a month:** re-run the loader with `--force` only if the
  upstream Parquet file itself changed; otherwise re-running without
  `--force` is a no-op. Re-validate (§2.2) and re-`COPY` (§2.3) before
  rebuilding dbt models on top of it.
- **Tests:**

  ```bash
  uv run pytest tests/unit
  ```

  Covers the monthly loader's month-spec parsing/S3-key logic and the
  Bronze expectation-suite construction. No integration tests exist yet
  (would require Docker/network fixtures against S3 and Redshift).

## 5. Not yet implemented (do not assume these exist)

- Raw Deutsche Bahn API polling and Bronze landing for the `plan`/`fchg`
  path.
- The structural parser (Spark) that conforms the API payloads to the
  canonical schema.
- dbt intermediate models (union, dedup, delay recompute, dimensions) and
  Gold marts.
- Airflow orchestration — all steps above are run manually from the CLI.
- The FastAPI reporting service and Streamlit dashboard.
- Automated `COPY` from S3 to Redshift (currently a manual SQL step, §2.3).
- Monthly reconciliation against the official Hugging Face release.
