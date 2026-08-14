#  Deutsche Bahn Operations Analytics Pipeline

A scheduled, incremental-batch data platform that turns Deutsche Bahn historical and API delay data into tested, modeled station/line/time performance datasets for daily and monthly reporting.

> **Status: v0.3 — Monthly pipeline landed through dbt staging.**
> Hugging Face → S3 Bronze → Great Expectations → Redshift Serverless →
> dbt staging is implemented and validated end to end for January–July
> 2026 (101,702,091 rows). See [Status](#status) below for what's next.

---

## Problem statement

Deutsche Bahn publishes both a monthly historical delay archive (Hugging
Face Parquet release) and a raw real-time API (`plan`/`fchg` payloads), but
neither is analysis-ready on its own.

The historical release behaves as a **poll-and-snapshot dataset**: the same
train-stop can be captured repeatedly while its delay, timestamps, and
cancellation state evolve. Therefore, the raw source grain is not the final
business reporting grain.

The project builds a reproducible data pipeline that transforms those raw
observations into validated, deduplicated, modeled, and queryable railway
performance datasets.

The final platform is intended to support consistent delay reporting by:

- station;
- railway line / train category;
- day and month;
- delay and on-time performance;
- cancellation behavior.

---

## Stakeholders

- **Transport planners** — monthly reporting to identify long-term delay
  patterns, compare stations and lines, and support planning.
- **Railway operations & performance teams** — daily reporting to monitor
  recent delays/cancellations and investigate operational problems.
- **Railway performance analysts** — both views, for operational
  monitoring and trend analysis.

---

## Data sources

- **Monthly historical archive** — [`piebro/deutsche-bahn-data`](https://huggingface.co/datasets/piebro/deutsche-bahn-data)
  on Hugging Face, a monthly Parquet release, 17 columns, CC BY 4.0,
  Germany-wide, available from July 2024 onward. Currently landed and
  loaded for January–July 2026 (101,702,091 rows). This is the pipeline's
  historical reporting source and future reconciliation reference.
- **Raw Deutsche Bahn API** (`plan` + `fchg`, ~6-hourly) — planned, not yet
  implemented.

Schema details, the source-grain finding (one row = one snapshot, not one
final state), and the full ingestion pipeline are documented in
[docs/architecture.md](docs/architecture.md).

---

## Status

| Component | Status |
|---|---|
| Monthly ingestion → S3 Bronze → Great Expectations → Redshift | ✅ Implemented |
| dbt staging (`stg_monthly_observations`) | ✅ Implemented |
| dbt intermediate + Gold marts | 🔜 Next |
| Raw API ingestion, structural parser | ⏳ Planned |
| Airflow orchestration | ⏳ Planned |
| FastAPI + Streamlit dashboard | ⏳ Planned |
| ML delay prediction | ⏳ Advanced / stretch |

See [docs/architecture.md](docs/architecture.md) for the implemented vs.
target architecture, and [docs/adr/](docs/adr/) for why each tool was
chosen.

---

## Tech stack

| Area | Tool |
|---|---|
| Ingestion | Python 3.11, boto3, huggingface_hub |
| Bronze storage | AWS S3 |
| Bronze validation | Great Expectations |
| Warehouse | Redshift Serverless |
| Transformations | dbt (`dbt-redshift`) |
| Local profiling | PySpark, JupyterLab (Docker) |
| Dependency management | uv |
| Testing | pytest |

Planned, not yet implemented: Airflow (orchestration), FastAPI (reporting
API), Streamlit (dashboard). See [docs/architecture.md](docs/architecture.md)
for the full target architecture.

---

## Setup

**Prerequisites:** Python 3.11, [`uv`](https://docs.astral.sh/uv/), Docker
+ Docker Compose, an AWS account with an S3 bucket and a Redshift
Serverless workgroup reachable from your machine.

1. Install dependencies:

   ```bash
   uv sync --extra ingestion --extra quality --extra warehouse --extra dev
   ```

2. Configure environment variables:

   ```bash
   cp .env.example .env
   # fill in your S3 bucket, AWS credentials, and Redshift connection details
   ```

3. Land a month of historical data in Bronze:

   ```bash
   uv run python -m ingestion.monthly_load.main --month 2026-07
   ```

4. Validate it:

   ```bash
   uv run python -m quality.bronze_monthly.validate --month 2026-07
   ```

5. Load it into Redshift and build the warehouse:

   ```bash
   # COPY step is currently manual — see docs/runbook.md §2.3
   cd warehouse/dbt && uv run dbt build
   ```

6. (Optional) Start the local Spark/Jupyter profiling environment:

   ```bash
   docker compose up -d
   ```

   Historical data assessment notebooks live under
   `notebooks/data-assessment/monthly_catalog_profiling/`.

Full operational detail — failure handling, required Redshift grants,
troubleshooting — is in [docs/runbook.md](docs/runbook.md).

Production orchestration with Airflow is not yet implemented; the current
pipeline components are being validated individually before orchestration is
added.

---

## Documentation

- [docs/architecture.md](docs/architecture.md) — data architecture, pipeline
  diagrams, and tool choices
- [docs/runbook.md](docs/runbook.md) — operational manual: running the
  pipeline, handling failures, managing the environment
- [docs/adr/](docs/adr/) — architecture decision records

This README evolves with the actual implementation rather than documenting
components that have not yet been built.