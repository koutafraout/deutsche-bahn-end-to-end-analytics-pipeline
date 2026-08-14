#  Deutsche Bahn Operations Analytics Pipeline

A scheduled, incremental-batch data platform that turns Deutsche Bahn historical and API delay data into tested, modeled station/line/time performance datasets for daily and monthly reporting.

> **Status: v0.2 — Monthly Bronze Pipeline & Warehouse Landing.**
>
> The historical data assessment is complete, including the data catalog and
> January–July 2026 profiling. The first production pipeline path is now
> implemented:
>
> **Hugging Face → S3 Bronze → Great Expectations → Redshift Serverless**
>
> The next milestone is the dbt modeling layer:
> staging → intermediate → Gold reporting marts.

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

### Transport planners

Use monthly reporting to:

- identify long-term delay patterns;
- compare stations and railway lines;
- support timetable, capacity, and infrastructure planning.

### Railway operations and performance teams

Use daily reporting to:

- monitor recent delays and cancellations;
- investigate operational problems;
- identify issues that persist across several days.

### Railway performance analysts

Use both daily and monthly views for operational monitoring and trend analysis.

---

## Data sources

The project uses the `piebro/deutsche-bahn-data` dataset as the historical
reference source.

**Geographic scope:** Germany  
**Historical availability:** July 2024 onward  
**License:** CC BY 4.0

### Monthly historical archive

**Source:** [piebro/deutsche-bahn-data](https://huggingface.co/datasets/piebro/deutsche-bahn-data)

Characteristics:

- monthly Parquet release;
- flat tabular structure;
- 17 source columns;
- historical reporting source;
- future reconciliation reference.

The current implemented pipeline contains:

```text
data-2026-01.parquet
...
data-2026-07.parquet
```

representing:

```text
101,702,091 source observations
```

across January–July 2026.

### Raw Deutsche Bahn API

Planned source:

```text
plan + fchg XML payloads
```

with approximately 6-hourly collection.

**Status:** not yet implemented.

The future API path will have its own structural parsing stage before
converging with the monthly path in the shared dbt modeling layer.

---

# Architecture

The project follows a **Medallion-inspired batch architecture** with clear
separation between:

- immutable raw storage;
- structural data-quality validation;
- warehouse landing;
- semantic transformation;
- business-ready reporting models.

## Current implemented architecture

```text
                    DEUTSCHE BAHN
                  MONTHLY DATA SOURCE
                         │
                         │
                         ▼
             Hugging Face Parquet archive
             data-YYYY-MM.parquet
                         │
                         │
                         │ Python ingestion
                         ▼
        ┌─────────────────────────────────────┐
        │              AWS S3                 │
        │              BRONZE                 │
        │                                     │
        │  bronze/monthly-raw/                │
        │    year=2026/                       │
        │      month=01/                      │
        │      month=02/                      │
        │      ...                            │
        │      month=07/                      │
        │                                     │
        │  Raw Parquet preserved unchanged    │
        └─────────────────┬───────────────────┘
                          │
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │       GREAT EXPECTATIONS            │
        │       BRONZE VALIDATION             │
        │                                     │
        │  • file/readability checks          │
        │  • schema checks                    │
        │  • structural validation            │
        │  • non-empty dataset checks         │
        └─────────────────┬───────────────────┘
                          │
                    VALIDATION GATE
                          │
               ┌──────────┴──────────┐
               │                     │
            PASS ✅                FAIL ❌
               │                     │
               │                     ▼
               │          ┌─────────────────────┐
               │          │ STOP PIPELINE       │
               │          │                     │
               │          │ • no Redshift COPY │
               │          │ • log failure       │
               │          │ • keep raw S3 file │
               │          │ • investigate      │
               │          │ • reprocess later  │
               │          └─────────────────────┘
               │
               ▼
        ┌─────────────────────────────────────┐
        │        REDSHIFT SERVERLESS          │
        │                                     │
        │ Namespace                           │
        │ deutsche-bahn-pipeline              │
        │                                     │
        │ Workgroup                           │
        │ deutsche-bahn-pipeline-wg           │
        │                                     │
        │ Database                            │
        │ dev                                 │
        └─────────────────┬───────────────────┘
                          │
                          │ COPY FORMAT AS PARQUET
                          ▼
        ┌─────────────────────────────────────┐
        │            db_monthly               │
        │                                     │
        │        raw_observations             │
        │                                     │
        │ • 17 source columns                 │
        │ • source_month                      │
        │ • 101,702,091 rows                  │
        │ • Jan–Jul 2026                      │
        └─────────────────┬───────────────────┘
                          │
                          │
                          ▼
                 NEXT: dbt modeling
```

---

## Implemented monthly flow

The currently working path is:

```text
Hugging Face
      ↓
Python monthly ingestion
      ↓
S3 Bronze
      ↓
Great Expectations
      ↓
 ┌────┴────┐
 │         │
PASS      FAIL
 │         │
 ▼         ▼
COPY      Stop pipeline
 │        Log validation failure
 │        Preserve Bronze data
 ▼
Redshift Serverless
      ↓
db_monthly.raw_observations
```

### S3 Bronze layout

```text
s3://deutsche-bahn-delay-data-lake/
└── bronze/
    ├── api-raw/
    │
    └── monthly-raw/
        └── year=2026/
            ├── month=01/
            │   └── data-2026-01.parquet
            ├── month=02/
            │   └── data-2026-02.parquet
            ├── month=03/
            │   └── data-2026-03.parquet
            ├── month=04/
            │   └── data-2026-04.parquet
            ├── month=05/
            │   └── data-2026-05.parquet
            ├── month=06/
            │   └── data-2026-06.parquet
            └── month=07/
                └── data-2026-07.parquet
```

Bronze data is treated as **immutable**.

A Great Expectations validation failure therefore does not delete or
overwrite the source file. The pipeline stops before warehouse loading,
records the validation failure, and preserves the Bronze object for
investigation and reprocessing.

---

# Redshift Serverless

Current warehouse configuration:

```text
Namespace:
deutsche-bahn-pipeline

Workgroup:
deutsche-bahn-pipeline-wg

Database:
dev

Raw schema:
db_monthly

Raw table:
db_monthly.raw_observations
```

The historical monthly files are loaded directly from S3 using Redshift
`COPY ... FORMAT AS PARQUET`.

The monthly Redshift landing table currently contains:

```text
101,702,091 rows
```

for January–July 2026.

Validated monthly row counts:

| Month | Rows |
|---|---:|
| 2026-01 | 15,582,748 |
| 2026-02 | 13,721,520 |
| 2026-03 | 15,016,329 |
| 2026-04 | 14,149,788 |
| 2026-05 | 14,427,217 |
| 2026-06 | 14,752,336 |
| 2026-07 | 14,052,153 |
| **Total** | **101,702,091** |

The `id` field has also been validated as unique:

- within every loaded month;
- across the full January–July 2026 Redshift table.

`id` remains a **source snapshot identifier**, not the final business key.

---

# Data quality strategy

Data quality responsibilities are intentionally separated by layer.

### Data profiling

Development-time Spark profiling is used to discover the real behavior of
the source data.

Completed profiling includes:

- schema stability;
- null behavior;
- temporal coverage;
- station identity;
- delay distributions;
- timestamp semantics;
- cancellation behavior;
- ride-stop grain investigation;
- snapshot repetition.

Profiling is evidence for production rules, not production pipeline logic.

### Great Expectations

Great Expectations validates the **Bronze structural layer** before data is
loaded into Redshift.

Its responsibility is primarily:

> Is the dataset that arrived structurally valid and safe to continue
> processing?

If validation fails:

```text
GE FAIL
   ↓
STOP
   ↓
No Redshift COPY
   ↓
Log validation result
   ↓
Preserve Bronze object
   ↓
Investigate / reprocess
```

### dbt tests

dbt will later validate the **semantic/modeling layer**, including:

- business-grain uniqueness;
- deterministic snapshot deduplication;
- delay calculation rules;
- accepted values;
- station/line dimensions;
- reporting-mart assertions.

---

# Important profiling finding: source grain

The historical source is not one final row per train-stop.

Instead:

> **One source row represents one captured snapshot of a train-stop state.**

The same:

```text
train_line_ride_id
+
train_line_station_num
```

can appear repeatedly as operational information changes.

Examples of changing state include:

- delay;
- arrival/departure timestamps;
- cancellation status.

Therefore the future reporting grain will be:

> **One selected final state per train ride and station sequence position.**

This transformation will be implemented in dbt intermediate models rather
than in Bronze ingestion.

---

# Planned architecture

The implemented monthly path will eventually extend into the following
architecture:

```text
MONTHLY HISTORICAL PATH

Hugging Face
      ↓
Python ingestion
      ↓
S3 Bronze
      ↓
Great Expectations
      ↓
Redshift raw landing
      ↓
dbt staging
      ↓
dbt intermediate
      ↓
dbt Gold marts
      ↓
Dashboard


FUTURE API PATH

Deutsche Bahn plan/fchg API
      ↓
API ingestion
      ↓
S3 Bronze
      ↓
Great Expectations
      ↓
Structural parsing
      ↓
Canonical observations
      ↓
Redshift
      ↓
shared dbt intermediate layer
      ↓
dbt Gold marts
      ↓
Daily dashboard
```

Both paths will eventually converge into the same semantic reporting model.

---

# Current implementation status

| Component | Status |
|---|---|
| Local Docker/Spark environment | ✅ Complete |
| Data catalog | ✅ Complete |
| Jan–Jul 2026 data profiling | ✅ Complete |
| Monthly Python ingestion | ✅ Implemented |
| S3 Bronze storage | ✅ Implemented |
| Great Expectations Bronze validation | ✅ Implemented |
| Validation failure gate | ✅ Implemented |
| Redshift Serverless setup | ✅ Implemented |
| S3 → Redshift Parquet COPY | ✅ Implemented |
| Jan–Jul 2026 warehouse load | ✅ Validated |
| dbt source/staging | 🔜 Next |
| dbt deduplication | ⏳ Planned |
| dbt enrichment | ⏳ Planned |
| Gold reporting marts | ⏳ Planned |
| Raw API ingestion | ⏳ Planned |
| Airflow orchestration | ⏳ Planned |
| Streamlit dashboard | ⏳ Planned |
| Terraform | ⏳ Later hardening phase |
| ML delay prediction | ⏳ Advanced / stretch |

---

# Next milestone — dbt modeling

The next implementation phase starts from:

```text
db_monthly.raw_observations
```

and introduces:

```text
db_monthly.raw_observations
        ↓
dbt source
        ↓
stg_monthly_observations
        ↓
int_observations_deduped
        ↓
int_observations_enriched
        ↓
Gold marts
```

### `stg_monthly_observations`

Thin source-aligned staging model:

- rename/cast only;
- no deduplication;
- no reporting calculations;
- source-level dbt tests.

### `int_observations_deduped`

Resolve repeated train-stop snapshots using a deterministic precedence rule.

Expected business key:

```text
train_line_ride_id
+
train_line_station_num
+
source_month
```

### `int_observations_enriched`

Add shared semantic fields such as:

- service date;
- service hour;
- weekday;
- delay calculations;
- on-time flag;
- station coverage era;
- canonical station/line attributes.

### Gold marts

Planned reporting models include:

```text
mart_daily_station_perf
mart_monthly_station_perf

mart_daily_line_perf
mart_monthly_line_perf
```

These will become the contract consumed by the reporting dashboard.

---

## Quickstart

Start the local development environment:

```bash
docker compose up -d
```

Historical data assessment notebooks are available under:

```text
notebooks/data-assessment/monthly_catalog_profiling/
```

The monthly ingestion pipeline loads source Parquet files into the project's
S3 Bronze layer.

Production orchestration with Airflow is not yet implemented; the current
pipeline components are being validated individually before orchestration is
added.

---

## Development approach

The project is intentionally built incrementally.

Each layer is implemented and validated before orchestration is added:

```text
Source
  ↓
Ingestion
  ↓
Bronze storage
  ↓
Bronze validation
  ↓
Warehouse landing
  ↓
Semantic modeling
  ↓
Reporting
  ↓
Orchestration / hardening
```

This README evolves with the actual implementation rather than documenting
components that have not yet been built.