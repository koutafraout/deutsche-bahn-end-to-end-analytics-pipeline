# Architecture

## 1. Pattern

**Medallion architecture (Bronze → Silver → Gold), scheduled incremental
batch.** Two independent batch sources — a monthly Hugging Face Parquet
release and a ~6-hourly Deutsche Bahn API poll — land in Bronze unchanged,
converge on one canonical observation schema, and are modeled once in dbt.
There is no continuous event stream, so there is no Kafka or Spark
Structured Streaming in this design.

| Layer | Storage | Role |
|---|---|---|
| Bronze | S3, immutable | Raw source bytes, unchanged, partitioned |
| Silver | Redshift Serverless, dbt staging + intermediate | Conform, dedupe, compute business rules |
| Gold | Redshift Serverless, dbt marts | Reporting-ready star schema |
| Serving | FastAPI + Streamlit (planned) | Reads Gold only |

## 2. Currently implemented path (monthly)

Everything below is running code, not a plan.

```text
Hugging Face (piebro/deutsche-bahn-data)
  monthly_processed_data/data-YYYY-MM.parquet
                    │
                    │  ingestion/monthly_load  (Python, boto3 + huggingface_hub)
                    ▼
  S3 Bronze — s3://<bucket>/bronze/monthly-raw/year=YYYY/month=MM/
              raw Parquet, byte-for-byte, skip-if-exists unless --force
                    │
                    │  quality/bronze_monthly  (Great Expectations)
                    ▼
        ┌─────────────────────────────┐
        │  Bronze validation gate     │
        │  1. valid Parquet, readable │
        │  2. row_count >= 1          │
        │  3. schema == 17 columns    │
        │  4. not-null on 10 columns  │
        │     confirmed always-       │
        │     populated by profiling  │
        │  5. time within source month│
        └──────────────┬──────────────┘
                PASS │       │ FAIL
                     │       └──► stop; log; keep Bronze object;
                     │            no Redshift COPY (investigate/reprocess)
                     ▼
  Redshift Serverless — manual `COPY ... FORMAT AS PARQUET`
  dev.db_monthly.raw_observations (17 source columns + source_month,
  BIGINT nanosecond-epoch timestamps preserved as-landed, not auto-decoded)
                    │
                    │  dbt (warehouse/dbt)
                    ▼
  dbt staging — stg_monthly_observations
    • 1:1 rename/cast only, no business logic
    • decodes nanosecond BIGINT → naive TIMESTAMP (Europe/Berlin wall clock)
    • not_null + unique(id) schema tests
                    │
                    ▼
              NEXT: dbt intermediate (not yet built)
```

Validated as of the last load: 101,702,091 rows across January–July 2026,
`id` confirmed unique within and across all loaded months.

## 3. Target architecture

```text
  ══════════════════════ SOURCES ══════════════════════
  HF monthly Parquet (1×/month)       DB raw API: plan + fchg (6-hourly)
          │                                    │
  ═════════════════ BRONZE — S3, immutable ════════════════
  bronze/monthly-raw/year=/month=/    bronze/api-raw/year=/month=/day=/hour=/
          │                                    │
  ══════ STRUCTURAL LAYER — Spark (parse & conform ONLY) ══════
   (no Spark job — already tabular,     spark_parse_api: parse plan+fchg,
    COPY straight to Redshift)          join by train/station, explode to
                                        one row/train-stop, cast to
                                        canonical columns → Parquet → S3
          │                                    │
          │                             COPY → Redshift
          └────────────────┬───────────────────┘
                            ▼
  ═══ dbt STAGING — thin, 1:1 with source, rename/cast only ═══
     stg_monthly_observations         stg_api_observations
                            │
                            ▼
  ═══ dbt INTERMEDIATE — shared core, written ONCE ═══
     int_observations_unioned → int_observations_deduped →
     int_observations_enriched (delay recompute, on-time flag,
     service date/hour/weekday, station_coverage_era flag)
                            │
                            ▼
  ══════════ GOLD — dbt marts on Redshift Serverless ══════════
   mart_daily_station_perf / mart_daily_line_perf (incremental)
   mart_monthly_station_perf / mart_monthly_line_perf (full refresh)
   dim_station, dim_train_line
                            │
                            ▼
        FastAPI (reporting endpoints)  ·  Streamlit (daily + monthly)
              [stretch] Gold feature_* → ML delay prediction

  Airflow: api_pull (6-hourly) · monthly_load · spark_parse · dbt build ·
           monthly_reconcile
  Great Expectations: Bronze — freshness, status_code, non-empty payload
  dbt tests: staging (not-null, types) · intermediate (key uniqueness,
             delay sanity bounds) · marts (row-count deltas, assertions)
```

## 4. Why each tool was chosen

| Tool | Role | Why |
|---|---|---|
| **S3** | Bronze landing | Cheap, durable, immutable object storage; a parser bug is recoverable by reprocessing, not re-pulling the source |
| **Redshift Serverless** | Silver + Gold warehouse | Single SQL warehouse target; auto-pause keeps a limited AWS credit budget usable; no need for a second warehouse product |
| **dbt-redshift** | All business logic — dedup, delay calc, dimensions, marts | Version-controlled SQL with built-in testing, docs, and lineage; both ingestion paths converge here so rules are written exactly once instead of duplicated per source |
| **PySpark** | Structural parsing of the raw API's `plan`/`fchg` payloads only | Parsing and joining nested XML/JSON is awkward in SQL and natural in Spark; not used on the monthly path (already flat/typed) and not used for business logic |
| **Great Expectations** | Bronze structural gate | Distinct concern from dbt tests — "did the payload arrive intact and match the expected shape," not "is the business rule correct" |
| **Airflow** | Orchestration (planned) | Scheduled batch DAGs fit the 6-hourly/monthly cadence; no need for a streaming scheduler |
| **FastAPI + Streamlit** | Serving (planned) | Thin reporting layer reading Gold marts only — no business logic lives in the API or dashboard |

## 5. Key architectural rules

- **Spark owns structure, dbt owns semantics.** Spark's scope stops at
  "make it tabular and match the canonical schema." Dedup, delay
  recomputation, dimensions, and source precedence all live in dbt
  intermediate — never in the structural parser.
- **Both sources are unioned inside dbt intermediate**, not upstream — this
  makes "shared logic, written once" structural, not a matter of
  discipline.
- **Bronze is immutable.** A validation failure stops the pipeline before
  the Redshift `COPY`; it never deletes or mutates the landed object.
- **The Gold marts are the contract.** The API and dashboard never read
  Bronze or the raw landing table directly.
- **Data quality is split by layer, not duplicated:** Great Expectations
  gates Bronze structure; dbt tests gate staging/intermediate/mart
  semantics; a monthly reconciliation job checks correctness against the
  official Hugging Face release.

## 6. Known data-quality realities carried through the design

- The publicly documented monthly schema (16 columns) is stale — the
  actual files have 17 columns (`train_name` replaced by `train_number` +
  `line_number`). Schema is checked as a hard gate on every load, not
  assumed.
- Source grain is **snapshot-level**, not final-state: the same
  `train_line_ride_id` + `train_line_station_num` can appear multiple
  times as delay, timestamps, and cancellation status evolve.
  Deduplication to one row per train-stop happens in
  `int_observations_deduped` (not yet built), never in Bronze or staging.
- A station-coverage break exists in the source history (a smaller set of
  major stations before a given date, all reachable stations after) and
  must be carried as a `station_coverage_era` flag so trend charts don't
  silently compare incomparable periods.
- Timestamps land in Redshift as raw nanosecond-epoch `BIGINT` and are
  decoded to naive (Europe/Berlin wall-clock) `TIMESTAMP` in staging —
  keeping the raw landing table byte-faithful to Bronze.
