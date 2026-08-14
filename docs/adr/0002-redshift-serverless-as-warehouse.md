# ADR 0002: Redshift Serverless as the warehouse

## Status

Accepted — implemented (`dev.db_monthly.raw_observations`,
`warehouse/dbt/`)

## Context

The project needs one SQL warehouse to hold the Silver (dbt staging +
intermediate) and Gold (dbt marts) layers, reachable from a local
`dbt-redshift` setup, and affordable within a fixed, limited AWS credit.

Options considered:

- **Snowflake** — strong dbt support, but a separate paid platform outside
  the AWS credit, adding a second billing surface and account to manage
  for a capstone-scale project.
- **DuckDB** — free and fast for local/single-node analytics, but not a
  shared, network-reachable warehouse; doesn't fit a design where
  ingestion, dbt, and (eventually) a dashboard/API all need to reach the
  same data independently.
- **Redshift provisioned clusters** — always-on compute, billed
  continuously regardless of usage; a poor fit for a pipeline that runs on
  a monthly/6-hourly batch cadence with long idle periods.
- **Redshift Serverless** — same SQL engine and `dbt-redshift` adapter as
  provisioned Redshift, but bills by usage and auto-pauses when idle,
  which matches this project's batch cadence and budget constraint.

## Decision

Use a single Redshift Serverless workgroup (namespace
`deutsche-bahn-pipeline`, workgroup `deutsche-bahn-pipeline-wg`, database
`dev`) as the only warehouse target for both Silver and Gold. Bronze
remains on S3. No second warehouse product is introduced.

Raw monthly data lands in `db_monthly.raw_observations` via a direct S3
`COPY ... FORMAT AS PARQUET` after the Bronze validation gate passes; dbt,
connecting as a dedicated non-superuser `dbt_user`, builds staging and
(eventually) intermediate/mart models into a separate `dbt_dev` schema.

## Consequences

- Auto-pause keeps idle cost near zero between the monthly load and
  6-hourly poll runs, which matters directly for staying inside a fixed
  credit.
- The five timestamp columns are declared `BIGINT` on
  `raw_observations`, not `TIMESTAMP`, so `COPY ... FORMAT AS PARQUET`
  lands the Parquet nanosecond-epoch values unchanged; decoding to a
  proper timestamp happens in dbt staging, keeping the raw landing table
  byte-faithful to Bronze.
- dbt-redshift's create-and-swap materialization needs `TEMP` and
  `CREATE` grants at the database level, in addition to schema-level
  `USAGE`/`SELECT` on the raw schema — a grant most other warehouses
  don't require explicitly at this scope, and a common source of
  `permission denied` failures if missed (see `runbook.md` §3).
- Public accessibility is restricted to the developer's IP on port 5439
  via a dedicated security group, rather than open access, since this is
  a single-developer capstone setup, not a multi-user production
  environment.
- Committing to one warehouse product means no cross-warehouse comparison
  or migration path is being kept open; this is an accepted trade-off
  given the project's scope and timeline.
