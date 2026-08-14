# ADR 0001: Great Expectations for Bronze validation

## Status

Accepted — implemented (`quality/bronze_monthly/`)

## Context

Landed Bronze files (raw monthly Parquet) are untrusted until checked: the
source has already shown schema drift (the publicly documented 16-column
form doesn't match the actual 17-column files), and a corrupt or
structurally wrong file must not silently reach the warehouse. A gate was
needed between S3 Bronze and the Redshift `COPY` step, distinct from the
dbt tests that later validate staging/intermediate/mart *semantics*
(business rules, not raw structure).

Options considered:

- **Ad hoc Python checks** (row count, column list) with no framework —
  fast to write, but checks and their results aren't declarative,
  reusable, or easy to extend without growing custom plumbing.
- **dbt tests on a source** — would require loading the file into
  Redshift first to test it, defeating the point of gating *before* the
  `COPY`.
- **Great Expectations**, run directly against the Parquet file (via a
  pandas batch) before it ever reaches Redshift.

## Decision

Use Great Expectations as the Bronze structural gate, run before the
Redshift `COPY`. The suite checks, in order: the object is valid,
readable Parquet; row count ≥ 1; the schema is exactly the 17 confirmed
columns, in order; the columns profiling confirmed are always populated
are still non-null; and `time` values fall inside their own source month.
A failure stops the pipeline — no `COPY` runs — logs the reason, and
leaves the Bronze object untouched for investigation and reprocessing.

dbt tests remain the separate, later gate for semantic correctness
(uniqueness, business-key integrity) once data is in the warehouse.

## Consequences

- Checks are declarative expectation objects, not scattered `if` blocks,
  and results are structured (pass/fail per expectation), which makes
  logging and debugging a specific failure straightforward.
- The gate runs against the Parquet file directly (metadata for schema/row
  count, a pruned pandas load for the not-null/freshness checks), so
  nothing bad reaches Redshift — no rollback or delete-after-load logic is
  needed.
- The expected schema and not-null column list are hardcoded from a
  specific profiling pass, not inferred generically — if the real upstream
  schema changes again, the suite must be updated deliberately rather than
  silently passing or silently failing on unrelated columns.
- Adds a dependency (`great_expectations`, `pandas`, `pyarrow`) scoped to
  the `quality` extra, not required for ingestion or warehouse work.
