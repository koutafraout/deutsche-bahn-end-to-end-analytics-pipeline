
# Dataset context

## Role of this folder

`notebooks/data-assessment/` is used only for data catalog and data
profiling — development-time investigation done before ETL pipeline
development starts. 

- `monthly_catalog_profiling/` — investigation of the monthly Parquet
  subset as downloaded from Hugging Face (schema load, data catalog, data
  profiling). This is what's done so far.
- `api_catalog_profiling/` — placeholder for the equivalent investigation of the raw
  API (`plan`/`fchg`) subset, not started yet.

The capstone project ingests **two batch subsets from the same Deutsche
Bahn historical delay archive** — they are not alternatives, the project
uses both:

- **Monthly Parquet**: flat, already-typed
  tabular releases, as downloaded; the MVP monthly reporting source and
  the reconciliation reference.
- **API Raw collected data**: raw `timetables/v1/plan` and
  `timetables/v1/fchg` API responses (XML/JSON), collected hourly; the
  reference for the Bronze API schema and the candidate ~6-hourly
  ingestion source.

The monthly Parquet data feeds the **monthly** reports; the API data
feeds the **daily** reports. See the root [README](../../README.md) for
more information.

## Dataset source

| | |
|---|---|
| **Source** | [huggingface.co/datasets/piebro/deutsche-bahn-data](https://huggingface.co/datasets/piebro/deutsche-bahn-data) (community-archived, CC BY 4.0) |
| **Subsets used** | `monthly_processed_data/data-YYYY-MM.parquet` (monthly release); `raw_data/year=/month=/day=/` (raw API responses, hourly files) |
| **Type** | Batch Parquet (monthly processed); batch/hourly raw API responses (XML/JSON in `response_data`) |
| **Update frequency** | Monthly processed: monthly release. Raw collected: hourly files. |
| **Coverage** | Germany-wide, available from July 2024 onward |
