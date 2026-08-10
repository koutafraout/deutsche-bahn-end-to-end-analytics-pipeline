# Transit Ops Delay Prediction — Deutsche Bahn

A scheduled, incremental-batch data platform that turns Deutsche Bahn historical and API delay data into tested, modeled station/line/time performance datasets for daily and monthly reporting.



> **Status: v0.1 — Foundation (Milestone 0).** This README is a first
> version and will evolve as the project progresses. Today, only data
> profiling and the local Spark environment exist — the pipeline itself
> has not been built yet.

## Problem statement

Deutsche Bahn publishes both a monthly historical delay archive (Hugging
Face Parquet release) and a raw real-time API (`plan`/`fchg` payloads), but
neither is analysis-ready on its own: the historical release is a
poll-and-snapshot dataset (the same train-stop is captured repeatedly as
its state evolves, not one row per final event), and the two sources
overlap in time without an agreed precedence rule. This project ingests
both sources into a single canonical schema so delay performance can be
reported consistently by station, line, and time.

Deutsche Bahn delay data contains useful information for operational monitoring and long-term transport planning, but the raw observations are not yet a business-ready reporting model.

The project builds a reproducible data pipeline that turns those observations into validated and queryable performance datasets.

### Stakeholders

**Transport planners**

Use monthly reporting to:

- identify long-term delay patterns;
- compare stations and railway lines;
- support timetable, capacity, and infrastructure planning.

**Railway operations and performance teams**

Use daily reporting to:

- monitor recent delays and cancellations;
- investigate operational problems;
- identify issues that persist across several days.

**Railway performance analysts**

Use both daily and monthly views for operational monitoring and trend analysis.

## Data sources
The project uses the `piebro/deutsche-bahn-data` dataset as the historical reference source.

**Geographic scope:** Germany  
**Historical availability:** July 2024 onward  
**License:** CC BY 4.0

- **[DB Monthly historical archive](https://huggingface.co/datasets/piebro/deutsche-bahn-data)** — Hugging Face Parquet release,

  Characteristics:
  - flat Parquet data;
  - monthly release;
  - 17 columns observed in the validated files;
  - used for historical reporting and future reconciliation.

- **DB Raw Deutsche Bahn API** — `plan`/`fchg` XML payloads, polled ~6-hourly
  (not yet implemented).



## Quickstart

Only the Spark profiling environment is runnable today:

```bash
docker compose up -d
# then open notebooks/data-assessment/deutsche_bahn/*.ipynb
```

## Architecture

Architecture will be documented separately as the pipeline is built.

---

This README will grow with the project — sections above will be filled in
and expanded milestone by milestone rather than written ahead of what
actually exists.