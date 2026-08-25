# bronze_monthly

Great Expectations quality gate for the monthly Bronze Parquet landed by
`ingestion/monthly_load/`. Run after `ingest_monthly`/`main.py` lands a
month, before that month is trusted by `stg_monthly_observations`.

Checks (see `validate.py` docstring for detail):

- valid Parquet
- non-empty
- 17-column schema match
- non-null on the columns confirmed always-populated in profiling
- `time` values falling inside their own source month

Every threshold traces back to `docs/data-profiling-2026-01-07.md` §3-§4,
§12: nothing here is a guessed bound.

Install deps: `pip install -e ".[quality]"` (or `uv pip install -e ".[quality]"`)
from the repo root.

```bash
python -m quality.bronze_monthly.validate --month 2026-07
python -m quality.bronze_monthly.validate --month 2026-01 --month 2026-02
```

Exits non-zero if any month fails a check. Wire this as a hard gate in the
future `dbt_build`/`monthly_load` Airflow DAG, not an optional step.
