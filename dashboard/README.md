# Dashboard

Scripts that build the "Deutsche Bahn Operations Analytics" Metabase
dashboard against this project's Gold marts. Metabase itself is not an
app hosted by this repo: it's an external BI tool, run via
`docker compose up -d metabase`, that connects directly to the Gold
marts schema in Redshift Serverless.

Nothing here queries Bronze, staging, or intermediate. The Redshift
connection this script creates is restricted (via schema filtering) to
the `dbt_dev_marts` schema only.

## What's here

- `metabase_client.py`: thin REST client (stdlib `urllib` only, no new
  dependency) wrapping the Metabase API calls the setup script needs.
- `setup_metabase.py`: builds the dashboard, including the Redshift
  connection, field remapping (so the Station filter searches by name
  but filters by `eva`), 3 tabs, 4 global filters (with Train Type to
  Line linked), and every card, laid out and wired with click-to-filter
  behavior.

## Prerequisites (can't be scripted)

1. `docker compose up -d metabase`, then open `http://localhost:3000`
   and complete Metabase's first-run setup (this creates *your* admin
   account, which nothing can do on your behalf).
2. Generate an API key: **Settings → Admin settings → Settings → API
   Keys → Create API Key**.
3. Make sure the `REDSHIFT_*` variables in `.env` are set (same ones
   `dbt` uses, see `docs/runbook.md`).

## Run it

```bash
export METABASE_API_KEY="<the key from step 2>"
set -a; source .env; set +a
python dashboard/setup_metabase.py
```

Prints the dashboard URL on success.

## Idempotency

- **Database connection & schema sync**: safe to re-run, reuses the
  existing "Deutsche Bahn Gold" connection by name if present, always
  re-syncs the schema.
- **Field remapping**: safe to re-run, every call is a `PUT`/idempotent
  upsert.
- **The dashboard itself is not**: if a dashboard named "Deutsche Bahn
  Operations Analytics" already exists, the script logs a warning and
  exits without touching it. There's no incremental-update/diff logic
  (building that was judged out of scope for a setup script this size). To
  rebuild from scratch, delete the existing dashboard (and its cards) in
  the Metabase UI or via the API first.

## What you still have to do by hand

- Visually review layout/spacing/colors in the actual UI (the script
  sets grid positions but nothing here has been screenshotted).
- Confirm the Line filter widget visibly narrows when Train Type is set
  (the linked-filter config is verified correct via the API; the
  rendered widget behavior wasn't).
- Anything cosmetic: dashboard description, default tab, collection
  placement, embedding/sharing settings.

## Why this exists as a script instead of manual setup

Everything above was originally built by scripting the Metabase REST API
directly rather than clicking through the UI, then cleaned up into this
reusable form afterward. The full decision log, every SQL query, the
two Field Filter mistakes that broke things the first time, and a real
ranking bug found along the way, are written up in
`docs/metabase-dashboard-walkthrough.md` (local-only, not pushed, see
`.gitignore`; it references internal details freely since it's not a
public artifact).
