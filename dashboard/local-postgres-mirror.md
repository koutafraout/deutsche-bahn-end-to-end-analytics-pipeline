# Local Postgres mirror of Gold marts

A runbook for mirroring the Redshift Gold marts into a local Postgres
container and pointing Metabase at it — useful for offline work or to
avoid Redshift Serverless usage while Metabase is being poked at. This is
a local-only mirror of Gold; it doesn't replace Redshift Serverless as the
warehouse (ADR 0002) and Metabase still only ever reads Gold-mart-shaped
tables.

**Editing the existing Metabase database connection in place — not
deleting and re-adding it — is what keeps your dashboards, questions, and
field-filter parameter mappings (e.g. the `dim_service_month`-driven
month/year filters `setup_metabase.py` wires up) intact.** Metabase's
dashboard/question metadata lives in its own embedded H2 app-db (the
`metabase-data` volume in `docker-compose.yml`), separate from the
Redshift connection, so switching the connection's target doesn't touch
that data — but deleting the database entry does delete everything wired
to its field IDs.

## 1. Add a local Postgres service

In `docker-compose.yml`, add:

```yaml
  postgres:
    image: postgres:16
    container_name: db-ops-postgres
    restart: unless-stopped
    environment:
      POSTGRES_DB: gold_marts
      POSTGRES_USER: metabase
      POSTGRES_PASSWORD: metabase
    volumes:
      - postgres-data:/var/lib/postgresql/data
    ports:
      - "127.0.0.1:5433:5432"   # 5433 to avoid clashing with any local Postgres on 5432
```

And add `postgres-data:` under the top-level `volumes:` block.

```bash
docker compose up -d postgres
```

## 2. Recreate the Gold mart tables locally (schema-faithful, from Redshift's own catalog)

Source your `.env` first (`REDSHIFT_HOST`, `REDSHIFT_USER`, etc.).

`information_schema.columns` and `pg_table_def` both fail on Redshift
Serverless here with `Function "format_type(oid,integer)" not
supported.` — use Redshift's own `svv_columns` view instead, and
`listagg(...) within group (order by ...)` since Redshift's `string_agg`
doesn't support an inline `order by`. Tables are created directly under a
`dbt_dev_marts` schema locally too, to match Redshift's layout (so
Metabase's schema filter and any hand-written SQL referencing
`dbt_dev_marts.*` work unchanged against either target):

```bash
export PGPASSWORD="$REDSHIFT_PASSWORD"
docker exec -i db-ops-postgres psql -U metabase -d gold_marts -c "create schema if not exists dbt_dev_marts;"

psql "host=$REDSHIFT_HOST port=$REDSHIFT_PORT dbname=$REDSHIFT_DBNAME user=$REDSHIFT_USER sslmode=require" \
  -Atc "
    select 'create table if not exists dbt_dev_marts.' || table_name || ' (' ||
           listagg(column_name || ' ' ||
             case data_type
               when 'character varying' then 'varchar'
               when 'double precision'  then 'double precision'
               else data_type
             end, ', ') within group (order by ordinal_position) || ');'
    from svv_columns
    where table_schema = 'dbt_dev_marts'
      and table_name in ('dim_station','dim_train_line','dim_service_month',
                          'mart_monthly_station_perf','mart_monthly_line_perf')
    group by table_name;
  " > /tmp/gold_ddl.sql

docker exec -i db-ops-postgres psql -U metabase -d gold_marts < /tmp/gold_ddl.sql
```

## 3. Stream each table's data straight from Redshift into local Postgres

Redshift only implements `COPY FROM` (loading from S3/etc.) — there's no
`COPY ... TO STDOUT` and no subquery form, so `\copy` can't be used for
the export side. Use a plain `SELECT` with psql's `--csv` output instead
(client-side formatting, works against any server) piped into a real
`\copy` on the Postgres side:

```bash
for t in dim_station dim_train_line dim_service_month mart_monthly_station_perf mart_monthly_line_perf; do
  psql "host=$REDSHIFT_HOST port=$REDSHIFT_PORT dbname=$REDSHIFT_DBNAME user=$REDSHIFT_USER sslmode=require" \
    --csv -c "select * from dbt_dev_marts.$t" \
  | docker exec -i db-ops-postgres psql -U metabase -d gold_marts \
    -c "\copy dbt_dev_marts.$t from stdin with csv header"
done
```

Note: `dbt_dev_marts` is `$REDSHIFT_SCHEMA` (`dbt_dev`) with dbt's custom
schema suffix (`+schema: marts` in `dbt_project.yml`) appended — confirm
yours matches with `select schema_name from information_schema.schemata
where schema_name like '$REDSHIFT_SCHEMA%'` if unsure.

## 4. Point Metabase at the local Postgres — edit, don't recreate

In Metabase (`http://localhost:3000`) → **Admin settings → Databases →
click your existing Redshift connection** (do **not** click "Add
database"):

- Database type: switch to `PostgreSQL`
- Host: `postgres` (if Metabase and Postgres are on the same compose
  network) or `host.docker.internal`
- Port: `5432` (in-network) — use `5433` only if connecting from outside
  Docker
- Database name: `gold_marts`
- Username / Password: `metabase` / `metabase`
- Schema filter: `dbt_dev_marts` — same schema name locally as on
  Redshift
- SSL: turn **off** — Redshift required it, this local Postgres isn't
  configured for it
- Save, then **Sync database schema now** (button near the top of the
  same database page)

Field IDs referenced by `setup_metabase.py`'s field-filter tags are keyed
to this database record, not to the host/engine, so dashboards and
questions keep working — Metabase just re-resolves them against the new
host on next sync. Confirm by opening the dashboard: cards should render
data and the month/year filters should still work.

## 5. Switching back to Redshift later

Same page, same connection record — edit in place, don't recreate:

- Database type: back to `Amazon Redshift`
- Host: `$REDSHIFT_HOST` (the `...redshift-serverless.amazonaws.com` endpoint)
- Port: `$REDSHIFT_PORT` (`5439`)
- Database name: `$REDSHIFT_DBNAME` (`dev`)
- Username / Password: `$REDSHIFT_USER` / `$REDSHIFT_PASSWORD`
- Schema filter: restore to `dbt_dev_marts`
- SSL: turn back **on**
- Save, then **Sync database schema now**

If Redshift Serverless has auto-paused since you last used it, the first
sync after switching back may be slow (~1 min) while it resumes — that's
normal, not a broken connection.

## Known issue: `DATEADD` breaks on Postgres

Several cards built by `setup_metabase.py` use Redshift's
`DATEADD(month, -1, p.selected_month)` syntax. PostgreSQL doesn't have
`DATEADD` at all — it parses `month` as a bare identifier and fails with:

```
ERROR: column "month" does not exist
```

Both Redshift and Postgres support standard SQL interval arithmetic, so
the fix is portable — change the affected line in each card's SQL, e.g.:

```sql
-- from:
m.service_month >= DATEADD(month, -1, p.selected_month)
-- to:
m.service_month >= CAST(p.selected_month - INTERVAL '1 month' AS DATE)
```

`setup_metabase.py` itself still emits `DATEADD` (11 occurrences) since
it targets Redshift as the production source of truth — this only needs
patching per-card in the Metabase UI when running against the local
Postgres mirror, not in the script.

## Optional: browse it with pgAdmin

If pgAdmin 4 (desktop app) is already installed, no new container is
needed:

1. Right-click **Servers** → **Register → Server...**
2. **General** tab → Name: `db-ops-postgres` (anything you like).
3. **Connection** tab:
   - Host name/address: `localhost`
   - Port: `5433` — **not** the Postgres default `5432`. If another local
     Postgres is already running on your machine (check with `lsof -nP
     -iTCP:5432 -sTCP:LISTEN`), pgAdmin will silently connect to *that*
     one instead and fail with `role "metabase" does not exist`.
   - Maintenance database: `gold_marts`
   - Username / Password: `metabase` / `metabase`
4. Save.

You should see the 5 mirrored tables under **Schemas → dbt_dev_marts →
Tables**.
