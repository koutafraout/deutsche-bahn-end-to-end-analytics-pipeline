"""Reproduces the "Deutsche Bahn Operations Analytics" Metabase dashboard
from scratch against this project's Gold marts.

Prerequisites (cannot be scripted — see dashboard/README.md):
1. `docker compose up -d metabase`, then complete Metabase's first-run
   setup at http://localhost:3000 (creates your admin account).
2. Generate an API key: Settings -> Admin settings -> Settings ->
   API Keys -> Create API Key.
3. Export METABASE_API_KEY (and the REDSHIFT_* vars from .env) into the
   environment this script runs in.

Idempotent for the database connection and schema sync. NOT idempotent
for the dashboard itself — if a dashboard with the same name already
exists, this exits without changes rather than attempting a partial
rebuild/diff (out of scope for a setup script this size; delete the
existing dashboard in the UI first if you want a clean rebuild).

Run: python dashboard/setup_metabase.py
"""

from __future__ import annotations

import logging
import os
import sys

from metabase_client import (
    MARTS_SCHEMA,
    METABASE_URL,
    build_field_id_map,
    call,
    create_card,
    crossfilter_click,
    field_filter_tag,
    get_or_create_dashboard,
    get_or_create_database,
    pm_dim,
    pm_var,
    sync_and_wait_for_tables,
    text_variable_tag,
    wait_for_health,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_NAME = "Deutsche Bahn Operations Analytics"
DATABASE_NAME = "Deutsche Bahn Gold"
EXPECTED_TABLES = {"dim_station", "dim_train_line", "mart_monthly_station_perf", "mart_monthly_line_perf"}

PARAM_MONTH = "svcmonth001"
PARAM_STATION = "station001"
PARAM_TRAIN_TYPE = "traintype001"
PARAM_LINE = "line001"


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required — set it in your environment (see .env).")
    return value


def setup_redshift_connection() -> int:
    details = {
        "host": require_env("REDSHIFT_HOST"),
        "port": int(os.environ.get("REDSHIFT_PORT", "5439")),
        "db": require_env("REDSHIFT_DBNAME"),
        "user": require_env("REDSHIFT_USER"),
        "password": require_env("REDSHIFT_PASSWORD"),
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": MARTS_SCHEMA,
        "ssl": True,
    }
    db_id = get_or_create_database(DATABASE_NAME, details)
    logger.info("syncing schema %s ...", MARTS_SCHEMA)
    metadata = sync_and_wait_for_tables(db_id, EXPECTED_TABLES)
    return db_id, metadata


def setup_station_name_remap(fields: dict[tuple[str, str], int]) -> None:
    """Station filter searches by station_name, filters by eva. See
    dashboard/README.md and docs/metabase-dashboard-walkthrough.md (local,
    not pushed) for why this needs an explicit FK + field remap rather
    than a UI checkbox."""
    dim_eva = fields[("dim_station", "eva")]
    dim_name = fields[("dim_station", "station_name")]
    fact_eva = fields[("mart_monthly_station_perf", "eva")]

    call("PUT", f"/api/field/{dim_eva}", {"semantic_type": "type/PK"})
    call("PUT", f"/api/field/{dim_name}", {"semantic_type": "type/Name"})
    call("PUT", f"/api/field/{fact_eva}", {"semantic_type": "type/FK", "fk_target_field_id": dim_eva})
    call("POST", f"/api/field/{fact_eva}/dimension", {
        "type": "external", "name": "station_name", "human_readable_field_id": dim_name,
    })
    logger.info("station_name remap configured on mart_monthly_station_perf.eva")


def setup_dashboard_shell() -> tuple[int, dict[str, int]]:
    dash_id, existed = get_or_create_dashboard(DASHBOARD_NAME, "Station and line performance from the Gold marts.")
    if existed:
        logger.warning(
            "Dashboard %r already exists (id=%s) — leaving it untouched. "
            "Delete it in the Metabase UI first if you want a full rebuild.",
            DASHBOARD_NAME, dash_id,
        )
        return dash_id, None

    result = call("PUT", f"/api/dashboard/{dash_id}", {
        "tabs": [{"id": -1, "name": "Overview"}, {"id": -2, "name": "Stations"}, {"id": -3, "name": "Trends"}],
        "dashcards": [],
        "parameters": [
            {"id": PARAM_MONTH, "name": "Service Month", "slug": "service_month", "type": "date/month-year", "sectionId": "date"},
            {"id": PARAM_STATION, "name": "Station", "slug": "station", "type": "string/=", "sectionId": "string"},
            {"id": PARAM_TRAIN_TYPE, "name": "Train Type", "slug": "train_type", "type": "string/=", "sectionId": "string"},
            {"id": PARAM_LINE, "name": "Line", "slug": "line", "type": "string/=", "sectionId": "string",
             "filteringParameters": [PARAM_TRAIN_TYPE]},
        ],
    })
    tabs = {t["name"]: t["id"] for t in result["tabs"]}
    return dash_id, tabs


def create_all_cards(db_id: int, f: dict[tuple[str, str], int]) -> dict[str, int]:
    """f: field id map. Returns {logical_name: card_id}."""
    c = {}

    smp = "mart_monthly_station_perf"
    mlp = "mart_monthly_line_perf"

    def month_station_tags():
        return {
            "service_month": field_filter_tag("service_month", "Service Month", f[(smp, "service_month")], "date/month-year"),
            "station": field_filter_tag("station", "Station", f[(smp, "eva")], "string/="),
        }

    def station_only_tag():
        return {"station": field_filter_tag("station", "Station", f[(smp, "eva")], "string/=")}

    # --- Overview: network KPIs (weighted, see walkthrough §5) ---
    c["kpi_avg_delay"] = create_card(db_id, "KPI: Average Delay (network)", '''select
  round(sum(avg_delay_min * non_canceled_observation_count)::float
        / nullif(sum(non_canceled_observation_count), 0), 1) as "Average Delay (min)"
from %s.%s
where 1=1
[[ and {{service_month}} ]]
[[ and {{station}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar",
        description="Observation-count-weighted average delay, not a naive average of per-station averages.")

    c["kpi_on_time"] = create_card(db_id, "KPI: On-Time Rate (network)", '''select
  round(100.0 * sum(on_time_count) / nullif(sum(non_canceled_observation_count), 0), 1) as "On-Time Rate (%%)"
from %s.%s
where 1=1
[[ and {{service_month}} ]]
[[ and {{station}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    c["kpi_cancel"] = create_card(db_id, "KPI: Cancellation Rate (network)", '''select
  round(100.0 * sum(cancellation_count) / nullif(sum(observation_count), 0), 1) as "Cancellation Rate (%%)"
from %s.%s
where 1=1
[[ and {{service_month}} ]]
[[ and {{station}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    c["kpi_obs"] = create_card(db_id, "KPI: Observation Count (network)", '''select
  sum(observation_count) as "Observation Count"
from %s.%s
where 1=1
[[ and {{service_month}} ]]
[[ and {{station}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    # --- Overview: monthly network trends ---
    c["trend_delay"] = create_card(db_id, "Monthly Average Delay", '''select
  service_month as "Month",
  round(sum(avg_delay_min * non_canceled_observation_count)::float
        / nullif(sum(non_canceled_observation_count), 0), 2) as "Average Delay (min)"
from %s.%s
where 1=1
[[ and {{station}} ]]
group by service_month
order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["Average Delay (min)"]})

    c["trend_ontime"] = create_card(db_id, "Monthly On-Time Rate", '''select
  service_month as "Month",
  round(100.0 * sum(on_time_count) / nullif(sum(non_canceled_observation_count), 0), 2) as "On-Time Rate (%%)"
from %s.%s
where 1=1
[[ and {{station}} ]]
group by service_month
order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["On-Time Rate (%)"]})

    c["trend_cancel"] = create_card(db_id, "Monthly Cancellation Rate", '''select
  service_month as "Month",
  round(100.0 * sum(cancellation_count) / nullif(sum(observation_count), 0), 2) as "Cancellation Rate (%%)"
from %s.%s
where 1=1
[[ and {{station}} ]]
group by service_month
order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["Cancellation Rate (%)"]})

    # --- Overview: rankings (>=30 obs AND >0 non-canceled obs — see walkthrough §4) ---
    c["worst_stations"] = create_card(db_id, "Worst Performing Stations", '''select
  dim_station.station_name as "Station",
  dim_station.eva as "eva",
  round(sum(%(smp)s.avg_delay_min * %(smp)s.non_canceled_observation_count)::float
        / nullif(sum(%(smp)s.non_canceled_observation_count), 0), 1) as "Average Delay (min)",
  sum(%(smp)s.observation_count) as "Observation Count"
from %(schema)s.%(smp)s
join %(schema)s.dim_station on dim_station.eva = %(smp)s.eva
where 1=1
[[ and {{service_month}} ]]
group by dim_station.station_name, dim_station.eva
having sum(%(smp)s.observation_count) >= 30
   and sum(%(smp)s.non_canceled_observation_count) > 0
order by "Average Delay (min)" desc
limit 10''' % {"schema": MARTS_SCHEMA, "smp": smp},
        {"service_month": field_filter_tag("service_month", "Service Month", f[(smp, "service_month")], "date/month-year")},
        "bar",
        visualization_settings={"graph.dimensions": ["Station"], "graph.metrics": ["Average Delay (min)"], "graph.x_axis.scale": "ordinal"},
        description="Top 10 by weighted average delay. Requires >=30 observations AND at least one non-canceled observation (a 100%-canceled station has a NULL avg_delay_min, which would otherwise sort first under ORDER BY DESC).")

    c["worst_lines"] = create_card(db_id, "Worst Performing Lines / Train Types", '''select
  case when line_number is null then train_type else train_type || ' ' || line_number end as "Train Type / Line",
  train_type as "train_type",
  line_number as "line_number",
  round(sum(avg_delay_min * non_canceled_observation_count)::float
        / nullif(sum(non_canceled_observation_count), 0), 1) as "Average Delay (min)",
  sum(observation_count) as "Observation Count"
from %s.%s
where 1=1
[[ and {{service_month}} ]]
[[ and {{train_type}} ]]
[[ and {{line}} ]]
group by 1, train_type, line_number
having sum(observation_count) >= 30
   and sum(non_canceled_observation_count) > 0
order by "Average Delay (min)" desc
limit 10''' % (MARTS_SCHEMA, mlp), {
            "service_month": field_filter_tag("service_month", "Service Month", f[(mlp, "service_month")], "date/month-year"),
            "train_type": field_filter_tag("train_type", "Train Type", f[(mlp, "train_type")], "string/="),
            "line": field_filter_tag("line", "Line", f[(mlp, "line_number")], "string/="),
        }, "bar",
        visualization_settings={"graph.dimensions": ["Train Type / Line"], "graph.metrics": ["Average Delay (min)"], "graph.x_axis.scale": "ordinal"},
        description="Null line_number (long-distance types) shown as train_type alone, never discarded.")

    # --- Stations: KPIs ---
    c["s_kpi_avg"] = create_card(db_id, "Station KPI: Average Delay", '''select round(sum(avg_delay_min * non_canceled_observation_count)::float
       / nullif(sum(non_canceled_observation_count), 0), 1) as "Average Delay (min)"
     from %s.%s
     where {{station}}
     [[ and {{service_month}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    c["s_kpi_ontime"] = create_card(db_id, "Station KPI: On-Time Rate", '''select round(100.0 * sum(on_time_count) / nullif(sum(non_canceled_observation_count), 0), 1) as "On-Time Rate (%%)"
     from %s.%s
     where {{station}}
     [[ and {{service_month}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    c["s_kpi_cancel"] = create_card(db_id, "Station KPI: Cancellation Rate", '''select round(100.0 * sum(cancellation_count) / nullif(sum(observation_count), 0), 1) as "Cancellation Rate (%%)"
     from %s.%s
     where {{station}}
     [[ and {{service_month}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    c["s_kpi_obs"] = create_card(db_id, "Station KPI: Observation Count", '''select sum(observation_count) as "Observation Count"
     from %s.%s
     where {{station}}
     [[ and {{service_month}} ]]''' % (MARTS_SCHEMA, smp), month_station_tags(), "scalar")

    # median/p90: require station, resolve to selected month or latest
    # available month for that station — never averaged across months
    # (percentiles can't be correctly recombined). See walkthrough §5.1.
    ranked_sql = '''with ranked as (
    select
        *,
        row_number() over (
            partition by eva
            order by case when service_month = {{service_month_val}}::date then 0 else 1 end,
                     service_month desc
        ) as rn
    from %s.%s
    where {{station}}
)
select %%s as "%%s" from ranked where rn = 1''' % (MARTS_SCHEMA, smp)

    median_tags = {
        "station": field_filter_tag("station", "Station", f[(smp, "eva")], "string/="),
        "service_month_val": text_variable_tag("service_month_val", "Service Month", default="1900-01-01"),
    }
    c["s_kpi_median"] = create_card(db_id, "Station KPI: Median Delay",
        ranked_sql % ("median_delay_min", "Median Delay (min)"), median_tags, "scalar")
    c["s_kpi_p90"] = create_card(db_id, "Station KPI: P90 Delay",
        ranked_sql % ("p90_delay_min", "P90 Delay (min)"), median_tags, "scalar")

    # --- Stations: over-time charts (grain already matches the mart, no re-aggregation) ---
    c["s_trend_delay"] = create_card(db_id, "Station Average Delay Over Time", '''select service_month as "Month", avg_delay_min as "Average Delay (min)"
     from %s.%s
     where {{station}}
     order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["Average Delay (min)"]})

    c["s_trend_ontime"] = create_card(db_id, "Station On-Time Rate Over Time", '''select service_month as "Month", round(100.0 * on_time_rate, 1) as "On-Time Rate (%%)"
     from %s.%s
     where {{station}}
     order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["On-Time Rate (%)"]})

    c["s_trend_cancel"] = create_card(db_id, "Station Cancellation Rate Over Time", '''select service_month as "Month", round(100.0 * cancellation_rate, 1) as "Cancellation Rate (%%)"
     from %s.%s
     where {{station}}
     order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["Cancellation Rate (%)"]})

    c["s_trend_p90"] = create_card(db_id, "Station P90 Delay Over Time", '''select service_month as "Month", p90_delay_min as "P90 Delay (min)"
     from %s.%s
     where {{station}}
     order by service_month''' % (MARTS_SCHEMA, smp), station_only_tag(), "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["P90 Delay (min)"]},
        description="Real per-station-month values, one point per stored row.")

    # --- Trends: line-level ---
    c["train_type_trend"] = create_card(db_id, "Train Type Trend", '''select
    service_month as "Month",
    train_type as "Train Type",
    round(sum(avg_delay_min * non_canceled_observation_count)::float
          / nullif(sum(non_canceled_observation_count), 0), 2) as "Average Delay (min)"
  from %s.%s
  where 1=1
  [[ and {{train_type}} ]]
  group by service_month, train_type
  order by service_month, train_type''' % (MARTS_SCHEMA, mlp),
        {"train_type": field_filter_tag("train_type", "Train Type", f[(mlp, "train_type")], "string/=")}, "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["Average Delay (min)"], "series_settings.groupBy": "Train Type"},
        description="Weighted average delay per train_type per month, aggregated across that type's line_numbers.")

    c["line_trend"] = create_card(db_id, "Line Trend", '''select
    service_month as "Month",
    case when line_number is null then train_type else train_type || ' ' || line_number end as "Line",
    avg_delay_min as "Average Delay (min)"
  from %s.%s
  where 1=1
  [[ and {{train_type}} ]]
  [[ and {{line}} ]]
  order by service_month''' % (MARTS_SCHEMA, mlp), {
            "train_type": field_filter_tag("train_type", "Train Type", f[(mlp, "train_type")], "string/="),
            "line": field_filter_tag("line", "Line", f[(mlp, "line_number")], "string/="),
        }, "line",
        visualization_settings={"graph.dimensions": ["Month"], "graph.metrics": ["Average Delay (min)"]},
        description="Grain already matches the mart (one row per line per month). Respects the Train Type -> Line linked filter.")

    return c


def assemble_dashcards(tabs: dict[str, int], cards: dict[str, int]) -> list[dict]:
    t_overview, t_stations, t_trends = tabs["Overview"], tabs["Stations"], tabs["Trends"]

    return [
        # --- Overview ---
        {"id": -1, "card_id": cards["kpi_avg_delay"], "dashboard_tab_id": t_overview, "col": 0, "row": 0, "size_x": 6, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month"), pm_dim(PARAM_STATION, "station")]},
        {"id": -2, "card_id": cards["kpi_on_time"], "dashboard_tab_id": t_overview, "col": 6, "row": 0, "size_x": 6, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month"), pm_dim(PARAM_STATION, "station")]},
        {"id": -3, "card_id": cards["kpi_cancel"], "dashboard_tab_id": t_overview, "col": 12, "row": 0, "size_x": 6, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month"), pm_dim(PARAM_STATION, "station")]},
        {"id": -4, "card_id": cards["kpi_obs"], "dashboard_tab_id": t_overview, "col": 18, "row": 0, "size_x": 6, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month"), pm_dim(PARAM_STATION, "station")]},
        {"id": -5, "card_id": cards["trend_delay"], "dashboard_tab_id": t_overview, "col": 0, "row": 4, "size_x": 8, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_MONTH: "month"})}},
        {"id": -6, "card_id": cards["trend_ontime"], "dashboard_tab_id": t_overview, "col": 8, "row": 4, "size_x": 8, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_MONTH: "month"})}},
        {"id": -7, "card_id": cards["trend_cancel"], "dashboard_tab_id": t_overview, "col": 16, "row": 4, "size_x": 8, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_MONTH: "month"})}},
        {"id": -8, "card_id": cards["worst_stations"], "dashboard_tab_id": t_overview, "col": 0, "row": 10, "size_x": 12, "size_y": 7,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_STATION: "eva"})}},
        {"id": -9, "card_id": cards["worst_lines"], "dashboard_tab_id": t_overview, "col": 12, "row": 10, "size_x": 12, "size_y": 7,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month"), pm_dim(PARAM_TRAIN_TYPE, "train_type"), pm_dim(PARAM_LINE, "line")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_TRAIN_TYPE: "train_type", PARAM_LINE: "line_number"})}},

        # --- Stations ---
        {"id": -101, "card_id": cards["s_kpi_avg"], "dashboard_tab_id": t_stations, "col": 0, "row": 0, "size_x": 4, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station"), pm_dim(PARAM_MONTH, "service_month")]},
        {"id": -102, "card_id": cards["s_kpi_median"], "dashboard_tab_id": t_stations, "col": 4, "row": 0, "size_x": 4, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station"), pm_var(PARAM_MONTH, "service_month_val")]},
        {"id": -103, "card_id": cards["s_kpi_p90"], "dashboard_tab_id": t_stations, "col": 8, "row": 0, "size_x": 4, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station"), pm_var(PARAM_MONTH, "service_month_val")]},
        {"id": -104, "card_id": cards["s_kpi_ontime"], "dashboard_tab_id": t_stations, "col": 12, "row": 0, "size_x": 4, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station"), pm_dim(PARAM_MONTH, "service_month")]},
        {"id": -105, "card_id": cards["s_kpi_cancel"], "dashboard_tab_id": t_stations, "col": 16, "row": 0, "size_x": 4, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station"), pm_dim(PARAM_MONTH, "service_month")]},
        {"id": -106, "card_id": cards["s_kpi_obs"], "dashboard_tab_id": t_stations, "col": 20, "row": 0, "size_x": 4, "size_y": 4,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station"), pm_dim(PARAM_MONTH, "service_month")]},
        {"id": -107, "card_id": cards["s_trend_delay"], "dashboard_tab_id": t_stations, "col": 0, "row": 4, "size_x": 6, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")]},
        {"id": -108, "card_id": cards["s_trend_ontime"], "dashboard_tab_id": t_stations, "col": 6, "row": 4, "size_x": 6, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")]},
        {"id": -109, "card_id": cards["s_trend_cancel"], "dashboard_tab_id": t_stations, "col": 12, "row": 4, "size_x": 6, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")]},
        {"id": -110, "card_id": cards["s_trend_p90"], "dashboard_tab_id": t_stations, "col": 18, "row": 4, "size_x": 6, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")]},
        {"id": -111, "card_id": cards["worst_stations"], "dashboard_tab_id": t_stations, "col": 0, "row": 10, "size_x": 24, "size_y": 8,
         "parameter_mappings": [pm_dim(PARAM_MONTH, "service_month")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_STATION: "eva"})}},

        # --- Trends ---
        {"id": -201, "card_id": cards["trend_delay"], "dashboard_tab_id": t_trends, "col": 0, "row": 0, "size_x": 8, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_MONTH: "month"})}},
        {"id": -202, "card_id": cards["trend_ontime"], "dashboard_tab_id": t_trends, "col": 8, "row": 0, "size_x": 8, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_MONTH: "month"})}},
        {"id": -203, "card_id": cards["trend_cancel"], "dashboard_tab_id": t_trends, "col": 16, "row": 0, "size_x": 8, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")],
         "visualization_settings": {"click_behavior": crossfilter_click({PARAM_MONTH: "month"})}},
        {"id": -204, "card_id": cards["s_trend_delay"], "dashboard_tab_id": t_trends, "col": 0, "row": 6, "size_x": 12, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_STATION, "station")]},
        {"id": -205, "card_id": cards["train_type_trend"], "dashboard_tab_id": t_trends, "col": 12, "row": 6, "size_x": 12, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_TRAIN_TYPE, "train_type")]},
        {"id": -206, "card_id": cards["line_trend"], "dashboard_tab_id": t_trends, "col": 0, "row": 12, "size_x": 24, "size_y": 6,
         "parameter_mappings": [pm_dim(PARAM_TRAIN_TYPE, "train_type"), pm_dim(PARAM_LINE, "line")]},
    ]


def main() -> int:
    wait_for_health()

    db_id, metadata = setup_redshift_connection()
    field_map = build_field_id_map(metadata)
    setup_station_name_remap(field_map)

    dash_id, tabs = setup_dashboard_shell()
    if tabs is None:
        return 0  # already existed, nothing more to do

    cards = create_all_cards(db_id, field_map)
    dashcards = assemble_dashcards(tabs, cards)

    result = call("PUT", f"/api/dashboard/{dash_id}", {
        "dashcards": dashcards,
        "tabs": [{"id": tid, "name": name} for name, tid in tabs.items()],
    })
    logger.info("dashboard %r ready: %s cards across %s tabs", DASHBOARD_NAME,
                len(result.get("dashcards", [])), len(tabs))
    logger.info("open %s/dashboard/%s", METABASE_URL, dash_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
