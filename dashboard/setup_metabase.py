"""Reproduces the "Deutsche Bahn Operations Analytics" Metabase dashboard
from scratch against this project's Gold marts. This mirrors the dashboard
as it was hand-tuned in the Metabase UI (3 tabs, 31 cards, 5 linked
filters) — every card's SQL, filter widget, and layout position below was
captured from that live dashboard via the API, not written independently,
so this script and the UI stay in sync. If the dashboard changes in the
UI, re-run the capture (see docs/metabase-dashboard-walkthrough.md,
local-only) and update CARDS/TEXT_BLOCKS/setup_dashboard_shell to match —
this script does not read the live dashboard back.

Prerequisites (cannot be scripted — see dashboard/README.md):
1. `docker compose up -d metabase`, then complete Metabase's first-run
   setup at http://localhost:3000 (creates your admin account).
2. Generate an API key: Settings -> Admin settings -> Settings ->
   API Keys -> Create API Key.
3. Export METABASE_API_KEY (and the REDSHIFT_* vars from .env) into the
   environment this script runs in.
4. `dbt build` must have already created dim_service_month — it backs
   every Year/Month filter dropdown (and, being derived from
   mart_monthly_station_perf's actual distinct months, only ever offers
   months that have data).

Idempotent for the collection, database connection, and schema sync. NOT
idempotent for the dashboard itself — if a dashboard with the same name
already exists, this exits without changes rather than attempting a
partial rebuild/diff (out of scope for a setup script this size; delete
the existing dashboard in the UI first if you want a clean rebuild).

Run: python dashboard/setup_metabase.py
"""

from __future__ import annotations

import json
import logging
import os
import sys

from metabase_client import (
    MARTS_SCHEMA,
    METABASE_URL,
    build_field_id_map,
    call,
    create_card,
    field_filter_tag,
    get_or_create_collection,
    get_or_create_dashboard,
    get_or_create_database,
    pm_dim,
    sync_and_wait_for_tables,
    text_dashcard,
    wait_for_health,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_NAME = "Deutsche Bahn Operations Analytics"
DASHBOARD_DESCRIPTION = (
    "Monthly performance monitoring for Deutsche Bahn delays, punctuality, "
    "cancellations, stations, and train lines."
)
COLLECTION_NAME = "Deutsche Bahn Operations Analytics collection"
COLLECTION_DESCRIPTION = (
    "Analytics and reporting for Deutsche Bahn monthly delay, punctuality, "
    "cancellation, station, and train-line performance."
)
DATABASE_NAME = "Deutsche Bahn Gold"
EXPECTED_TABLES = {
    "dim_station", "dim_train_line", "dim_service_month",
    "mart_monthly_station_perf", "mart_monthly_line_perf",
}

PARAM_YEAR = "year001"
PARAM_MONTH = "month001"
PARAM_STATION = "station001"
PARAM_SERVICE_CATEGORY = "category001"
PARAM_LINE = "line001"

TAG_DISPLAY_NAME = {
    "service_year": "Service Year",
    "service_month": "Service Month",
    "station": "Station",
    "service_category": "Service Category",
    "line": "Line",
}

# tab logical-key -> tab title, matching the dashboard's actual tab names
TAB_TITLE = {
    "overview": "Overview",
    "stations": "Stations",
    "service_line": "Service & Line Performance",
}


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required — set it in your environment (see .env).")
    return value


def setup_redshift_connection() -> tuple[int, dict]:
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


def setup_dashboard_shell(collection_id: int) -> tuple[int, dict[str, int] | None]:
    dash_id, existed = get_or_create_dashboard(DASHBOARD_NAME, DASHBOARD_DESCRIPTION, collection_id)
    if existed:
        logger.warning(
            "Dashboard %r already exists (id=%s) — leaving it untouched. "
            "Delete it in the Metabase UI first if you want a full rebuild.",
            DASHBOARD_NAME, dash_id,
        )
        return dash_id, None

    result = call("PUT", f"/api/dashboard/{dash_id}", {
        "tabs": [
            {"id": -1, "name": "Overview"},
            {"id": -2, "name": "Stations"},
            {"id": -3, "name": "Service & Line Performance"},
        ],
        "dashcards": [],
        "parameters": [
            {"id": PARAM_YEAR, "name": "Year", "slug": "year", "type": "string/=", "sectionId": "string"},
            {"id": PARAM_MONTH, "name": "Month", "slug": "month", "type": "string/=", "sectionId": "string"},
            {"id": PARAM_STATION, "name": "Station", "slug": "station", "type": "string/=", "sectionId": "string",
             "default": ["Berlin Hauptbahnhof"]},
            {"id": PARAM_SERVICE_CATEGORY, "name": "Service Category", "slug": "service_category",
             "type": "string/=", "sectionId": "string", "required": True, "default": ["ICE"]},
            {"id": PARAM_LINE, "name": "Line", "slug": "line", "type": "string/=", "sectionId": "string",
             "filteringParameters": [PARAM_SERVICE_CATEGORY], "required": False, "default": []},
        ],
    })
    tabs = {t["name"]: t["id"] for t in result["tabs"]}
    return dash_id, tabs


# Every card's SQL, filter tags, description, display type, and dashboard
# layout position — captured verbatim from the live "Deutsche Bahn
# Operations Analytics" dashboard (see module docstring). `%(schema)s` is
# substituted with MARTS_SCHEMA at card-creation time; literal "%" in
# column aliases (e.g. "On-Time Rate (%)") is escaped as "%%" so it
# survives that substitution untouched.

CARDS = [
    dict(
        key='kpi_avg_delay', tab='overview', pos=(0, 2, 6, 3), live_id=50,
        name='Average Delay (min)',
        description='Weighted network average delay in minutes, excluding canceled observations.',
        display='smartscalar',
        tags=[
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'scalar.segments': [], 'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month
    FROM %(schema)s.dim_service_month AS dsm
    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

network_month_values AS (

    SELECT
        m.service_month,

        ROUND(
            SUM(
                m.avg_delay_min
                * m.non_canceled_observation_count
            )::DECIMAL
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS average_delay_min

    FROM %(schema)s.mart_monthly_station_perf AS m

    CROSS JOIN selected_period AS p

    WHERE m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    average_delay_min AS "Average Delay (min)"
FROM network_month_values
ORDER BY service_month;''',
    ),
    dict(
        key='kpi_ontime', tab='overview', pos=(6, 2, 6, 3), live_id=46,
        name='On-Time Rate (%)',
        description='Percentage of non-canceled train-stop observations with a delay below the on-time threshold.',
        display='smartscalar',
        tags=[
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'scalar.segments': []},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month
    FROM %(schema)s.dim_service_month AS dsm
    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

network_month_values AS (

    SELECT
        m.service_month,

        ROUND(
            100.0 * SUM(m.on_time_count)
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS on_time_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    CROSS JOIN selected_period AS p

    WHERE m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    on_time_rate AS "On-Time Rate (%%)"

FROM network_month_values

ORDER BY service_month;''',
    ),
    dict(
        key='kpi_cancel', tab='overview', pos=(12, 2, 6, 3), live_id=47,
        name='Cancellation Rate (%)',
        description='Percentage of train-stop observations that were canceled.',
        display='smartscalar',
        tags=[
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'scalar.segments': [], 'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month
    FROM %(schema)s.dim_service_month AS dsm
    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

network_month_values AS (

    SELECT
        m.service_month,

        ROUND(
            100.0 * SUM(m.cancellation_count)
            / NULLIF(SUM(m.observation_count), 0),
            1
        ) AS cancellation_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    CROSS JOIN selected_period AS p

    WHERE m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    cancellation_rate AS "Cancellation Rate (%%)"

FROM network_month_values

ORDER BY service_month;''',
    ),
    dict(
        key='kpi_activity', tab='overview', pos=(18, 2, 6, 3), live_id=48,
        name='Network Activity Volume',
        description='Shows the total number of station-level service observations analyzed across the Deutsche Bahn network for the selected service month.',
        display='scalar',
        tags=[
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'scalar.segments': []},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
)

SELECT
    SUM(m.observation_count) AS "Activity Volume"

FROM %(schema)s.mart_monthly_station_perf AS m

CROSS JOIN selected_period AS p

WHERE m.service_month = p.selected_month;''',
    ),
    dict(
        key='trend_delay', tab='overview', pos=(0, 7, 8, 5), live_id=51,
        name='Monthly Average Delay (min)',
        description='Shows the monthly weighted average delay across the Deutsche Bahn network, excluding canceled observations, to highlight how delay performance changes over time.',
        display='area',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'table.pivot_column': 'average delay (min)', 'table.cell_column': 'month', 'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['average delay (min)']},
        sql='''SELECT
    m.service_month AS "Month",
    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)"
FROM %(schema)s.mart_monthly_station_perf AS m
JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month
WHERE 1 = 1
[[AND {{service_year}}]]
GROUP BY m.service_month
ORDER BY m.service_month;''',
    ),
    dict(
        key='trend_ontime', tab='overview', pos=(8, 7, 8, 5), live_id=52,
        name='Monthly On-Time Rate (%)',
        description='Shows the monthly percentage of non-canceled train stops that were on time, highlighting changes in network punctuality throughout the selected year.',
        display='area',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['on-time rate (%)']},
        sql='''SELECT
    m.service_month AS "Month",
    ROUND(
        100.0 * SUM(m.on_time_count)
        / NULLIF(SUM(m.non_canceled_observation_count), 0),
        1
    ) AS "On-Time Rate (%%)"
FROM %(schema)s.mart_monthly_station_perf AS m
JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month
WHERE 1 = 1
[[AND {{service_year}}]]
GROUP BY m.service_month
ORDER BY m.service_month;''',
    ),
    dict(
        key='trend_cancel', tab='overview', pos=(16, 7, 8, 5), live_id=53,
        name='Monthly Cancellation Rate (%)',
        description='Shows the monthly percentage of train-stop observations that were canceled, highlighting changes in network reliability throughout the selected year.',
        display='area',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        ],
        viz_settings={'series_settings': {'cancellation rate (%)': {'line.interpolate': 'linear'}}, 'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['cancellation rate (%)']},
        sql='''SELECT
    m.service_month AS "Month",
    ROUND(
        100.0 * SUM(m.cancellation_count)
        / NULLIF(SUM(m.observation_count), 0),
        1
    ) AS "Cancellation Rate (%%)"
FROM %(schema)s.mart_monthly_station_perf AS m
JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month
WHERE 1 = 1
[[AND {{service_year}}]]
GROUP BY m.service_month
ORDER BY m.service_month;''',
    ),
    dict(
        key='worst_stations', tab='overview', pos=(0, 14, 12, 7), live_id=54,
        name='Highest-Delay Stations (min)',
        description='Shows the 10 stations with the highest weighted average delay for the selected service month. Stations with fewer than 30 observations are excluded.',
        display='row',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        ],
        viz_settings={'graph.show_goal': False, 'graph.show_values': False, 'graph.series_order_dimension': None, 'graph.x_axis.axis_enabled': True, 'graph.metrics': ['average delay (min)'], 'graph.label_value_formatting': 'full', 'graph.series_order': None, 'graph.x_axis.scale': 'ordinal', 'graph.dimensions': ['station'], 'stackable.stack_type': None},
        dc_viz_override={'graph.show_goal': False, 'graph.show_values': False, 'graph.series_order_dimension': None, 'graph.x_axis.axis_enabled': True, 'graph.label_value_formatting': 'full', 'stackable.stack_type': None, 'series_settings': {'average delay (min)': {'color': '#7172AD'}}},
        sql='''SELECT
    s.station_name AS "Station",
    m.eva AS "Station EVA",
    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)",
    SUM(m.observation_count) AS "Service Stops Analyzed"

FROM %(schema)s.mart_monthly_station_perf AS m

JOIN %(schema)s.dim_station AS s
    ON m.eva = s.eva

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{service_month}}]]

GROUP BY
    s.station_name,
    m.eva

HAVING
    SUM(m.observation_count) >= 30
    AND SUM(m.non_canceled_observation_count) > 0

ORDER BY "Average Delay (min)" DESC

LIMIT 10;''',
    ),
    dict(
        key='worst_services', tab='overview', pos=(12, 14, 12, 7), live_id=55,
        name='Highest-Delay Services & Lines',
        description='Shows the 10 service categories or lines with the highest weighted average delay for the selected service month. Entries with fewer than 30 service-stop observations are excluded.',
        display='row',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        ],
        viz_settings={'graph.dimensions': ['service / line'], 'series_settings': {'average delay (min)': {'color': '#88BF4D'}}, 'graph.metrics': ['average delay (min)']},
        sql='''SELECT
    CASE
        WHEN m.line_number IS NULL OR TRIM(m.line_number) = ''
            THEN m.train_type
        ELSE m.train_type || ' ' || m.line_number
    END AS "Service / Line",

    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)",

    SUM(m.observation_count) AS "Service Stops Analyzed"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{service_month}}]]

GROUP BY
    m.train_type,
    m.line_number

HAVING
    SUM(m.observation_count) >= 30
    AND SUM(m.non_canceled_observation_count) > 0

ORDER BY "Average Delay (min)" DESC

LIMIT 10;''',
    ),
    dict(
        key='cat_avg_delay', tab='overview', pos=(0, 23, 24, 6), live_id=72,
        name='Average Delay by Service Category (min)',
        description='Compares the weighted average delay across all service categories for the selected service month. Categories with fewer than 30 observations are excluded.',
        display='bar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        ],
        viz_settings={'graph.dimensions': ['service category'], 'graph.x_axis.scale': 'ordinal', 'graph.metrics': ['average delay (min)']},
        sql='''SELECT
    m.train_type AS "Service Category",

    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{service_month}}]]

GROUP BY
    m.train_type

HAVING
    SUM(m.observation_count) >= 30
    AND SUM(m.non_canceled_observation_count) > 0

ORDER BY
    "Average Delay (min)" DESC;''',
    ),
    dict(
        key='cat_cancel', tab='overview', pos=(0, 29, 24, 6), live_id=74,
        name='Cancellation Rate by Service Category (%)',
        description='Compares cancellation rates across all service categories for the selected service month. Categories with fewer than 30 observations are excluded.',
        display='bar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        ],
        viz_settings={'graph.x_axis.scale': 'ordinal', 'graph.dimensions': ['service category'], 'graph.metrics': ['cancellation rate (%)']},
        sql='''SELECT
    m.train_type AS "Service Category",

    ROUND(
        100.0 * SUM(m.cancellation_count)
        / NULLIF(
            SUM(m.observation_count),
            0
        ),
        1
    ) AS "Cancellation Rate (%%)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{service_month}}]]

GROUP BY
    m.train_type

HAVING
    SUM(m.observation_count) >= 30

ORDER BY
    "Cancellation Rate (%%)" DESC;''',
    ),
    dict(
        key='cat_ontime', tab='overview', pos=(0, 35, 24, 6), live_id=73,
        name='On-Time Rate by Service Category (%)',
        description='Compares the on-time rate across all service categories for the selected service month. Canceled observations are excluded from the rate, and categories with fewer than 30 observations are excluded.',
        display='bar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        ],
        viz_settings={'graph.x_axis.scale': 'ordinal', 'graph.dimensions': ['service category'], 'graph.metrics': ['on-time rate (%)']},
        sql='''SELECT
    m.train_type AS "Service Category",

    ROUND(
        100.0 * SUM(m.on_time_count)
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "On-Time Rate (%%)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{service_month}}]]

GROUP BY
    m.train_type

HAVING
    SUM(m.observation_count) >= 30
    AND SUM(m.non_canceled_observation_count) > 0

ORDER BY
    "On-Time Rate (%%)" ASC;''',
    ),
    dict(
        key='svc_kpi_avg', tab='service_line', pos=(0, 2, 6, 3), live_id=67,
        name='Service Average Delay (min)',
        description='Shows the selected service category’s average delay for the selected month and compares it with the previous month. The average is weighted by non-canceled observations.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
)

SELECT
    m.service_month AS "Month",

    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)"

FROM %(schema)s.mart_monthly_line_perf AS m

CROSS JOIN selected_period AS p

WHERE
    m.service_month >= DATEADD(month, -1, p.selected_month)
    AND m.service_month <= p.selected_month

[[AND {{service_category}}]]

GROUP BY
    m.service_month

ORDER BY
    m.service_month;''',
    ),
    dict(
        key='svc_kpi_ontime', tab='service_line', pos=(6, 2, 6, 3), live_id=68,
        name='Service On-Time Rate (%)',
        description='Shows the selected service category’s on-time rate for the selected month and compares it with the previous month. Canceled observations are excluded from the on-time-rate denominator.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
)

SELECT
    m.service_month AS "Month",

    ROUND(
        100.0 * SUM(m.on_time_count)
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "On-Time Rate (%%)"

FROM %(schema)s.mart_monthly_line_perf AS m

CROSS JOIN selected_period AS p

WHERE
    m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

[[AND {{service_category}}]]

GROUP BY
    m.service_month

ORDER BY
    m.service_month;''',
    ),
    dict(
        key='svc_kpi_cancel', tab='service_line', pos=(12, 2, 6, 3), live_id=69,
        name='Service Cancellation Rate (%)',
        description='Shows the selected service category’s cancellation rate for the selected month and compares it with the previous month.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
)

SELECT
    m.service_month AS "Month",

    ROUND(
        100.0 * SUM(m.cancellation_count)
        / NULLIF(
            SUM(m.observation_count),
            0
        ),
        1
    ) AS "Cancellation Rate (%%)"

FROM %(schema)s.mart_monthly_line_perf AS m

CROSS JOIN selected_period AS p

WHERE
    m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

[[AND {{service_category}}]]

GROUP BY
    m.service_month

ORDER BY
    m.service_month;''',
    ),
    dict(
        key='svc_kpi_activity', tab='service_line', pos=(18, 2, 6, 3), live_id=70,
        name='Service Activity Volume',
        description='Shows the number of service-stop observations analyzed for the selected service category and service month.',
        display='scalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={'scalar.segments': []},
        sql='''SELECT
    SUM(m.observation_count) AS "Service Activity Volume"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{service_month}}]]
[[AND {{service_category}}]];''',
    ),
    dict(
        key='line_avg_delay', tab='service_line', pos=(0, 7, 12, 6), live_id=75,
        name='Average Delay by Line (min)',
        description='Shows the lines with the highest weighted average delay within the selected service category and service month. Only lines with a valid line number and at least 30 observations are included.',
        display='bar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={'graph.x_axis.scale': 'ordinal', 'graph.dimensions': ['line'], 'graph.metrics': ['average delay (min)']},
        sql='''SELECT
    m.line_number AS "Line",

    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1

[[AND {{service_year}}]]
[[AND {{service_month}}]]
[[AND {{service_category}}]]

-- Line-level analysis requires a real line number
AND m.line_number IS NOT NULL
AND TRIM(m.line_number) <> ''

GROUP BY
    m.line_number

HAVING
    SUM(m.observation_count) >= 30
    AND SUM(m.non_canceled_observation_count) > 0

ORDER BY
    "Average Delay (min)" DESC

LIMIT 15;''',
    ),
    dict(
        key='line_cancel', tab='service_line', pos=(12, 7, 12, 6), live_id=76,
        name='Cancellation Rate by Line (%)',
        description='Shows the lines with the highest cancellation rates within the selected service category and service month. Only lines with a valid line number and at least 30 observations are included.',
        display='bar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/=', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={'graph.x_axis.scale': 'ordinal', 'graph.dimensions': ['line'], 'graph.metrics': ['cancellation rate (%)']},
        sql='''SELECT
    m.line_number AS "Line",

    ROUND(
        100.0 * SUM(m.cancellation_count)
        / NULLIF(
            SUM(m.observation_count),
            0
        ),
        1
    ) AS "Cancellation Rate (%%)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1

[[AND {{service_year}}]]
[[AND {{service_month}}]]
[[AND {{service_category}}]]

AND m.line_number IS NOT NULL
AND TRIM(m.line_number) <> ''

GROUP BY
    m.line_number

HAVING
    SUM(m.observation_count) >= 30

ORDER BY
    "Cancellation Rate (%%)" DESC

LIMIT 15;''',
    ),
    dict(
        key='line_details', tab='service_line', pos=(0, 13, 24, 6), live_id=77,
        name='Line Performance Details',
        description='Provides detailed delay, punctuality, cancellation, and activity metrics for all valid lines within the selected service category and month. Lines with fewer than 30 observations are excluded.\nFor categories whose line_number is NULL or blank, the table should simply return no rows.',
        display='table',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        ],
        viz_settings={'table.pivot_column': 'on-time rate (%)', 'table.cell_column': 'average delay (min)'},
        sql='''SELECT
    m.line_number AS "Line",

    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)",

    ROUND(
        100.0 * SUM(m.on_time_count)
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "On-Time Rate (%%)",

    ROUND(
        100.0 * SUM(m.cancellation_count)
        / NULLIF(
            SUM(m.observation_count),
            0
        ),
        1
    ) AS "Cancellation Rate (%%)",

    SUM(m.observation_count) AS "Activity Volume"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1

[[AND {{service_year}}]]
[[AND {{service_month}}]]
[[AND {{service_category}}]]

AND m.line_number IS NOT NULL
AND TRIM(m.line_number) <> ''

GROUP BY
    m.line_number

HAVING
    SUM(m.observation_count) >= 30

ORDER BY
    "Average Delay (min)" DESC;''',
    ),
    dict(
        key='svc_trend_delay', tab='service_line', pos=(0, 21, 12, 6), live_id=78,
        name='Average Delay Over Time',
        description='Tracks weighted average delay by month for the selected service category. When one or more lines are selected, the trend is restricted to those lines; otherwise, all observations in the selected category are included.',
        display='area',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/=', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        dict(tag='line', table='dim_train_line', column='line_number', widget='string/contains', alias='m.line_number', default=None, required=None, param=PARAM_LINE),
        ],
        viz_settings={'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['average delay (min)']},
        sql='''SELECT
    m.service_month AS "Month",

    ROUND(
        SUM(
            m.avg_delay_min
            * m.non_canceled_observation_count
        )::DECIMAL
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "Average Delay (min)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1

[[AND {{service_year}}]]
[[AND {{service_category}}]]
[[AND {{line}}]]

GROUP BY
    m.service_month

HAVING
    SUM(m.non_canceled_observation_count) > 0

ORDER BY
    m.service_month;''',
    ),
    dict(
        key='svc_trend_reliability', tab='service_line', pos=(12, 21, 12, 6), live_id=79,
        name='Reliability Over Time',
        description='Tracks monthly on-time and cancellation rates for the selected service category. When one or more lines are selected, the trend is restricted to those lines; otherwise, the full selected category is analyzed.',
        display='area',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_category', table='dim_train_line', column='train_type', widget='string/contains', alias='m.train_type', default=['RE'], required=True, param=PARAM_SERVICE_CATEGORY),
        dict(tag='line', table='dim_train_line', column='line_number', widget='string/contains', alias='m.line_number', default=None, required=None, param=PARAM_LINE),
        ],
        viz_settings={'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['on-time rate (%)', 'cancellation rate (%)']},
        sql='''SELECT
    m.service_month AS "Month",

    ROUND(
        100.0 * SUM(m.on_time_count)
        / NULLIF(
            SUM(m.non_canceled_observation_count),
            0
        ),
        1
    ) AS "On-Time Rate (%%)",

    ROUND(
        100.0 * SUM(m.cancellation_count)
        / NULLIF(
            SUM(m.observation_count),
            0
        ),
        1
    ) AS "Cancellation Rate (%%)"

FROM %(schema)s.mart_monthly_line_perf AS m

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1

[[AND {{service_year}}]]
[[AND {{service_category}}]]
[[AND {{line}}]]

GROUP BY
    m.service_month

ORDER BY
    m.service_month;''',
    ),
    dict(
        key='s_kpi_avg', tab='stations', pos=(0, 2, 8, 3), live_id=56,
        name='Station Average Delay (min)',
        description='Weighted average delay in minutes for the selected station and service period, excluding canceled train stops.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['München Hbf'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'scalar.segments': [], 'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

station_months AS (

    SELECT
        m.service_month,
        ROUND(
            SUM(
                m.avg_delay_min
                * m.non_canceled_observation_count
            )::DECIMAL
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS average_delay_min

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    CROSS JOIN selected_period AS p

    WHERE 1 = 1
    [[AND {{station}}]]
    AND m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    average_delay_min AS "Average Delay (min)"

FROM station_months

ORDER BY service_month;''',
    ),
    dict(
        key='s_kpi_median', tab='stations', pos=(8, 2, 8, 3), live_id=60,
        name='Station Median Delay (min)',
        description='Shows the median delay for the selected station and service month, compared with the previous month when data is available.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['München Hbf'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

station_month_values AS (

    SELECT
        m.service_month,
        MAX(m.median_delay_min) AS median_delay_min

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    CROSS JOIN selected_period AS p

    WHERE 1 = 1
    [[AND {{station}}]]

    AND m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    median_delay_min AS "Median Delay (min)"

FROM station_month_values

ORDER BY service_month;''',
    ),
    dict(
        key='s_kpi_p90', tab='stations', pos=(16, 2, 8, 3), live_id=61,
        name='90% Delay Threshold (min)',
        description='90% of train stops at the selected station had a delay at or below this number of minutes.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['München Hbf'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

station_month_values AS (

    SELECT
        m.service_month,
        MAX(m.p90_delay_min) AS p90_delay_min

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    CROSS JOIN selected_period AS p

    WHERE 1 = 1
    [[AND {{station}}]]

    AND m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    p90_delay_min AS "P90 Delay (min)"

FROM station_month_values

ORDER BY service_month;''',
    ),
    dict(
        key='s_kpi_ontime', tab='stations', pos=(0, 5, 8, 3), live_id=57,
        name='Station On-Time Rate (%)',
        description='Shows the on-time rate for the selected station and service month, compared with the previous month when data is available.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['München Hbf'], required=True, param=PARAM_STATION),
        ],
        viz_settings={},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month
    FROM %(schema)s.dim_service_month AS dsm
    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

station_month_values AS (

    SELECT
        m.service_month,

        ROUND(
            100.0 * SUM(m.on_time_count)
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS on_time_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    CROSS JOIN selected_period AS p

    WHERE 1 = 1
    [[AND {{station}}]]

    AND m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    on_time_rate AS "On-Time Rate (%%)"

FROM station_month_values

ORDER BY service_month;''',
    ),
    dict(
        key='s_kpi_cancel', tab='stations', pos=(8, 5, 8, 3), live_id=58,
        name='Station Cancellation Rate (%)',
        description='Shows the cancellation rate for the selected station and service month, compared with the previous month when data is available.',
        display='smartscalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['München Hbf'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'scalar.switch_positive_negative': True},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month
    FROM %(schema)s.dim_service_month AS dsm
    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
),

station_month_values AS (

    SELECT
        m.service_month,

        ROUND(
            100.0 * SUM(m.cancellation_count)
            / NULLIF(SUM(m.observation_count), 0),
            1
        ) AS cancellation_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    CROSS JOIN selected_period AS p

    WHERE 1 = 1
    [[AND {{station}}]]

    AND m.service_month IN (
        p.selected_month,
        DATEADD(month, -1, p.selected_month)
    )

    GROUP BY m.service_month
)

SELECT
    service_month AS "Month",
    cancellation_rate AS "Cancellation Rate (%%)"

FROM station_month_values

ORDER BY service_month;''',
    ),
    dict(
        key='s_kpi_activity', tab='stations', pos=(16, 5, 8, 3), live_id=59,
        name='Station Activity Volume',
        description='Shows the number of stop observations for the selected station and service month, compared with the previous month when data is available.',
        display='scalar',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='service_month', table='dim_service_month', column='month_label', widget='string/=', alias='dsm.month_label', default=['01 - Jan'], required=True, param=PARAM_MONTH),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['Berlin Hauptbahnhof'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'scalar.comparisons': [{'id': '2bb35263-9143-4375-a5fa-960a8f2f88b8', 'type': 'previousValue'}], 'scalar.compact_primary_number': False, 'scalar.segments': []},
        sql='''WITH selected_period AS (

    SELECT
        MAX(dsm.service_month) AS selected_month

    FROM %(schema)s.dim_service_month AS dsm

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{service_month}}]]
)

SELECT
    SUM(m.observation_count) AS "Train Stops Analyzed"

FROM %(schema)s.mart_monthly_station_perf AS m

JOIN %(schema)s.dim_station AS s
    ON m.eva = s.eva

CROSS JOIN selected_period AS p

WHERE 1 = 1
[[AND {{station}}]]

AND m.service_month = p.selected_month;''',
    ),
    dict(
        key='s_vs_network_delay', tab='stations', pos=(0, 10, 12, 6), live_id=62,
        name='Station vs Network Average Delay',
        description='Compares the selected station’s monthly weighted average delay with the Deutsche Bahn network average throughout the selected year.',
        display='area',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['Berlin Hauptbahnhof'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['selected station', 'network average']},
        dc_viz_override={'visualization': {'display': 'line', 'columnValuesMapping': {'COLUMN_1': [{'sourceId': 'card:62', 'originalName': 'month', 'name': 'COLUMN_1'}], 'COLUMN_2': [{'sourceId': 'card:62', 'originalName': 'selected station', 'name': 'COLUMN_2'}], 'COLUMN_3': [{'sourceId': 'card:62', 'originalName': 'network average', 'name': 'COLUMN_3'}]}, 'settings': {'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['COLUMN_1'], 'graph.metrics': ['COLUMN_2', 'COLUMN_3'], 'series_settings': {'COLUMN_3': {'title': 'db network average'}}, 'card.title': 'Station vs Network Average Delay (min)'}}},
        sql='''WITH station_delay AS (

    SELECT
        m.service_month,
        ROUND(
            SUM(
                m.avg_delay_min
                * m.non_canceled_observation_count
            )::DECIMAL
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS average_delay_min

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    JOIN %(schema)s.dim_service_month AS dsm
        ON m.service_month = dsm.service_month

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{station}}]]

    GROUP BY m.service_month
),

network_delay AS (

    SELECT
        m.service_month,
        ROUND(
            SUM(
                m.avg_delay_min
                * m.non_canceled_observation_count
            )::DECIMAL
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS average_delay_min

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_service_month AS dsm
        ON m.service_month = dsm.service_month

    WHERE 1 = 1
    [[AND {{service_year}}]]

    GROUP BY m.service_month
)

SELECT
    s.service_month AS "Month",
    s.average_delay_min AS "Selected Station",
    n.average_delay_min AS "Network Average"

FROM station_delay AS s

JOIN network_delay AS n
    ON s.service_month = n.service_month

ORDER BY s.service_month;''',
    ),
    dict(
        key='s_vs_network_ontime', tab='stations', pos=(12, 10, 12, 6), live_id=63,
        name='Station vs Network On-Time Rate',
        description='Compares the selected station’s monthly on-time rate with the Deutsche Bahn network average throughout the selected year.',
        display='line',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['Berlin Hauptbahnhof'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['selected station', 'network average']},
        dc_viz_override={'visualization': {'display': 'line', 'columnValuesMapping': {'COLUMN_1': [{'sourceId': 'card:63', 'originalName': 'month', 'name': 'COLUMN_1'}], 'COLUMN_2': [{'sourceId': 'card:63', 'originalName': 'selected station', 'name': 'COLUMN_2'}], 'COLUMN_3': [{'sourceId': 'card:63', 'originalName': 'network average', 'name': 'COLUMN_3'}]}, 'settings': {'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['COLUMN_1'], 'graph.metrics': ['COLUMN_2', 'COLUMN_3'], 'series_settings': {'COLUMN_2': {'color': '#7172AD'}, 'COLUMN_3': {'color': '#F2A86F', 'title': 'db network average'}}, 'card.title': 'Station vs Network On-Time Rate (%)'}}},
        sql='''WITH station_rate AS (

    SELECT
        m.service_month,
        ROUND(
            100.0 * SUM(m.on_time_count)
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS on_time_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    JOIN %(schema)s.dim_service_month AS dsm
        ON m.service_month = dsm.service_month

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{station}}]]

    GROUP BY m.service_month
),

network_rate AS (

    SELECT
        m.service_month,
        ROUND(
            100.0 * SUM(m.on_time_count)
            / NULLIF(
                SUM(m.non_canceled_observation_count),
                0
            ),
            1
        ) AS on_time_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_service_month AS dsm
        ON m.service_month = dsm.service_month

    WHERE 1 = 1
    [[AND {{service_year}}]]

    GROUP BY m.service_month
)

SELECT
    s.service_month AS "Month",
    s.on_time_rate AS "Selected Station",
    n.on_time_rate AS "Network Average"

FROM station_rate AS s

JOIN network_rate AS n
    ON s.service_month = n.service_month

ORDER BY s.service_month;''',
    ),
    dict(
        key='s_vs_network_cancel', tab='stations', pos=(0, 16, 12, 6), live_id=64,
        name='Station vs Network Cancellation Rate',
        description='Compares the selected station’s monthly cancellation rate with the Deutsche Bahn network average throughout the selected year.',
        display='line',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['Berlin Hauptbahnhof'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'series_settings': {'network average': {'title': 'db network average'}}, 'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['selected station', 'network average']},
        dc_viz_override={'visualization': {'display': 'line', 'columnValuesMapping': {'COLUMN_1': [{'sourceId': 'card:64', 'originalName': 'month', 'name': 'COLUMN_1'}], 'COLUMN_2': [{'sourceId': 'card:64', 'originalName': 'selected station', 'name': 'COLUMN_2'}], 'COLUMN_3': [{'sourceId': 'card:64', 'originalName': 'network average', 'name': 'COLUMN_3'}]}, 'settings': {'series_settings': {'COLUMN_3': {'title': 'db network average'}}, 'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['COLUMN_1'], 'graph.metrics': ['COLUMN_2', 'COLUMN_3'], 'card.title': 'Station vs Network Cancellation Rate (%)'}}},
        sql='''WITH station_rate AS (

    SELECT
        m.service_month,
        ROUND(
            100.0 * SUM(m.cancellation_count)
            / NULLIF(SUM(m.observation_count), 0),
            1
        ) AS cancellation_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_station AS s
        ON m.eva = s.eva

    JOIN %(schema)s.dim_service_month AS dsm
        ON m.service_month = dsm.service_month

    WHERE 1 = 1
    [[AND {{service_year}}]]
    [[AND {{station}}]]

    GROUP BY m.service_month
),

network_rate AS (

    SELECT
        m.service_month,
        ROUND(
            100.0 * SUM(m.cancellation_count)
            / NULLIF(SUM(m.observation_count), 0),
            1
        ) AS cancellation_rate

    FROM %(schema)s.mart_monthly_station_perf AS m

    JOIN %(schema)s.dim_service_month AS dsm
        ON m.service_month = dsm.service_month

    WHERE 1 = 1
    [[AND {{service_year}}]]

    GROUP BY m.service_month
)

SELECT
    s.service_month AS "Month",
    s.cancellation_rate AS "Selected Station",
    n.cancellation_rate AS "Network Average"

FROM station_rate AS s

JOIN network_rate AS n
    ON s.service_month = n.service_month

ORDER BY s.service_month;''',
    ),
    dict(
        key='s_delay_profile', tab='stations', pos=(12, 16, 12, 6), live_id=65,
        name='Station Delay Profile Over Time',
        description='Shows how the selected station’s average, median, and 90% delay threshold change throughout the selected year.',
        display='line',
        tags=[
        dict(tag='service_year', table='dim_service_month', column='service_year', widget='string/=', alias='dsm.service_year', default=['2026'], required=True, param=PARAM_YEAR),
        dict(tag='station', table='dim_station', column='station_name', widget='string/contains', alias='s.station_name', default=['Berlin Hauptbahnhof'], required=True, param=PARAM_STATION),
        ],
        viz_settings={'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['month'], 'graph.metrics': ['average delay', 'median delay', '90% delay threshold']},
        dc_viz_override={'visualization': {'display': 'line', 'columnValuesMapping': {'COLUMN_1': [{'sourceId': 'card:65', 'originalName': 'month', 'name': 'COLUMN_1'}], 'COLUMN_2': [{'sourceId': 'card:65', 'originalName': 'average delay', 'name': 'COLUMN_2'}], 'COLUMN_3': [{'sourceId': 'card:65', 'originalName': 'median delay', 'name': 'COLUMN_3'}], 'COLUMN_4': [{'sourceId': 'card:65', 'originalName': '90% delay threshold', 'name': 'COLUMN_4'}]}, 'settings': {'graph.x_axis.scale': 'timeseries', 'graph.dimensions': ['COLUMN_1'], 'graph.metrics': ['COLUMN_2', 'COLUMN_3', 'COLUMN_4'], 'card.title': 'Station Delay Profile Over Time (min)'}}},
        sql='''SELECT
    m.service_month AS "Month",
    m.avg_delay_min AS "Average Delay",
    m.median_delay_min AS "Median Delay",
    m.p90_delay_min AS "90%% Delay Threshold"

FROM %(schema)s.mart_monthly_station_perf AS m

JOIN %(schema)s.dim_station AS s
    ON m.eva = s.eva

JOIN %(schema)s.dim_service_month AS dsm
    ON m.service_month = dsm.service_month

WHERE 1 = 1
[[AND {{service_year}}]]
[[AND {{station}}]]

ORDER BY m.service_month;''',
    ),
]

TEXT_BLOCKS = [
    dict(tab='overview', kind='heading', pos=(0, 0, 24, 1), text='Network Performance'),
    dict(tab='overview', kind='text', pos=(0, 1, 24, 1), text='Monitor Deutsche Bahn network performance for the selected service period and compare key indicators with the previous month.'),
    dict(tab='overview', kind='heading', pos=(0, 5, 24, 1), text='Performance Over Time'),
    dict(tab='overview', kind='text', pos=(0, 6, 24, 1), text='Track how average delay, punctuality, and cancellations change from month to month.'),
    dict(tab='overview', kind='heading', pos=(0, 12, 24, 1), text='Performance Hotspots'),
    dict(tab='overview', kind='text', pos=(0, 13, 24, 1), text='Identify stations and services with the highest average delays for the selected service period.'),
    dict(tab='overview', kind='heading', pos=(0, 21, 24, 1), text='Performance by Service Category'),
    dict(tab='overview', kind='text', pos=(0, 22, 24, 1), text='Compare delay, punctuality, and cancellations across Deutsche Bahn service categories for the selected service month.'),
    dict(tab='stations', kind='heading', pos=(0, 0, 24, 1), text='Station Performance'),
    dict(tab='stations', kind='text', pos=(0, 1, 24, 1), text='Analyze delay, punctuality, cancellations, and performance trends for an individual Deutsche Bahn station.'),
    dict(tab='stations', kind='heading', pos=(0, 8, 24, 1), text='Station Performance Over Time'),
    dict(tab='stations', kind='text', pos=(0, 9, 24, 1), text='Compare the selected station with the network across the selected year. The Month filter applies to the snapshot KPIs above, while these charts show the full year.'),
    dict(tab='service_line', kind='heading', pos=(0, 0, 24, 1), text='Selected Service Category Performance'),
    dict(tab='service_line', kind='text', pos=(0, 1, 24, 1), text='Analyze delay, punctuality, cancellations, and activity for the selected service category during the selected service month.'),
    dict(tab='service_line', kind='heading', pos=(0, 5, 24, 1), text='Line Performance'),
    dict(tab='service_line', kind='text', pos=(0, 6, 24, 1), text='Compare individual lines within the selected service category and identify lines with elevated delays or cancellation rates.'),
    dict(tab='service_line', kind='heading', pos=(0, 19, 24, 1), text='Service & Line Performance Over Time'),
    dict(tab='service_line', kind='text', pos=(0, 20, 24, 1), text='Track monthly delay and reliability trends for the selected service category or selected line across the chosen year.'),
]


def create_all_cards(db_id: int, f: dict[tuple[str, str], int]) -> dict[str, int]:
    """f: field id map. Returns {logical_key: card_id}."""
    cards = {}
    for spec in CARDS:
        tags = {
            t["tag"]: field_filter_tag(
                t["tag"], TAG_DISPLAY_NAME[t["tag"]], f[(t["table"], t["column"])], t["widget"],
                alias=t["alias"], default=t["default"], required=t["required"],
            )
            for t in spec["tags"]
        }
        sql = spec["sql"] % {"schema": MARTS_SCHEMA}
        cards[spec["key"]] = create_card(
            db_id, spec["name"], sql, tags, spec["display"],
            visualization_settings=spec["viz_settings"], description=spec["description"],
        )
    return cards


def assemble_dashcards(tabs: dict[str, int], cards: dict[str, int]) -> list[dict]:
    dashcards = []
    next_id = -1

    for block in TEXT_BLOCKS:
        col, row, size_x, size_y = block["pos"]
        dashcards.append(text_dashcard(
            next_id, tabs[TAB_TITLE[block["tab"]]], col, row, size_x, size_y,
            block["text"], heading=(block["kind"] == "heading"),
        ))
        next_id -= 1

    for spec in CARDS:
        col, row, size_x, size_y = spec["pos"]
        card_id = cards[spec["key"]]
        dashcard = {
            "id": next_id, "card_id": card_id, "dashboard_tab_id": tabs[TAB_TITLE[spec["tab"]]],
            "col": col, "row": row, "size_x": size_x, "size_y": size_y,
            "parameter_mappings": [pm_dim(t["param"], t["tag"]) for t in spec["tags"]],
        }
        if "dc_viz_override" in spec:
            # The multi-series "visualizer" charts (station-vs-network,
            # delay-profile) embed their own card_id as a sourceId inside
            # visualization_settings — rewrite the captured placeholder id
            # to the id actually assigned to this freshly created card.
            raw = json.dumps(spec["dc_viz_override"])
            raw = raw.replace(f'"card:{spec["live_id"]}"', f'"card:{card_id}"')
            dashcard["visualization_settings"] = json.loads(raw)
        dashcards.append(dashcard)
        next_id -= 1

    return dashcards


def main() -> int:
    wait_for_health()

    db_id, metadata = setup_redshift_connection()
    field_map = build_field_id_map(metadata)
    setup_station_name_remap(field_map)

    collection_id = get_or_create_collection(COLLECTION_NAME, COLLECTION_DESCRIPTION)
    dash_id, tabs = setup_dashboard_shell(collection_id)
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
