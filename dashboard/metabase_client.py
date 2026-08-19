"""Thin REST client for scripting Metabase setup against this project's
Gold marts. Stdlib only (urllib) — no new dependency for a handful of
HTTP calls. Not a general-purpose Metabase SDK; only the calls
setup_metabase.py actually needs.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
METABASE_API_KEY = os.environ.get("METABASE_API_KEY")

MARTS_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "dbt_dev") + "_marts"


def call(method: str, path: str, body: dict | None = None) -> dict | list | None:
    if not METABASE_API_KEY:
        raise RuntimeError(
            "METABASE_API_KEY is not set. Generate one in Metabase: "
            "Settings -> Admin settings -> Settings -> API Keys -> Create API Key."
        )
    url = f"{METABASE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("x-api-key", METABASE_API_KEY)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        logger.error("HTTP %s on %s %s: %s", exc.code, method, path, exc.read().decode())
        raise


def wait_for_health(timeout_seconds: int = 120, interval_seconds: int = 5) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(f"{METABASE_URL}/api/health")
            with urllib.request.urlopen(req) as resp:
                if resp.status == 200:
                    logger.info("Metabase is healthy")
                    return
        except (urllib.error.URLError, urllib.error.HTTPError):
            pass
        time.sleep(interval_seconds)
    raise TimeoutError(f"Metabase did not become healthy within {timeout_seconds}s")


def get_or_create_database(name: str, details: dict) -> int:
    databases = call("GET", "/api/database")
    for db in databases.get("data", databases if isinstance(databases, list) else []):
        if db.get("name") == name and db.get("engine") == "redshift":
            logger.info("database %r already exists (id=%s), reusing", name, db["id"])
            return db["id"]
    logger.info("creating database %r", name)
    result = call("POST", "/api/database", {
        "engine": "redshift", "name": name, "details": details, "is_full_sync": True,
    })
    return result["id"]


def sync_and_wait_for_tables(db_id: int, expected_table_names: set[str],
                              timeout_seconds: int = 60, interval_seconds: int = 3) -> dict:
    call("POST", f"/api/database/{db_id}/sync_schema")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        metadata = call("GET", f"/api/database/{db_id}/metadata")
        found = {t["name"] for t in metadata.get("tables", [])}
        if expected_table_names <= found:
            logger.info("schema sync complete: found %s", sorted(found))
            return metadata
        time.sleep(interval_seconds)
    raise TimeoutError(
        f"Tables {expected_table_names} did not appear in schema {MARTS_SCHEMA} "
        f"within {timeout_seconds}s — check the Redshift connection and dbt build state."
    )


def build_field_id_map(metadata: dict) -> dict[tuple[str, str], int]:
    """{(table_name, column_name): field_id} for every synced table/field."""
    field_map = {}
    for table in metadata.get("tables", []):
        for field in table.get("fields", []):
            field_map[(table["name"], field["name"])] = field["id"]
    return field_map


def get_or_create_collection(name: str, description: str) -> int:
    collections = call("GET", "/api/collection")
    for coll in collections:
        if coll.get("name") == name and not coll.get("archived"):
            logger.info("collection %r already exists (id=%s), reusing", name, coll["id"])
            return coll["id"]
    logger.info("creating collection %r", name)
    result = call("POST", "/api/collection", {"name": name, "description": description})
    return result["id"]


def get_or_create_dashboard(name: str, description: str, collection_id: int | None = None) -> tuple[int, bool]:
    """Returns (dashboard_id, already_existed)."""
    dashboards = call("GET", "/api/dashboard")
    for dash in dashboards:
        if dash.get("name") == name and not dash.get("archived"):
            return dash["id"], True
    result = call("POST", "/api/dashboard", {
        "name": name, "description": description, "collection_id": collection_id,
    })
    return result["id"], False


def create_card(database_id: int, name: str, sql: str, template_tags: dict,
                 display: str, visualization_settings: dict | None = None,
                 description: str | None = None) -> int:
    payload = {
        "name": name,
        "description": description,
        "display": display,
        "visualization_settings": visualization_settings or {},
        "dataset_query": {
            "type": "native",
            "native": {"query": sql, "template-tags": template_tags},
            "database": database_id,
        },
    }
    result = call("POST", "/api/card", payload)
    logger.info("created card %s: %s", result["id"], name)
    return result["id"]


def field_filter_tag(name: str, display_name: str, field_id: int, widget_type: str,
                      alias: str | None = None, default=None, required: bool | None = None) -> dict:
    import uuid
    tag = {
        "id": str(uuid.uuid4()), "name": name, "display-name": display_name,
        "type": "dimension", "dimension": ["field", field_id, None],
        "widget-type": widget_type, "default": default,
    }
    if alias is not None:
        tag["alias"] = alias
    if required is not None:
        tag["required"] = required
    return tag


def text_dashcard(dashcard_id: int, tab_id: int, col: int, row: int, size_x: int, size_y: int,
                   text: str, heading: bool = False) -> dict:
    """A virtual (no card_id) dashcard rendering static heading/text markdown."""
    settings = {
        "dashcard.background": False,
        "virtual_card": {"name": None, "display": "heading" if heading else "text",
                          "visualization_settings": {}, "archived": False},
        "text": text,
    }
    if not heading:
        settings["text.align_vertical"] = "top"
        settings["text.align_horizontal"] = "left"
    return {
        "id": dashcard_id, "dashboard_tab_id": tab_id, "col": col, "row": row,
        "size_x": size_x, "size_y": size_y, "visualization_settings": settings,
    }


def pm_dim(param_id: str, tag_name: str) -> dict:
    return {"parameter_id": param_id, "target": ["dimension", ["template-tag", tag_name]]}
