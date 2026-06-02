#!/usr/bin/env python3
"""
superset_setup.py — Bootstrap Superset with ClickHouse connection, datasets, and dashboards.

Creates programmatically:
  - 1 ClickHouse database connection
  - 5 physical datasets (fact_candles, mart_volatility, silver_klines, dim_coin, silver_coin_metadata)
  - Charts and 3 dashboards: Market Overview, Asset Research, Data Health

Run once after Superset is healthy:
    docker compose run --rm app python ingestion/superset_setup.py
"""

import json
import os
import sys
import time
import logging

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level="INFO",
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("superset_setup")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SUPERSET_URL  = os.getenv("SUPERSET_URL",           "http://superset:8088")
SUPERSET_USER = os.getenv("SUPERSET_ADMIN_USERNAME", "admin")
SUPERSET_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",          "clickhouse")
CH_PORT     = os.getenv("CLICKHOUSE_HTTP_PORT",     "8123")
CH_DB       = os.getenv("CLICKHOUSE_DB",            "crypto")
CH_USER     = os.getenv("CLICKHOUSE_USER",          "crypto_user")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD",      "")

CH_URI = f"clickhousedb+connect://{CH_USER}:{CH_PASSWORD}@{CH_HOST}:{CH_PORT}/{CH_DB}"


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

class SupersetClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def login(self, username: str, password: str) -> None:
        resp = self.session.post(f"{self.base}/api/v1/security/login", json={
            "username": username, "password": password,
            "provider": "db", "refresh": True,
        })
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"
        log.info("Logged in to Superset as %s", username)

    def get(self, path: str, **kwargs) -> dict:
        r = self.session.get(f"{self.base}{path}", **kwargs)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict) -> dict:
        r = self.session.post(f"{self.base}{path}", json=payload)
        if not r.ok:
            log.error("POST %s → %d: %s", path, r.status_code, r.text[:400])
        r.raise_for_status()
        return r.json()

    def put(self, path: str, payload: dict) -> dict:
        r = self.session.put(f"{self.base}{path}", json=payload)
        r.raise_for_status()
        return r.json()

    def find_by_name(self, path: str, name_field: str, name: str) -> int | None:
        """Return id of the first match, or None."""
        data = self.get(path)
        for item in data.get("result", []):
            if item.get(name_field) == name:
                return item["id"]
        return None


# ---------------------------------------------------------------------------
# Step 1 — Database connection
# ---------------------------------------------------------------------------

def ensure_database(api: SupersetClient) -> int:
    db_name = "ClickHouse — Crypto Research"
    existing = api.find_by_name("/api/v1/database/", "database_name", db_name)
    if existing:
        log.info("Database connection already exists (id=%d)", existing)
        return existing

    resp = api.post("/api/v1/database/", {
        "database_name": db_name,
        "sqlalchemy_uri": CH_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_dml": False,
        "allow_csv_upload": False,
        "extra": json.dumps({"engine_params": {}, "metadata_params": {}}),
    })
    db_id = resp["id"]
    log.info("Created database connection (id=%d)", db_id)
    return db_id


# ---------------------------------------------------------------------------
# Step 2 — Datasets
# ---------------------------------------------------------------------------

DATASETS = [
    {"table_name": "fact_candles",         "description": "Gold layer: enriched OHLCV candles with derived metrics"},
    {"table_name": "mart_volatility",      "description": "Daily rolling realized volatility (7d, 30d) and ATR per symbol"},
    {"table_name": "silver_klines",        "description": "Silver layer: normalized OHLCV candles (UTC datetime)"},
    {"table_name": "dim_coin",             "description": "Coin dimension: id, symbol, name, market_cap_rank"},
    {"table_name": "silver_coin_metadata", "description": "Normalized coin metadata snapshots from CoinGecko"},
]


def ensure_datasets(api: SupersetClient, db_id: int) -> dict[str, int]:
    existing_raw = api.get("/api/v1/dataset/?q=(page_size:100)")
    existing = {r["table_name"]: r["id"] for r in existing_raw.get("result", [])}

    ids: dict[str, int] = {}
    for ds in DATASETS:
        name = ds["table_name"]
        if name in existing:
            log.info("Dataset %s already exists (id=%d)", name, existing[name])
            ids[name] = existing[name]
            continue
        resp = api.post("/api/v1/dataset/", {
            "database":   db_id,
            "table_name": name,
            "schema":     CH_DB,
        })
        ids[name] = resp["id"]
        log.info("Created dataset %s (id=%d)", name, resp["id"])

    return ids


# ---------------------------------------------------------------------------
# Step 3 — Charts
# ---------------------------------------------------------------------------

def _table_chart(name: str, dataset_id: int, columns: list[str],
                 metrics: list[dict] | None = None,
                 adhoc_filters: list | None = None,
                 row_limit: int = 50) -> dict:
    params = {
        "viz_type":    "table",
        "query_mode":  "aggregate",
        "groupby":     columns,
        "metrics":     metrics or [],
        "all_columns": [],
        "row_limit":   row_limit,
        "order_desc":  True,
        "adhoc_filters": adhoc_filters or [],
    }
    return {
        "slice_name":      name,
        "viz_type":        "table",
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params":          json.dumps(params),
    }


def _big_number(name: str, dataset_id: int, metric_expr: str,
                suffix: str = "", adhoc_filters: list | None = None) -> dict:
    params = {
        "viz_type":      "big_number_total",
        "metric":        {"expressionType": "SQL", "sqlExpression": metric_expr, "label": name},
        "subheader":     suffix,
        "adhoc_filters": adhoc_filters or [],
    }
    return {
        "slice_name":      name,
        "viz_type":        "big_number_total",
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params":          json.dumps(params),
    }


def _bar_chart(name: str, dataset_id: int, metric_expr: str,
               groupby: list[str], adhoc_filters: list | None = None) -> dict:
    params = {
        "viz_type": "echarts_timeseries_bar",
        "metrics":  [{"expressionType": "SQL", "sqlExpression": metric_expr, "label": name}],
        "groupby":  groupby,
        "row_limit": 20,
        "adhoc_filters": adhoc_filters or [],
        "x_axis": groupby[0] if groupby else None,
        "time_grain_sqla": None,
    }
    return {
        "slice_name":      name,
        "viz_type":        "echarts_timeseries_bar",
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params":          json.dumps(params),
    }


def ensure_charts(api: SupersetClient, ds: dict[str, int]) -> dict[str, int]:
    existing_raw = api.get("/api/v1/chart/?q=(page_size:100)")
    existing = {r["slice_name"]: r["id"] for r in existing_raw.get("result", [])}

    definitions = {
        # ── Market Overview ──────────────────────────────────────────────
        "Total Candles": _big_number(
            "Total Candles", ds["fact_candles"], "COUNT(*)"
        ),
        "Symbols Tracked": _big_number(
            "Symbols Tracked", ds["fact_candles"], "COUNT(DISTINCT symbol)"
        ),
        "24h Movers": _table_chart(
            "24h Movers", ds["fact_candles"],
            columns=["symbol"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "round(avg(price_change_pct),3)", "label": "avg_pct_change"},
                {"expressionType": "SQL", "sqlExpression": "round(sum(quote_volume)/1e6,1)", "label": "quote_vol_M"},
            ],
        ),
        "Volatility Snapshot": _table_chart(
            "Volatility Snapshot", ds["mart_volatility"],
            columns=["symbol", "interval"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "round(avg(realized_vol_7d)*100,1)",  "label": "vol_7d_pct"},
                {"expressionType": "SQL", "sqlExpression": "round(avg(realized_vol_30d)*100,1)", "label": "vol_30d_pct"},
                {"expressionType": "SQL", "sqlExpression": "round(avg(avg_true_range),4)",       "label": "atr_14d"},
            ],
        ),
        "Market Regime": _table_chart(
            "Market Regime", ds["mart_volatility"],
            columns=["symbol"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "round(max(realized_vol_30d)*100,1)", "label": "vol_30d"},
                {"expressionType": "SQL", "sqlExpression": "max(window_start)",                  "label": "as_of"},
            ],
        ),
        # ── Asset Research ───────────────────────────────────────────────
        "Candle Stats by Symbol": _table_chart(
            "Candle Stats by Symbol", ds["fact_candles"],
            columns=["symbol", "interval"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "COUNT(*)",                               "label": "candles"},
                {"expressionType": "SQL", "sqlExpression": "round(avg(price_change_pct),4)",          "label": "avg_pct_change"},
                {"expressionType": "SQL", "sqlExpression": "round(stddevPop(price_change_pct),4)",    "label": "std_pct_change"},
                {"expressionType": "SQL", "sqlExpression": "round(sum(is_bullish)*100.0/COUNT(*),1)", "label": "pct_bullish"},
                {"expressionType": "SQL", "sqlExpression": "round(avg(candle_range),4)",              "label": "avg_range"},
            ],
        ),
        "Top 50 Candles by Range": _table_chart(
            "Top 50 Candles by Range", ds["fact_candles"],
            columns=["symbol", "open_time", "open", "high", "low", "close"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "round(max(candle_range),4)", "label": "range"},
            ],
            row_limit=50,
        ),
        "Coin Directory": _table_chart(
            "Coin Directory", ds["dim_coin"],
            columns=["symbol", "name", "market_cap_rank", "coin_id"],
        ),
        # ── Data Health ──────────────────────────────────────────────────
        "Data Freshness": _table_chart(
            "Data Freshness", ds["silver_klines"],
            columns=["exchange", "symbol", "interval"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "COUNT(*)",        "label": "total_candles"},
                {"expressionType": "SQL", "sqlExpression": "max(open_time)", "label": "latest_candle"},
            ],
        ),
        "Candles per Symbol": _table_chart(
            "Candles per Symbol", ds["silver_klines"],
            columns=["symbol", "interval"],
            metrics=[
                {"expressionType": "SQL", "sqlExpression": "COUNT(*)",                 "label": "candles"},
                {"expressionType": "SQL", "sqlExpression": "min(open_time)",           "label": "earliest"},
                {"expressionType": "SQL", "sqlExpression": "max(open_time)",           "label": "latest"},
            ],
        ),
    }

    ids: dict[str, int] = {}
    for chart_name, payload in definitions.items():
        if chart_name in existing:
            log.info("Chart '%s' already exists (id=%d)", chart_name, existing[chart_name])
            ids[chart_name] = existing[chart_name]
            continue
        resp = api.post("/api/v1/chart/", payload)
        ids[chart_name] = resp["id"]
        log.info("Created chart '%s' (id=%d)", chart_name, resp["id"])

    return ids


# ---------------------------------------------------------------------------
# Step 4 — Dashboards
# ---------------------------------------------------------------------------

DASHBOARDS = {
    "Market Overview": {
        "slug": "market-overview",
        "charts": ["Total Candles", "Symbols Tracked", "24h Movers",
                   "Volatility Snapshot", "Market Regime"],
    },
    "Asset Research": {
        "slug": "asset-research",
        "charts": ["Candle Stats by Symbol", "Top 50 Candles by Range", "Coin Directory"],
    },
    "Data Health": {
        "slug": "data-health",
        "charts": ["Data Freshness", "Candles per Symbol"],
    },
}


def ensure_dashboards(api: SupersetClient, chart_ids: dict[str, int]) -> dict[int, list[int]]:
    existing_raw = api.get("/api/v1/dashboard/?q=(page_size:50)")
    existing = {r["dashboard_title"]: r["id"] for r in existing_raw.get("result", [])}
    result: dict[int, list[int]] = {}

    for title, cfg in DASHBOARDS.items():
        chart_id_list = [chart_ids[c] for c in cfg["charts"] if c in chart_ids]

        if title in existing:
            dash_id = existing[title]
            log.info("Dashboard '%s' already exists (id=%d)", title, dash_id)
        else:
            resp = api.post("/api/v1/dashboard/", {
                "dashboard_title": title,
                "slug": cfg["slug"],
                "published": True,
                "owners": [],
            })
            dash_id = resp["id"]
            log.info("Created dashboard '%s' (id=%d)", title, dash_id)

        # Update layout (position_json)
        api.put(f"/api/v1/dashboard/{dash_id}", {
            "position_json": json.dumps(build_grid(chart_id_list)),
        })
        result[dash_id] = chart_id_list
        log.info("  → layout updated with %d charts", len(chart_id_list))

    return result


def build_grid(chart_ids: list[int]) -> dict:
    """Superset v2 layout: ROOT > GRID > ROW* > CHART*.
    Charts must live inside ROW nodes — placing them directly under GRID
    causes 'Cannot read properties of undefined (reading width)' in React.
    """
    nodes: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT",  "id": "ROOT_ID",  "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID",  "id": "GRID_ID",  "children": [], "parents": ["ROOT_ID"]},
    }
    row_ids: list[str] = []

    # Two charts per row; last row gets a single 12-wide chart if odd count
    for row_idx, i in enumerate(range(0, len(chart_ids), 2)):
        row_id = f"ROW-{row_idx + 1}"
        pair   = chart_ids[i : i + 2]
        width  = 12 // len(pair)          # 6 for 2 cols, 12 for 1 col
        row_children: list[str] = []

        for cid in pair:
            node_id = f"CHART-{cid}-{row_idx}"   # unique suffix avoids key collisions
            nodes[node_id] = {
                "type":     "CHART",
                "id":       node_id,
                "children": [],
                "parents":  ["ROOT_ID", "GRID_ID", row_id],
                "meta":     {"chartId": cid, "width": width, "height": 50},
            }
            row_children.append(node_id)

        nodes[row_id] = {
            "type":     "ROW",
            "id":       row_id,
            "children": row_children,
            "parents":  ["ROOT_ID", "GRID_ID"],
            "meta":     {"background": "BACKGROUND_TRANSPARENT"},
        }
        row_ids.append(row_id)

    nodes["GRID_ID"]["children"] = row_ids
    return nodes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def wait_for_superset(max_wait: int = 60) -> None:
    for i in range(max_wait // 5):
        try:
            requests.get(f"{SUPERSET_URL}/health", timeout=3).raise_for_status()
            return
        except Exception:
            log.info("Waiting for Superset... (%ds)", (i + 1) * 5)
            time.sleep(5)
    raise RuntimeError("Superset did not become healthy in time")


def fix_slice_associations(dashboard_chart_map: dict[int, list[int]]) -> None:
    """Associate charts with dashboards via ORM inside the Superset container.

    The REST API PUT endpoint in Superset 3.0.x does not expose a `slices` field,
    so we must use the ORM directly after updating position_json.
    Run this step inside the Superset container with:
        docker exec workbench-superset python3 -c "..."
    or call this function when running inside the Superset process.
    """
    try:
        from superset.app import create_app as _create_app
        _app = _create_app()
        with _app.app_context():
            from superset.models.dashboard import Dashboard
            from superset.models.slice import Slice
            from superset import db as _db
            for dash_id, chart_ids in dashboard_chart_map.items():
                dash = _db.session.get(Dashboard, dash_id)
                if not dash:
                    log.warning("Dashboard id=%d not found for slice association", dash_id)
                    continue
                slices = [s for s in [_db.session.get(Slice, cid) for cid in chart_ids] if s]
                dash.slices = slices
                _db.session.add(dash)
                log.info("  ORM: dashboard %d linked %d slices", dash_id, len(slices))
            _db.session.commit()
    except ImportError:
        log.warning("Superset package not available — skipping ORM slice association")
        log.warning("Run manually: docker exec workbench-superset python3 ingestion/superset_setup.py")


def main() -> None:
    wait_for_superset()

    api = SupersetClient(SUPERSET_URL)
    api.login(SUPERSET_USER, SUPERSET_PASS)

    db_id     = ensure_database(api)
    ds_ids    = ensure_datasets(api, db_id)
    chart_ids = ensure_charts(api, ds_ids)
    dash_ids  = ensure_dashboards(api, chart_ids)

    # ORM step: associate charts with dashboards (REST API doesn't expose slices in 3.0.x)
    fix_slice_associations(dash_ids)

    log.info("Superset setup complete.")
    log.info("Open: %s/dashboard/list/", SUPERSET_URL.replace("superset", "localhost"))


if __name__ == "__main__":
    main()
