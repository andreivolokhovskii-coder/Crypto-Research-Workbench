#!/usr/bin/env bash
# Superset initialization entrypoint.
# Runs DB migrations, creates the admin user, then serves.
set -euo pipefail

superset db upgrade

superset fab create-admin \
    --username  "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
    --lastname  "${SUPERSET_ADMIN_LASTNAME:-User}" \
    --email     "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
    --password  "${SUPERSET_ADMIN_PASSWORD:-admin}" \
    || true

# Always sync password from env (fixes stale volume with old password on redeploy)
superset fab reset-password \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}"

superset init

# Auto-register ClickHouse DB and create datasets for all tables
python3 - <<'PYEOF'
import os

from superset import create_app

app = create_app()
with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable

    # 1. Register / update DB connection
    user = os.environ.get("CLICKHOUSE_USER", "crypto_user")
    pw   = os.environ.get("CLICKHOUSE_PASSWORD", "")
    host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
    name = os.environ.get("CLICKHOUSE_DB", "crypto")
    uri  = f"clickhouse+http://{user}:{pw}@{host}:{port}/{name}"

    # Always drop and recreate DB entry so URI is never stale from old volume
    db_name = "ClickHouse"
    existing = db.session.query(Database).filter_by(database_name=db_name).first()
    if existing:
        db.session.query(SqlaTable).filter_by(database_id=existing.id).delete()
        db.session.delete(existing)
        db.session.commit()
        print(f"[init] Dropped stale database entry: {db_name}")

    database = Database(database_name=db_name, sqlalchemy_uri=uri)
    db.session.add(database)
    db.session.commit()
    print(f"[init] Registered database: {db_name} → {uri[:40]}...")

    # 2. Auto-create datasets for all tables (schema=None — DB already set in URI)
    TABLES = [
        "bronze_klines", "bronze_coin_metadata", "bronze_trades",
        "silver_klines", "silver_coin_metadata",
        "fact_candles", "dim_coin", "dim_exchange",
        "mart_volatility", "mart_market_regime", "mart_volume_profile",
        "rt_latest_kline", "rt_signals",
    ]
    for table_name in TABLES:
        existing = db.session.query(SqlaTable).filter_by(
            database_id=database.id,
            table_name=table_name,
        ).first()
        if not existing:
            tbl = SqlaTable(
                table_name=table_name,
                database_id=database.id,
            )
            db.session.add(tbl)
            print(f"[init] Created dataset: {table_name}")
        else:
            print(f"[init] Dataset exists: {table_name}")
    db.session.commit()

    # 3. Sync column metadata for all datasets
    datasets = {t.table_name: t for t in db.session.query(SqlaTable).filter_by(database_id=database.id).all()}
    for tbl in datasets.values():
        try:
            tbl.fetch_metadata()
            print(f"[init] Synced columns: {tbl.table_name}")
        except Exception as e:
            print(f"[init] Warning syncing {tbl.table_name}: {e}")
    db.session.commit()

    # 4. Create charts and dashboard (skip if already exist)
    import json
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard

    def ds(name):
        return datasets.get(name)

    def metric(col, agg="AVG"):
        return {"expressionType": "SIMPLE", "column": {"column_name": col},
                "aggregate": agg, "label": f"{agg}({col})"}

    def make_line(name, table, x_col, metric_col, groupby=None):
        d = ds(table)
        if not d:
            return None
        params = {
            "viz_type": "echarts_timeseries_line",
            "datasource": f"{d.id}__table",
            "x_axis": x_col,
            "metrics": [metric(metric_col, "AVG")],
            "groupby": groupby or [],
            "time_grain_sqla": None,
            "time_range": "No filter",
            "adhoc_filters": [],
            "row_limit": 10000,
            "zoomable": True,
        }
        return Slice(slice_name=name, viz_type="echarts_timeseries_line",
                     datasource_type="table", datasource_id=d.id, params=json.dumps(params))

    def make_bar(name, table, x_col, metric_col, groupby=None):
        d = ds(table)
        if not d:
            return None
        params = {
            "viz_type": "echarts_timeseries_bar",
            "datasource": f"{d.id}__table",
            "x_axis": x_col,
            "metrics": [metric(metric_col, "SUM")],
            "groupby": groupby or [],
            "time_grain_sqla": None,
            "time_range": "No filter",
            "adhoc_filters": [],
            "row_limit": 10000,
        }
        return Slice(slice_name=name, viz_type="echarts_timeseries_bar",
                     datasource_type="table", datasource_id=d.id, params=json.dumps(params))

    def make_table(name, table, columns, row_limit=100):
        d = ds(table)
        if not d:
            return None
        params = {
            "viz_type": "table",
            "datasource": f"{d.id}__table",
            "query_mode": "raw",
            "all_columns": columns,
            "metrics": [],
            "order_desc": True,
            "row_limit": row_limit,
            "time_range": "No filter",
            "adhoc_filters": [],
        }
        return Slice(slice_name=name, viz_type="table",
                     datasource_type="table", datasource_id=d.id, params=json.dumps(params))

    DASHBOARD_NAME = "Crypto Market Overview"
    if db.session.query(Dashboard).filter_by(dashboard_title=DASHBOARD_NAME).first():
        print(f"[init] Dashboard already exists: {DASHBOARD_NAME}")
    else:
        chart_defs = [
            make_line("Price History",   "fact_candles",   "open_time", "close",          ["symbol"]),
            make_bar( "Volume History",  "fact_candles",   "open_time", "volume",          ["symbol"]),
            make_line("Volatility 7d",   "mart_volatility","window_start","realized_vol_7d",["symbol"]),
            make_table("Live Prices",    "rt_latest_kline",
                       ["symbol","close","high","low","volume","updated_at"], 50),
            make_table("Signals",        "rt_signals",
                       ["detected_at","symbol","signal_type","description","value"], 100),
        ]
        charts = [c for c in chart_defs if c is not None]
        for c in charts:
            db.session.add(c)
        db.session.flush()  # get chart IDs

        # Build grid layout: row1 = 3 charts, row2 = 2 charts
        def chart_block(chart, cid, row_id, w=8, h=50):
            return {
                f"CHART-{cid}": {
                    "type": "CHART", "id": f"CHART-{cid}",
                    "children": [], "parents": [row_id, "GRID_ID"],
                    "meta": {"chartId": chart.id, "width": w, "height": h, "sliceName": chart.slice_name},
                }
            }

        row1_ids = [f"CHART-{i}" for i in range(len(charts)) if i < 3]
        row2_ids = [f"CHART-{i}" for i in range(len(charts)) if i >= 3]

        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID":  {"type": "ROOT",  "id": "ROOT_ID",  "children": ["GRID_ID"]},
            "GRID_ID":  {"type": "GRID",  "id": "GRID_ID",  "children": ["ROW-1", "ROW-2"], "parents": ["ROOT_ID"]},
            "ROW-1":    {"type": "ROW",   "id": "ROW-1",    "children": row1_ids, "parents": ["GRID_ID"],
                         "meta": {"background": "BACKGROUND_TRANSPARENT"}},
            "ROW-2":    {"type": "ROW",   "id": "ROW-2",    "children": row2_ids, "parents": ["GRID_ID"],
                         "meta": {"background": "BACKGROUND_TRANSPARENT"}},
        }
        for i, c in enumerate(charts):
            w = 8 if i < 3 else 12
            position.update(chart_block(c, i, "ROW-1" if i < 3 else "ROW-2", w=w))

        dashboard = Dashboard(
            dashboard_title=DASHBOARD_NAME,
            slug="crypto-market-overview",
            position_json=json.dumps(position),
            published=True,
        )
        dashboard.slices = charts
        db.session.add(dashboard)
        db.session.commit()
        print(f"[init] Created dashboard: {DASHBOARD_NAME} with {len(charts)} charts")
PYEOF

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 4 \
    --worker-class gthread \
    --threads 2 \
    --timeout 120 \
    --no-sendfile \
    --access-logfile - \
    --access-logformat '%(h)s "%(r)s" %(s)s %(b)s %(M)sms' \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
