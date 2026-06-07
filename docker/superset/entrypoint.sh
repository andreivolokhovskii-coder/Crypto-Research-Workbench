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

    # 2a. Create a daily-aggregated view to avoid clickhouse-sqlalchemy double-grain bug.
    # clickhouse-sqlalchemy wraps time_grain_sqla twice, producing:
    #   toStartOfDay(toDateTime(toStartOfDay(toDateTime(col)))) — invalid in ClickHouse strict mode.
    # Solution: pre-aggregate to daily in a view and use time_grain_sqla=None in charts.
    import urllib.request, urllib.parse as _urlparse

    _ch_base = f"http://{host}:{port}/"
    _ch_auth = _urlparse.urlencode({"user": user, "password": pw, "database": name})

    def _ch_exec(sql):
        req = urllib.request.Request(
            f"{_ch_base}?{_ch_auth}", data=sql.encode(), method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.read().decode().strip()
        except Exception as e:
            print(f"[init] ClickHouse exec warning: {e}")
            return None

    _ch_exec("""
        CREATE OR REPLACE VIEW crypto.v_daily_klines AS
        SELECT
            exchange,
            symbol,
            toDate(open_time)             AS trade_date,
            argMin(open,  open_time)      AS day_open,
            max(high)                     AS day_high,
            min(low)                      AS day_low,
            argMax(close, open_time)      AS day_close,
            sum(volume)                   AS day_volume,
            sum(quote_volume)             AS day_quote_volume,
            sum(trade_count)              AS day_trade_count
        FROM crypto.silver_klines
        WHERE interval = '1m'
        GROUP BY exchange, symbol, trade_date
    """)
    print("[init] Ensured view: v_daily_klines")

    # 2b. Auto-create datasets for all tables (schema=None — DB already set in URI)
    TABLES = [
        "bronze_klines", "bronze_coin_metadata", "bronze_trades",
        "silver_klines", "silver_coin_metadata",
        "fact_candles", "dim_coin", "dim_exchange",
        "mart_volatility", "mart_market_regime", "mart_volume_profile",
        "rt_latest_kline", "rt_signals",
        "v_daily_klines",
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

    # 4. Create / recreate dashboard (version-gated)
    import json
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard

    DASHBOARD_NAME = "Crypto Market Overview"
    DASHBOARD_V    = "v4"

    existing_dash = db.session.query(Dashboard).filter_by(dashboard_title=DASHBOARD_NAME).first()
    if existing_dash:
        meta = json.loads(existing_dash.json_metadata or "{}")
        if meta.get("init_version") == DASHBOARD_V:
            print(f"[init] Dashboard {DASHBOARD_V} already up to date, skipping")
            existing_dash = "skip"
        else:
            for sl in list(existing_dash.slices):
                db.session.delete(sl)
            db.session.delete(existing_dash)
            db.session.commit()
            existing_dash = None

    if existing_dash != "skip":
        def ds(name):
            return datasets.get(name)

        def m(col, agg="AVG"):
            return {"expressionType": "SIMPLE", "column": {"column_name": col},
                    "aggregate": agg, "label": f"{agg}({col})"}

        def line(name, table, x_col, m_col, groupby=None, limit=5000, agg="AVG"):
            d = ds(table)
            if not d: return None
            return Slice(slice_name=name, viz_type="echarts_timeseries_line",
                         datasource_type="table", datasource_id=d.id,
                         params=json.dumps({
                             "viz_type": "echarts_timeseries_line",
                             "datasource": f"{d.id}__table",
                             "x_axis": x_col,
                             "metrics": [m(m_col, agg)],
                             "groupby": groupby or [],
                             "time_grain_sqla": None,
                             "time_range": "No filter",
                             "adhoc_filters": [],
                             "row_limit": limit,
                             "zoomable": True,
                             "show_legend": True,
                             "rich_tooltip": True,
                         }))

        def bar(name, table, x_col, m_col, groupby=None, limit=5000):
            d = ds(table)
            if not d: return None
            return Slice(slice_name=name, viz_type="echarts_timeseries_bar",
                         datasource_type="table", datasource_id=d.id,
                         params=json.dumps({
                             "viz_type": "echarts_timeseries_bar",
                             "datasource": f"{d.id}__table",
                             "x_axis": x_col,
                             "metrics": [m(m_col, "SUM")],
                             "groupby": groupby or [],
                             "time_grain_sqla": None,
                             "time_range": "No filter",
                             "adhoc_filters": [],
                             "row_limit": limit,
                             "zoomable": True,
                             "show_legend": True,
                         }))

        def table(name, tbl, columns, limit=100):
            d = ds(tbl)
            if not d: return None
            return Slice(slice_name=name, viz_type="table",
                         datasource_type="table", datasource_id=d.id,
                         params=json.dumps({
                             "viz_type": "table",
                             "datasource": f"{d.id}__table",
                             "query_mode": "raw",
                             "all_columns": columns,
                             "metrics": [],
                             "order_desc": True,
                             "row_limit": limit,
                             "time_range": "No filter",
                             "adhoc_filters": [],
                             "show_cell_bars": True,
                         }))

        # 6 charts in 3 rows × 2 columns
        # v_daily_klines: pre-aggregated daily view (avoids double-grain bug with clickhouse-sqlalchemy)
        # time_grain_sqla=None on all time-series charts — data is already at target granularity
        chart_defs = [
            line( "Price History (Daily)",   "v_daily_klines",  "trade_date",   "day_close",      ["symbol"]),
            bar(  "Volume History (Daily)",   "v_daily_klines",  "trade_date",   "day_volume",     ["symbol"]),
            line( "Realized Volatility 7d",  "mart_volatility", "window_start", "realized_vol_7d",["symbol"]),
            table("Market Regime",           "mart_market_regime",
                  ["symbol", "trade_date", "regime", "realized_vol_7d", "atr_14"], 50),
            table("Live Prices",             "rt_latest_kline",
                  ["symbol", "close", "high", "low", "volume", "updated_at"], 20),
            table("Trading Signals",         "rt_signals",
                  ["detected_at", "symbol", "signal_type", "description", "value"], 100),
        ]
        charts = [c for c in chart_defs if c is not None]
        for c in charts:
            db.session.add(c)
        db.session.flush()

        # Layout: 3 rows × 2 charts, each chart width=12
        rows_def = [("ROW-1", charts[0:2]),
                    ("ROW-2", charts[2:4]),
                    ("ROW-3", charts[4:6])]
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID",
                        "children": [r[0] for r in rows_def], "parents": ["ROOT_ID"]},
        }
        idx = 0
        for row_id, row_charts in rows_def:
            keys = []
            for c in row_charts:
                key = f"CHART-{idx}"
                position[key] = {"type": "CHART", "id": key, "children": [],
                                  "parents": [row_id, "GRID_ID"],
                                  "meta": {"chartId": c.id, "width": 12, "height": 50,
                                           "sliceName": c.slice_name}}
                keys.append(key)
                idx += 1
            position[row_id] = {"type": "ROW", "id": row_id, "children": keys,
                                  "parents": ["GRID_ID"],
                                  "meta": {"background": "BACKGROUND_TRANSPARENT"}}

        dash = Dashboard(
            dashboard_title=DASHBOARD_NAME,
            slug="crypto-market-overview",
            position_json=json.dumps(position),
            json_metadata=json.dumps({"init_version": DASHBOARD_V}),
            published=True,
        )
        dash.slices = charts
        db.session.add(dash)
        db.session.commit()
        print(f"[init] Created dashboard {DASHBOARD_V}: {len(charts)} charts")
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
