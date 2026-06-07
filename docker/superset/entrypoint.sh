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
    uri  = f"clickhouse+connect://{user}:{pw}@{host}:{port}/{name}"

    db_name = "ClickHouse"
    database = db.session.query(Database).filter_by(database_name=db_name).first()
    if not database:
        database = Database(database_name=db_name, sqlalchemy_uri=uri)
        db.session.add(database)
        db.session.commit()
        print(f"[init] Registered database: {db_name}")
    else:
        database.sqlalchemy_uri = uri
        db.session.commit()
        print(f"[init] Updated database connection: {db_name}")

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
    for tbl in db.session.query(SqlaTable).filter_by(database_id=database.id).all():
        try:
            tbl.fetch_metadata()
            print(f"[init] Synced columns: {tbl.table_name}")
        except Exception as e:
            print(f"[init] Warning syncing {tbl.table_name}: {e}")
    db.session.commit()
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
