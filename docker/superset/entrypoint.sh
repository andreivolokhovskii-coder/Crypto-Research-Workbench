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

# Auto-register ClickHouse database connection
python3 - <<'PYEOF'
import os
from superset import create_app
from superset.extensions import db
from superset.models.core import Database

app = create_app()
with app.app_context():
    db_name = "ClickHouse"
    existing = db.session.query(Database).filter_by(database_name=db_name).first()
    user = os.environ.get("CLICKHOUSE_USER", "crypto_user")
    pw   = os.environ.get("CLICKHOUSE_PASSWORD", "")
    host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
    name = os.environ.get("CLICKHOUSE_DB", "crypto")
    uri  = f"clickhouse+connect://{user}:{pw}@{host}:{port}/{name}"
    if not existing:
        db.session.add(Database(database_name=db_name, sqlalchemy_uri=uri))
        db.session.commit()
        print(f"[init] Registered database: {db_name}")
    else:
        existing.sqlalchemy_uri = uri
        db.session.commit()
        print(f"[init] Updated database connection: {db_name}")
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
