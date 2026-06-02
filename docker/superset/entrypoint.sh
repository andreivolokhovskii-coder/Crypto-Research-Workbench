#!/usr/bin/env bash
# Superset initialization entrypoint.
# Runs DB migrations, creates the admin user on first start, then serves.
set -euo pipefail

superset db upgrade

superset fab create-admin \
    --username  "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
    --lastname  "${SUPERSET_ADMIN_LASTNAME:-User}" \
    --email     "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
    --password  "${SUPERSET_ADMIN_PASSWORD:-admin}" \
    || true   # already exists on restarts — ignore the error

superset init

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 2 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
