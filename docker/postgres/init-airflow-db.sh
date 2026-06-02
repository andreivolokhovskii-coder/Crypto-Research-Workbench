#!/usr/bin/env bash
# Creates the Airflow metadata database and user inside the shared Postgres container.
# Runs once on first start via /docker-entrypoint-initdb.d/.
# Environment variables are injected by docker-compose from the .env file.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "${AIRFLOW_DB_USER}" WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE "${AIRFLOW_DB}" OWNER "${AIRFLOW_DB_USER}";
    GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_DB}" TO "${AIRFLOW_DB_USER}";
EOSQL

echo "Airflow database '${AIRFLOW_DB}' and user '${AIRFLOW_DB_USER}' created."
