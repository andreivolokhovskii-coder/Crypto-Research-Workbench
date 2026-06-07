# ==============================
# Crypto Research Workbench
# Makefile
# ==============================

SHELL := /bin/bash

# --------------------------------------------------
# Project variables
# --------------------------------------------------
COMPOSE ?= docker compose
PROJECT_NAME ?= crypto-research-workbench

# --------------------------------------------------
# Default target
# --------------------------------------------------
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  deploy             - First-time setup: generate secrets, build images, start stack"
	@echo "  setup              - Generate .env with random secrets (skips if .env exists)"
	@echo "  setup-force        - Regenerate .env, overwriting existing secrets"
	@echo "  up                 - Start all services in detached mode"
	@echo "  down               - Stop all services"
	@echo "  restart            - Restart all services"
	@echo "  logs               - Show compose logs"
	@echo "  ps                 - Show running services"
	@echo "  build              - Build images"
	@echo "  pull               - Pull base images"
	@echo "  reset              - Stop stack and remove volumes"
	@echo "  init               - Create local folders if missing"
	@echo "  validate-env       - Check that .env exists"
	@echo "  airflow-init       - Initialize Airflow metadata DB"
	@echo "  airflow-create-admin - Create Airflow admin user"
	@echo "  dbt-build          - Run dbt build in dbt container"
	@echo "  dbt-test           - Run dbt tests"
	@echo "  dbt-docs           - Generate dbt docs"
	@echo "  pytest             - Run Python tests"
	@echo "  lint               - Run lint placeholder"
	@echo "  format             - Run format placeholder"
	@echo "  backfill           - Run historical backfill placeholder"
	@echo "  metadata-refresh   - Run metadata refresh placeholder"
	@echo "  seed-buckets       - Create MinIO buckets placeholder"
	@echo "  clickhouse-client  - Open ClickHouse client shell"
	@echo "  postgres-client    - Open Postgres client shell"
	@echo "  spark-shell        - Open shell in spark-master container"

# --------------------------------------------------
# One-command deploy (clone → make deploy → done)
# --------------------------------------------------
.PHONY: deploy
deploy:
	@bash setup.sh
	DOCKER_BUILDKIT=0 $(COMPOSE) up --build -d
	@echo ""
	@echo "=========================================="
	@echo " Stack is up. Service credentials:"
	@echo "=========================================="
	@echo " Airflow:  http://localhost:8080"
	@echo "   login:  admin / $$(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)"
	@echo " Superset: http://localhost:8088"
	@echo "   login:  admin / $$(grep SUPERSET_ADMIN_PASSWORD .env | cut -d= -f2)"
	@echo " MinIO:    http://localhost:9002"
	@echo "   login:  minioadmin / $$(grep MINIO_ROOT_PASSWORD .env | cut -d= -f2)"
	@echo " Jupyter:  http://localhost:8888  (no auth)"
	@echo "=========================================="

.PHONY: setup
setup:
	@bash setup.sh

.PHONY: setup-force
setup-force:
	@bash setup.sh --force

# --------------------------------------------------
# Basic stack commands
# --------------------------------------------------
.PHONY: validate-env
validate-env:
	@test -f .env || (echo ".env file not found. Create it from .env.example first." && exit 1)

.PHONY: init
init:
	@mkdir -p docker ingestion/historical ingestion/realtime ingestion/metadata spark_jobs dbt airflow/dags clickhouse/ddl clickhouse/views notebooks queries dashboards tests .github/workflows
	@echo "Project folders ensured."

.PHONY: up
up: validate-env
	$(COMPOSE) up -d

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: restart
restart: down up

.PHONY: logs
logs:
	$(COMPOSE) logs -f --tail=200

.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: build
build: validate-env
	$(COMPOSE) build

.PHONY: pull
pull:
	$(COMPOSE) pull

.PHONY: reset
reset:
	$(COMPOSE) down -v --remove-orphans

# --------------------------------------------------
# Airflow helpers
# --------------------------------------------------
.PHONY: airflow-init
airflow-init: validate-env
	$(COMPOSE) run --rm airflow-webserver airflow db migrate

.PHONY: airflow-create-admin
airflow-create-admin: validate-env
	$(COMPOSE) run --rm airflow-webserver airflow users create \
		--username "$${AIRFLOW_ADMIN_USERNAME:-admin}" \
		--password "$${AIRFLOW_ADMIN_PASSWORD:-admin}" \
		--firstname "$${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
		--lastname "$${AIRFLOW_ADMIN_LASTNAME:-User}" \
		--role Admin \
		--email "$${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"

# --------------------------------------------------
# dbt helpers
# --------------------------------------------------
.PHONY: dbt-build
dbt-build: validate-env
	$(COMPOSE) run --rm dbt dbt build

.PHONY: dbt-test
dbt-test: validate-env
	$(COMPOSE) run --rm dbt dbt test

.PHONY: dbt-docs
dbt-docs: validate-env
	$(COMPOSE) run --rm dbt dbt docs generate

# --------------------------------------------------
# Testing / quality
# --------------------------------------------------
.PHONY: pytest
pytest: validate-env
	$(COMPOSE) run --rm app pytest -q

.PHONY: lint
lint: validate-env
	$(COMPOSE) run --rm app bash -lc "ruff check . || true"

.PHONY: format
format: validate-env
	$(COMPOSE) run --rm app bash -lc "ruff format . || true"

# --------------------------------------------------
# Data tasks (placeholders for future implementation)
# --------------------------------------------------
.PHONY: backfill
backfill: validate-env
	$(COMPOSE) run --rm app python ingestion/historical/klines_backfill.py

.PHONY: metadata-refresh
metadata-refresh: validate-env
	$(COMPOSE) run --rm app python ingestion/metadata/coingecko_dims.py

.PHONY: seed-buckets
seed-buckets: validate-env
	@echo "Create MinIO buckets step to be implemented in docker-compose or a bootstrap script."

# --------------------------------------------------
# Utility shells
# --------------------------------------------------
.PHONY: clickhouse-client
clickhouse-client: validate-env
	$(COMPOSE) exec clickhouse clickhouse-client --user "$${CLICKHOUSE_USER}" --password "$${CLICKHOUSE_PASSWORD}" --database "$${CLICKHOUSE_DB}"

.PHONY: postgres-client
postgres-client: validate-env
	$(COMPOSE) exec postgres psql -U "$${POSTGRES_USER}" -d "$${POSTGRES_DB}"

.PHONY: spark-shell
spark-shell: validate-env
	$(COMPOSE) exec spark-master bash

.PHONY: spark-volatility
spark-volatility: validate-env
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages com.clickhouse:clickhouse-jdbc:0.6.5 \
		--conf spark.executor.memory=1g \
		--conf spark.driver.memory=512m \
		/app/spark_jobs/volatility_batch.py
