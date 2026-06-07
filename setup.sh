#!/usr/bin/env bash
# One-shot bootstrap: generates all secrets and creates .env
# Usage: ./setup.sh          — skip if .env already exists
#        ./setup.sh --force  — regenerate even if .env exists
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── prerequisite check ────────────────────────────────────────────────────────
for cmd in docker openssl python3; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' is required but not found." >&2
    exit 1
  fi
done
if ! docker compose version &>/dev/null; then
  echo "ERROR: 'docker compose' (v2) is required." >&2
  exit 1
fi

# ── guard: skip if already set up ────────────────────────────────────────────
FORCE="${1:-}"
if [[ -f .env && "$FORCE" != "--force" ]]; then
  echo ".env already exists — skipping secret generation."
  echo "Run './setup.sh --force' to regenerate all secrets (destructive)."
  exit 0
fi

# ── secret generators ─────────────────────────────────────────────────────────
gen_pass()   { openssl rand -hex 24; }   # 48 hex chars, no special chars
gen_fernet() {
  python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
}

echo "Generating secrets..."

POSTGRES_PASSWORD=$(gen_pass)
AIRFLOW_DB_PASSWORD=$(gen_pass)
CLICKHOUSE_PASSWORD=$(gen_pass)
MINIO_ROOT_PASSWORD=$(gen_pass)
AIRFLOW_FERNET_KEY=$(gen_fernet)
AIRFLOW_SECRET_KEY=$(gen_pass)
AIRFLOW_ADMIN_PASSWORD=$(gen_pass)
SUPERSET_SECRET_KEY=$(gen_pass)
SUPERSET_ADMIN_PASSWORD=$(gen_pass)

# ── write .env ────────────────────────────────────────────────────────────────
cp .env.example .env

# Use | as the sed delimiter so base64 = signs and slashes don't conflict
sed -i "s|change_me_postgres_password|${POSTGRES_PASSWORD}|g"              .env
sed -i "s|change_me_airflow_db_password|${AIRFLOW_DB_PASSWORD}|g"          .env
sed -i "s|change_me_clickhouse_password|${CLICKHOUSE_PASSWORD}|g"          .env
sed -i "s|change_me_minio_password|${MINIO_ROOT_PASSWORD}|g"               .env
sed -i "s|change_me_fernet_key_32_chars_or_more|${AIRFLOW_FERNET_KEY}|g"  .env
sed -i "s|change_me_airflow_secret_key|${AIRFLOW_SECRET_KEY}|g"            .env
sed -i "s|change_me_airflow_admin_password|${AIRFLOW_ADMIN_PASSWORD}|g"    .env
sed -i "s|change_me_superset_secret_key|${SUPERSET_SECRET_KEY}|g"          .env
sed -i "s|change_me_superset_admin_password|${SUPERSET_ADMIN_PASSWORD}|g"  .env

echo ""
echo "  .env written. Service credentials (save these now):"
echo "  ──────────────────────────────────────────────────────"
echo "  Airflow:   http://localhost:8080  →  admin / ${AIRFLOW_ADMIN_PASSWORD}"
echo "  Superset:  http://localhost:8088  →  admin / ${SUPERSET_ADMIN_PASSWORD}"
echo "  MinIO:     http://localhost:9002  →  minioadmin / ${MINIO_ROOT_PASSWORD}"
echo "  Jupyter:   http://localhost:8888  →  (no auth)"
echo "  ──────────────────────────────────────────────────────"
echo ""
