"""
Superset configuration for Crypto Research Workbench.
Loaded from /app/pythonpath/ at container startup.
"""
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-production")

# SQLite for local development — swap for Postgres in production-like setups
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}

# Disable Flask-Talisman CSP — the nonce-per-request approach breaks
# React dynamic imports (webpack code splitting) in local dev.
TALISMAN_ENABLED = False

SQL_MAX_ROW = 50000

# Allow embedding dashboards without authentication in local dev
SESSION_COOKIE_SAMESITE = None
WTF_CSRF_ENABLED = False

# Increase query timeout for heavy analytical queries
SUPERSET_WEBSERVER_TIMEOUT = 300
