"""
pytest configuration — stubs out C-extension and I/O dependencies so unit
tests can import ingestion modules without a running Kafka / ClickHouse.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock

# Stub heavy / external packages before any project module is imported.
# Tests that need real behaviour should override these with their own fixtures.
_STUBS = [
    "confluent_kafka",
    "clickhouse_connect",
    "websockets",
    "boto3",
    "botocore",
    "minio",
    "ccxt",
    # Airflow is only available inside the Docker container; stub it so unit
    # tests that import dag modules (e.g. to reach _check_query) can run locally.
    "airflow",
    "airflow.models",
    "airflow.operators",
    "airflow.operators.bash",
    "airflow.operators.python",
    "airflow.utils",
    "airflow.utils.trigger_rule",
]
for _mod in _STUBS:
    sys.modules.setdefault(_mod, MagicMock())

# Make dotenv a no-op so module-level load_dotenv() calls don't read .env
_dotenv_mock = MagicMock()
_dotenv_mock.load_dotenv = lambda *a, **kw: None
sys.modules["dotenv"] = _dotenv_mock
