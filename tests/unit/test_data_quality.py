"""Unit tests for data_quality.py — _check_query with mocked ClickHouse."""
from __future__ import annotations

import importlib.util
import pathlib
import sys
from unittest.mock import MagicMock

import pytest

# Load data_quality.py directly by file path — the airflow/ directory is not a
# Python package, so `from airflow.dags.data_quality import ...` won't work.
_dag_file = pathlib.Path(__file__).parents[2] / "airflow" / "dags" / "data_quality.py"
_spec = importlib.util.spec_from_file_location("data_quality", _dag_file)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_check_query = _mod._check_query


def _mock_ch_client(result_rows: list):
    """Return a mock clickhouse_connect.get_client() whose .query().result_rows is result_rows."""
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = result_rows
    return mock_client


# ---------------------------------------------------------------------------
# _check_query
# ---------------------------------------------------------------------------

def test_check_query_passes_when_no_rows(monkeypatch):
    mock_ch = sys.modules["clickhouse_connect"]
    mock_ch.get_client.return_value = _mock_ch_client([])

    # Should complete without raising
    _check_query("SELECT 1", "test check")
    mock_ch.get_client.assert_called_once()


def test_check_query_raises_on_anomaly_rows(monkeypatch):
    mock_ch = sys.modules["clickhouse_connect"]
    mock_ch.get_client.return_value = _mock_ch_client([("BTC/USDT", 42), ("ETH/USDT", 7)])

    with pytest.raises(ValueError, match="anomalies found"):
        _check_query("SELECT 1", "freshness check")


def test_check_query_error_message_includes_rows(monkeypatch):
    mock_ch = sys.modules["clickhouse_connect"]
    rows = [("SOL/USDT", 53)]
    mock_ch.get_client.return_value = _mock_ch_client(rows)

    with pytest.raises(ValueError) as exc_info:
        _check_query("SELECT 1", "row count check")

    assert "SOL/USDT" in str(exc_info.value)
    assert "53" in str(exc_info.value)


def test_check_query_passes_sql_to_client(monkeypatch):
    mock_ch = sys.modules["clickhouse_connect"]
    mock_client = _mock_ch_client([])
    mock_ch.get_client.return_value = mock_client

    sql = "SELECT symbol, count() FROM crypto.silver_klines GROUP BY symbol HAVING count() < 10"
    _check_query(sql, "custom check")

    mock_client.query.assert_called_once_with(sql)


def test_check_query_single_anomaly_raises(monkeypatch):
    mock_ch = sys.modules["clickhouse_connect"]
    mock_ch.get_client.return_value = _mock_ch_client([("BNB/USDT", 999)])

    with pytest.raises(ValueError):
        _check_query("SELECT 1", "row count check")
