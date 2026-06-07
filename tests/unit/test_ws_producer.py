"""Unit tests for ws_producer.py — pure functions, no I/O."""
from __future__ import annotations

from ingestion.realtime.ws_producer import _normalize_symbol, parse_kline


# ---------------------------------------------------------------------------
# _normalize_symbol
# ---------------------------------------------------------------------------

def test_normalize_usdt_pair():
    assert _normalize_symbol("BTCUSDT") == "BTC/USDT"


def test_normalize_eth_usdt():
    assert _normalize_symbol("ETHUSDT") == "ETH/USDT"


def test_normalize_sol_usdt():
    assert _normalize_symbol("SOLUSDT") == "SOL/USDT"


def test_normalize_btc_quoted_pair():
    assert _normalize_symbol("ETHBTC") == "ETH/BTC"


def test_normalize_bnb_pair():
    assert _normalize_symbol("SOLBNB") == "SOL/BNB"


def test_normalize_busd_pair():
    assert _normalize_symbol("BTCBUSD") == "BTC/BUSD"


def test_normalize_lowercase_input():
    assert _normalize_symbol("btcusdt") == "BTC/USDT"


def test_normalize_unknown_quote_returns_as_is():
    assert _normalize_symbol("FOOBAR") == "FOOBAR"


def test_normalize_single_known_quote_not_split():
    # "USDT" alone — no valid base, must not produce "/USDT"
    result = _normalize_symbol("USDT")
    assert result == "USDT"


# ---------------------------------------------------------------------------
# parse_kline
# ---------------------------------------------------------------------------

def _make_kline_msg(symbol: str = "BTCUSDT", closed: bool = True) -> dict:
    return {
        "data": {
            "e": "kline",
            "E": 1_700_000_000_000,
            "k": {
                "s": symbol,
                "i": "1m",
                "t": 1_699_999_940_000,
                "T": 1_699_999_999_999,
                "o": "43000.01",
                "h": "43050.00",
                "l": "42990.00",
                "c": "43020.00",
                "v": "12.345",
                "q": "531234.56",
                "n": 320,
                "x": closed,
            },
        }
    }


def test_parse_kline_returns_expected_fields():
    result = parse_kline(_make_kline_msg())
    assert result is not None
    assert result["exchange"] == "binance"
    assert result["symbol"] == "BTC/USDT"
    assert result["interval"] == "1m"
    assert result["open"] == 43000.01
    assert result["high"] == 43050.00
    assert result["low"] == 42990.00
    assert result["close"] == 43020.00
    assert result["is_closed"] is True


def test_parse_kline_open_candle():
    result = parse_kline(_make_kline_msg(closed=False))
    assert result is not None
    assert result["is_closed"] is False


def test_parse_kline_non_kline_event_returns_none():
    msg = {"data": {"e": "trade", "E": 1_700_000_000_000}}
    assert parse_kline(msg) is None


def test_parse_kline_empty_message_returns_none():
    assert parse_kline({}) is None


def test_parse_kline_missing_k_returns_none():
    msg = {"data": {"e": "kline"}}
    assert parse_kline(msg) is None


def test_parse_kline_timestamps_are_ints():
    result = parse_kline(_make_kline_msg())
    assert isinstance(result["open_time"], int)
    assert isinstance(result["close_time"], int)
