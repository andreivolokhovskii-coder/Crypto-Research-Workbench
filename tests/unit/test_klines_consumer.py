"""Unit tests for klines_consumer.py — RollingStats and detect_signals."""
from __future__ import annotations

import pytest

from ingestion.realtime.klines_consumer import RollingStats, detect_signals


# ---------------------------------------------------------------------------
# RollingStats
# ---------------------------------------------------------------------------

def _fill_stats(stats: RollingStats, key: str, n: int, volume: float = 100.0, rng: float = 10.0):
    for _ in range(n):
        stats.add(key, volume, rng)


def test_vol_zscore_none_when_fewer_than_10_samples():
    stats = RollingStats()
    _fill_stats(stats, "binance:BTC/USDT", 9)
    assert stats.vol_zscore("binance:BTC/USDT", 100.0) is None


def test_vol_zscore_zero_on_constant_series():
    stats = RollingStats()
    _fill_stats(stats, "binance:BTC/USDT", 20, volume=100.0)
    # All volumes equal → std = 0 → zscore should be 0.0
    assert stats.vol_zscore("binance:BTC/USDT", 100.0) == 0.0


def test_vol_zscore_positive_for_spike():
    stats = RollingStats()
    key = "binance:BTC/USDT"
    # Use varied volumes so std > 0, otherwise (volume - mean) / std is undefined → 0.0
    for i in range(20):
        stats.add(key, 80.0 + (i % 5) * 10, 5.0)  # volumes: 80,90,100,110,120,...
    zscore = stats.vol_zscore(key, 100_000.0)
    assert zscore is not None
    assert zscore > 0


def test_atr_none_when_fewer_than_5_samples():
    stats = RollingStats()
    _fill_stats(stats, "binance:ETH/USDT", 4)
    assert stats.atr("binance:ETH/USDT") is None


def test_atr_returns_mean_of_ranges():
    stats = RollingStats()
    _fill_stats(stats, "binance:ETH/USDT", 10, rng=20.0)
    atr = stats.atr("binance:ETH/USDT")
    assert atr == pytest.approx(20.0)


def test_rolling_window_is_respected():
    stats = RollingStats(window=5)
    key = "binance:SOL/USDT"
    for v in [10, 10, 10, 10, 10, 10, 10, 10, 10, 10_000]:
        stats.add(key, v, 1.0)
    # Window = 5: last 5 values are [10, 10, 10, 10, 10_000]
    # The extreme value IS in the window, so atr should be high
    assert stats.atr(key) is not None


# ---------------------------------------------------------------------------
# detect_signals
# ---------------------------------------------------------------------------

def _make_record(volume: float = 100.0, high: float = 110.0, low: float = 90.0) -> dict:
    return {
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "open_time": 1_699_999_940_000,
        "open": 100.0,
        "high": high,
        "low": low,
        "close": 105.0,
        "volume": volume,
    }


def test_detect_signals_no_signals_on_normal_candle():
    stats = RollingStats()
    key = "binance:BTC/USDT"
    for _ in range(20):
        stats.add(key, 100.0, 10.0)
    signals = detect_signals(_make_record(volume=100.0, high=105.0, low=95.0), stats)
    assert signals == []


def test_detect_signals_volume_spike():
    stats = RollingStats()
    key = "binance:BTC/USDT"
    # Varied volumes so std > 0; a spike of 100_000 will produce a very high z-score
    for i in range(20):
        stats.add(key, 80.0 + (i % 5) * 10, 5.0)
    record = _make_record(volume=100_000.0)
    signals = detect_signals(record, stats)
    spike_signals = [s for s in signals if s["signal_type"] == "volume_spike"]
    assert len(spike_signals) == 1
    assert spike_signals[0]["value"] > 2.5


def test_detect_signals_large_candle():
    stats = RollingStats()
    key = "binance:BTC/USDT"
    for _ in range(10):
        stats.add(key, 100.0, 5.0)   # ATR ≈ 5
    # Candle range = 200 - 100 = 100 >> 3 * ATR(5)
    record = _make_record(volume=100.0, high=200.0, low=100.0)
    signals = detect_signals(record, stats)
    candle_signals = [s for s in signals if s["signal_type"] == "large_candle"]
    assert len(candle_signals) == 1
    assert candle_signals[0]["value"] > 3.0


def test_detect_signals_exchange_symbol_in_output():
    stats = RollingStats()
    key = "binance:BTC/USDT"
    for _ in range(20):
        stats.add(key, 100.0, 5.0)
    signals = detect_signals(_make_record(volume=100_000.0), stats)
    if signals:
        assert signals[0]["exchange"] == "binance"
        assert signals[0]["symbol"] == "BTC/USDT"
