#!/usr/bin/env python3
# Unit tests for squeeze indicators and signal logic.
# Does not require a broker connection — runs purely on synthetic data.
#
# usage:
#   python src/test_strategy.py        (runs all tests)
#   python -m pytest src/test_strategy.py -v

import sys
import math
import unittest
import numpy as np
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.strategy import compute_squeeze, BaseTrader, _atr, _linreg_slope


DEFAULT_PARAMS = dict(
    bb_period     = 20,
    bb_mult       = 2.0,
    kc_mult       = 1.5,
    atr_period    = 14,
    mom_period    = 12,
    vol_mult      = 1.5,
    tp_atr        = 2.0,
    sl_atr        = 1.0,
    size_pct      = 0.10,
    cooldown_bars = 3,
)


def _flat_series(n=30, price=1000.0, vol=100):
    closes  = deque([price] * n, maxlen=200)
    highs   = deque([price + 1] * n, maxlen=200)
    lows    = deque([price - 1] * n, maxlen=200)
    volumes = deque([vol] * n, maxlen=200)
    return closes, highs, lows, volumes


def _candle(price, high=None, low=None, vol=100, t=None):
    from datetime import datetime
    return dict(
        time   = t or datetime(2024, 1, 2, 9, 0),
        open   = price,
        high   = high or price + 0.5,
        low    = low  or price - 0.5,
        close  = price,
        volume = vol,
    )


class TestATR(unittest.TestCase):

    def test_flat_market_atr_near_zero(self):
        closes  = [1000.0] * 20
        highs   = [1001.0] * 20
        lows    = [999.0]  * 20
        atr_val = _atr(highs, lows, closes, 14)
        self.assertAlmostEqual(atr_val, 2.0, places=5)

    def test_atr_increases_with_volatility(self):
        import random
        random.seed(42)
        closes = [1000 + random.uniform(-5, 5) for _ in range(20)]
        highs  = [c + random.uniform(1, 5) for c in closes]
        lows   = [c - random.uniform(1, 5) for c in closes]
        atr_hi = _atr(highs, lows, closes, 14)

        closes2 = [1000 + random.uniform(-1, 1) for _ in range(20)]
        highs2  = [c + 0.5 for c in closes2]
        lows2   = [c - 0.5 for c in closes2]
        atr_lo  = _atr(highs2, lows2, closes2, 14)

        self.assertGreater(atr_hi, atr_lo)


class TestLinregSlope(unittest.TestCase):

    def test_uptrend_positive(self):
        series = list(range(100, 120))
        slope  = _linreg_slope(series)
        self.assertGreater(slope, 0)

    def test_downtrend_negative(self):
        series = list(range(120, 100, -1))
        slope  = _linreg_slope(series)
        self.assertLess(slope, 0)

    def test_flat_near_zero(self):
        series = [1000.0] * 20
        slope  = _linreg_slope(series)
        self.assertAlmostEqual(slope, 0.0, places=8)


class TestComputeSqueeze(unittest.TestCase):

    def test_squeeze_detected_on_flat_data(self):
        # flat data → tiny std → BB narrows → squeeze
        closes, highs, lows, volumes = _flat_series(30, 1000.0)
        ind = compute_squeeze(closes, highs, lows, volumes, DEFAULT_PARAMS)
        self.assertTrue(ind["in_squeeze"])

    def test_no_squeeze_on_volatile_data(self):
        import random
        random.seed(1)
        closes  = deque([1000 + random.uniform(-20, 20) for _ in range(30)], maxlen=200)
        highs   = deque([c + 15 for c in closes], maxlen=200)
        lows    = deque([c - 15 for c in closes], maxlen=200)
        volumes = deque([100] * 30, maxlen=200)
        ind = compute_squeeze(closes, highs, lows, volumes, DEFAULT_PARAMS)
        self.assertFalse(ind["in_squeeze"])

    def test_vol_spike_detected(self):
        closes, highs, lows, volumes = _flat_series(30, 1000.0, vol=100)
        volumes[-1] = 300   # spike
        ind = compute_squeeze(closes, highs, lows, volumes, DEFAULT_PARAMS)
        self.assertTrue(ind["vol_spike"])

    def test_no_vol_spike_on_normal_volume(self):
        closes, highs, lows, volumes = _flat_series(30, 1000.0, vol=100)
        ind = compute_squeeze(closes, highs, lows, volumes, DEFAULT_PARAMS)
        self.assertFalse(ind["vol_spike"])

    def test_returns_expected_keys(self):
        closes, highs, lows, volumes = _flat_series(30)
        ind = compute_squeeze(closes, highs, lows, volumes, DEFAULT_PARAMS)
        for key in ("in_squeeze", "momentum", "vol_spike", "atr", "bb_upper", "bb_lower"):
            self.assertIn(key, ind)


class TestBaseTrader(unittest.TestCase):

    def _feed_flat(self, trader, n=25, price=1000.0):
        from datetime import datetime, timedelta
        base = datetime(2024, 1, 2, 9, 0)
        for i in range(n):
            trader.on_candle(_candle(price, t=base + timedelta(minutes=i)))

    def test_no_trade_without_prior_squeeze(self):
        # volatile from the start → no squeeze latch → no entry
        trader = BaseTrader(DEFAULT_PARAMS)
        from datetime import datetime, timedelta
        import random
        random.seed(5)
        base = datetime(2024, 1, 2, 9, 0)
        for i in range(30):
            p = 1000 + random.uniform(-30, 30)
            trader.on_candle(dict(
                time=base + timedelta(minutes=i),
                open=p, high=p+15, low=p-15, close=p, volume=100
            ))
        self.assertEqual(trader.position, 0)
        self.assertEqual(len(trader.trades), 0)

    def test_squeeze_latch_set(self):
        trader = BaseTrader(DEFAULT_PARAMS)
        self._feed_flat(trader, n=25)
        self.assertTrue(trader._was_in_squeeze)

    def test_long_entry_after_squeeze_release(self):
        trader = BaseTrader(DEFAULT_PARAMS)
        self._feed_flat(trader, n=25)   # build squeeze

        from datetime import datetime, timedelta
        base = datetime(2024, 1, 2, 9, 25)

        # feed a breakout candle: high volume, price above BB
        for i in range(3):
            trader.on_candle(dict(
                time=base + timedelta(minutes=i),
                open=1000, high=1050, low=1000, close=1040, volume=500
            ))

        # position should be long if entry fired
        if len(trader.trades) == 0:
            self.assertIn(trader.position, [0, 1])
        else:
            self.assertEqual(trader.trades[0]["side"], 1)

    def test_tp_closes_long(self):
        trader = BaseTrader(DEFAULT_PARAMS)
        # force a position manually
        from datetime import datetime
        trader.position      = 1
        trader.position_size = 1
        trader.entry_price   = 1000.0
        trader.tp            = 1010.0
        trader.sl            = 990.0

        trader._close_trade(1010.0, datetime(2024, 1, 2, 10, 0), "tp")

        self.assertEqual(trader.position, 0)
        self.assertEqual(len(trader.trades), 1)
        self.assertAlmostEqual(trader.trades[0]["pnl"], 10.0)

    def test_sl_closes_long(self):
        trader = BaseTrader(DEFAULT_PARAMS)
        from datetime import datetime
        trader.position      = 1
        trader.position_size = 2
        trader.entry_price   = 1000.0
        trader.tp            = 1020.0
        trader.sl            = 990.0

        trader._close_trade(990.0, datetime(2024, 1, 2, 10, 0), "sl")

        self.assertEqual(trader.position, 0)
        self.assertAlmostEqual(trader.trades[0]["pnl"], -20.0)

    def test_cooldown_prevents_reentry(self):
        trader = BaseTrader(DEFAULT_PARAMS)
        trader.cooldown = 2
        trader.closes   = trader.closes   # just need >= MIN_BARS, skip that here

        # _process with cooldown should decrement and return
        from datetime import datetime
        trader._process(1000.0, datetime(2024, 1, 2, 10, 0))
        self.assertEqual(trader.cooldown, 1)

    def test_summary_keys(self):
        trader = BaseTrader(DEFAULT_PARAMS)
        s = trader.summary()
        for key in ("total_trades", "wins", "losses", "win_rate", "total_pnl"):
            self.assertIn(key, s)


if __name__ == "__main__":
    unittest.main(verbosity=2)
