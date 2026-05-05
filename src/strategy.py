#!/usr/bin/env python3
# Core squeeze breakout logic shared by backtesting, testing, and live trading.
# Import this module; do not run it directly.

import numpy as np
from collections import deque


# -- indicators ---------------------------------------------------------------

def _atr(highs, lows, closes, period):
    h    = np.array(list(highs)[-(period + 1):])
    l    = np.array(list(lows)[-(period + 1):])
    c    = np.array(list(closes)[-(period + 1):])
    prev = c[:-1]
    tr   = np.maximum(h[1:] - l[1:],
           np.maximum(np.abs(h[1:] - prev),
                      np.abs(l[1:] - prev)))
    return tr[-period:].mean()


def _linreg_slope(series):
    # normalised slope — sign gives breakout direction
    y     = np.array(series, dtype=float)
    x     = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]
    return slope / (y.mean() or 1.0)


def compute_squeeze(closes, highs, lows, volumes, params):
    bb_period  = params["bb_period"]
    bb_mult    = params["bb_mult"]
    kc_mult    = params["kc_mult"]
    atr_period = params["atr_period"]
    mom_period = params["mom_period"]
    vol_mult   = params["vol_mult"]

    c = np.array(list(closes)[-bb_period:])
    h = np.array(list(highs)[-bb_period:])
    l = np.array(list(lows)[-bb_period:])
    v = np.array(list(volumes)[-bb_period:])

    sma = c.mean()
    std = c.std()

    bb_upper = sma + bb_mult * std
    bb_lower = sma - bb_mult * std

    atr_val  = _atr(highs, lows, closes, atr_period)
    kc_upper = sma + kc_mult * atr_val
    kc_lower = sma - kc_mult * atr_val

    in_squeeze = (bb_upper < kc_upper) and (bb_lower > kc_lower)
    momentum   = _linreg_slope(list(closes)[-mom_period:])
    vol_spike  = bool(v[-1] > vol_mult * v[:-1].mean()) if len(v) > 1 else False

    return dict(
        in_squeeze=in_squeeze,
        momentum=momentum,
        vol_spike=vol_spike,
        atr=atr_val,
        bb_upper=bb_upper,
        bb_lower=bb_lower,
    )


# -- base trader state --------------------------------------------------------
# Used by both the backtester and the live trader.
# Place/close order mechanics are implemented in subclasses.

class BaseTrader:

    def __init__(self, params):
        self.params = params

        bb_period = params["bb_period"]
        self.min_bars = bb_period + 2

        self.closes  = deque(maxlen=200)
        self.highs   = deque(maxlen=200)
        self.lows    = deque(maxlen=200)
        self.volumes = deque(maxlen=200)

        self.position      = 0   # 1=long, -1=short, 0=flat
        self.position_size = 0
        self.entry_price   = None
        self.tp            = None
        self.sl            = None

        self._was_in_squeeze = False
        self.cooldown        = 0

        self.pnl    = 0.0
        self.trades = []   # list of dicts: entry, exit, side, qty, pnl

    def on_candle(self, candle):
        self.closes.append(candle["close"])
        self.highs.append(candle["high"])
        self.lows.append(candle["low"])
        self.volumes.append(candle["volume"])
        self._process(candle["close"], candle["time"])

    def _process(self, price, ts):
        if self.cooldown > 0:
            self.cooldown -= 1
            return

        if len(self.closes) < self.min_bars:
            return

        ind = compute_squeeze(
            self.closes, self.highs, self.lows, self.volumes, self.params
        )

        self._log_indicators(ind, ts)

        if self.position != 0:
            self._check_exit(price, ts)
            return

        if ind["in_squeeze"]:
            self._was_in_squeeze = True
            return

        if not self._was_in_squeeze:
            return

        if not ind["vol_spike"]:
            return

        if ind["momentum"] > 0 and price > ind["bb_upper"]:
            self._open_trade(1, price, ind["atr"], ts)

        elif ind["momentum"] < 0 and price < ind["bb_lower"]:
            self._open_trade(-1, price, ind["atr"], ts)

    def _check_exit(self, price, ts):
        hit_tp = (self.position ==  1 and price >= self.tp) or \
                 (self.position == -1 and price <= self.tp)
        hit_sl = (self.position ==  1 and price <= self.sl) or \
                 (self.position == -1 and price >= self.sl)

        if hit_tp or hit_sl:
            reason = "tp" if hit_tp else "sl"
            self._close_trade(price, ts, reason)

    def _open_trade(self, side, price, atr, ts):
        # override in subclasses for real order placement
        qty = self._compute_qty(price)
        if qty == 0:
            return

        self.position        = side
        self.position_size   = qty
        self.entry_price     = price
        self.tp              = price + self.params["tp_atr"] * atr * side
        self.sl              = price - self.params["sl_atr"] * atr * side
        self._was_in_squeeze = False

        side_str = "BUY" if side == 1 else "SELL"
        print(f"  {side_str} {qty} @ {price}  tp={self.tp:.2f}  sl={self.sl:.2f}")

    def _close_trade(self, price, ts, reason=""):
        side_str  = "SELL" if self.position == 1 else "BUY"
        pnl       = (price - self.entry_price) * self.position * self.position_size
        self.pnl += pnl

        self.trades.append(dict(
            entry_price = self.entry_price,
            exit_price  = price,
            side        = self.position,
            qty         = self.position_size,
            pnl         = pnl,
            reason      = reason,
            ts          = ts,
        ))

        print(f"  exit {side_str} @ {price}  [{reason}]  pnl={pnl:+.2f}  total={self.pnl:+.2f}"
              f"  ({len(self.trades)} trades)")

        self.position      = 0
        self.position_size = 0
        self.entry_price   = None
        self.tp            = None
        self.sl            = None
        self.cooldown      = self.params["cooldown_bars"]

    def _compute_qty(self, price):
        # default: fixed 1 contract — override for capital-aware sizing
        return 1

    @staticmethod
    def _log_indicators(ind, ts):
        sq = "SQ" if ind["in_squeeze"] else "  "
        vs = "VOL" if ind["vol_spike"] else "   "
        print(f"  [{sq}] [{vs}]  mom={ind['momentum']:+.4f}  atr={ind['atr']:.2f}")

    def summary(self):
        wins   = [t["pnl"] for t in self.trades if t["pnl"] > 0]
        losses = [t["pnl"] for t in self.trades if t["pnl"] <= 0]
        return dict(
            total_trades = len(self.trades),
            wins         = len(wins),
            losses       = len(losses),
            win_rate     = len(wins) / len(self.trades) if self.trades else 0,
            total_pnl    = self.pnl,
            avg_win      = np.mean(wins)   if wins   else 0,
            avg_loss     = np.mean(losses) if losses else 0,
        )
