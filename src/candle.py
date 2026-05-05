#!/usr/bin/env python3
# Tick-to-candle aggregation and CSV persistence.

import csv
import os
from datetime import datetime


class CandleBuilder:
    def __init__(self):
        self._minute = None
        self._candle = None

    def update(self, timestamp: datetime, price: float, volume: float):
        # returns the closed candle when the minute rolls, otherwise None
        minute = timestamp.replace(second=0, microsecond=0)

        if minute != self._minute:
            finished     = self._candle
            self._minute = minute
            self._candle = dict(
                time=minute, open=price, high=price,
                low=price,   close=price, volume=volume or 0
            )
            return finished

        c = self._candle
        c["high"]    = max(c["high"], price)
        c["low"]     = min(c["low"],  price)
        c["close"]   = price
        c["volume"] += volume or 0
        return None


class CandleCSVWriter:
    HEADER = ["time", "open", "high", "low", "close", "volume"]

    def __init__(self, filename="candles.csv"):
        new_file = not os.path.isfile(filename)
        self._f  = open(filename, "a", newline="")
        self._w  = csv.writer(self._f)
        if new_file:
            self._w.writerow(self.HEADER)

    def write(self, candle: dict):
        self._w.writerow([candle[k] for k in self.HEADER])
        self._f.flush()

    def close(self):
        self._f.close()
