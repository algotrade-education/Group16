#!/usr/bin/env python3
# Backtester for the squeeze breakout strategy.
# Reads a CSV of 1-min OHLCV bars and replays them through BaseTrader.
#
# usage:
#   python src/backtest.py --mode in_sample  --config config/config.yaml
#   python src/backtest.py --mode out_sample --config config/config.yaml

import argparse
import os
import sys
import json
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.strategy import BaseTrader
from src.utils    import load_config, load_csv, metrics_from_trades, save_results


class BacktestTrader(BaseTrader):
    # capital-aware qty sizing for backtesting

    def __init__(self, params, capital, contract_value, margin_rate, fee_per_trade):
        super().__init__(params)
        self.capital        = capital
        self.contract_value = contract_value
        self.margin_rate    = margin_rate
        self.fee            = fee_per_trade
        self.equity         = [capital]

    def _compute_qty(self, price):
        margin_per_contract = self.contract_value * self.margin_rate
        available           = self.capital * self.params["size_pct"]
        qty                 = int(available / margin_per_contract)
        return max(1, qty)

    def _close_trade(self, price, ts, reason=""):
        super()._close_trade(price, ts, reason)
        # deduct fees (open + close)
        self.pnl    -= self.fee * 2 * self.position_size if self.position_size else 0
        self.capital = 400000000 + self.pnl   # recalc from base capital
        self.equity.append(self.capital)


def run_backtest(cfg, mode):
    params = cfg["strategy"]

    if mode == "in_sample":
        data_file   = cfg["data"]["in_sample_file"]
        start_date  = cfg["in_sample"]["start_date"]
        end_date    = cfg["in_sample"]["end_date"]
        out_dir     = Path(cfg["results"]["base_directory"]) / "backtest" / f"in_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    else:
        data_file   = cfg["data"]["out_sample_file"]
        start_date  = cfg["out_sample"]["start_date"]
        end_date    = cfg["out_sample"]["end_date"]
        out_dir     = Path(cfg["results"]["base_directory"]) / "backtest" / f"out_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_csv(data_file, start_date, end_date)
    print(f"loaded {len(df)} bars from {data_file} ({start_date} to {end_date})")

    trader = BacktestTrader(
        params         = params,
        capital        = cfg["strategy"]["capital"],
        contract_value = cfg["strategy"]["contract_value"],
        margin_rate    = cfg["strategy"]["margin_rate"],
        fee_per_trade  = cfg["strategy"]["fee_per_trade"],
    )

    for _, row in df.iterrows():
        candle = row.to_dict()
        candle["time"] = row.name
        trader.on_candle(candle)

    m = metrics_from_trades(trader.trades, trader.equity, cfg["strategy"]["capital"])
    save_results(m, trader.trades, out_dir)

    print(f"\nresults saved to {out_dir}")
    print(f"total trades: {m['total_trades']}")
    print(f"net pnl:      {m['net_pnl']:+.2f}")
    print(f"hpr:          {m['hpr']:.2%}")
    print(f"sharpe:       {m['sharpe']:.2f}")
    print(f"max dd:       {m['max_drawdown']:.2%}")

    return m


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="squeeze breakout backtester")
    parser.add_argument("--mode",   choices=["in_sample", "out_sample"], required=True)
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_backtest(cfg, args.mode)
