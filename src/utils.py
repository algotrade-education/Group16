#!/usr/bin/env python3
# Shared utilities: config, data loading, performance metrics, result saving.

import json
import yaml
import numpy as np
import pandas as pd
from pathlib import Path


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def load_csv(filepath, start_date=None, end_date=None):
    # data_generate.py writes the datetime index under the column name "time"
    df = pd.read_csv(filepath, parse_dates=["time"], index_col="time")
    df.index.name = "time"
    df.sort_index(inplace=True)

    # keep only the four OHLCV columns backtest needs; raise early if missing
    required = ("open", "high", "low", "close", "volume")
    for col in required:
        if col not in df.columns:
            raise ValueError(
                f"CSV missing required column: '{col}'  (found: {list(df.columns)})\n"
                f"run src/data_generate.py to produce a compatible file."
            )
    df = df[list(required)]

    if start_date:
        df = df[df.index >= start_date]
    if end_date:
        df = df[df.index <= end_date]
    return df


def metrics_from_trades(trades, equity_curve, initial_capital):
    if not trades:
        return dict(
            total_trades=0, net_pnl=0, hpr=0,
            sharpe=0, max_drawdown=0, win_rate=0,
            avg_win=0, avg_loss=0,
        )

    pnls    = np.array([t["pnl"] for t in trades])
    net_pnl = pnls.sum()
    hpr     = net_pnl / initial_capital

    sharpe = (pnls.mean() / pnls.std()) * np.sqrt(252) if pnls.std() > 0 else 0.0

    eq   = np.array(equity_curve)
    peak = np.maximum.accumulate(eq)
    dd   = (eq - peak) / peak
    max_dd = dd.min()

    wins = pnls[pnls > 0]

    return dict(
        total_trades = len(trades),
        net_pnl      = float(net_pnl),
        hpr          = float(hpr),
        sharpe       = float(sharpe),
        max_drawdown = float(max_dd),
        win_rate     = float(len(wins) / len(pnls)),
        avg_win      = float(wins.mean())            if len(wins) > 0    else 0,
        avg_loss     = float(pnls[pnls <= 0].mean()) if (pnls <= 0).any() else 0,
    )


def save_results(metrics, trades, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "performance_metrics.json").write_text(
        json.dumps(metrics, indent=2)
    )

    lines = [
        f"total trades:  {metrics['total_trades']}",
        f"net pnl:       {metrics['net_pnl']:+.2f}",
        f"hpr:           {metrics['hpr']:.2%}",
        f"sharpe:        {metrics['sharpe']:.4f}",
        f"max drawdown:  {metrics['max_drawdown']:.2%}",
        f"win rate:      {metrics['win_rate']:.2%}",
        f"avg win:       {metrics['avg_win']:+.2f}",
        f"avg loss:      {metrics['avg_loss']:+.2f}",
    ]
    (out_dir / "performance_metrics.txt").write_text("\n".join(lines))

    if trades:
        import csv
        keys = list(trades[0].keys())
        with open(out_dir / "trade_log.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(trades)
