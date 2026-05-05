#!/usr/bin/env python3
# Parameter optimisation using Optuna.
# Objective: maximise Sharpe ratio on in-sample data.
#
# usage:
#   python src/optimize.py --config config/config.yaml

import argparse
import sys
from pathlib import Path
from datetime import datetime

import optuna
optuna.logging.set_verbosity(optuna.logging.WARNING)

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.strategy import BaseTrader
from src.utils    import load_config, load_csv, metrics_from_trades, save_results


class OptimTrader(BaseTrader):
    def _compute_qty(self, price):
        return 1   # fixed 1 contract during optimisation


def objective(trial, df, cfg):
    opt = cfg["optimization"]

    params = dict(
        bb_period     = cfg["strategy"]["bb_period"],   # not tuned, keep fixed
        atr_period    = cfg["strategy"]["atr_period"],
        bb_mult       = trial.suggest_float("bb_mult",       *opt["bb_mult_range"]),
        kc_mult       = trial.suggest_float("kc_mult",       *opt["kc_mult_range"]),
        vol_mult      = trial.suggest_float("vol_mult",       *opt["vol_mult_range"]),
        tp_atr        = trial.suggest_float("tp_atr",         *opt["tp_atr_range"]),
        sl_atr        = trial.suggest_float("sl_atr",         *opt["sl_atr_range"]),
        mom_period    = trial.suggest_int(  "mom_period",     *opt["mom_period_range"]),
        cooldown_bars = trial.suggest_int(  "cooldown_bars",  *opt["cooldown_bars_range"]),
        size_pct      = cfg["strategy"]["size_pct"],
    )

    trader = OptimTrader(params)

    for _, row in df.iterrows():
        candle = row.to_dict()
        candle["time"] = row.name
        trader.on_candle(candle)

    if len(trader.trades) < 5:
        return -999.0

    equity = [cfg["strategy"]["capital"]] + \
             [cfg["strategy"]["capital"] + sum(t["pnl"] for t in trader.trades[:i+1])
              for i in range(len(trader.trades))]

    m = metrics_from_trades(trader.trades, equity, cfg["strategy"]["capital"])
    return m["sharpe"]


def run_optimize(cfg):
    data_file  = cfg["data"]["in_sample_file"]
    start_date = cfg["in_sample"]["start_date"]
    end_date   = cfg["in_sample"]["end_date"]
    out_dir    = Path(cfg["results"]["base_directory"]) / "optimize" / \
                 datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_csv(data_file, start_date, end_date)
    print(f"loaded {len(df)} bars — running {cfg['optimization']['n_trials']} trials")

    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: objective(trial, df, cfg),
        n_trials = cfg["optimization"]["n_trials"],
        show_progress_bar = True,
    )

    best = study.best_params
    print(f"\nbest sharpe: {study.best_value:.4f}")
    print("best params:")
    for k, v in best.items():
        print(f"  {k}: {v}")

    # save best params
    import json
    (out_dir / "best_params.json").write_text(json.dumps(best, indent=2))
    print(f"\nresults saved to {out_dir}")

    return best


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="squeeze breakout optimiser")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_optimize(cfg)
