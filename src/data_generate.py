#!/usr/bin/env python3
# Fetch raw tick data from the quote DB and aggregate into 1-minute OHLCV bars.
# Output CSVs are drop-in compatible with backtest.py and optimize.py.
#
# usage:
#   python src/data_generate.py \
#       --in-start  2021-01-01 --in-end  2025-08-30 \
#       --out-start 2025-09-01 --out-end 2026-02-28
#
# output:
#   data/sample/vn30_in_sample.csv
#   data/sample/vn30_out_sample.csv
#
# CSV schema expected by backtest / optimize:
#   time (index, datetime), open, high, low, close, volume

import os
import argparse
import psycopg
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


# -- db -----------------------------------------------------------------------

def _connect():
    return psycopg.connect(
        host     = os.getenv("DB_HOST"),
        port     = os.getenv("DB_PORT"),
        dbname   = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
    )


def fetch_ticks(start_date_str: str, end_date_str: str) -> pd.DataFrame:
    start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date_str,   "%Y-%m-%d").date()

    query = """
        SELECT
            m.datetime                  AS datetime,
            m.tickersymbol,
            m.price,
            tb_open.price               AS open_price,
            tb_close.price              AS close_price,
            tb_max.price                AS high_price,
            tb_min.price                AS low_price,
            tb.quantity                 AS total_quantity
        FROM "quote"."matched" m
        INNER JOIN "quote"."open"  tb_open
            ON  m.tickersymbol    = tb_open.tickersymbol
            AND m.datetime::DATE  = tb_open.datetime::DATE
        INNER JOIN "quote"."close" tb_close
            ON  m.tickersymbol    = tb_close.tickersymbol
            AND m.datetime::DATE  = tb_close.datetime::DATE
        INNER JOIN "quote"."max"   tb_max
            ON  m.tickersymbol    = tb_max.tickersymbol
            AND m.datetime::DATE  = tb_max.datetime::DATE
        INNER JOIN "quote"."min"   tb_min
            ON  m.tickersymbol    = tb_min.tickersymbol
            AND m.datetime::DATE  = tb_min.datetime::DATE
        INNER JOIN "quote"."matchedvolume" tb
            ON  m.tickersymbol    = tb.tickersymbol
            AND m.datetime        = tb.datetime
        WHERE m.datetime::DATE BETWEEN %s AND %s
          AND m.tickersymbol LIKE 'VN30F%%'
        ORDER BY m.datetime, m.tickersymbol;
    """

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (start, end))
            rows = cur.fetchall()

    cols = [
        "datetime", "tickersymbol", "price",
        "open_price", "close_price", "high_price", "low_price", "total_quantity",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


# -- aggregation --------------------------------------------------------------

def aggregate_to_1min(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate raw tick rows into 1-minute OHLCV bars per ticker,
    then pick the front-month contract for each minute (lowest expiry suffix).

    The DB already carries open/high/low/close per day from separate tables,
    but we need per-minute bars, so we re-derive them from the tick prices.
    volume is the sum of matched quantity within each minute bucket.
    """
    if df.empty:
        raise ValueError("no rows returned from DB")

    df = df.copy()
    df["minute"] = df["datetime"].dt.floor("min")

    # front-month: sort tickers and take the first (e.g. VN30F2501 < VN30F2502)
    front = df.groupby("minute")["tickersymbol"].min().rename("front_ticker")
    df = df.merge(front, on="minute")
    df = df[df["tickersymbol"] == df["front_ticker"]].copy()

    ohlcv = (
        df.groupby("minute")
        .agg(
            open   = ("price", "first"),
            high   = ("price", "max"),
            low    = ("price", "min"),
            close  = ("price", "last"),
            volume = ("total_quantity", "sum"),
        )
        .reset_index()
        .rename(columns={"minute": "time"})
    )

    ohlcv.set_index("time", inplace=True)
    ohlcv.sort_index(inplace=True)

    # drop any bars outside normal trading hours (09:00–15:00 HCM time)
    ohlcv = ohlcv.between_time("09:00", "15:00")

    return ohlcv


# -- save ---------------------------------------------------------------------

def save(df: pd.DataFrame, filename: str):
    out_dir = "data/sample"
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    df.to_csv(path)
    print(f"saved {len(df)} bars -> {path}")


# -- main ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="fetch VN30F tick data and aggregate to 1-min OHLCV"
    )
    parser.add_argument("--in-start",  required=True, help="in-sample start  YYYY-MM-DD")
    parser.add_argument("--in-end",    required=True, help="in-sample end    YYYY-MM-DD")
    parser.add_argument("--out-start", required=True, help="out-sample start YYYY-MM-DD")
    parser.add_argument("--out-end",   required=True, help="out-sample end   YYYY-MM-DD")
    args = parser.parse_args()

    print(f"fetching in-sample  {args.in_start} -> {args.in_end}")
    in_ticks = fetch_ticks(args.in_start, args.in_end)
    print(f"  {len(in_ticks)} tick rows")
    in_ohlcv = aggregate_to_1min(in_ticks)
    save(in_ohlcv, "vn30_in_sample.csv")

    print(f"\nfetching out-sample {args.out_start} -> {args.out_end}")
    out_ticks = fetch_ticks(args.out_start, args.out_end)
    print(f"  {len(out_ticks)} tick rows")
    out_ohlcv = aggregate_to_1min(out_ticks)
    save(out_ohlcv, "vn30_out_sample.csv")

    print("\ndone")


if __name__ == "__main__":
    main()
