#!/usr/bin/env python3
# Live paper-trading runner.
# Connects to PaperBroker via FIX and streams market data via Kafka.
# Applies the squeeze breakout strategy on 1-minute candles in real time.
#
# usage:
#   python src/live.py --config config/config.yaml

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from threading import Event as ThreadEvent

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from paperbroker.market_data import KafkaMarketDataClient
from paperbroker.client      import PaperBrokerClient

from src.strategy import BaseTrader
from src.candle   import CandleBuilder, CandleCSVWriter
from src.utils    import load_config

# -- logging ------------------------------------------------------------------

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-7s  %(message)s",
    handlers = [
        logging.FileHandler(LOG_DIR / f"live_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# -- order manager ------------------------------------------------------------

class OrderManager:
    def __init__(self):
        self.accepted        = ThreadEvent()
        self.filled          = ThreadEvent()
        self.order_id        = None
        self.last_fill_price = None


# -- live trader --------------------------------------------------------------

class LiveTrader(BaseTrader):
    # Extends BaseTrader with real FIX order placement.

    def __init__(self, params, fix_client, manager, symbol):
        super().__init__(params)
        self.fix     = fix_client
        self.manager = manager
        self.symbol  = symbol

    def _compute_qty(self, price):
        result = self.fix.get_max_placeable(
            symbol=self.symbol, price=price,
            side="BUY" if True else "SELL"   # side resolved in _open_trade
        )
        if not result.get("success"):
            return 0
        return max(1, int(result["maxQty"] * self.params["size_pct"]))

    def _open_trade(self, side, price, atr, ts):
        side_str = "BUY" if side == 1 else "SELL"

        result = self.fix.get_max_placeable(
            symbol=self.symbol, price=price, side=side_str
        )
        if not result.get("success"):
            log.warning("get_max_placeable failed, skipping")
            return

        qty = max(1, int(result["maxQty"] * self.params["size_pct"]))

        self.manager.accepted.clear()
        self.manager.filled.clear()

        order_id = self.fix.place_order(
            full_symbol=self.symbol,
            side=side_str,
            qty=qty,
            price=price,
            ord_type="LIMIT",
        )
        self.manager.order_id = order_id

        if not self.manager.accepted.wait(timeout=15):
            log.warning("order not accepted within 15s, abort")
            return
        if not self.manager.filled.wait(timeout=20):
            log.warning("order not filled within 20s, abort")
            return

        fill = self.manager.last_fill_price

        self.position        = side
        self.position_size   = qty
        self.entry_price     = fill
        self.tp              = fill + self.params["tp_atr"] * atr * side
        self.sl              = fill - self.params["sl_atr"] * atr * side
        self._was_in_squeeze = False

        log.info(f"{side_str} {qty} @ {fill}  tp={self.tp:.2f}  sl={self.sl:.2f}")

    def _close_trade(self, price, ts, reason=""):
        side_str = "SELL" if self.position == 1 else "BUY"

        self.manager.accepted.clear()
        self.manager.filled.clear()

        order_id = self.fix.place_order(
            full_symbol=self.symbol,
            side=side_str,
            qty=self.position_size,
            price=price,
            ord_type="LIMIT",
        )
        self.manager.order_id = order_id
        self.manager.filled.wait(timeout=10)

        exit_price = self.manager.last_fill_price or price

        # delegate pnl tracking to base
        saved_pos  = self.position
        saved_size = self.position_size
        saved_entry = self.entry_price

        pnl = (exit_price - saved_entry) * saved_pos * saved_size
        self.pnl += pnl
        self.trades.append(dict(
            entry_price = saved_entry,
            exit_price  = exit_price,
            side        = saved_pos,
            qty         = saved_size,
            pnl         = pnl,
            reason      = reason,
            ts          = ts,
        ))

        log.info(f"exit {side_str} @ {exit_price}  [{reason}]  pnl={pnl:+.2f}  total={self.pnl:+.2f}"
                 f"  ({len(self.trades)} trades)")

        self.position      = 0
        self.position_size = 0
        self.entry_price   = None
        self.tp            = None
        self.sl            = None
        self.cooldown      = self.params["cooldown_bars"]

    @staticmethod
    def _log_indicators(ind, ts):
        sq = "SQ" if ind["in_squeeze"] else "  "
        vs = "VOL" if ind["vol_spike"] else "   "
        log.info(f"  [{sq}] [{vs}]  mom={ind['momentum']:+.4f}  atr={ind['atr']:.2f}")


# -- main ---------------------------------------------------------------------

async def main(cfg):
    params     = cfg["strategy"]
    instrument = os.getenv("INSTRUMENT", "HNXDS:VN30F2605")
    env_id     = os.getenv("PAPERBROKER_ENV_ID", "test")

    fix = PaperBrokerClient(
        default_sub_account = os.getenv("PAPER_ACCOUNT_ID_D1", "D1"),
        username            = os.getenv("PAPER_USERNAME", "BL01"),
        password            = os.getenv("PAPER_PASSWORD"),
        rest_base_url       = os.getenv("PAPER_REST_BASE_URL", "http://localhost:9090"),
        socket_connect_host = os.getenv("SOCKET_HOST", "localhost"),
        socket_connect_port = int(os.getenv("SOCKET_PORT", "5001")),
        sender_comp_id      = os.getenv("SENDER_COMP_ID", "cross-FIX"),
        target_comp_id      = os.getenv("TARGET_COMP_ID", "SERVER"),
        order_store_path    = "orders.db",
        console             = False,
    )

    manager = OrderManager()

    def on_logon(session_id, **_):
        log.info(f"FIX connected: {session_id}")

    def on_accepted(cl_ord_id, **_):
        if cl_ord_id == manager.order_id:
            manager.accepted.set()

    def on_filled(cl_ord_id, last_px, last_qty, **_):
        if cl_ord_id == manager.order_id:
            log.info(f"  filled {last_qty} @ {last_px}")
            manager.last_fill_price = last_px
            manager.filled.set()

    fix.on("fix:logon",          on_logon)
    fix.on("fix:order:accepted", on_accepted)
    fix.on("fix:order:filled",   on_filled)

    fix.connect()
    if not fix.wait_until_logged_on(timeout=10):
        log.error("FIX login failed")
        return

    log.info("FIX ready")

    kafka = KafkaMarketDataClient(
        bootstrap_servers = os.getenv("PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS"),
        username          = os.getenv("PAPERBROKER_KAFKA_USERNAME"),
        password          = os.getenv("PAPERBROKER_KAFKA_PASSWORD"),
        env_id            = env_id,
        merge_updates     = True,
    )

    builder = CandleBuilder()
    trader  = LiveTrader(params, fix, manager, instrument)
    writer  = CandleCSVWriter(f"logs/candles_{datetime.now().strftime('%Y%m%d')}.csv")

    def on_quote(instrument, quote):
        if not quote.latest_matched_price:
            return

        ts = getattr(quote, "timestamp", None)
        if isinstance(ts, datetime):
            timestamp = ts
        elif isinstance(ts, (int, float)):
            timestamp = datetime.fromtimestamp(ts)
        else:
            timestamp = datetime.now()

        candle = builder.update(
            timestamp = timestamp,
            price     = quote.latest_matched_price,
            volume    = quote.total_matched_quantity or 0,
        )

        if candle:
            log.info(f"{candle['time']} | O:{candle['open']}  H:{candle['high']}  "
                     f"L:{candle['low']}  C:{candle['close']}  V:{candle['volume']}")
            writer.write(candle)
            trader.on_candle(candle)

    log.info(f"subscribing to {instrument}")
    await kafka.subscribe(instrument, on_quote)
    await kafka.start()

    log.info("running (ctrl-c to stop)")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        await kafka.stop()
        writer.close()
        s = trader.summary()
        log.info(f"session done  trades={s['total_trades']}  pnl={s['total_pnl']:+.2f}"
                 f"  win_rate={s['win_rate']:.1%}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    asyncio.run(main(cfg))
