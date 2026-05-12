"""A3 — Market Momentum dashboard.

Each minute, for each symbol, write one row to Cassandra `market_momentum`:
    - current_price       — last trade price in the minute
    - price_change_pct    — (last_price - prev_minute_last_price) / prev * 100
    - volume              — sum of trade sizes (USD-equivalent) in the minute
    - buy_sell_ratio      — buy_volume / sell_volume in the minute

A background flusher emits the row when a minute is "closed" (i.e. when the
current wall clock has crossed into the next minute) so that even when trades
stop arriving briefly the dashboard still gets a row written.
"""

import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from common import (
    cassandra_session,
    kafka_consumer,
    parse_bitmex_ts,
    setup_logging,
    utcnow,
)

FLUSH_INTERVAL_SECONDS = 10

log = setup_logging("market-momentum")


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


@dataclass
class MinuteState:
    minute: datetime | None = None
    first_price: float = 0.0
    last_price: float = 0.0
    volume_buy: float = 0.0
    volume_sell: float = 0.0
    trade_count: int = 0


class MomentumAggregator:
    def __init__(self, session, insert_stmt) -> None:
        self.session = session
        self.insert_stmt = insert_stmt
        self.state: dict[str, MinuteState] = defaultdict(MinuteState)
        self.prev_minute_price: dict[str, float] = {}
        self.lock = threading.Lock()

    def on_trade(self, symbol: str, ts: datetime, price: float, size: float, side: str) -> None:
        minute = minute_floor(ts)
        with self.lock:
            s = self.state[symbol]
            if s.minute is None:
                s.minute = minute
                s.first_price = price
            elif minute != s.minute:
                # Trade belongs to a different minute: flush the old one first.
                self._flush_locked(symbol, s)
                s.minute = minute
                s.first_price = price
                s.last_price = 0.0
                s.volume_buy = 0.0
                s.volume_sell = 0.0
                s.trade_count = 0

            s.last_price = price
            s.trade_count += 1
            if side == "Buy":
                s.volume_buy += size
            else:
                s.volume_sell += size

    def flush_closed_minutes(self) -> None:
        """Flush any per-symbol minute that is strictly older than the current wall-clock minute."""
        current_minute = minute_floor(utcnow())
        with self.lock:
            for symbol, s in list(self.state.items()):
                if s.minute is not None and s.minute < current_minute:
                    self._flush_locked(symbol, s)
                    s.minute = None

    def _flush_locked(self, symbol: str, s: MinuteState) -> None:
        if s.minute is None or s.trade_count == 0:
            return

        volume = s.volume_buy + s.volume_sell
        prev_price = self.prev_minute_price.get(symbol, s.first_price)
        change_pct = ((s.last_price - prev_price) / prev_price * 100.0) if prev_price > 0 else 0.0

        if s.volume_sell > 0:
            ratio = s.volume_buy / s.volume_sell
        else:
            ratio = 999.99 if s.volume_buy > 0 else 0.0
        ratio = min(ratio, 999.99)

        self.session.execute_async(
            self.insert_stmt,
            (symbol, s.minute, float(s.last_price), float(round(change_pct, 4)),
             float(round(volume, 2)), float(round(ratio, 4))),
        )
        self.prev_minute_price[symbol] = s.last_price
        log.info(
            "MOMENTUM %s %s price=%.2f change=%.2f%% vol=%.0f buy/sell=%.2f trades=%d",
            symbol, s.minute.strftime("%H:%M"), s.last_price, change_pct, volume, ratio, s.trade_count,
        )


def flusher_loop(agg: MomentumAggregator, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            agg.flush_closed_minutes()
        except Exception:
            log.exception("flusher error")
        stop.wait(FLUSH_INTERVAL_SECONDS)


def main() -> None:
    consumer = kafka_consumer("trades-raw", group_id="market-momentum")
    session = cassandra_session()
    insert_stmt = session.prepare(
        """
        INSERT INTO market_momentum (
            symbol, minute_ts, current_price, price_change_pct, volume, buy_sell_ratio
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
    )

    agg = MomentumAggregator(session, insert_stmt)
    stop = threading.Event()
    flusher = threading.Thread(target=flusher_loop, args=(agg, stop), daemon=True)
    flusher.start()
    log.info("market-momentum started (flush_interval=%ds)", FLUSH_INTERVAL_SECONDS)

    try:
        for msg in consumer:
            trade = msg.value
            try:
                symbol = trade["symbol"]
                ts = parse_bitmex_ts(trade["trade_time"])
                price = float(trade.get("price") or 0)
                size = float(trade.get("size") or 0)
                side = trade.get("side") or ""
            except (KeyError, ValueError):
                continue
            if price <= 0 or size <= 0 or not side:
                continue
            agg.on_trade(symbol, ts, price, size, side)
    finally:
        stop.set()


if __name__ == "__main__":
    main()
