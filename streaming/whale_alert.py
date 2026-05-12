"""A1 — Whale Alert.

For each symbol maintain a 10-minute sliding window of trade sizes. For every
new trade compute the 95th percentile of sizes in the window; if the trade is
above the threshold, emit a whale alert to Kafka and persist it in Cassandra.

State is held in memory in the consumer process — restarts lose the window
but Kafka offsets persist via the consumer group, so the next trade refills
the window within minutes.
"""

from collections import defaultdict, deque
from typing import Deque, Tuple

import numpy as np

from common import (
    cassandra_session,
    kafka_consumer,
    kafka_producer,
    parse_bitmex_ts,
    setup_logging,
    utcnow,
)

WINDOW_SECONDS = 10 * 60
PERCENTILE = 95
MIN_SAMPLES = 30  # don't fire alerts until the window has warmed up

log = setup_logging("whale-alert")


class SlidingWindow:
    """Time-bounded window of (epoch_seconds, size) tuples."""

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self.items: Deque[Tuple[float, float]] = deque()

    def evict(self, now_seconds: float) -> None:
        cutoff = now_seconds - self.seconds
        while self.items and self.items[0][0] < cutoff:
            self.items.popleft()

    def add(self, ts: float, size: float) -> None:
        self.items.append((ts, size))

    def percentile(self, p: int) -> float | None:
        if len(self.items) < MIN_SAMPLES:
            return None
        sizes = np.fromiter((s for _, s in self.items), dtype=float, count=len(self.items))
        return float(np.percentile(sizes, p))


def main() -> None:
    consumer = kafka_consumer("trades-raw", group_id="whale-alert")
    producer = kafka_producer()
    session = cassandra_session()
    insert_alert = session.prepare(
        """
        INSERT INTO whale_alerts (
            symbol, alert_time, trade_size, threshold_95p, price, side, deviation_percent
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )

    windows: dict[str, SlidingWindow] = defaultdict(lambda: SlidingWindow(WINDOW_SECONDS))
    log.info("whale-alert started (window=%ds, p=%d, min_samples=%d)", WINDOW_SECONDS, PERCENTILE, MIN_SAMPLES)

    for msg in consumer:
        trade = msg.value
        try:
            symbol = trade["symbol"]
            ts = parse_bitmex_ts(trade["trade_time"]).timestamp()
            # size is USD-equivalent for BitMEX inverse contracts; use it as the canonical metric.
            size = float(trade.get("size") or 0)
            price = float(trade.get("price") or 0)
            side = trade.get("side") or ""
        except (KeyError, ValueError) as exc:
            log.warning("malformed trade: %s (%s)", trade, exc)
            continue

        if size <= 0:
            continue

        w = windows[symbol]
        w.evict(ts)
        threshold = w.percentile(PERCENTILE)

        if threshold is not None and size > threshold:
            deviation = (size - threshold) / threshold * 100.0
            now = utcnow()
            alert = {
                "alert_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "trade_size": size,
                "threshold_95p": round(threshold, 2),
                "price": price,
                "side": side,
                "deviation_percent": round(deviation, 2),
            }
            producer.send("whale-alerts", key=symbol, value=alert)
            session.execute_async(
                insert_alert,
                (symbol, now, size, threshold, price, side, deviation),
            )
            log.info("WHALE %s size=%.0f threshold=%.0f dev=%.1f%%", symbol, size, threshold, deviation)

        # Add AFTER computing percentile so the current trade isn't compared against itself.
        w.add(ts, size)


if __name__ == "__main__":
    main()
