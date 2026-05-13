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
HALF_SECONDS = 5 * 60
MIN_SAMPLES_PER_HALF = 30
VOL_MULT = 2.0
ALERT_COOLDOWN_SECONDS = 60

log = setup_logging("volatility-monitor")


def main() -> None:
    consumer = kafka_consumer("trades-raw", group_id="volatility-monitor")
    producer = kafka_producer()
    session = cassandra_session()
    insert_alert = session.prepare(
        """
        INSERT INTO volatility_alerts (
            symbol, alert_time, current_volatility, previous_volatility, ratio
        ) VALUES (?, ?, ?, ?, ?)
        """
    )

    windows: dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
    last_alert_ts: dict[str, float] = {}
    log.info("volatility-monitor started (window=%ds, half=%ds, mult=%.2f)", WINDOW_SECONDS, HALF_SECONDS, VOL_MULT)

    for msg in consumer:
        trade = msg.value
        try:
            symbol = trade["symbol"]
            ts = parse_bitmex_ts(trade["trade_time"]).timestamp()
            price = float(trade.get("price") or 0)
        except (KeyError, ValueError):
            continue

        if price <= 0:
            continue

        w = windows[symbol]
        w.append((ts, price))

        cutoff = ts - WINDOW_SECONDS
        while w and w[0][0] < cutoff:
            w.popleft()

        split = ts - HALF_SECONDS
        current_prices = [p for t, p in w if t >= split]
        previous_prices = [p for t, p in w if t < split]

        if len(current_prices) < MIN_SAMPLES_PER_HALF or len(previous_prices) < MIN_SAMPLES_PER_HALF:
            continue

        current_std = float(np.std(current_prices))
        previous_std = float(np.std(previous_prices))

        if previous_std <= 1e-9:
            continue

        ratio = current_std / previous_std
        if ratio < VOL_MULT:
            continue

        last_ts = last_alert_ts.get(symbol)
        if last_ts is not None and ts - last_ts < ALERT_COOLDOWN_SECONDS:
            continue
        last_alert_ts[symbol] = ts

        now = utcnow()
        alert = {
            "alert_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "current_volatility": round(current_std, 4),
            "previous_volatility": round(previous_std, 4),
            "ratio": round(ratio, 2),
        }
        producer.send("volatility-alerts", key=symbol, value=alert)
        session.execute_async(insert_alert, (symbol, now, current_std, previous_std, ratio))
        log.info(
            "VOL %s current=%.4f prev=%.4f ratio=%.2fx", symbol, current_std, previous_std, ratio
        )


if __name__ == "__main__":
    main()
