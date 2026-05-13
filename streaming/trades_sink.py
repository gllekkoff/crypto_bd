from common import (
    cassandra_session,
    kafka_consumer,
    parse_bitmex_ts,
    setup_logging,
)

log = setup_logging("trades-sink")


def main() -> None:
    consumer = kafka_consumer("trades-raw", group_id="trades-sink")
    session = cassandra_session()
    insert_stmt = session.prepare(
        """
        INSERT INTO trades (symbol, date, trade_time, trade_id, price, size, side)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )

    written = 0
    for msg in consumer:
        trade = msg.value
        try:
            symbol = trade["symbol"]
            ts = parse_bitmex_ts(trade["trade_time"])
            trade_id = trade.get("trade_id") or ""
            price = float(trade.get("price") or 0)
            size = float(trade.get("size") or 0)
            side = trade.get("side") or ""
        except (KeyError, ValueError):
            continue

        if not trade_id or price <= 0:
            continue

        date_str = ts.strftime("%Y-%m-%d")
        session.execute_async(insert_stmt, (symbol, date_str, ts, trade_id, price, size, side))
        written += 1
        if written % 500 == 0:
            log.info("persisted %d trades", written)


if __name__ == "__main__":
    main()
