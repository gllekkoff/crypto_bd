import time

from psycopg2.extras import execute_values

from common import kafka_consumer, parse_bitmex_ts, pg_connect, setup_logging

BATCH_SIZE = 200
FLUSH_INTERVAL_SECONDS = 5

log = setup_logging("trades-consumer")

INSERT_SQL = """
    INSERT INTO trades (
        trade_id, symbol, trade_time, price, size,
        home_notional, foreign_notional, side
    ) VALUES %s
    ON CONFLICT (trade_id) DO NOTHING
"""


def main() -> None:
    consumer = kafka_consumer("trades-raw", group_id="warehouse-trades")
    conn = pg_connect()
    log.info("trades-consumer started")

    batch: list[tuple] = []
    last_flush = time.time()
    written = 0

    def flush() -> None:
        nonlocal batch, last_flush, written
        if not batch:
            return
        try:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, batch, page_size=500)
            conn.commit()
            written += len(batch)
            log.info("persisted batch of %d trades (total=%d)", len(batch), written)
        except Exception:
            conn.rollback()
            log.exception("flush failed; rolled back")
        batch = []
        last_flush = time.time()

    try:
        for msg in consumer:
            t = msg.value
            try:
                trade_id = t.get("trade_id") or ""
                if not trade_id:
                    continue
                row = (
                    trade_id,
                    t["symbol"],
                    parse_bitmex_ts(t["trade_time"]),
                    float(t.get("price") or 0),
                    float(t.get("size") or 0),
                    float(t.get("home_notional") or 0),
                    float(t.get("foreign_notional") or 0),
                    t.get("side") or "",
                )
                batch.append(row)
            except (KeyError, ValueError) as exc:
                log.warning("malformed trade: %s (%s)", t, exc)
                continue

            if len(batch) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush()
    finally:
        flush()
        conn.close()


if __name__ == "__main__":
    main()
