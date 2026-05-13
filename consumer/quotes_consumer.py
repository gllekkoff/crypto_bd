import time

from psycopg2.extras import execute_values

from common import kafka_consumer, parse_bitmex_ts, pg_connect, setup_logging

BATCH_SIZE = 500
FLUSH_INTERVAL_SECONDS = 5

log = setup_logging("quotes-consumer")

INSERT_SQL = """
    INSERT INTO quotes (symbol, quote_time, bid_price, ask_price, bid_size, ask_size)
    VALUES %s
"""


def main() -> None:
    consumer = kafka_consumer("quotes-raw", group_id="warehouse-quotes")
    conn = pg_connect()
    log.info("quotes-consumer started")

    batch: list[tuple] = []
    last_flush = time.time()
    written = 0

    def flush() -> None:
        nonlocal batch, last_flush, written
        if not batch:
            return
        try:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, batch, page_size=1000)
            conn.commit()
            written += len(batch)
            if written % 5000 < len(batch):
                log.info("persisted %d quotes total", written)
        except Exception:
            conn.rollback()
            log.exception("flush failed; rolled back")
        batch = []
        last_flush = time.time()

    try:
        for msg in consumer:
            q = msg.value
            try:
                row = (
                    q["symbol"],
                    parse_bitmex_ts(q["quote_time"]),
                    float(q.get("bid_price") or 0) or None,
                    float(q.get("ask_price") or 0) or None,
                    float(q.get("bid_size") or 0) or None,
                    float(q.get("ask_size") or 0) or None,
                )
                batch.append(row)
            except (KeyError, ValueError):
                continue

            if len(batch) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush()
    finally:
        flush()
        conn.close()


if __name__ == "__main__":
    main()
