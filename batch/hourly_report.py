from common import pg_connect, setup_logging

log = setup_logging("batch-hourly-report")

SQL = """
WITH bounds AS (
    SELECT
        date_trunc('hour', now()) - interval '12 hours' AS start_hour,
        date_trunc('hour', now())                       AS end_hour
),
hourly AS (
    SELECT
        symbol,
        date_trunc('hour', trade_time) AS hour_ts,
        count(*)                                            AS trade_count,
        sum(size)                                           AS total_volume_usd,
        min(price)                                          AS min_price,
        max(price)                                          AS max_price,
        avg(price)                                          AS avg_price,
        stddev_samp(price)                                  AS price_std,
        sum(CASE WHEN side = 'Buy'  THEN size ELSE 0 END)   AS buy_volume,
        sum(CASE WHEN side = 'Sell' THEN size ELSE 0 END)   AS sell_volume
    FROM trades, bounds
    WHERE trade_time >= bounds.start_hour
      AND trade_time <  bounds.end_hour
    GROUP BY symbol, date_trunc('hour', trade_time)
)
INSERT INTO hourly_reports (
    symbol, hour_ts, trade_count, total_volume_usd,
    min_price, max_price, avg_price, price_std,
    buy_volume, sell_volume, dominant_side, computed_at
)
SELECT
    symbol, hour_ts, trade_count, total_volume_usd,
    min_price, max_price, avg_price, price_std,
    buy_volume, sell_volume,
    CASE WHEN buy_volume >= sell_volume THEN 'Buy' ELSE 'Sell' END,
    now()
FROM hourly
ON CONFLICT (symbol, hour_ts) DO UPDATE SET
    trade_count       = EXCLUDED.trade_count,
    total_volume_usd  = EXCLUDED.total_volume_usd,
    min_price         = EXCLUDED.min_price,
    max_price         = EXCLUDED.max_price,
    avg_price         = EXCLUDED.avg_price,
    price_std         = EXCLUDED.price_std,
    buy_volume        = EXCLUDED.buy_volume,
    sell_volume       = EXCLUDED.sell_volume,
    dominant_side     = EXCLUDED.dominant_side,
    computed_at       = EXCLUDED.computed_at
"""


def run() -> int:
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.rowcount
        conn.commit()
        log.info("upserted %d (symbol, hour) rows", rows)
        return rows
    finally:
        conn.close()


if __name__ == "__main__":
    run()
