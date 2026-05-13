from common import pg_connect, setup_logging

log = setup_logging("batch-trading-patterns")

SQL_TRADE_STATS = """
WITH per_hour_bucket AS (
    SELECT
        symbol,
        date_trunc('hour', trade_time)                AS hour_ts,
        extract(hour FROM trade_time)::smallint       AS hod,
        count(*)                                      AS cnt,
        sum(size)                                     AS vol,
        stddev_samp(price)                            AS vol_std
    FROM trades
    GROUP BY symbol, date_trunc('hour', trade_time), extract(hour FROM trade_time)
)
SELECT
    symbol,
    hod,
    avg(cnt)::double precision      AS avg_trade_count,
    avg(vol)::double precision      AS avg_volume_usd,
    avg(vol_std)::double precision  AS avg_volatility,
    count(*)::int                   AS sample_hours
FROM per_hour_bucket
GROUP BY symbol, hod
"""

SQL_SPREAD_STATS = """
SELECT
    symbol,
    extract(hour FROM quote_time)::smallint AS hod,
    avg(ask_price - bid_price)::double precision AS avg_spread
FROM quotes
WHERE bid_price IS NOT NULL AND ask_price IS NOT NULL
  AND bid_price > 0 AND ask_price > 0
GROUP BY symbol, extract(hour FROM quote_time)
"""

UPSERT_SQL = """
INSERT INTO trading_patterns (
    symbol, hour_of_day, avg_trade_count, avg_volume_usd,
    avg_volatility, avg_spread, sample_hours, computed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (symbol, hour_of_day) DO UPDATE SET
    avg_trade_count = EXCLUDED.avg_trade_count,
    avg_volume_usd  = EXCLUDED.avg_volume_usd,
    avg_volatility  = EXCLUDED.avg_volatility,
    avg_spread      = EXCLUDED.avg_spread,
    sample_hours    = EXCLUDED.sample_hours,
    computed_at     = now()
"""


def run() -> int:
    conn = pg_connect()
    written = 0
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_TRADE_STATS)
            trade_rows = cur.fetchall()
            cur.execute(SQL_SPREAD_STATS)
            spreads = {(r[0], r[1]): r[2] for r in cur.fetchall()}

            for symbol, hod, avg_cnt, avg_vol, avg_vol_std, samples in trade_rows:
                avg_spread = spreads.get((symbol, hod))
                cur.execute(
                    UPSERT_SQL,
                    (
                        symbol,
                        int(hod),
                        float(avg_cnt or 0),
                        float(avg_vol or 0),
                        float(avg_vol_std or 0),
                        float(avg_spread) if avg_spread is not None else None,
                        int(samples or 0),
                    ),
                )
                written += 1
        conn.commit()
        log.info("upserted %d (symbol, hour_of_day) rows", written)
        return written
    finally:
        conn.close()


if __name__ == "__main__":
    run()
