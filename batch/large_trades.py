from typing import Optional

from common import pg_connect, setup_logging

log = setup_logging("batch-large-trades")

SQL = """
WITH window_bounds AS (
    SELECT
        now() - %(period)s::interval AS period_start,
        now()                        AS period_end
),
threshold AS (
    SELECT
        symbol,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY size) AS p90
    FROM trades, window_bounds
    WHERE trade_time >= window_bounds.period_start
      AND (%(symbol)s::text IS NULL OR symbol = %(symbol)s)
    GROUP BY symbol
),
large_trades AS (
    SELECT t.trade_id, t.symbol, t.trade_time, t.price, t.size, t.side
    FROM trades t
    JOIN threshold th ON t.symbol = th.symbol
    JOIN window_bounds wb ON true
    WHERE t.size > th.p90
      AND t.trade_time >= wb.period_start
      -- ensure a full 5-minute after-window exists within the period
      AND t.trade_time <= wb.period_end - interval '5 minutes'
),
impacts AS (
    SELECT
        lt.symbol,
        lt.trade_id,
        lt.trade_time,
        lt.price,
        lt.size,
        lt.side,
        (SELECT avg(t2.price) FROM trades t2
          WHERE t2.symbol = lt.symbol
            AND t2.trade_time >= lt.trade_time - interval '5 minutes'
            AND t2.trade_time <  lt.trade_time) AS avg_before,
        (SELECT avg(t2.price) FROM trades t2
          WHERE t2.symbol = lt.symbol
            AND t2.trade_time >  lt.trade_time
            AND t2.trade_time <= lt.trade_time + interval '5 minutes') AS avg_after
    FROM large_trades lt
)
SELECT
    impacts.symbol,
    count(*)::bigint                                         AS large_trade_count,
    avg(size)::double precision                              AS avg_large_trade_size,
    (SELECT p90 FROM threshold WHERE threshold.symbol = impacts.symbol) AS p90_threshold,
    avg(CASE WHEN avg_before > 0
             THEN (avg_after - avg_before) / avg_before * 100 END)::double precision  AS avg_impact_pct,
    avg(CASE WHEN side = 'Buy'  AND avg_before > 0
             THEN (avg_after - avg_before) / avg_before * 100 END)::double precision  AS avg_buy_impact_pct,
    avg(CASE WHEN side = 'Sell' AND avg_before > 0
             THEN (avg_after - avg_before) / avg_before * 100 END)::double precision  AS avg_sell_impact_pct
FROM impacts
WHERE avg_before IS NOT NULL AND avg_after IS NOT NULL
GROUP BY impacts.symbol
ORDER BY impacts.symbol
"""


def compute(period: str = "24 hours", symbol: Optional[str] = None) -> list[dict]:
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL, {"period": period, "symbol": symbol})
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def run() -> int:
    rows = compute("24 hours")
    log.info("whale-impact: %d symbols analyzed (24h window)", len(rows))
    for r in rows:
        log.info(
            "  %s: count=%s avg_size=%.0f p90=%.0f impact=%.3f%%",
            r["symbol"],
            r["large_trade_count"],
            r["avg_large_trade_size"] or 0,
            r["p90_threshold"] or 0,
            r["avg_impact_pct"] or 0,
        )
    return len(rows)


if __name__ == "__main__":
    run()
