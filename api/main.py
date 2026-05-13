import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import psycopg2
import psycopg2.extras
from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


# connections

def connect_cassandra() -> Session:
    hosts = [h.strip() for h in os.getenv("CASSANDRA_HOSTS", "cassandra").split(",") if h.strip()]
    last_exc: Optional[Exception] = None
    for _ in range(60):
        try:
            return Cluster(hosts, protocol_version=5).connect("crypto")
        except Exception as exc:
            last_exc = exc
            time.sleep(3)
    raise RuntimeError(f"Cassandra not reachable: {last_exc}")


PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://crypto:crypto@postgres:5432/crypto")


def pg_conn():
    return psycopg2.connect(PG_DSN)


cass_session: Session = connect_cassandra()

app = FastAPI(
    title="Crypto Analytics API",
    description="Streaming (Cassandra) + batch (Postgres) endpoints in one service.",
    version="0.2.0",
)


# models

class Trade(BaseModel):
    symbol: str
    trade_time: datetime
    trade_id: str
    price: float
    size: float
    side: str


class MomentumRow(BaseModel):
    symbol: str
    minute_ts: datetime
    current_price: float
    price_change_pct: float
    volume: float
    buy_sell_ratio: float


class WhaleAlertRow(BaseModel):
    symbol: str
    alert_time: datetime
    trade_size: float
    threshold_95p: float
    price: float
    side: str
    deviation_percent: float


class VolatilityAlertRow(BaseModel):
    symbol: str
    alert_time: datetime
    current_volatility: float
    previous_volatility: float
    ratio: float


class HourlyReportRow(BaseModel):
    symbol: str
    hour_ts: datetime
    trade_count: int
    total_volume_usd: float
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    avg_price: Optional[float] = None
    price_std: Optional[float] = None
    buy_volume: Optional[float] = None
    sell_volume: Optional[float] = None
    dominant_side: Optional[str] = None


class TradingPatternRow(BaseModel):
    hour_of_day: int
    avg_trade_count: Optional[float] = None
    avg_volume_usd: Optional[float] = None
    avg_volatility: Optional[float] = None
    avg_spread: Optional[float] = None
    sample_hours: Optional[int] = None


class TradingPatternsResponse(BaseModel):
    symbol: str
    computed_at: Optional[datetime] = None
    patterns: List[TradingPatternRow]
    top_activity_hours: List[int]
    top_volatility_hours: List[int]


class WhaleImpactRow(BaseModel):
    symbol: str
    large_trade_count: int
    avg_large_trade_size: Optional[float] = None
    p90_threshold: Optional[float] = None
    avg_impact_pct: Optional[float] = None
    avg_buy_impact_pct: Optional[float] = None
    avg_sell_impact_pct: Optional[float] = None


class OHLCVBar(BaseModel):
    bucket_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


# health

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# trade lookup

@app.get(
    "/api/trades",
    response_model=List[Trade],
    summary="Trade lookup with optional filters",
)
def trade_lookup(
    symbol: str = Query(..., description="Trading pair, e.g. XBTUSD"),
    min_size: Optional[float] = Query(None, ge=0, description="Minimum trade size (USD)"),
    side: Optional[str] = Query(None, pattern="^(Buy|Sell)$", description="Trade side filter"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return"),
    days: int = Query(2, ge=1, le=7, description="How many recent UTC days to scan"),
) -> List[Trade]:
    now = datetime.now(timezone.utc)
    date_keys = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

    over_fetch = limit * 10
    stmt = SimpleStatement(
        "SELECT symbol, trade_time, trade_id, price, size, side "
        "FROM trades WHERE symbol = %s AND date = %s LIMIT %s"
    )

    out: List[Trade] = []
    for d in date_keys:
        try:
            rows = cass_session.execute(stmt, (symbol, d, over_fetch))
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"cassandra error: {exc}") from exc

        for r in rows:
            if min_size is not None and r.size < min_size:
                continue
            if side is not None and r.side != side:
                continue
            out.append(
                Trade(
                    symbol=r.symbol,
                    trade_time=r.trade_time,
                    trade_id=r.trade_id,
                    price=r.price,
                    size=r.size,
                    side=r.side,
                )
            )
            if len(out) >= limit:
                return out
    return out


@app.get("/api/momentum/{symbol}", response_model=List[MomentumRow], summary="Recent per-minute market momentum")
def get_momentum(
    symbol: str,
    minutes: int = Query(60, ge=1, le=1440),
) -> List[MomentumRow]:
    rows = cass_session.execute(
        "SELECT symbol, minute_ts, current_price, price_change_pct, volume, buy_sell_ratio "
        "FROM market_momentum WHERE symbol = %s LIMIT %s",
        (symbol, minutes),
    )
    return [
        MomentumRow(
            symbol=r.symbol,
            minute_ts=r.minute_ts,
            current_price=r.current_price,
            price_change_pct=r.price_change_pct,
            volume=r.volume,
            buy_sell_ratio=r.buy_sell_ratio,
        )
        for r in rows
    ]


@app.get("/api/alerts/whale/{symbol}", response_model=List[WhaleAlertRow])
def get_whale_alerts(symbol: str, limit: int = Query(50, ge=1, le=500)) -> List[WhaleAlertRow]:
    rows = cass_session.execute(
        "SELECT symbol, alert_time, trade_size, threshold_95p, price, side, deviation_percent "
        "FROM whale_alerts WHERE symbol = %s LIMIT %s",
        (symbol, limit),
    )
    return [
        WhaleAlertRow(
            symbol=r.symbol,
            alert_time=r.alert_time,
            trade_size=r.trade_size,
            threshold_95p=r.threshold_95p,
            price=r.price,
            side=r.side,
            deviation_percent=r.deviation_percent,
        )
        for r in rows
    ]


@app.get("/api/alerts/volatility/{symbol}", response_model=List[VolatilityAlertRow])
def get_volatility_alerts(symbol: str, limit: int = Query(50, ge=1, le=500)) -> List[VolatilityAlertRow]:
    rows = cass_session.execute(
        "SELECT symbol, alert_time, current_volatility, previous_volatility, ratio "
        "FROM volatility_alerts WHERE symbol = %s LIMIT %s",
        (symbol, limit),
    )
    return [
        VolatilityAlertRow(
            symbol=r.symbol,
            alert_time=r.alert_time,
            current_volatility=r.current_volatility,
            previous_volatility=r.previous_volatility,
            ratio=r.ratio,
        )
        for r in rows
    ]


# hourly trading report

@app.get(
    "/api/reports/hourly",
    response_model=List[HourlyReportRow],
    summary="Pre-computed hourly trading report",
)
def hourly_report(
    symbol: str = Query(..., description="Trading pair, e.g. XBTUSD"),
    hours: int = Query(12, ge=1, le=168, description="How many closed hours back from now"),
) -> List[HourlyReportRow]:
    try:
        with pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT symbol, hour_ts, trade_count, total_volume_usd,
                           min_price, max_price, avg_price, price_std,
                           buy_volume, sell_volume, dominant_side
                    FROM hourly_reports
                    WHERE symbol = %s
                      AND hour_ts >= date_trunc('hour', now()) - %s::interval
                      AND hour_ts <  date_trunc('hour', now())
                    ORDER BY hour_ts DESC
                    """,
                    (symbol, f"{hours} hours"),
                )
                rows = cur.fetchall()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"postgres error: {exc}") from exc
    return [HourlyReportRow(**r) for r in rows]


# trading patterns

@app.get(
    "/api/analytics/trading-patterns",
    response_model=TradingPatternsResponse,
    summary="Per hour-of-day trading patterns",
)
def trading_patterns(symbol: str = Query(..., description="Trading pair, e.g. XBTUSD")) -> TradingPatternsResponse:
    try:
        with pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT hour_of_day, avg_trade_count, avg_volume_usd,
                           avg_volatility, avg_spread, sample_hours, computed_at
                    FROM trading_patterns
                    WHERE symbol = %s
                    ORDER BY hour_of_day
                    """,
                    (symbol,),
                )
                rows = cur.fetchall()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"postgres error: {exc}") from exc

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"no trading patterns yet for {symbol}; let the system accumulate data and wait for the next batch run",
        )

    patterns = [TradingPatternRow(**{k: v for k, v in r.items() if k != "computed_at"}) for r in rows]
    top_activity = sorted(rows, key=lambda r: -(r["avg_trade_count"] or 0))[:3]
    top_vol = sorted(rows, key=lambda r: -(r["avg_volatility"] or 0))[:3]

    return TradingPatternsResponse(
        symbol=symbol,
        computed_at=rows[0]["computed_at"],
        patterns=patterns,
        top_activity_hours=[r["hour_of_day"] for r in top_activity],
        top_volatility_hours=[r["hour_of_day"] for r in top_vol],
    )


# whale impact

_PERIOD_MAP = {"1h": "1 hour", "6h": "6 hours", "12h": "12 hours", "24h": "24 hours", "7d": "7 days"}


@app.get(
    "/api/analytics/whale-impact",
    response_model=List[WhaleImpactRow],
    summary="Large-trade price impact analysis (live)",
)
def whale_impact(
    symbol: Optional[str] = Query(None, description="Optional symbol filter"),
    period: str = Query("24h", description="Lookback period: 1h | 6h | 12h | 24h | 7d"),
) -> List[WhaleImpactRow]:
    pg_interval = _PERIOD_MAP.get(period)
    if pg_interval is None:
        raise HTTPException(status_code=400, detail=f"unsupported period '{period}'; allowed: {list(_PERIOD_MAP)}")

    sql = """
        WITH window_bounds AS (
            SELECT now() - %(period)s::interval AS period_start, now() AS period_end
        ),
        threshold AS (
            SELECT symbol, percentile_cont(0.9) WITHIN GROUP (ORDER BY size) AS p90
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
              AND t.trade_time <= wb.period_end - interval '5 minutes'
        ),
        impacts AS (
            SELECT
                lt.symbol, lt.trade_id, lt.trade_time, lt.price, lt.size, lt.side,
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
            count(*)::bigint                                                AS large_trade_count,
            avg(size)::double precision                                     AS avg_large_trade_size,
            (SELECT p90 FROM threshold WHERE threshold.symbol = impacts.symbol) AS p90_threshold,
            avg(CASE WHEN avg_before > 0
                     THEN (avg_after - avg_before) / avg_before * 100 END)::double precision AS avg_impact_pct,
            avg(CASE WHEN side = 'Buy'  AND avg_before > 0
                     THEN (avg_after - avg_before) / avg_before * 100 END)::double precision AS avg_buy_impact_pct,
            avg(CASE WHEN side = 'Sell' AND avg_before > 0
                     THEN (avg_after - avg_before) / avg_before * 100 END)::double precision AS avg_sell_impact_pct
        FROM impacts
        WHERE avg_before IS NOT NULL AND avg_after IS NOT NULL
        GROUP BY impacts.symbol
        ORDER BY impacts.symbol
    """
    try:
        with pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, {"period": pg_interval, "symbol": symbol})
                rows = cur.fetchall()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"postgres error: {exc}") from exc
    return [WhaleImpactRow(**r) for r in rows]


# price history

_INTERVAL_SECONDS = {"1m": 60, "5m": 300, "1h": 3600}


@app.get(
    "/api/price/{symbol}",
    response_model=List[OHLCVBar],
    summary="OHLCV price history",
)
def price_history(
    symbol: str,
    from_: datetime = Query(..., alias="from", description="ISO 8601 UTC start, e.g. 2026-05-13T00:00:00Z"),
    to: datetime = Query(..., description="ISO 8601 UTC end"),
    interval: str = Query("1m", description="1m | 5m | 1h"),
) -> List[OHLCVBar]:
    seconds = _INTERVAL_SECONDS.get(interval)
    if seconds is None:
        raise HTTPException(status_code=400, detail=f"unsupported interval '{interval}'; allowed: {list(_INTERVAL_SECONDS)}")
    if to <= from_:
        raise HTTPException(status_code=400, detail="`to` must be after `from`")

    sql = """
        WITH bucketed AS (
            SELECT
                to_timestamp(floor(extract(epoch from trade_time) / %(sec)s) * %(sec)s)
                    AT TIME ZONE 'UTC' AS bucket_ts,
                price,
                size,
                trade_time
            FROM trades
            WHERE symbol = %(symbol)s
              AND trade_time >= %(from_ts)s
              AND trade_time <= %(to_ts)s
        ),
        ranked AS (
            SELECT
                bucket_ts, price, size,
                row_number() OVER (PARTITION BY bucket_ts ORDER BY trade_time ASC)  AS rn_asc,
                row_number() OVER (PARTITION BY bucket_ts ORDER BY trade_time DESC) AS rn_desc
            FROM bucketed
        )
        SELECT
            bucket_ts,
            max(CASE WHEN rn_asc  = 1 THEN price END) AS open,
            max(price)                                AS high,
            min(price)                                AS low,
            max(CASE WHEN rn_desc = 1 THEN price END) AS close,
            sum(size)                                 AS volume
        FROM ranked
        GROUP BY bucket_ts
        ORDER BY bucket_ts ASC
    """
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"sec": seconds, "symbol": symbol, "from_ts": from_, "to_ts": to})
                rows = cur.fetchall()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"postgres error: {exc}") from exc

    return [
        OHLCVBar(bucket_ts=r[0], open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5])
        for r in rows
    ]
