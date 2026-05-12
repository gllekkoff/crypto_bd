"""Crypto Analytics REST API.

Person 1 implements:
    - C2  GET /api/trades         — Trade Lookup over hot Cassandra storage.
    - Bonus dashboards over `market_momentum`, `whale_alerts`, `volatility_alerts`.

Person 2 will add B1/B2/B3 and C1 endpoints to this same FastAPI app.
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


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


session: Session = connect_cassandra()

app = FastAPI(
    title="Crypto Analytics API",
    description="Person 1 endpoints (streaming/real-time). Person 2 adds batch endpoints.",
    version="0.1.0",
)


# ---------- Models ----------

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


# ---------- Endpoints ----------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get(
    "/api/trades",
    response_model=List[Trade],
    summary="C2 — Trade lookup with optional filters",
)
def trade_lookup(
    symbol: str = Query(..., description="Trading pair, e.g. XBTUSD"),
    min_size: Optional[float] = Query(None, ge=0, description="Minimum trade size (USD)"),
    side: Optional[str] = Query(None, pattern="^(Buy|Sell)$", description="Trade side filter"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return"),
    days: int = Query(2, ge=1, le=7, description="How many recent UTC days to scan"),
) -> List[Trade]:
    """Return the most recent trades matching the filters.

    Cassandra partitions are (symbol, date), so we walk back up to `days` UTC days
    and merge results. Side/size filtering happens server-side after fetch because
    those are low-cardinality columns and post-filtering keeps the partition
    schema simple and writes cheap.
    """
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
            rows = session.execute(stmt, (symbol, d, over_fetch))
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


@app.get(
    "/api/momentum/{symbol}",
    response_model=List[MomentumRow],
    summary="A3 — recent market momentum minutes",
)
def get_momentum(
    symbol: str,
    minutes: int = Query(60, ge=1, le=1440, description="Number of most recent minute rows"),
) -> List[MomentumRow]:
    rows = session.execute(
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


@app.get(
    "/api/alerts/whale/{symbol}",
    response_model=List[WhaleAlertRow],
    summary="A1 — recent whale alerts",
)
def get_whale_alerts(symbol: str, limit: int = Query(50, ge=1, le=500)) -> List[WhaleAlertRow]:
    rows = session.execute(
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


@app.get(
    "/api/alerts/volatility/{symbol}",
    response_model=List[VolatilityAlertRow],
    summary="A2 — recent volatility alerts",
)
def get_volatility_alerts(symbol: str, limit: int = Query(50, ge=1, le=500)) -> List[VolatilityAlertRow]:
    rows = session.execute(
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
