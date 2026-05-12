"""Bitmex WebSocket -> Kafka ingestion service.

Subscribes to trade and quote channels on Bitmex public WS API and republishes
each message to Kafka. One service instance per cluster; horizontal scaling is
not needed at the data rates Bitmex publishes for a handful of symbols.
"""

import asyncio
import json
import logging
import os
import signal
from typing import Optional

import websockets
from aiokafka import AIOKafkaProducer

BITMEX_WS_URL = os.getenv("BITMEX_WS_URL", "wss://www.bitmex.com/realtime")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "XBTUSD,ETHUSD").split(",") if s.strip()]
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades-raw")
QUOTES_TOPIC = os.getenv("QUOTES_TOPIC", "quotes-raw")
RECONNECT_BACKOFF_SECONDS = 5

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("ingest")


def _flt(v) -> float:
    return float(v) if v is not None else 0.0


def normalize_trade(row: dict) -> dict:
    return {
        "symbol": row["symbol"],
        "trade_time": row["timestamp"],
        "price": _flt(row.get("price")),
        "size": _flt(row.get("size")),
        "home_notional": _flt(row.get("homeNotional")),
        "foreign_notional": _flt(row.get("foreignNotional")),
        "side": row.get("side", ""),
        "trade_id": row.get("trdMatchID", ""),
    }


def normalize_quote(row: dict) -> dict:
    return {
        "symbol": row["symbol"],
        "quote_time": row["timestamp"],
        "bid_price": _flt(row.get("bidPrice")),
        "ask_price": _flt(row.get("askPrice")),
        "bid_size": _flt(row.get("bidSize")),
        "ask_size": _flt(row.get("askSize")),
    }


async def consume_bitmex(producer: AIOKafkaProducer, stop: asyncio.Event) -> None:
    subscribe_msg = {
        "op": "subscribe",
        "args": [f"trade:{s}" for s in SYMBOLS] + [f"quote:{s}" for s in SYMBOLS],
    }

    trade_count = 0
    quote_count = 0

    while not stop.is_set():
        try:
            log.info("connecting to %s", BITMEX_WS_URL)
            async with websockets.connect(
                BITMEX_WS_URL, ping_interval=20, ping_timeout=10, max_size=2**22
            ) as ws:
                await ws.send(json.dumps(subscribe_msg))
                log.info("subscribed: %s", subscribe_msg["args"])

                while not stop.is_set():
                    raw = await ws.recv()
                    data = json.loads(raw)

                    # BitMEX sends control messages (welcome, subscribe ack, etc.)
                    if "table" not in data or "data" not in data:
                        if "error" in data:
                            log.warning("bitmex error: %s", data)
                        continue

                    # "partial" is an initial snapshot; "insert" is new data.
                    action = data.get("action")
                    if action not in ("insert", "partial"):
                        continue

                    table = data["table"]
                    rows = data["data"]

                    if table == "trade":
                        for row in rows:
                            payload = normalize_trade(row)
                            await producer.send_and_wait(
                                TRADES_TOPIC,
                                json.dumps(payload).encode(),
                                key=payload["symbol"].encode(),
                            )
                            trade_count += 1
                            if trade_count % 100 == 0:
                                log.info("ingested %d trades", trade_count)
                    elif table == "quote":
                        for row in rows:
                            payload = normalize_quote(row)
                            await producer.send_and_wait(
                                QUOTES_TOPIC,
                                json.dumps(payload).encode(),
                                key=payload["symbol"].encode(),
                            )
                            quote_count += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("ws connection error: %s; reconnecting in %ds", exc, RECONNECT_BACKOFF_SECONDS)
            await asyncio.sleep(RECONNECT_BACKOFF_SECONDS)


async def main() -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        linger_ms=50,
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    log.info("kafka producer started (bootstrap=%s)", KAFKA_BOOTSTRAP)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await consume_bitmex(producer, stop)
    finally:
        await producer.stop()
        log.info("producer stopped")


if __name__ == "__main__":
    asyncio.run(main())
