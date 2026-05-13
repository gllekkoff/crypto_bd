"""Synthetic trade producer for local Kafka testing.

Publishes fake XBTUSD/ETHUSD trade events to Kafka so the platform can be
validated without relying on live Bitmex market data.
"""

import argparse
import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

SYMBOL_START_PRICES = {
    "XBTUSD": 67_000.0,
    "ETHUSD": 3_500.0,
}


def parse_args() -> argparse.Namespace:
    description = __doc__.splitlines()[0] if __doc__ else "Synthetic trade producer for Kafka testing"
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    ap.add_argument("--topic", default="trades-raw")
    ap.add_argument("--rate", type=float, default=10.0, help="trades/sec per symbol")
    ap.add_argument(
        "--whale-rate",
        type=float,
        default=0.005,
        help="fraction of trades that are whale-sized (default: 0.005)",
    )
    ap.add_argument(
        "--duration",
        type=int,
        default=60,
        help="seconds to run; pass 0 to run until Ctrl-C",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=None,
        help="optional random seed for reproducibility",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        linger_ms=20,
        acks="all",
    )
    print(f"connected to kafka at {args.bootstrap}")

    prices = dict(SYMBOL_START_PRICES)
    interval = 1.0 / args.rate
    start = time.time()
    sent = 0
    whales = 0

    stop = False

    def handle_signal(*_args):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(
        f"publishing to {args.topic} @ {args.rate:.1f} t/s/symbol "
        f"(whale_rate={args.whale_rate}, duration={args.duration or '∞'}s)"
    )

    while not stop:
        if args.duration and time.time() - start >= args.duration:
            break

        for symbol in SYMBOL_START_PRICES:
            prices[symbol] *= 1.0 + random.uniform(-0.0005, 0.0005)

            is_whale = random.random() < args.whale_rate
            size = (
                random.uniform(80_000, 250_000)
                if is_whale
                else random.uniform(100, 5_000)
            )

            payload = {
                "symbol": symbol,
                "trade_time": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "price": round(prices[symbol], 2),
                "size": round(size, 2),
                "home_notional": round(size / prices[symbol], 6),
                "foreign_notional": round(size, 2),
                "side": random.choice(["Buy", "Sell"]),
                "trade_id": uuid.uuid4().hex,
            }
            producer.send(args.topic, value=payload, key=symbol)
            sent += 1
            if is_whale:
                whales += 1

            if sent % 500 == 0:
                print(f"  ... sent {sent} (whales={whales})")

        time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start
    print(f"done: {sent} trades sent ({whales} whales) in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
