import json
import logging
import os
import time
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)


def parse_bitmex_ts(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def kafka_consumer(topic: str, group_id: str) -> KafkaConsumer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    for _ in range(30):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                group_id=group_id,
                auto_offset_reset=os.getenv("KAFKA_OFFSET_RESET", "earliest"),
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            time.sleep(2)
    raise RuntimeError(f"Kafka not reachable at {bootstrap}")


def pg_connect():
    dsn = os.getenv("POSTGRES_DSN", "postgresql://crypto:crypto@postgres:5432/crypto")
    last_exc = None
    for _ in range(60):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            last_exc = exc
            time.sleep(2)
    raise RuntimeError(f"Postgres not reachable: {last_exc}")
