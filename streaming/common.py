"""Shared helpers for streaming jobs."""

import json
import logging
import os
import time
from datetime import datetime, timezone

from cassandra.cluster import Cluster, Session
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)


def parse_bitmex_ts(s: str) -> datetime:
    """Parse Bitmex ISO timestamp 'YYYY-MM-DDTHH:MM:SS.fffZ' as UTC datetime."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def kafka_consumer(topic: str, group_id: str, bootstrap: str = None) -> KafkaConsumer:
    bootstrap = bootstrap or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    # Retry until the broker is reachable; useful at cold start in docker-compose.
    for attempt in range(30):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                group_id=group_id,
                auto_offset_reset=os.getenv("KAFKA_OFFSET_RESET", "latest"),
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda v: v.decode("utf-8") if v else None,
                consumer_timeout_ms=0,
            )
        except NoBrokersAvailable:
            time.sleep(2)
    raise RuntimeError(f"Kafka broker not reachable at {bootstrap}")


def kafka_producer(bootstrap: str = None) -> KafkaProducer:
    bootstrap = bootstrap or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                acks="all",
                linger_ms=20,
            )
        except NoBrokersAvailable:
            time.sleep(2)
    raise RuntimeError(f"Kafka broker not reachable at {bootstrap}")


def cassandra_session(keyspace: str = "crypto") -> Session:
    hosts = [h.strip() for h in os.getenv("CASSANDRA_HOSTS", "cassandra").split(",") if h.strip()]
    last_exc = None
    for attempt in range(60):
        try:
            cluster = Cluster(hosts, protocol_version=5)
            return cluster.connect(keyspace)
        except Exception as exc:  # cluster not ready yet
            last_exc = exc
            time.sleep(3)
    raise RuntimeError(f"Cassandra not reachable at {hosts}: {last_exc}")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
