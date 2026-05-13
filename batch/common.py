import logging
import os
import time

import psycopg2


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)


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
