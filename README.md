# Crypto Analytics Platform

Real-time and historical analytics for crypto markets. Streams live trade data from Bitmex into Kafka, processes it with streaming jobs into Cassandra (operational store), and persists raw events to PostgreSQL (warehouse) for batch analytics.

---

## Stack

| Layer | Technology |
|---|---|
| Data source | Bitmex WebSocket API |
| Message bus | Apache Kafka |
| Stream processing | Python 3.11 + kafka-python + NumPy |
| Operational store | Cassandra 4.1 |
| Warehouse | PostgreSQL 16 |
| Batch jobs | Python + SQL (scheduled) |
| API | FastAPI |
| Infra | Docker Compose |

## Architecture

```
                Bitmex WebSocket
                       │
                   ingestion
                       │
              Kafka: trades-raw, quotes-raw
                       │
        ┌──────────────┴──────────────┐
        │ streaming jobs              │ warehouse consumers
        │  ├─ whale-alert             │  ├─ trades-consumer
        │  ├─ volatility-monitor      │  └─ quotes-consumer
        │  ├─ market-momentum         │             │
        │  └─ trades-sink             │             ▼
        │             │               │       ┌──────────┐
        │             ▼               │       │ Postgres │
        │       ┌───────────┐         │       └────┬─────┘
        │       │ Cassandra │         │            │
        │       └───────────┘         │  ┌─────────▼─────────┐
        │                             │  │ batch-scheduler   │
        │                             │  │ (hourly report,   │
        │                             │  │  patterns, impact)│
        │                             │  └───────────────────┘
        └──────────────┬──────────────┘
                       ▼
                  FastAPI :8000
   (trades · momentum · alerts · hourly report ·
    patterns · whale impact · OHLCV price history)
```

## Components

**Ingestion** — `ingestion/ingest.py`. Async WebSocket client to Bitmex, publishes trades and quotes to Kafka.

**Streaming jobs** — `streaming/`. Each job is a Kafka consumer maintaining per-symbol sliding-window state in memory:
- `whale_alert.py` — flags trades above the 10-minute 95th-percentile of size.
- `volatility_monitor.py` — alerts when the std of price over the last 5 minutes exceeds 2× the previous 5 minutes.
- `market_momentum.py` — writes per-minute aggregates (price, change %, volume, buy/sell ratio) to Cassandra.
- `trades_sink.py` — persists raw trades to Cassandra for the `/api/trades` lookup endpoint.

**Warehouse consumers** — `consumer/`. Independent Kafka consumer group that mirrors raw trades and quotes into PostgreSQL for batch processing.

**Batch jobs** — `batch/`. SQL aggregations run on a schedule:
- `hourly_report.py` — per-symbol hourly stats over the last 12 closed hours.
- `trading_patterns.py` — per hour-of-day patterns across all history.
- `large_trades.py` — 90th-percentile trades + ±5 min price impact analysis.
- `scheduler.py` — runs hourly report every 5 min, patterns every hour, impact every 30 min.

**API** — `api/main.py`. FastAPI service exposing all endpoints over Cassandra and Postgres.

## Requirements

- Docker + Docker Compose v2
- Internet access (connects to `wss://www.bitmex.com/realtime`)

## Run

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f ingestion whale-alert trades-consumer batch-scheduler
```

Cassandra takes 30–60s on first start. Postgres is ready in a few seconds. All services wait via healthchecks.

```bash
docker compose down       # stop, keep data
docker compose down -v    # stop + wipe everything
```

## Verify it works

### Streaming

**Kafka topics**
```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Live trade stream**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic trades-raw --max-messages 3
```

**Whale alerts**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic whale-alerts
```
```json
{"alert_time":"2026-05-13 18:32:15","symbol":"XBTUSD","trade_size":125000,"threshold_95p":85000,"price":42350.5,"side":"Buy","deviation_percent":47.1}
```

**Volatility alerts**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic volatility-alerts
```

**Market momentum in Cassandra**
```bash
docker compose exec cassandra cqlsh -e \
  "SELECT * FROM crypto.market_momentum WHERE symbol='XBTUSD' LIMIT 10;"
```

### Batch

**Raw warehouse counts**
```bash
docker compose exec postgres psql -U crypto -d crypto -c \
  "SELECT symbol, count(*) FROM trades GROUP BY symbol;"
```

**Pre-computed hourly reports**
```bash
docker compose exec postgres psql -U crypto -d crypto -c \
  "SELECT symbol, hour_ts, trade_count, total_volume_usd, dominant_side
   FROM hourly_reports ORDER BY hour_ts DESC LIMIT 12;"
```

**Trading patterns by hour-of-day**
```bash
docker compose exec postgres psql -U crypto -d crypto -c \
  "SELECT hour_of_day, avg_trade_count, avg_volatility
   FROM trading_patterns WHERE symbol='XBTUSD' ORDER BY hour_of_day;"
```

**Manually trigger any batch job**
```bash
docker compose exec batch-scheduler python -u hourly_report.py
docker compose exec batch-scheduler python -u trading_patterns.py
docker compose exec batch-scheduler python -u large_trades.py
```

### REST API

```bash
# Trade lookup
curl 'http://localhost:8000/api/trades?symbol=XBTUSD&min_size=10000&side=Buy&limit=10'

# Recent per-minute momentum
curl 'http://localhost:8000/api/momentum/XBTUSD?minutes=30'

# Recent whale alerts
curl 'http://localhost:8000/api/alerts/whale/XBTUSD'

# Hourly trading report (last 12 closed hours)
curl 'http://localhost:8000/api/reports/hourly?symbol=XBTUSD&hours=12'

# Trading patterns by hour-of-day
curl 'http://localhost:8000/api/analytics/trading-patterns?symbol=XBTUSD'

# Whale impact (24h window)
curl 'http://localhost:8000/api/analytics/whale-impact?symbol=XBTUSD&period=24h'

# OHLCV price history
curl 'http://localhost:8000/api/price/XBTUSD?from=2026-05-13T00:00:00Z&to=2026-05-13T06:00:00Z&interval=5m'

# Swagger UI
open http://localhost:8000/docs
```

## Project structure

```
ingestion/
  ingest.py                    Bitmex WS -> Kafka producer

streaming/
  whale_alert.py               whale detection
  volatility_monitor.py        volatility spikes
  market_momentum.py           per-minute market stats
  trades_sink.py               raw trades -> Cassandra

consumer/
  trades_consumer.py           trades-raw -> Postgres
  quotes_consumer.py           quotes-raw -> Postgres

batch/
  hourly_report.py             hourly trading report
  trading_patterns.py          patterns by hour-of-day
  large_trades.py              whale impact
  scheduler.py                 runs batch jobs on a cadence

api/
  main.py                      FastAPI: all REST endpoints

ddl/
  cassandra/init.cql           Cassandra schema
  warehouse/init.sql           Postgres schema

tests/
  smoke.sh                     service + data-flow health checks
  api_test.sh                  exercise every REST endpoint
  synthetic_producer.py        publish fake trades to Kafka

docker-compose.yml
```

## Algorithm settings

| Setting | Value | File |
|---|---|---|
| Whale window | 10 min | streaming/whale_alert.py |
| Whale percentile | 95th | streaming/whale_alert.py |
| Volatility window | 2 × 5 min | streaming/volatility_monitor.py |
| Volatility threshold | 2× increase | streaming/volatility_monitor.py |
| Momentum bucket | 1 min | streaming/market_momentum.py |
| Hourly report schedule | every 5 min | batch/scheduler.py |
| Trading patterns schedule | every 1 hour | batch/scheduler.py |
| Whale impact schedule | every 30 min | batch/scheduler.py |
| Whale impact lookback | ±5 min around trade | batch/large_trades.py |

## Design notes

**Streaming path** — Kafka decouples ingestion from processing; each streaming job is a stateless consumer maintaining its own per-symbol sliding window in memory. Cassandra is partitioned by `(symbol, date)` with descending clustering on time — natural fit for the read patterns.

**Batch path** — Postgres stores raw trades and quotes; batch jobs run SQL aggregations on a schedule and write to summary tables (`hourly_reports`, `trading_patterns`). All upserts are idempotent on natural keys, so re-running over the same window is safe. The whale-impact analysis is computed live by the API because the lookback period is user-driven; the hourly report and trading patterns are pre-computed because they have no parameters.

**Delivery guarantees** — Kafka producer uses `acks=all` + idempotence; consumers use at-least-once with auto-commit. Cassandra inserts are upserts on `trade_id`; Postgres inserts use `ON CONFLICT DO NOTHING` on the same key. Duplicate deliveries are therefore safe.

## Team contributions

**Roman Pavlosiuk**
- Bitmex WebSocket → Kafka ingestion service
- Kafka and Cassandra infrastructure in Docker Compose
- Whale-trade detection streaming job (sliding-window 95th percentile)
- Volatility-spike streaming job (rolling std comparison)
- Per-minute market momentum streaming job writing into Cassandra
- Raw-trade sink to Cassandra
- Cassandra schema for operational tables
- Trade lookup endpoint with symbol/size/side filtering
- Endpoints for recent momentum and recent alerts

**Andrii Kravchuk**
- Kafka → PostgreSQL consumers for trades and quotes
- PostgreSQL warehouse schema (raw + summary tables)
- Hourly trading report batch job (count, volume, OHLC, std, dominant side)
- Trading patterns batch job (activity, volatility, spread by hour-of-day)
- Large-trade price impact batch job (90th percentile + ±5 min window)
- Scheduler service that runs the batch jobs on a cadence
- Hourly report endpoint
- Trading patterns endpoint
- Whale impact endpoint
- OHLCV price history endpoint with configurable interval
