# Crypto Analytics Platform

Real-time and historical analytics for crypto markets. Streams live trade data from Bitmex into Kafka, processes it with streaming jobs, and stores results in Cassandra.

---

## Stack

| Layer | Technology |
|---|---|
| Data source | Bitmex WebSocket API |
| Message bus | Apache Kafka |
| Stream processing | Python 3.11 + kafka-python + NumPy |
| Storage | Cassandra 4.1 |
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
  ┌────┼────────┬──────────────┐
  ▼    ▼        ▼              ▼
whale  volatility  momentum  trades-sink
alert  monitor     (A3)          │
  │      │          │            ▼
Kafka  Kafka    Cassandra   Cassandra
       + Cassandra              │
                             FastAPI :8000
```

## Requirements

- Docker + Docker Compose v2
- Internet access (connects to `wss://www.bitmex.com/realtime`)
- ~4 GB free RAM

## Run

```bash
cp .env.example .env 
docker compose up -d --build
docker compose logs -f ingestion whale-alert market-momentum
```

Cassandra takes 30–60s on first start. All services wait for it via healthcheck.

```bash
docker compose down
docker compose down -v
```

## Verify it works

**Kafka topics**
```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Live trade stream**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic trades-raw --max-messages 3
```

**Whale alerts (A1)**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic whale-alerts
```
```json
{"alert_time":"2026-05-11 18:32:15","symbol":"XBTUSD","trade_size":125000,"threshold_95p":85000,"price":42350.5,"side":"Buy","deviation_percent":47.1}
```

**Volatility alerts (A2)**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic volatility-alerts
```

**Market momentum in Cassandra (A3)**
```bash
docker compose exec cassandra cqlsh -e \
  "SELECT * FROM crypto.market_momentum WHERE symbol='XBTUSD' LIMIT 10;"
```

**API**
```bash
curl 'http://localhost:8000/api/trades?symbol=XBTUSD&min_size=10000&side=Buy&limit=10'
curl 'http://localhost:8000/api/momentum/XBTUSD?minutes=30'
curl 'http://localhost:8000/api/alerts/whale/XBTUSD'

# Swagger UI
open http://localhost:8000/docs
```

## Project structure

```
ingestion/
  ingest.py               Bitmex WS -> Kafka producer
streaming/
  common.py               shared Kafka/Cassandra helpers
  whale_alert.py          A1 — whale detection
  volatility_monitor.py   A2 — volatility spikes
  market_momentum.py      A3 — per-minute market stats
  trades_sink.py          writes raw trades to Cassandra
api/
  main.py                 FastAPI (C2 + dashboards)
ddl/cassandra/init.cql    Cassandra schema
docker-compose.yml
```

## Algorithm settings

| Setting | Value | File |
|---|---|---|
| Whale window | 10 min | whale_alert.py |
| Whale percentile | 95th | whale_alert.py |
| Volatility window | 2 × 5 min | volatility_monitor.py |
| Volatility threshold | 2× increase | volatility_monitor.py |
| Momentum bucket | 1 min | market_momentum.py |
