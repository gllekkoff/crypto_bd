-- Schema for Person 1 (operational / real-time storage).
-- Run once on a fresh Cassandra cluster.

CREATE KEYSPACE IF NOT EXISTS crypto
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto;

-- A3 output: per-minute market momentum snapshot.
-- Partition by symbol => one node owns all data for a symbol; clustering DESC by time
-- means the latest minute is the first row read for typical "show me the dashboard" queries.
CREATE TABLE IF NOT EXISTS market_momentum (
    symbol             text,
    minute_ts          timestamp,
    current_price      double,
    price_change_pct   double,
    volume             double,
    buy_sell_ratio     double,
    PRIMARY KEY (symbol, minute_ts)
) WITH CLUSTERING ORDER BY (minute_ts DESC);

-- Raw trades, written by trades_sink. Backs the C2 /api/trades endpoint.
-- Composite partition key (symbol, date) bounds partition size (~tens of MB per symbol/day).
CREATE TABLE IF NOT EXISTS trades (
    symbol     text,
    date       text,        -- YYYY-MM-DD (UTC)
    trade_time timestamp,
    trade_id   text,
    price      double,
    size       double,       -- USD value (BitMEX inverse contracts: 1 contract = $1)
    side       text,         -- "Buy" | "Sell"
    PRIMARY KEY ((symbol, date), trade_time, trade_id)
) WITH CLUSTERING ORDER BY (trade_time DESC, trade_id ASC);

-- A1 audit log of whale alerts (Kafka topic whale-alerts is the primary output;
-- this table is a queryable persisted copy).
CREATE TABLE IF NOT EXISTS whale_alerts (
    symbol             text,
    alert_time         timestamp,
    trade_size         double,
    threshold_95p      double,
    price              double,
    side               text,
    deviation_percent  double,
    PRIMARY KEY (symbol, alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC);

-- A2 audit log of volatility alerts.
CREATE TABLE IF NOT EXISTS volatility_alerts (
    symbol               text,
    alert_time           timestamp,
    current_volatility   double,
    previous_volatility  double,
    ratio                double,
    PRIMARY KEY (symbol, alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC);
