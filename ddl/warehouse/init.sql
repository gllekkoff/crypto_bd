CREATE TABLE IF NOT EXISTS trades (
    trade_id          text PRIMARY KEY,
    symbol            text NOT NULL,
    trade_time        timestamptz NOT NULL,
    price             double precision NOT NULL,
    size              double precision NOT NULL,
    home_notional     double precision NOT NULL DEFAULT 0,
    foreign_notional  double precision NOT NULL DEFAULT 0,
    side              text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_time        ON trades (trade_time DESC);

CREATE TABLE IF NOT EXISTS quotes (
    id          bigserial PRIMARY KEY,
    symbol      text NOT NULL,
    quote_time  timestamptz NOT NULL,
    bid_price   double precision,
    ask_price   double precision,
    bid_size    double precision,
    ask_size    double precision
);
CREATE INDEX IF NOT EXISTS idx_quotes_symbol_time ON quotes (symbol, quote_time DESC);

-- hourly aggregates

CREATE TABLE IF NOT EXISTS hourly_reports (
    symbol             text NOT NULL,
    hour_ts            timestamptz NOT NULL,
    trade_count        bigint NOT NULL,
    total_volume_usd   double precision NOT NULL,
    min_price          double precision,
    max_price          double precision,
    avg_price          double precision,
    price_std          double precision,
    buy_volume         double precision,
    sell_volume        double precision,
    dominant_side      text,
    computed_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, hour_ts)
);

-- per hour-of-day trading patterns

CREATE TABLE IF NOT EXISTS trading_patterns (
    symbol            text NOT NULL,
    hour_of_day       smallint NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    avg_trade_count   double precision,
    avg_volume_usd    double precision,
    avg_volatility    double precision,
    avg_spread        double precision,
    sample_hours      int,
    computed_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, hour_of_day)
);
