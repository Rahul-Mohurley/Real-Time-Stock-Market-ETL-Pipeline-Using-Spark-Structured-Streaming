CREATE TABLE IF NOT EXISTS candles_1m (
    symbol VARCHAR(20) NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_end TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(18, 6) NOT NULL,
    high_price NUMERIC(18, 6) NOT NULL,
    low_price NUMERIC(18, 6) NOT NULL,
    close_price NUMERIC(18, 6) NOT NULL,
    volume BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    vwap NUMERIC(18, 6) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, candle_start)
);

CREATE INDEX IF NOT EXISTS idx_candles_symbol_time
ON candles_1m (symbol, candle_start);

CREATE TABLE IF NOT EXISTS candles_1h_backfill (
    symbol VARCHAR(20) NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_end TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(18, 6) NOT NULL,
    high_price NUMERIC(18, 6) NOT NULL,
    low_price NUMERIC(18, 6) NOT NULL,
    close_price NUMERIC(18, 6) NOT NULL,
    volume BIGINT NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'finnhub',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, candle_start)
);

CREATE INDEX IF NOT EXISTS idx_candles_1h_backfill_symbol_time
ON candles_1h_backfill (symbol, candle_start);

CREATE TABLE IF NOT EXISTS pair_analysis_24h (
    symbol VARCHAR(20) PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    open_24h NUMERIC(18, 6) NOT NULL,
    high_24h NUMERIC(18, 6) NOT NULL,
    low_24h NUMERIC(18, 6) NOT NULL,
    close_24h NUMERIC(18, 6) NOT NULL,
    price_change NUMERIC(18, 6) NOT NULL,
    price_change_pct NUMERIC(10, 4) NOT NULL,
    total_volume BIGINT NOT NULL,
    avg_volume_per_min NUMERIC(18, 2) NOT NULL,
    avg_price NUMERIC(18, 6) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
