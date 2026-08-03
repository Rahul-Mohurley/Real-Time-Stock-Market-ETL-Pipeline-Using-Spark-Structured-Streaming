# Real-Time Stock Market ETL

A real-time market analytics pipeline using Kafka, Spark Structured Streaming, PostgreSQL, Airflow, and Grafana.

## Architecture

```text
Finnhub WebSocket -> Kafka -> Spark Structured Streaming -> PostgreSQL -> Grafana
Twelve Data hourly candle API -> Airflow one-time backfill -> PostgreSQL and 24-hour summary
```

## Data Flow

1. `finnhub_websocket_producer.py` streams live Finnhub trades.
2. Kafka stores normalized market events in the `market_ticks` topic.
3. Spark Structured Streaming reads the topic, handles late records with an event-time watermark, and builds one-minute OHLCV candles.
4. Spark upserts candles into PostgreSQL and refreshes `pair_analysis_24h`.
5. Airflow performs one initial fetch of the previous 24 hours of hourly candles to fill gaps in live coverage.
6. Grafana displays candlesticks, volume, and per-symbol 24-hour metrics.

## Prerequisites

Run Kafka, PostgreSQL, Airflow, and Grafana locally. Configure the following default addresses as needed:

- Kafka: `localhost:9092`
- PostgreSQL: `localhost:5432`
- Airflow: `http://localhost:8080`
- Grafana: `http://localhost:3000`

Create the five-partition `market_ticks` topic and apply [sql/postgres/schema.sql](sql/postgres/schema.sql) to the PostgreSQL database before starting the pipeline.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic market_ticks --partitions 5 --replication-factor 1
```

## Quick Start

1. Create local configuration files:

```bash
cp .env.example .env
```

2. Set `FINNHUB_API_KEY`, `TWELVE_DATA_API_KEY`, and `POSTGRES_PASSWORD` in `.env`.

3. Start the live producer manually when needed:

```bash
python3 src/producer/finnhub_websocket_producer.py
```

## Components

- Kafka and Zookeeper: event ingestion and buffering
- Spark Structured Streaming: one-minute OHLCV, trade count, and VWAP aggregation
- PostgreSQL: candle, backfill, and 24-hour reporting tables
- Airflow: one-time Twelve Data hourly-candle backfill
- Grafana: operational market dashboard

## PostgreSQL Tables

- `candles_1m`: live one-minute OHLCV candles
- `candles_1h_backfill`: hourly Twelve Data candles
- `pair_analysis_24h`: latest rolling 24-hour market metrics per symbol

## Airflow

The retained DAG is `finnhub_24h_hourly_backfill`. Configure your local Airflow installation to load the `airflow/dags` directory. It uses Airflow's `@once` schedule to fetch up to 24 Twelve Data hourly OHLCV candles per configured symbol once, upsert them into `candles_1h_backfill`, and refresh `pair_analysis_24h`.

It uses Airflow's TaskFlow API with `@dag` and `@task.python` decorators.

## Configuration

Set the private credentials in `.env`:

```text
FINNHUB_API_KEY=your_finnhub_key_here
TWELVE_DATA_API_KEY=your_twelve_data_key_here
POSTGRES_PASSWORD=your_postgres_password
```
