from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg2
from airflow.sdk import dag, task
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "stock_market",
    "user": "stock_user",
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
BACKFILL_SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN"]
BACKFILL_INTERVAL = "1h"
BACKFILL_LOOKBACK_HOURS = 24


def fetch_twelve_data_hourly_candles(symbol, from_time, to_time):
    if not TWELVE_DATA_API_KEY:
        raise ValueError("TWELVE_DATA_API_KEY is required for Twelve Data hourly backfill.")

    params = {
        "symbol": symbol,
        "interval": BACKFILL_INTERVAL,
        "outputsize": 30,
        "timezone": "UTC",
        "apikey": TWELVE_DATA_API_KEY,
    }
    url = f"https://api.twelvedata.com/time_series?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "stock-market-etl-airflow/1.0"})

    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if payload.get("status") != "ok":
        raise ValueError(f"Twelve Data returned an error for {symbol}: {payload}")

    rows = []
    for candle in payload.get("values", []):
        candle_start = datetime.fromisoformat(candle["datetime"]).replace(tzinfo=timezone.utc)
        if not from_time <= candle_start < to_time:
            continue
        candle_end = candle_start + timedelta(hours=1)
        rows.append(
            (
                symbol,
                candle_start,
                candle_end,
                float(candle["open"]),
                float(candle["high"]),
                float(candle["low"]),
                float(candle["close"]),
                int(float(candle.get("volume", 0))),
                "twelve_data",
            )
        )

    return rows


def upsert_backfill_candles(connection, rows):
    if not rows:
        return

    query = """
        INSERT INTO candles_1h_backfill (
            symbol, candle_start, candle_end, open_price, high_price, low_price,
            close_price, volume, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, candle_start) DO UPDATE SET
            candle_end = EXCLUDED.candle_end,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source,
            updated_at = CURRENT_TIMESTAMP
    """
    with connection.cursor() as cursor:
        cursor.executemany(query, rows)


def refresh_pair_analysis_24h(connection):
    query = """
        INSERT INTO pair_analysis_24h (
            symbol,
            window_start,
            window_end,
            open_24h,
            high_24h,
            low_24h,
            close_24h,
            price_change,
            price_change_pct,
            total_volume,
            avg_volume_per_min,
            avg_price,
            updated_at
        )
        WITH live_hourly AS (
            SELECT
                symbol,
                date_trunc('hour', candle_start) AS candle_start,
                date_trunc('hour', candle_start) + INTERVAL '1 hour' AS candle_end,
                (array_agg(open_price ORDER BY candle_start ASC))[1] AS open_price,
                MAX(high_price) AS high_price,
                MIN(low_price) AS low_price,
                (array_agg(close_price ORDER BY candle_start DESC))[1] AS close_price,
                SUM(volume) AS volume
            FROM candles_1m
            WHERE candle_start >= NOW() - INTERVAL '24 hours'
            GROUP BY symbol, date_trunc('hour', candle_start)
        ),
        backfill_hourly AS (
            SELECT
                symbol,
                candle_start,
                candle_end,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            FROM candles_1h_backfill
            WHERE candle_start >= NOW() - INTERVAL '24 hours'
        ),
        combined_hourly AS (
            SELECT * FROM live_hourly
            UNION ALL
            SELECT backfill_hourly.*
            FROM backfill_hourly
            WHERE NOT EXISTS (
                SELECT 1
                FROM live_hourly
                WHERE live_hourly.symbol = backfill_hourly.symbol
                  AND live_hourly.candle_start = backfill_hourly.candle_start
            )
        ),
        symbol_rollup AS (
            SELECT
                symbol,
                MIN(candle_start) AS window_start,
                MAX(candle_end) AS window_end,
                (array_agg(open_price ORDER BY candle_start ASC))[1] AS open_24h,
                MAX(high_price) AS high_24h,
                MIN(low_price) AS low_24h,
                (array_agg(close_price ORDER BY candle_start DESC))[1] AS close_24h,
                SUM(volume) AS total_volume,
                SUM(volume) / NULLIF(
                    SUM(EXTRACT(EPOCH FROM candle_end - candle_start) / 60),
                    0
                ) AS avg_volume_per_min,
                AVG(close_price) AS avg_price
            FROM combined_hourly
            GROUP BY symbol
        )
        SELECT
            symbol,
            window_start,
            window_end,
            open_24h,
            high_24h,
            low_24h,
            close_24h,
            close_24h - open_24h AS price_change,
            ROUND(((close_24h - open_24h) / NULLIF(open_24h, 0)) * 100, 4) AS price_change_pct,
            total_volume,
            avg_volume_per_min,
            avg_price,
            CURRENT_TIMESTAMP AS updated_at
        FROM symbol_rollup
        ON CONFLICT (symbol) DO UPDATE SET
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            open_24h = EXCLUDED.open_24h,
            high_24h = EXCLUDED.high_24h,
            low_24h = EXCLUDED.low_24h,
            close_24h = EXCLUDED.close_24h,
            price_change = EXCLUDED.price_change,
            price_change_pct = EXCLUDED.price_change_pct,
            total_volume = EXCLUDED.total_volume,
            avg_volume_per_min = EXCLUDED.avg_volume_per_min,
            avg_price = EXCLUDED.avg_price,
            updated_at = CURRENT_TIMESTAMP
    """
    with connection.cursor() as cursor:
        cursor.execute(query)


@task.python(task_id="fetch_hourly_candles")
def fetch_all_hourly_candles():
    to_time = datetime.now(timezone.utc)
    from_time = to_time - timedelta(hours=BACKFILL_LOOKBACK_HOURS)

    candle_rows = []
    per_symbol_rows = {}
    for symbol in BACKFILL_SYMBOLS:
        rows = fetch_twelve_data_hourly_candles(symbol, from_time, to_time)
        candle_rows.extend(
            {
                "symbol": row[0],
                "candle_start": row[1].isoformat(),
                "candle_end": row[2].isoformat(),
                "open_price": row[3],
                "high_price": row[4],
                "low_price": row[5],
                "close_price": row[6],
                "volume": row[7],
                "source": row[8],
            }
            for row in rows
        )
        per_symbol_rows[symbol] = len(rows)

    return {
        "symbols": BACKFILL_SYMBOLS,
        "provider": "twelve_data",
        "interval": BACKFILL_INTERVAL,
        "lookback_hours": BACKFILL_LOOKBACK_HOURS,
        "candle_rows": candle_rows,
        "per_symbol_rows": per_symbol_rows,
    }


@task.python(task_id="load_hourly_candles")
def load_hourly_candles(payload):
    rows = [
        (
            candle["symbol"],
            datetime.fromisoformat(candle["candle_start"]),
            datetime.fromisoformat(candle["candle_end"]),
            candle["open_price"],
            candle["high_price"],
            candle["low_price"],
            candle["close_price"],
            candle["volume"],
            candle["source"],
        )
        for candle in payload["candle_rows"]
    ]

    with psycopg2.connect(**POSTGRES_CONFIG) as connection:
        upsert_backfill_candles(connection, rows)
        connection.commit()

    return {
        "loaded_rows": len(rows),
        "per_symbol_rows": payload["per_symbol_rows"],
    }


@task.python(task_id="refresh_24h_analysis")
def refresh_24h_analysis(_load_result):
    with psycopg2.connect(**POSTGRES_CONFIG) as connection:
        refresh_pair_analysis_24h(connection)
        connection.commit()


@dag(
    dag_id="finnhub_24h_hourly_backfill",
    description="Perform a one-time Twelve Data backfill of the last 24 hours of hourly market candles.",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["stock", "finnhub", "backfill", "postgres"],
)
def finnhub_24h_hourly_backfill():
    fetched_candles = fetch_all_hourly_candles()
    loaded_candles = load_hourly_candles(fetched_candles)
    refresh_24h_analysis(loaded_candles)


dag = finnhub_24h_hourly_backfill()
