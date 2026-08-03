import os
from datetime import timezone
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, from_json, max, min, sum, window
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType, DoubleType


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market_ticks"
CHECKPOINT_DIR = "/home/sunbeam/bigd-proj-trial/checkpoints/spark-streaming"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "stock_market"
POSTGRES_USER = "stock_user"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
LOCAL_TIMEZONE = ZoneInfo("Asia/Kolkata")


tick_schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", LongType(), False),
        StructField("source", StringType(), False),
    ]
)


def postgres_connection():
    import psycopg2

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def candle_rows(batch_df):
    return [
        (
            row.symbol,
            normalize_timestamp(row.candle_start),
            normalize_timestamp(row.candle_end),
            row.open_price,
            row.high_price,
            row.low_price,
            row.close_price,
            row.volume,
            row.trade_count,
            row.vwap,
        )
        for row in batch_df.select(
            "symbol",
            "candle_start",
            "candle_end",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "trade_count",
            "vwap",
        ).collect()
    ]


def normalize_timestamp(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=LOCAL_TIMEZONE)
    return value.astimezone(timezone.utc)


def write_candles_to_postgres(rows):
    if not rows:
        return

    query = """
        INSERT INTO candles_1m (
            symbol, candle_start, candle_end, open_price, high_price, low_price,
            close_price, volume, trade_count, vwap
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, candle_start) DO UPDATE SET
            candle_end = EXCLUDED.candle_end,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            trade_count = EXCLUDED.trade_count,
            vwap = EXCLUDED.vwap,
            updated_at = CURRENT_TIMESTAMP
    """

    connection = postgres_connection()
    try:
        cursor = connection.cursor()
        cursor.executemany(query, rows)
        connection.commit()
    finally:
        connection.close()


def refresh_pair_analysis_24h():
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

    connection = postgres_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
    finally:
        connection.close()


def write_candle_batch(batch_df, _batch_id):
    batch_df.persist()
    try:
        rows = candle_rows(batch_df)

        if rows:
            write_candles_to_postgres(rows)
            refresh_pair_analysis_24h()
    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder.appName("stock-market-realtime-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "5")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    ticks = (
        raw_stream.select(from_json(col("value").cast("string"), tick_schema).alias("tick"))
        .select("tick.*")
        .withWatermark("event_time", "2 minutes")
    )

    candles_1m = (
        ticks.groupBy(window("event_time", "1 minute"), "symbol")
        .agg(
            expr("min_by(price, event_time)").alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            expr("max_by(price, event_time)").alias("close_price"),
            sum("volume").alias("volume"),
            count("*").alias("trade_count"),
            expr(
                "CASE WHEN sum(volume) > 0 "
                "THEN sum(price * volume) / sum(volume) "
                "ELSE max_by(price, event_time) END"
            ).alias("vwap"),
        )
        .select(
            "symbol",
            col("window.start").alias("candle_start"),
            col("window.end").alias("candle_end"),
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "trade_count",
            "vwap",
        )
    )

    candle_query = (
        candles_1m.writeStream.foreachBatch(write_candle_batch)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/candles")
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        candle_query.stop()
if __name__ == "__main__":
    main()
