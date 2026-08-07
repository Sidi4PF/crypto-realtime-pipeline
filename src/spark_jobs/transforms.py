from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("trade_id", LongType(), False),
        StructField("price", StringType(), False),
        StructField("quantity", StringType(), False),
        StructField("trade_time_ms", LongType(), False),
        StructField("event_time_ms", LongType(), False),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at_ms", LongType(), True),
    ]
)


def parse_trades(raw: DataFrame) -> DataFrame:
    """Decode Kafka value bytes into a typed trade DataFrame."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
        .select("t.*")
        .withColumn("price", F.col("price").cast("decimal(18,8)"))
        .withColumn("quantity", F.col("quantity").cast("decimal(18,8)"))
        .withColumn("trade_time", (F.col("trade_time_ms") / 1000).cast("timestamp"))
        .drop("event_time_ms")
    )


def compute_ohlc(trades: DataFrame, window_duration: str, watermark: str) -> DataFrame:
    """Aggregate trades into OHLC candles. Pure: same input, same output."""
    ordered_price = F.struct("trade_time", "trade_id", "price")

    return (
        trades.withWatermark("trade_time", watermark)
        .groupBy(F.window("trade_time", window_duration), F.col("symbol"))
        .agg(
            F.min(ordered_price).alias("first_tick"),
            F.max(ordered_price).alias("last_tick"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.sum("quantity").alias("volume"),
            F.sum(F.col("price") * F.col("quantity")).alias("quote_volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            F.col("first_tick.price").alias("open"),
            "high",
            "low",
            F.col("last_tick.price").alias("close"),
            "volume",
            "quote_volume",
            "trade_count",
        )
    )
