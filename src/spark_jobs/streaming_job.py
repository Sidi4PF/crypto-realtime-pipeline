import os

from pyspark.sql import SparkSession, functions as F

from spark_jobs.s3 import configure_s3, s3_path
from spark_jobs.transforms import compute_ohlc, parse_trades

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
RAW_TOPIC = os.getenv("RAW_TRADES_TOPIC", "trades.raw")
WINDOW = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK = os.getenv("WATERMARK", "10 seconds")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_DIR", "/opt/app/checkpoints")
AGG_TOPIC = os.getenv("AGG_OHLC_TOPIC", "ohlc.1m")


def partitioned(df, time_col: str):
    """Add the physical partition columns used across every layer."""
    return (
        df.withColumn("dt", F.to_date(time_col))
        .withColumn("hour", F.hour(time_col))
    )


def main() -> None:
    spark = SparkSession.builder.appName("ohlc-1m").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_s3(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    trades = parse_trades(raw)

    bronze = (
        partitioned(trades, "trade_time")
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", s3_path("bronze", "trades"))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_trades")
        .partitionBy("symbol", "dt", "hour")
        .trigger(processingTime="30 seconds")
        .start()
    )

    candles = compute_ohlc(trades, WINDOW, WATERMARK)

    silver = (
        partitioned(candles, "window_start")
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", s3_path("silver", "ohlc_1m"))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_ohlc_1m")
        .partitionBy("symbol", "dt", "hour")
        .trigger(processingTime="30 seconds")
        .start()
    )

    live = (
        candles.select(
            F.col("symbol").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream.outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", AGG_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/live_ohlc")
        .trigger(processingTime="2 seconds")
        .start()
    )

    console = (
        candles.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 5)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/console")
        .trigger(processingTime="15 seconds")
        .start()
    )

    for query in (bronze, silver, live, console):
        query.awaitTermination()


if __name__ == "__main__":
    main()