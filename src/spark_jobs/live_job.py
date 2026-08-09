import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.transforms import compute_ohlc, parse_trades

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
RAW_TOPIC = os.getenv("RAW_TRADES_TOPIC", "trades.raw")
AGG_TOPIC = os.getenv("AGG_OHLC_TOPIC", "ohlc.1m")
WINDOW = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK = os.getenv("WATERMARK", "10 seconds")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_DIR", "/opt/app/checkpoints")


def main() -> None:
    spark = SparkSession.builder.appName("live-ohlc-1m").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    candles = compute_ohlc(parse_trades(raw), WINDOW, WATERMARK)

    query = (
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

    query.awaitTermination()


if __name__ == "__main__":
    main()