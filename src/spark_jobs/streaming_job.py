import os

from pyspark.sql import SparkSession

from spark_jobs.transforms import compute_ohlc, parse_trades

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
RAW_TOPIC = os.getenv("RAW_TRADES_TOPIC", "trades.raw")
WINDOW = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK = os.getenv("WATERMARK", "10 seconds")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/opt/app/checkpoints/ohlc_console")


def main() -> None:
    spark = SparkSession.builder.appName("ohlc-1m").getOrCreate()
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
        candles.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()