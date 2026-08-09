import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.s3 import configure_s3, s3_path
from spark_jobs.transforms import compute_ohlc, parse_trades

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
RAW_TOPIC = os.getenv("RAW_TRADES_TOPIC", "trades.raw")
WINDOW = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK = os.getenv("WATERMARK", "10 seconds")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_DIR", "/opt/app/checkpoints")


def source(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_batch(df, batch_id: int) -> None:
    """Write one micro-batch as a plain batch Parquet write.

    The streaming Parquet sink maintains a transactional _spark_metadata log
    that deadlocks against a stateful operator when the target is S3, where
    renames are not atomic. foreachBatch sidesteps it entirely.
    """
    if df.isEmpty():
        return

    (
        df.withColumn("dt", F.to_date("window_start"))
        .withColumn("hour", F.hour("window_start"))
        .write.mode("append")
        .partitionBy("symbol", "dt", "hour")
        .parquet(s3_path("silver", "ohlc_1m"))
    )


def main() -> None:
    spark = SparkSession.builder.appName("silver-ohlc-1m").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_s3(spark)

    candles = compute_ohlc(parse_trades(source(spark)), WINDOW, WATERMARK)

    query = (
        candles.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_ohlc_1m")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()