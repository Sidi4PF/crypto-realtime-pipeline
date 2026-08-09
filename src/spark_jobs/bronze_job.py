import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.s3 import configure_s3, s3_path
from spark_jobs.transforms import parse_trades

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
RAW_TOPIC = os.getenv("RAW_TRADES_TOPIC", "trades.raw")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_DIR", "/opt/app/checkpoints")


def main() -> None:
    spark = SparkSession.builder.appName("bronze-trades").getOrCreate()
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

    trades = (
        parse_trades(raw)
        .withColumn("dt", F.to_date("trade_time"))
        .withColumn("hour", F.hour("trade_time"))
    )

    query = (
        trades.writeStream.outputMode("append")
        .format("parquet")
        .option("path", s3_path("bronze", "trades"))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_trades")
        .partitionBy("symbol", "dt", "hour")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()