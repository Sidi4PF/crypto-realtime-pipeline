import pytest
from pyspark.sql import SparkSession, functions as F

from spark_jobs.transforms import compute_ohlc


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_ohlc_respects_trade_order_not_arrival_order(spark):
    rows = [
        ("BTCUSDT", 3, "60300.00", "1.0", "2026-01-01 00:00:45"),
        ("BTCUSDT", 1, "60000.00", "2.0", "2026-01-01 00:00:05"),
        ("BTCUSDT", 4, "60100.00", "1.0", "2026-01-01 00:00:55"),
        ("BTCUSDT", 2, "59900.00", "1.0", "2026-01-01 00:00:20"),
    ]
    schema = "symbol string, trade_id long, price string, quantity string, ts string"

    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("price", F.col("price").cast("decimal(18,8)"))
        .withColumn("quantity", F.col("quantity").cast("decimal(18,8)"))
        .withColumn("trade_time", F.col("ts").cast("timestamp"))
        .drop("ts")
    )

    result = compute_ohlc(df, "1 minute", "10 seconds").collect()

    assert len(result) == 1
    candle = result[0]
    assert float(candle["open"]) == 60000.00
    assert float(candle["close"]) == 60100.00
    assert float(candle["high"]) == 60300.00
    assert float(candle["low"]) == 59900.00
    assert candle["trade_count"] == 4