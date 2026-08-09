import os

import duckdb
import pandas as pd


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{os.getenv('AWS_REGION', 'eu-west-3')}';")

    endpoint = os.getenv("S3_ENDPOINT", "")
    if endpoint:
        host = endpoint.replace("http://", "").replace("https://", "")
        con.execute(f"SET s3_endpoint='{host}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")

    key = os.getenv("AWS_ACCESS_KEY_ID", "")
    if key:
        con.execute(f"SET s3_access_key_id='{key}';")
        con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY', '')}';")

    return con


def load_history(symbol: str, resolution: str, limit: int = 120) -> pd.DataFrame:
    """Read the most recent candles from the lake.

    The six-hour window keeps orphan candles from earlier sessions out of the
    chart. Without it, a LIMIT over all history mixes days and Plotly stretches
    the time axis across the empty gap between them.
    """
    bucket = os.getenv("S3_BUCKET", "crypto-lake")

    if resolution == "1m":
        path = f"s3://{bucket}/silver/ohlc_1m_compacted/symbol={symbol}/*/*/*.parquet"
    else:
        path = f"s3://{bucket}/gold/ohlc_{resolution}/*/*/*.parquet"

    con = connect()
    try:
        df = con.execute(
            f"""
            SELECT window_start, symbol, open, high, low, close, volume, trade_count
            FROM read_parquet('{path}', hive_partitioning=true)
            WHERE symbol = ?
              AND window_start >= (
                  SELECT max(window_start) - INTERVAL 6 HOUR
                  FROM read_parquet('{path}', hive_partitioning=true)
                  WHERE symbol = ?
              )
            ORDER BY window_start DESC
            LIMIT {limit}
            """,
            [symbol, symbol],
        ).df()
    except Exception:
        return pd.DataFrame()
    finally:
        con.close()

    return df.sort_values("window_start").reset_index(drop=True)