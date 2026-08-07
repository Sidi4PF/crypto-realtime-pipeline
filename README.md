# crypto-realtime-pipeline

Real-time crypto trade pipeline: Binance websocket to Redpanda, aggregated into
OHLC candles with Spark Structured Streaming, stored as partitioned Parquet on S3,
orchestrated with Dagster, provisioned with Terraform.

Work in progress. Full documentation, architecture diagram and demo coming soon.

## Stack

Python, Redpanda, PySpark Structured Streaming, DuckDB, Dagster, Terraform,
Streamlit, Docker Compose, GitHub Actions.

## Layers

- bronze: raw trades, partitioned by symbol, date and hour
- silver: 1-minute OHLC candles, compacted hourly
- gold: 5m, 15m and 1h rollups

## License

MIT
