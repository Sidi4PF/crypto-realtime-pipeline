# crypto-realtime-pipeline

[![CI](https://github.com/Sidi4PF/crypto-realtime-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Sidi4PF/crypto-realtime-pipeline/actions/workflows/ci.yml)

A production-shaped streaming data platform built end to end: live Binance trades
ingested through Redpanda, aggregated into OHLC candles by Spark Structured
Streaming, stored as partitioned Parquet on S3, orchestrated by Dagster, and
served through a live Streamlit dashboard. Infrastructure is provisioned with
Terraform and the whole thing runs locally with a single command.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full diagram.

Two read paths feed the dashboard. Aggregates are published back to a Kafka topic
for sub-second latency on the current candle, while history is read from S3
Parquet through DuckDB. The dashboard never polls object storage in a loop.

## Layers

| Layer  | Content                | Written by                            | Partitioning       |
| ------ | ---------------------- | ------------------------------------- | ------------------ |
| bronze | Raw trades as received | Spark streaming                       | symbol, date, hour |
| silver | 1-minute OHLC candles  | Spark streaming, compacted by Dagster | symbol, date, hour |
| gold   | 5m, 15m and 1h rollups | Dagster                               | date, hour         |

## Measured behaviour

Numbers below come from actual runs, not estimates.

| Metric                         | Value                                         |
| ------------------------------ | --------------------------------------------- |
| Websocket to broker lag        | around 50 ms in steady state                  |
| Trade throughput               | around 29 trades per second across 3 symbols  |
| Silver files before compaction | around 180 per hour                           |
| Silver files after compaction  | 3 per hour, one per symbol                    |
| Backfill throughput            | 60 candles per symbol per hour, one REST call |

## Running it

### Fully local, no AWS account needed

```bash
cp .env.example .env
# leave S3_ENDPOINT set to http://minio:9000 in the compose services
docker compose -f docker/compose.yml --env-file .env --profile compute up
```

MinIO stands in for S3. Nothing is billed.

### Against real AWS

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set your alert email
terraform init
terraform apply
terraform output bucket_name
terraform output -raw secret_access_key
```

Copy the outputs into `.env`, leave `S3_ENDPOINT` empty, then:

```bash
docker compose -f docker/compose.yml --env-file .env --profile serve up
```

The dashboard is on `http://localhost:8501`, Redpanda console on `:8080`,
Dagster on `:3000` with the `orchestrate` profile.

### Tests

```bash
docker compose -f docker/compose.yml --profile compute run --rm --no-deps \
  -v "$(pwd)/docker/run-tests.sh:/opt/app/run-tests.sh" \
  --entrypoint bash spark /opt/app/run-tests.sh
```

## Design decisions

### Redpanda over Kafka

Kafka needs a broker plus either Zookeeper or a KRaft setup, and its memory
footprint is uncomfortable on a 16 GB laptop running Spark alongside it. Redpanda
ships as a single binary, speaks the Kafka protocol, and runs in a 1 GB container.
Every client library and every concept transfers directly to Kafka. Nothing in the
code is Redpanda-specific.

### Explicit partition mapping instead of key hashing

The first version let the client hash the symbol key. With only three distinct
keys and three partitions, murmur2 collided: one partition held 92 percent of
messages and another held none. Symbols are now mapped to partitions explicitly.
The tradeoff is that adding a symbol shifts the mapping of the ones after it,
which is acceptable for a fixed symbol set and would need consistent hashing if
the catalogue became dynamic.

### min on a struct, not first

`first()` and `last()` are not deterministic in a distributed aggregation because
arrival order is not guaranteed. Open and close are computed as
`min`/`max` over a struct whose leading field is the trade timestamp, with the
trade id breaking ties. Lexicographic struct comparison then gives a reproducible
result. A test feeds deliberately out-of-order trades and asserts the candle is
correct.

### Watermark of 10 seconds

Measured websocket lag sits around 50 ms, so 10 seconds is a 200x margin. It
absorbs a brief network stall while keeping streaming state tiny. A five-minute
watermark, the common reflex, would hold five minutes of open windows in memory
for no benefit.

### Plain Parquet, not Iceberg or Delta

A table format would add a moving part on top of Spark, Dagster and S3 without
adding to the story. More importantly, keeping plain Parquet makes the small-file
problem visible and turns compaction into a real, measurable Dagster job rather
than something hidden behind an `OPTIMIZE` call. Iceberg is the natural next step
if schema evolution or time travel became requirements.

### DuckDB for batch, Spark for streaming

An hourly rollup covers 180 candles, a few hundred kilobytes. Starting a JVM for
that would be absurd. Spark handles the stream, DuckDB handles the batch
analytics. The tool follows the volume, not the trend.

### Two write modes on the same stream

Silver is written in `append` mode so a candle is persisted only once the
watermark has closed its window, never rewritten. The dashboard topic is written
in `update` mode so the in-progress candle refreshes every two seconds. Both
queries read the same source.

### One switch between local and cloud

`S3_ENDPOINT` is the only difference between MinIO and real S3. Set, it selects
path-style access with static credentials; empty, it falls back to the standard
AWS credential chain. Switching from local development to AWS required changing
one environment variable and zero lines of Python.

### force_destroy on the bucket

Deliberate for a demo project: it allows a clean `terraform destroy` even with
objects present. It would be unacceptable in production.

## Known limitations

- The Spark job runs in local mode. Cluster deployment is out of scope.
- Exactly-once delivery is not guaranteed end to end. Checkpointing plus
  deduplication at compaction time gives effective idempotence for this use case.
- No schema registry. The payload is small and stable, and JSON parsing is done
  against an explicit Spark schema rather than inferred.
- Backfill and streaming write to the same silver location. Running both for the
  same hour would produce duplicates, resolved at compaction but wasteful.

## Stack

Python 3.11, Redpanda, PySpark 3.5 Structured Streaming, DuckDB, Dagster,
Terraform, Streamlit, Plotly, Docker Compose, GitHub Actions, AWS S3 and IAM.

## License

MIT
