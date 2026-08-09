# Architecture

```mermaid
flowchart TB
    binance["Binance websocket<br/>combined trade streams"]

    subgraph local["Local, docker compose"]
        producer["Producer<br/>Python asyncio"]
        redpanda[("Redpanda<br/>trades.raw, ohlc.1m")]
        sbronze["spark-bronze<br/>stateless archival"]
        ssilver["spark-silver<br/>1 min windows, append"]
        slive["spark-live<br/>1 min windows, update"]
        dashboard["Streamlit<br/>live candles + history"]
        dagster["Dagster<br/>hourly partitions"]
    end

    subgraph aws["AWS, provisioned by Terraform"]
        bronze[("S3 bronze<br/>raw trades")]
        silver[("S3 silver<br/>OHLC 1 min")]
        gold[("S3 gold<br/>5m, 15m, 1h")]
    end

    rest["Binance REST klines<br/>historical backfill"]

    binance --> producer --> redpanda
    redpanda --> sbronze
    redpanda --> ssilver
    redpanda --> slive
    slive --> redpanda
    redpanda --> dashboard
    sbronze --> bronze
    ssilver --> silver
    silver --> dagster
    rest --> dagster
    dagster --> gold
    gold --> dashboard
```

## Why three Spark jobs

Each streaming query runs in its own Spark driver and its own container. The three
have different failure modes and different guarantees: `spark-bronze` is stateless
and must never stop, `spark-silver` is stateful and writes only closed windows,
`spark-live` is stateful and republishes the in-progress candle every two seconds.

Isolating them started as a debugging necessity, documented in the main README,
and turned out to be the better shape. A stateful job that fails no longer takes
raw archival down with it, and each can be restarted, resized or reasoned about
independently.

## Checkpoints

All three jobs share the `checkpoint-data` Docker volume, each under its own
subdirectory. The volume is a named Docker volume rather than a bind mount: Spark
state stores do not survive on NTFS through Docker Desktop.
