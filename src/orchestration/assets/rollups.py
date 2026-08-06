from dagster import AssetExecutionContext, MetadataValue, asset

from orchestration.partitions import hourly_partitions, partition_to_path_parts
from orchestration.resources import LakeResource

RESOLUTIONS = {"5m": 5, "15m": 15, "1h": 60}


@asset(
    partitions_def=hourly_partitions,
    group_name="gold",
    deps=["compacted_ohlc_1m", "backfilled_ohlc_1m"],
    description="Aggregate 1-minute candles into 5m, 15m and 1h resolutions.",
)
def ohlc_rollups(context: AssetExecutionContext, lake: LakeResource) -> None:
    date_part, hour = partition_to_path_parts(context.partition_key)
    con = lake.connect()

    source = (
        f"{lake.path('silver', 'ohlc_1m_compacted')}"
        f"/*/dt={date_part}/hour={hour}/*.parquet"
    )

    try:
        con.execute(f"SELECT 1 FROM read_parquet('{source}', hive_partitioning=true) LIMIT 1")
    except Exception:
        context.log.warning("no compacted data for %s", context.partition_key)
        return

    written = {}

    for label, minutes in RESOLUTIONS.items():
        target = f"{lake.path('gold', f'ohlc_{label}')}/dt={date_part}/hour={hour}/data.parquet"

        con.execute(
            f"""
            COPY (
                SELECT
                    symbol,
                    time_bucket(INTERVAL '{minutes} minutes', window_start) AS window_start,
                    time_bucket(INTERVAL '{minutes} minutes', window_start)
                        + INTERVAL '{minutes} minutes' AS window_end,
                    arg_min(open, window_start) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    arg_max(close, window_start) AS close,
                    sum(volume) AS volume,
                    sum(quote_volume) AS quote_volume,
                    sum(trade_count) AS trade_count
                FROM read_parquet('{source}', hive_partitioning=true)
                GROUP BY symbol, 2, 3
                ORDER BY symbol, 2
            ) TO '{target}' (FORMAT parquet, COMPRESSION zstd);
            """
        )

        written[label] = con.execute(
            f"SELECT count(*) FROM read_parquet('{target}')"
        ).fetchone()[0]

    context.add_output_metadata(
        {f"rows_{k}": MetadataValue.int(v) for k, v in written.items()}
    )