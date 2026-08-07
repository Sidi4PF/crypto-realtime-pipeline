from dagster import AssetExecutionContext, MetadataValue, asset

from orchestration.partitions import hourly_partitions, partition_to_path_parts
from orchestration.resources import LakeResource

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


@asset(
    partitions_def=hourly_partitions,
    group_name="silver",
    description="Rewrite the many small streaming files of one hour into one file per symbol.",
)
def compacted_ohlc_1m(context: AssetExecutionContext, lake: LakeResource) -> None:
    date_part, hour = partition_to_path_parts(context.partition_key)
    con = lake.connect()

    files_before = 0
    rows_total = 0

    for symbol in SYMBOLS:
        prefix = f"{lake.path('silver', 'ohlc_1m')}/symbol={symbol}/dt={date_part}/hour={hour}"
        source = f"{prefix}/*.parquet"

        try:
            count = con.execute(f"SELECT count(*) FROM read_parquet('{source}')").fetchone()[0]
        except Exception:
            context.log.info("no data for %s at %s hour %s", symbol, date_part, hour)
            continue

        if count == 0:
            continue

        files_before += con.execute(
            f"SELECT count(DISTINCT filename) FROM read_parquet('{source}', filename=true)"
        ).fetchone()[0]
        rows_total += count

        target = (
            f"{lake.path('silver', 'ohlc_1m_compacted')}"
            f"/symbol={symbol}/dt={date_part}/hour={hour}/data.parquet"
        )

        con.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (dt, hour)
                FROM read_parquet('{source}')
                QUALIFY row_number() OVER (
                    PARTITION BY window_start ORDER BY trade_count DESC
                ) = 1
                ORDER BY window_start
            ) TO '{target}' (FORMAT parquet, COMPRESSION zstd);
            """
        )

    context.add_output_metadata(
        {
            "files_before": MetadataValue.int(files_before),
            "files_after": MetadataValue.int(len(SYMBOLS)),
            "rows": MetadataValue.int(rows_total),
            "partition": MetadataValue.text(context.partition_key),
        }
    )
