import time
from datetime import UTC, datetime

import requests
from dagster import AssetExecutionContext, MetadataValue, asset

from orchestration.partitions import hourly_partitions, partition_to_path_parts
from orchestration.resources import LakeResource

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
KLINES_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol: str, start_ms: int, end_ms: int) -> list[list]:
    """One hour of 1-minute klines. Binance caps a page at 1000, an hour is 60."""
    response = requests.get(
        KLINES_URL,
        params={
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms - 1,
            "limit": 1000,
        },
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


@asset(
    partitions_def=hourly_partitions,
    group_name="silver",
    description="Fill a past hour from the Binance REST API when streaming data is absent.",
)
def backfilled_ohlc_1m(context: AssetExecutionContext, lake: LakeResource) -> None:
    date_part, hour = partition_to_path_parts(context.partition_key)

    start = datetime.strptime(f"{date_part} {hour}", "%Y-%m-%d %H").replace(tzinfo=UTC)
    start_ms = int(start.timestamp() * 1000)
    end_ms = start_ms + 3_600_000

    con = lake.connect()
    rows_written = {}

    for symbol in SYMBOLS:
        klines = fetch_klines(symbol, start_ms, end_ms)
        time.sleep(0.3)

        if not klines:
            context.log.warning("no klines for %s at %s", symbol, context.partition_key)
            continue

        records = [
            {
                "window_start": datetime.fromtimestamp(k[0] / 1000, tz=UTC),
                "window_end": datetime.fromtimestamp((k[6] + 1) / 1000, tz=UTC),
                "symbol": symbol,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "quote_volume": float(k[7]),
                "trade_count": int(k[8]),
            }
            for k in klines
        ]

        con.register("staged", _to_arrow(records))

        target = (
            f"{lake.path('silver', 'ohlc_1m_compacted')}"
            f"/symbol={symbol}/dt={date_part}/hour={hour}/data.parquet"
        )

        con.execute(
            f"""
            COPY (
                SELECT
                    window_start, window_end, symbol,
                    CAST(open AS DECIMAL(18,8)) AS open,
                    CAST(high AS DECIMAL(18,8)) AS high,
                    CAST(low AS DECIMAL(18,8)) AS low,
                    CAST(close AS DECIMAL(18,8)) AS close,
                    CAST(volume AS DECIMAL(18,8)) AS volume,
                    CAST(quote_volume AS DECIMAL(38,8)) AS quote_volume,
                    trade_count
                FROM staged
                ORDER BY window_start
            ) TO '{target}' (FORMAT parquet, COMPRESSION zstd);
            """
        )
        con.unregister("staged")
        rows_written[symbol] = len(records)

    context.add_output_metadata(
        {
            "source": MetadataValue.text("binance REST klines"),
            "partition": MetadataValue.text(context.partition_key),
            **{f"rows_{k}": MetadataValue.int(v) for k, v in rows_written.items()},
        }
    )


def _to_arrow(records: list[dict]):
    import pyarrow as pa

    return pa.Table.from_pylist(records)
