from dagster import AssetCheckResult, MetadataValue, asset_check

from orchestration.resources import LakeResource


@asset_check(
    asset="compacted_ohlc_1m",
    description="OHLC invariants: high is the max, low is the min, volumes are positive.",
    blocking=True,
)
def ohlc_invariants(lake: LakeResource) -> AssetCheckResult:
    con = lake.connect()
    source = f"{lake.path('silver', 'ohlc_1m_compacted')}/*/*/*/*.parquet"

    violations = con.execute(
        f"""
        SELECT count(*)
        FROM read_parquet('{source}', hive_partitioning=true)
        WHERE high < open
           OR high < close
           OR high < low
           OR low > open
           OR low > close
           OR volume <= 0
           OR trade_count <= 0
        """
    ).fetchone()[0]

    return AssetCheckResult(
        passed=violations == 0,
        metadata={"violations": MetadataValue.int(violations)},
    )


@asset_check(
    asset="compacted_ohlc_1m",
    description="No missing minute inside a compacted hour.",
    blocking=False,
)
def no_minute_gaps(lake: LakeResource) -> AssetCheckResult:
    con = lake.connect()
    source = f"{lake.path('silver', 'ohlc_1m_compacted')}/*/*/*/*.parquet"

    gaps = con.execute(
        f"""
        WITH ordered AS (
            SELECT
                symbol,
                window_start,
                lag(window_start) OVER (PARTITION BY symbol ORDER BY window_start) AS prev
            FROM read_parquet('{source}', hive_partitioning=true)
        )
        SELECT count(*)
        FROM ordered
        WHERE prev IS NOT NULL
          AND date_diff('minute', prev, window_start) > 1
        """
    ).fetchone()[0]

    return AssetCheckResult(
        passed=gaps == 0,
        metadata={"gap_count": MetadataValue.int(gaps)},
    )
