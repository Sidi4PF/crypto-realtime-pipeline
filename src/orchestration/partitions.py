from dagster import HourlyPartitionsDefinition

hourly_partitions = HourlyPartitionsDefinition(
    start_date="2026-08-05-00:00",
    timezone="UTC",
    fmt="%Y-%m-%d-%H:%M",
)


def partition_to_path_parts(partition_key: str) -> tuple[str, int]:
    """Turn '2026-08-05-17:00' into ('2026-08-05', 17)."""
    date_part, hour_part = partition_key.rsplit("-", 1)
    return date_part, int(hour_part.split(":")[0])
