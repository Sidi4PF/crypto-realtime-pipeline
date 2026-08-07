from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from orchestration import assets
from orchestration.assets.checks import no_minute_gaps, ohlc_invariants
from orchestration.resources import LakeResource

all_assets = load_assets_from_package_module(assets)

hourly_job = define_asset_job(
    name="hourly_lake_maintenance",
    selection=AssetSelection.all(),
    description="Compact the previous hour, then build the gold rollups.",
)

hourly_schedule = ScheduleDefinition(
    job=hourly_job,
    cron_schedule="5 * * * *",
    execution_timezone="UTC",
)

defs = Definitions(
    assets=all_assets,
    asset_checks=[ohlc_invariants, no_minute_gaps],
    jobs=[hourly_job],
    schedules=[hourly_schedule],
    resources={"lake": LakeResource()},
)
