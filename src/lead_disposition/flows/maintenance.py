"""Prefect flows for scheduled disposition maintenance tasks."""

from __future__ import annotations

try:
    from prefect import flow, task
except ImportError:
    # Allow import without prefect installed
    def flow(*a, **kw):  # type: ignore[misc]
        def decorator(fn):
            return fn
        return decorator if not a else decorator(a[0])

    def task(*a, **kw):  # type: ignore[misc]
        def decorator(fn):
            return fn
        return decorator if not a else decorator(a[0])

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database
from lead_disposition.deconfliction import Deconfliction
from lead_disposition.state_machine import StateMachine
from lead_disposition.tam_tracker import TAMTracker


@task(name="process-expired-cooldowns")
async def process_cooldowns_task() -> int:
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        sm = StateMachine(db, settings)
        return await sm.process_expired_cooldowns()
    finally:
        await db.close()


@task(name="process-stale-data")
async def process_stale_data_task() -> int:
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        sm = StateMachine(db, settings)
        return await sm.process_stale_data()
    finally:
        await db.close()


@task(name="process-expired-ownerships")
async def process_ownerships_task() -> int:
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        decon = Deconfliction(db, settings)
        return await decon.process_expired_ownerships()
    finally:
        await db.close()


@task(name="capture-tam-snapshots")
async def capture_snapshots_task() -> dict:
    settings = Settings()
    db = Database(settings)
    await db.connect()
    try:
        tracker = TAMTracker(db, settings)
        results = await tracker.capture_all_snapshots()
        return {
            k or "global": {
                "total_universe": v.total_universe,
                "available_now": v.available_now,
                "burn_rate_weekly": v.burn_rate_weekly,
                "health_status": v.health_status,
            }
            for k, v in results.items()
        }
    finally:
        await db.close()


@flow(name="daily-disposition-maintenance", log_prints=True)
async def daily_maintenance_flow() -> dict:
    """Run all daily maintenance tasks:
    1. Process expired cooldowns -> retouch_eligible
    2. Flag stale data (enrichment > 6 months)
    3. Release expired company ownerships
    4. Capture TAM snapshots
    """
    cooldowns = await process_cooldowns_task()
    print(f"Cooldowns processed: {cooldowns} contacts moved to retouch_eligible")

    stale = await process_stale_data_task()
    print(f"Stale data flagged: {stale} contacts marked as stale_data")

    ownerships = await process_ownerships_task()
    print(f"Ownerships released: {ownerships} companies freed")

    snapshots = await capture_snapshots_task()
    for label, data in snapshots.items():
        print(
            f"[{label}] universe={data['total_universe']} "
            f"available={data['available_now']} "
            f"burn={data['burn_rate_weekly']}/wk "
            f"status={data['health_status']}"
        )

    return {
        "cooldowns_processed": cooldowns,
        "stale_flagged": stale,
        "ownerships_released": ownerships,
        "snapshots": snapshots,
    }
