"""Capture TAM snapshot (run as cron or Prefect flow)."""

from __future__ import annotations

import asyncio

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database
from lead_disposition.tam_tracker import TAMTracker


async def run_snapshot() -> None:
    settings = Settings()
    db = Database(settings)
    await db.connect()

    try:
        tracker = TAMTracker(db, settings)
        results = await tracker.capture_all_snapshots()

        for client_id, health in results.items():
            label = client_id or "GLOBAL"
            print(f"[{label}] TAM Health:")
            print(f"  Total Universe:     {health.total_universe}")
            print(f"  Never Touched:      {health.never_touched}")
            print(f"  Available Now:      {health.available_now}")
            print(f"  In Sequence:        {health.in_sequence}")
            print(f"  In Cooldown:        {health.in_cooldown}")
            print(f"  Permanent Suppress: {health.permanent_suppress}")
            print(f"  Won/Customer:       {health.won_customer}")
            print(f"  Burn Rate (weekly): {health.burn_rate_weekly}")
            eta = f"{health.exhaustion_eta_weeks:.1f} weeks" if health.exhaustion_eta_weeks else "N/A"
            print(f"  Exhaustion ETA:     {eta}")
            print(f"  Status:             {health.health_status}")
            print()
    finally:
        await db.close()


def main() -> None:
    asyncio.run(run_snapshot())


if __name__ == "__main__":
    main()
