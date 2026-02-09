"""TAM tracking - pool segmentation, burn rate, exhaustion ETA, and snapshots."""

from __future__ import annotations

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database
from lead_disposition.core.models import TAMHealth


class TAMTracker:
    """Computes TAM health metrics and captures daily snapshots."""

    def __init__(self, db: Database, settings: Settings | None = None):
        self.db = db
        self.settings = settings or Settings()

    async def get_health(self, client_id: str | None = None) -> TAMHealth:
        """Get current TAM health metrics for a client or globally."""
        pools = await self.db.get_tam_pools(client_id)
        burn_rate = await self.db.get_burn_rate(client_id)

        available = pools.get("available_now", 0)
        eta: float | None = None
        if burn_rate > 0:
            eta = available / burn_rate

        health_status = "healthy"
        if eta is not None:
            if eta < self.settings.tam_critical_weeks:
                health_status = "critical"
            elif eta < self.settings.tam_warning_weeks:
                health_status = "warning"

        return TAMHealth(
            total_universe=pools.get("total_universe", 0),
            never_touched=pools.get("never_touched", 0),
            in_cooldown=pools.get("in_cooldown", 0),
            available_now=available,
            permanent_suppress=pools.get("permanent_suppress", 0),
            in_sequence=pools.get("in_sequence", 0),
            won_customer=pools.get("won_customer", 0),
            burn_rate_weekly=burn_rate,
            exhaustion_eta_weeks=eta,
            health_status=health_status,
        )

    async def capture_snapshot(self, client_id: str | None = None) -> TAMHealth:
        """Capture a TAM snapshot for the current date."""
        health = await self.get_health(client_id)
        await self.db.insert_tam_snapshot(
            snapshot={
                "total_universe": health.total_universe,
                "never_touched": health.never_touched,
                "in_cooldown": health.in_cooldown,
                "available_now": health.available_now,
                "permanent_suppress": health.permanent_suppress,
                "in_sequence": health.in_sequence,
                "won_customer": health.won_customer,
                "burn_rate_weekly": health.burn_rate_weekly,
                "exhaustion_eta_weeks": health.exhaustion_eta_weeks,
            },
            client_id=client_id,
        )
        return health

    async def capture_all_snapshots(self) -> dict[str | None, TAMHealth]:
        """Capture snapshots for global and each client."""
        results: dict[str | None, TAMHealth] = {}

        # Global snapshot
        results[None] = await self.capture_snapshot(None)

        # Per-client snapshots
        clients = await self.db.get_distinct_clients()
        for cid in clients:
            results[cid] = await self.capture_snapshot(cid)

        return results

    async def get_trends(
        self, client_id: str | None = None, days: int = 30
    ) -> list[dict]:
        """Get TAM snapshot trends for the last N days."""
        return await self.db.get_snapshots(client_id, days)
