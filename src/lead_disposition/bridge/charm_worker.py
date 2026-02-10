"""Charm bridge worker - polls lead_pull_jobs and runs the waterfall engine.

Follows the same poll-based pattern as Charm's domain_generation and
inbox_purchase workers: poll every N seconds for pending jobs, process them
one at a time, update status.

All tables (Charm's + disposition's) live in the same Supabase PostgreSQL
instance, so only one database connection is needed.

Usage:
    disposition-bridge          # via pyproject.toml entrypoint
    python -m lead_disposition.bridge.charm_worker   # direct
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys

from lead_disposition.bridge.charm_mapper import build_waterfall_request
from lead_disposition.core.config import Settings
from lead_disposition.core.db_factory import create_database
from lead_disposition.providers.ai_ark import AIArkProvider
from lead_disposition.providers.clay import ClayProvider
from lead_disposition.providers.jina import JinaProvider
from lead_disposition.providers.spider import SpiderProvider
from lead_disposition.waterfall.engine import WaterfallEngine

logger = logging.getLogger(__name__)


class CharmBridgeWorker:
    """Polls lead_pull_jobs and executes waterfall fills.

    Uses the same database connection for reading Charm job tables AND
    writing disposition data — both schemas live in the same Supabase instance.
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self._running = False

        # Single database connection (Charm + disposition tables share one DB)
        self.db = create_database(self.settings)

        # External providers
        providers = []
        if self.settings.ai_ark_api_key:
            providers.append(AIArkProvider(self.settings))
        if self.settings.clay_webhook_url:
            providers.append(ClayProvider(self.settings))
        if self.settings.jina_api_key:
            providers.append(JinaProvider(self.settings))
        if self.settings.spider_api_key:
            providers.append(SpiderProvider(self.settings))

        self.waterfall = WaterfallEngine(self.db, providers, self.settings)
        self.providers = providers

    async def start(self) -> None:
        """Connect to database and start polling."""
        logger.info(
            "Connecting to database at %s:%s/%s...",
            self.settings.postgres_host,
            self.settings.postgres_port,
            self.settings.postgres_db,
        )
        await self.db.connect()

        # Verify schema configuration on startup
        row = await self.db.pool.fetchrow("SHOW search_path")
        logger.info("PostgreSQL search_path: %s", row[0] if row else "unknown")
        row = await self.db.pool.fetchrow(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'disposition'"
        )
        logger.info("Disposition schema tables: %s", row[0] if row else 0)
        row = await self.db.pool.fetchrow(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'lead_pull_jobs'"
        )
        logger.info("lead_pull_jobs table exists: %s", (row[0] > 0) if row else False)

        self._running = True
        logger.info(
            "Bridge worker started — polling every %ds",
            self.settings.poll_interval,
        )

        while self._running:
            try:
                await self._poll_once()
            except Exception:
                logger.exception("Poll cycle error")
            await asyncio.sleep(self.settings.poll_interval)

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False
        for p in self.providers:
            await p.close()
        await self.db.close()
        logger.info("Bridge worker stopped")

    async def _poll_once(self) -> None:
        """Check for pending jobs and process the oldest one."""
        # Claim the oldest pending job (SKIP LOCKED prevents double-processing)
        # lead_pull_jobs lives in public schema (Charm's tables)
        row = await self.db.pool.fetchrow(
            """
            UPDATE public.lead_pull_jobs
            SET status = 'processing', started_at = NOW()
            WHERE id = (
                SELECT id FROM public.lead_pull_jobs
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
        )

        if row is None:
            return

        job = dict(row)
        job_id = job["id"]
        logger.info("Processing lead_pull_job %s for client %s", job_id, job["client_id"])

        try:
            request = build_waterfall_request(job)
            result = await self.waterfall.fill_campaign(request)
            result_data = result.model_dump(mode="json")

            await self.db.pool.execute(
                """
                UPDATE public.lead_pull_jobs
                SET status = 'completed',
                    result_data = $1::jsonb,
                    completed_at = NOW()
                WHERE id = $2
                """,
                json.dumps(result_data),
                job_id,
            )

            logger.info(
                "Job %s completed: %d/%d assigned (internal=%d, external=%d)",
                job_id,
                result.total_assigned,
                result.total_requested,
                result.internal_filled,
                result.external_filled,
            )

        except Exception as e:
            logger.exception("Job %s failed", job_id)
            await self.db.pool.execute(
                """
                UPDATE public.lead_pull_jobs
                SET status = 'failed',
                    error_message = $1,
                    completed_at = NOW()
                WHERE id = $2
                """,
                str(e),
                job_id,
            )


async def _run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = CharmBridgeWorker()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
        except NotImplementedError:
            pass  # Windows

    try:
        await worker.start()
    finally:
        await worker.stop()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
