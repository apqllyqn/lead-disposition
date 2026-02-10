"""Database connection and CRUD operations - PostgreSQL backend (asyncpg).

Production database backend using asyncpg connection pool.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import asyncpg

from lead_disposition.core.config import Settings
from lead_disposition.core.models import Company, CompanyStatus, Contact, DispositionStatus


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_contact(row: asyncpg.Record) -> Contact:
    return Contact(**dict(row))


def _row_to_company(row: asyncpg.Record) -> Company:
    return Company(**dict(row))


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    return dict(row)


class PostgresDatabase:
    """Async PostgreSQL database connection manager and CRUD operations."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self.settings.database_url, min_size=2, max_size=10,
            server_settings={"search_path": "disposition, public"},
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._pool

    # -----------------------------------------------------------------------
    # Contact CRUD
    # -----------------------------------------------------------------------

    async def create_contact(self, contact: Contact) -> Contact:
        """Insert a single contact. PostgreSQL trigger auto-creates company."""
        row = await self.pool.fetchrow(
            """
            INSERT INTO contacts (
                email, client_id, company_domain, first_name, last_name,
                last_known_title, last_known_company, disposition_status,
                data_enriched_at, source_system, source_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *
            """,
            contact.email, contact.client_id, contact.company_domain,
            contact.first_name, contact.last_name, contact.last_known_title,
            contact.last_known_company, contact.disposition_status.value,
            contact.data_enriched_at, contact.source_system, contact.source_id,
        )
        return _row_to_contact(row)

    async def bulk_create_contacts(self, contacts: list[Contact]) -> int:
        """Bulk insert contacts, skipping duplicates. Returns count inserted."""
        if not contacts:
            return 0
        inserted = 0
        async with self.pool.acquire() as conn:
            for contact in contacts:
                try:
                    result = await conn.execute(
                        """
                        INSERT INTO contacts (
                            email, client_id, company_domain, first_name, last_name,
                            last_known_title, last_known_company, disposition_status,
                            data_enriched_at, source_system, source_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (email, client_id) DO NOTHING
                        """,
                        contact.email, contact.client_id, contact.company_domain,
                        contact.first_name, contact.last_name, contact.last_known_title,
                        contact.last_known_company, contact.disposition_status.value,
                        contact.data_enriched_at, contact.source_system, contact.source_id,
                    )
                    if result == "INSERT 0 1":
                        inserted += 1
                except asyncpg.UniqueViolationError:
                    pass
        return inserted

    async def get_contact(self, email: str, client_id: str) -> Contact | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM contacts WHERE email = $1 AND client_id = $2",
            email, client_id,
        )
        return _row_to_contact(row) if row else None

    async def get_contacts_by_domain(self, domain: str) -> list[Contact]:
        rows = await self.pool.fetch(
            "SELECT * FROM contacts WHERE company_domain = $1", domain
        )
        return [_row_to_contact(r) for r in rows]

    async def update_contact_fields(
        self, email: str, client_id: str, **fields: Any
    ) -> Contact | None:
        """Update arbitrary fields on a contact."""
        if not fields:
            return await self.get_contact(email, client_id)

        set_clauses = []
        values: list[Any] = []
        for i, (key, val) in enumerate(fields.items(), start=1):
            if isinstance(val, DispositionStatus):
                val = val.value
            elif isinstance(val, CompanyStatus):
                val = val.value
            set_clauses.append(f"{key} = ${i}")
            values.append(val)

        n = len(values)
        query = (
            f"UPDATE contacts SET {', '.join(set_clauses)} "
            f"WHERE email = ${n + 1} AND client_id = ${n + 2} "
            f"RETURNING *"
        )
        values.append(email)
        values.append(client_id)

        row = await self.pool.fetchrow(query, *values)
        return _row_to_contact(row) if row else None

    # -----------------------------------------------------------------------
    # Company CRUD
    # -----------------------------------------------------------------------

    async def _ensure_company(self, domain: str) -> None:
        """Create company if it doesn't exist (fallback for non-trigger use)."""
        await self.pool.execute(
            "INSERT INTO companies (domain) VALUES ($1) ON CONFLICT (domain) DO NOTHING",
            domain,
        )

    async def get_company(self, domain: str) -> Company | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM companies WHERE domain = $1", domain
        )
        return _row_to_company(row) if row else None

    async def create_company(self, company: Company) -> Company:
        row = await self.pool.fetchrow(
            """
            INSERT INTO companies (domain, name)
            VALUES ($1, $2)
            ON CONFLICT (domain) DO UPDATE SET name = COALESCE(EXCLUDED.name, companies.name)
            RETURNING *
            """,
            company.domain, company.name,
        )
        return _row_to_company(row)

    async def update_company_fields(self, domain: str, **fields: Any) -> Company | None:
        """Update arbitrary fields on a company."""
        if not fields:
            return await self.get_company(domain)

        set_clauses = []
        values: list[Any] = []
        for i, (key, val) in enumerate(fields.items(), start=1):
            if isinstance(val, (CompanyStatus, DispositionStatus)):
                val = val.value
            set_clauses.append(f"{key} = ${i}")
            values.append(val)

        n = len(values)
        query = (
            f"UPDATE companies SET {', '.join(set_clauses)} "
            f"WHERE domain = ${n + 1} RETURNING *"
        )
        values.append(domain)

        row = await self.pool.fetchrow(query, *values)
        return _row_to_company(row) if row else None

    # -----------------------------------------------------------------------
    # History
    # -----------------------------------------------------------------------

    async def insert_history(
        self,
        contact_email: str,
        contact_client_id: str,
        previous_status: DispositionStatus | None,
        new_status: DispositionStatus,
        reason: str | None = None,
        triggered_by: str = "system",
        campaign_id: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        await self.pool.execute(
            """
            INSERT INTO disposition_history (
                contact_email, contact_client_id, previous_status, new_status,
                transition_reason, triggered_by, campaign_id, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            contact_email, contact_client_id,
            previous_status.value if previous_status else None,
            new_status.value, reason, triggered_by, campaign_id,
            json.dumps(metadata or {}),
        )

    # -----------------------------------------------------------------------
    # Ownership
    # -----------------------------------------------------------------------

    async def insert_ownership_change(
        self,
        company_domain: str,
        previous_owner_id: str | None,
        new_owner_id: str | None,
        reason: str,
    ) -> None:
        await self.pool.execute(
            """
            INSERT INTO client_ownership (
                company_domain, previous_owner_id, new_owner_id, change_reason
            ) VALUES ($1, $2, $3, $4)
            """,
            company_domain, previous_owner_id, new_owner_id, reason,
        )

    # -----------------------------------------------------------------------
    # Assignments
    # -----------------------------------------------------------------------

    async def insert_assignment(
        self,
        contact_email: str,
        contact_client_id: str,
        campaign_id: str,
        client_id: str,
        channel: str = "email",
    ) -> None:
        await self.pool.execute(
            """
            INSERT INTO campaign_assignments (
                contact_email, contact_client_id, campaign_id, client_id, channel
            ) VALUES ($1, $2, $3, $4, $5)
            """,
            contact_email, contact_client_id, campaign_id, client_id, channel,
        )

    # -----------------------------------------------------------------------
    # TAM Queries
    # -----------------------------------------------------------------------

    async def get_tam_pools(self, client_id: str | None = None) -> dict[str, int]:
        """Get current TAM pool segmentation counts."""
        now = _now()
        if client_id:
            row = await self.pool.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_universe,
                    COUNT(*) FILTER (WHERE disposition_status = 'fresh' AND sequence_count = 0)
                        AS never_touched,
                    COUNT(*) FILTER (WHERE disposition_status IN (
                            'completed_no_response', 'replied_neutral',
                            'replied_negative', 'lost_closed'
                        ) AND email_cooldown_until IS NOT NULL AND email_cooldown_until > $1)
                        AS in_cooldown,
                    COUNT(*) FILTER (WHERE disposition_status IN ('fresh', 'retouch_eligible')
                        AND email_suppressed = false
                        AND (email_cooldown_until IS NULL OR email_cooldown_until <= $1))
                        AS available_now,
                    COUNT(*) FILTER (WHERE disposition_status IN (
                            'replied_hard_no', 'bounced', 'unsubscribed'))
                        AS permanent_suppress,
                    COUNT(*) FILTER (WHERE disposition_status = 'in_sequence')
                        AS in_sequence,
                    COUNT(*) FILTER (WHERE disposition_status = 'won_customer')
                        AS won_customer
                FROM contacts
                WHERE client_id = $2
                """,
                now, client_id,
            )
        else:
            row = await self.pool.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_universe,
                    COUNT(*) FILTER (WHERE disposition_status = 'fresh' AND sequence_count = 0)
                        AS never_touched,
                    COUNT(*) FILTER (WHERE disposition_status IN (
                            'completed_no_response', 'replied_neutral',
                            'replied_negative', 'lost_closed'
                        ) AND email_cooldown_until IS NOT NULL AND email_cooldown_until > $1)
                        AS in_cooldown,
                    COUNT(*) FILTER (WHERE disposition_status IN ('fresh', 'retouch_eligible')
                        AND email_suppressed = false
                        AND (email_cooldown_until IS NULL OR email_cooldown_until <= $1))
                        AS available_now,
                    COUNT(*) FILTER (WHERE disposition_status IN (
                            'replied_hard_no', 'bounced', 'unsubscribed'))
                        AS permanent_suppress,
                    COUNT(*) FILTER (WHERE disposition_status = 'in_sequence')
                        AS in_sequence,
                    COUNT(*) FILTER (WHERE disposition_status = 'won_customer')
                        AS won_customer
                FROM contacts
                """,
                now,
            )

        if not row:
            return {}
        return {
            "total_universe": row["total_universe"] or 0,
            "never_touched": row["never_touched"] or 0,
            "in_cooldown": row["in_cooldown"] or 0,
            "available_now": row["available_now"] or 0,
            "permanent_suppress": row["permanent_suppress"] or 0,
            "in_sequence": row["in_sequence"] or 0,
            "won_customer": row["won_customer"] or 0,
        }

    async def get_burn_rate(self, client_id: str | None = None) -> float:
        """Contacts moved to in_sequence in the last 7 days."""
        cutoff = _now() - timedelta(days=7)
        if client_id:
            row = await self.pool.fetchrow(
                """
                SELECT COUNT(*) AS burned
                FROM disposition_history
                WHERE new_status = 'in_sequence'
                AND created_at > $1
                AND contact_client_id = $2
                """,
                cutoff, client_id,
            )
        else:
            row = await self.pool.fetchrow(
                """
                SELECT COUNT(*) AS burned
                FROM disposition_history
                WHERE new_status = 'in_sequence'
                AND created_at > $1
                """,
                cutoff,
            )
        return float(row["burned"]) if row else 0.0

    async def insert_tam_snapshot(
        self, snapshot: dict, client_id: str | None = None
    ) -> None:
        """Insert a TAM snapshot record."""
        today = date.today()
        await self.pool.execute(
            """
            INSERT INTO tam_snapshots (
                snapshot_date, client_id, total_universe, never_touched,
                in_cooldown, available_now, permanent_suppress, in_sequence,
                won_customer, burn_rate_weekly, exhaustion_eta_weeks
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (snapshot_date, client_id) DO UPDATE SET
                total_universe = EXCLUDED.total_universe,
                never_touched = EXCLUDED.never_touched,
                in_cooldown = EXCLUDED.in_cooldown,
                available_now = EXCLUDED.available_now,
                permanent_suppress = EXCLUDED.permanent_suppress,
                in_sequence = EXCLUDED.in_sequence,
                won_customer = EXCLUDED.won_customer,
                burn_rate_weekly = EXCLUDED.burn_rate_weekly,
                exhaustion_eta_weeks = EXCLUDED.exhaustion_eta_weeks
            """,
            today, client_id,
            snapshot.get("total_universe", 0),
            snapshot.get("never_touched", 0),
            snapshot.get("in_cooldown", 0),
            snapshot.get("available_now", 0),
            snapshot.get("permanent_suppress", 0),
            snapshot.get("in_sequence", 0),
            snapshot.get("won_customer", 0),
            snapshot.get("burn_rate_weekly"),
            snapshot.get("exhaustion_eta_weeks"),
        )

    async def get_snapshots(
        self, client_id: str | None = None, days: int = 30
    ) -> list[dict]:
        """Get TAM snapshots for the last N days."""
        cutoff = date.today() - timedelta(days=days)
        if client_id:
            rows = await self.pool.fetch(
                """
                SELECT * FROM tam_snapshots
                WHERE client_id = $1 AND snapshot_date > $2
                ORDER BY snapshot_date DESC
                """,
                client_id, cutoff,
            )
        else:
            rows = await self.pool.fetch(
                """
                SELECT * FROM tam_snapshots
                WHERE client_id IS NULL AND snapshot_date > $1
                ORDER BY snapshot_date DESC
                """,
                cutoff,
            )
        return [_row_to_dict(r) for r in rows]

    # -----------------------------------------------------------------------
    # Pull logic queries
    # -----------------------------------------------------------------------

    async def query_eligible_contacts(
        self,
        client_id: str,
        channel: str = "email",
        title_keywords: list[str] | None = None,
        limit: int = 1000,
        status_filter: list[str] | None = None,
        exclude_reserved_pct: float = 0.0,
    ) -> list[Contact]:
        """Query contacts eligible for campaign assignment."""
        statuses = status_filter or ["fresh", "retouch_eligible"]
        now = _now()
        stale_cutoff = now - timedelta(days=180)

        cooldown_col = f"{channel}_cooldown_until"
        suppressed_col = f"{channel}_suppressed"

        # Build title filter
        title_clause = ""
        title_params: list[Any] = []
        param_idx = 4  # $1=client_id, $2=now, $3=client_id, $4=stale_cutoff
        if title_keywords:
            conditions = []
            for kw in title_keywords:
                param_idx += 1
                conditions.append(f"LOWER(c.last_known_title) LIKE ${param_idx}::text")
                title_params.append(f"%{kw.lower()}%")
            title_clause = "AND (" + " OR ".join(conditions) + ")"

        # Status IN clause
        status_placeholders = []
        status_params: list[Any] = []
        for s in statuses:
            param_idx += 1
            status_placeholders.append(f"${param_idx}::text")
            status_params.append(s)

        param_idx += 1
        limit_param = f"${param_idx}::int"

        query = f"""
            SELECT c.* FROM contacts c
            JOIN companies co ON c.company_domain = co.domain
            WHERE c.client_id = $1
            AND c.disposition_status::text IN ({', '.join(status_placeholders)})
            AND c.{suppressed_col} = false
            AND (c.{cooldown_col} IS NULL OR c.{cooldown_col} <= $2)
            AND co.company_suppressed = false
            AND co.is_customer = false
            AND (co.client_owner_id = $3 OR co.client_owner_id IS NULL)
            AND (c.data_enriched_at IS NULL OR c.data_enriched_at > $4)
            {title_clause}
            ORDER BY
                CASE WHEN c.disposition_status::text = 'fresh' THEN 0 ELSE 1 END,
                c.data_enriched_at DESC NULLS LAST,
                c.sequence_count ASC
            LIMIT {limit_param}
        """

        params: list[Any] = [client_id, now, client_id, stale_cutoff]
        params.extend(title_params)
        params.extend(status_params)
        params.append(limit)

        rows = await self.pool.fetch(query, *params)
        return [_row_to_contact(r) for r in rows]

    async def count_company_in_sequence(self, domain: str) -> int:
        row = await self.pool.fetchrow(
            "SELECT contacts_in_sequence FROM companies WHERE domain = $1", domain
        )
        return row["contacts_in_sequence"] if row else 0

    async def get_distinct_clients(self) -> list[str]:
        rows = await self.pool.fetch("SELECT DISTINCT client_id FROM contacts")
        return [r["client_id"] for r in rows]

    async def get_stale_contacts(self, months: int = 6) -> list[Contact]:
        """Get contacts whose enrichment data is older than N months."""
        cutoff = _now() - timedelta(days=months * 30)
        rows = await self.pool.fetch(
            """
            SELECT * FROM contacts
            WHERE data_enriched_at IS NOT NULL
            AND data_enriched_at < $1
            AND disposition_status NOT IN (
                'replied_hard_no', 'bounced', 'unsubscribed', 'won_customer', 'stale_data'
            )
            """,
            cutoff,
        )
        return [_row_to_contact(r) for r in rows]

    async def get_expired_cooldowns(self) -> list[Contact]:
        """Get contacts whose cooldown has expired."""
        now = _now()
        rows = await self.pool.fetch(
            """
            SELECT * FROM contacts
            WHERE disposition_status IN (
                'completed_no_response', 'replied_neutral', 'replied_negative', 'lost_closed'
            )
            AND email_cooldown_until IS NOT NULL
            AND email_cooldown_until <= $1
            """,
            now,
        )
        return [_row_to_contact(r) for r in rows]

    async def get_expired_ownerships(self) -> list[Company]:
        """Get companies whose ownership has expired and have no active sequences."""
        now = _now()
        rows = await self.pool.fetch(
            """
            SELECT * FROM companies
            WHERE client_owner_id IS NOT NULL
            AND ownership_expires_at IS NOT NULL
            AND ownership_expires_at <= $1
            AND contacts_in_sequence = 0
            """,
            now,
        )
        return [_row_to_company(r) for r in rows]

    # -----------------------------------------------------------------------
    # Web UI queries
    # -----------------------------------------------------------------------

    async def list_contacts(
        self,
        client_id: str | None = None,
        status: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Contact], int]:
        """Paginated contact listing with optional filters."""
        where_clauses: list[str] = []
        params: list[Any] = []
        idx = 0

        if client_id:
            idx += 1
            where_clauses.append(f"client_id = ${idx}")
            params.append(client_id)
        if status:
            idx += 1
            where_clauses.append(f"disposition_status = ${idx}")
            params.append(status)
        if search:
            idx += 1
            where_clauses.append(
                f"(LOWER(email) LIKE ${idx} OR LOWER(first_name) LIKE ${idx} "
                f"OR LOWER(last_name) LIKE ${idx} OR LOWER(last_known_company) LIKE ${idx} "
                f"OR LOWER(company_domain) LIKE ${idx})"
            )
            params.append(f"%{search.lower()}%")

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        count_row = await self.pool.fetchrow(
            f"SELECT COUNT(*) AS total FROM contacts {where}", *params
        )
        total = count_row["total"] if count_row else 0

        idx += 1
        limit_idx = idx
        idx += 1
        offset_idx = idx

        rows = await self.pool.fetch(
            f"SELECT * FROM contacts {where} ORDER BY updated_at DESC "
            f"LIMIT ${limit_idx} OFFSET ${offset_idx}",
            *params, limit, offset,
        )
        return [_row_to_contact(r) for r in rows], total

    async def list_owned_companies(
        self, client_id: str | None = None
    ) -> list[Company]:
        """List companies with active ownership."""
        if client_id:
            rows = await self.pool.fetch(
                "SELECT * FROM companies WHERE client_owner_id = $1 "
                "ORDER BY client_owned_at DESC",
                client_id,
            )
        else:
            rows = await self.pool.fetch(
                "SELECT * FROM companies WHERE client_owner_id IS NOT NULL "
                "ORDER BY client_owned_at DESC"
            )
        return [_row_to_company(r) for r in rows]

    async def get_contact_history(
        self, email: str, client_id: str, limit: int = 50
    ) -> list[dict]:
        """Get disposition history for a contact."""
        rows = await self.pool.fetch(
            "SELECT * FROM disposition_history "
            "WHERE contact_email = $1 AND contact_client_id = $2 "
            "ORDER BY created_at DESC LIMIT $3",
            email, client_id, limit,
        )
        return [_row_to_dict(r) for r in rows]
