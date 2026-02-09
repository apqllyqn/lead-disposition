"""Database connection and CRUD operations - SQLite backend.

Zero-install database backend using aiosqlite. Auto-creates schema on connect.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import aiosqlite

from lead_disposition.core.config import Settings
from lead_disposition.core.models import Company, CompanyStatus, Contact, DispositionStatus


# ---------------------------------------------------------------------------
# SQLite schema (auto-created on first connect)
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    domain              TEXT PRIMARY KEY,
    name                TEXT,
    company_status      TEXT NOT NULL DEFAULT 'fresh',
    company_suppressed  INTEGER NOT NULL DEFAULT 0,
    suppressed_reason   TEXT,
    suppressed_at       TEXT,
    contacts_total      INTEGER NOT NULL DEFAULT 0,
    contacts_in_sequence INTEGER NOT NULL DEFAULT 0,
    contacts_touched    INTEGER NOT NULL DEFAULT 0,
    last_contact_date   TEXT,
    company_cooldown_until TEXT,
    is_customer         INTEGER NOT NULL DEFAULT 0,
    customer_since      TEXT,
    client_owner_id     TEXT,
    client_owned_at     TEXT,
    ownership_expires_at TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS contacts (
    email               TEXT NOT NULL,
    client_id           TEXT NOT NULL,
    company_domain      TEXT NOT NULL REFERENCES companies(domain) ON DELETE CASCADE,
    first_name          TEXT,
    last_name           TEXT,
    last_known_title    TEXT,
    last_known_company  TEXT,
    disposition_status  TEXT NOT NULL DEFAULT 'fresh',
    disposition_updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    email_last_contacted    TEXT,
    linkedin_last_contacted TEXT,
    phone_last_contacted    TEXT,
    email_cooldown_until    TEXT,
    linkedin_cooldown_until TEXT,
    phone_cooldown_until    TEXT,
    email_suppressed        INTEGER NOT NULL DEFAULT 0,
    linkedin_suppressed     INTEGER NOT NULL DEFAULT 0,
    phone_suppressed        INTEGER NOT NULL DEFAULT 0,
    data_enriched_at    TEXT,
    sequence_count      INTEGER NOT NULL DEFAULT 0,
    source_system       TEXT,
    source_id           TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (email, client_id)
);

CREATE TABLE IF NOT EXISTS disposition_history (
    id                  TEXT PRIMARY KEY,
    contact_email       TEXT NOT NULL,
    contact_client_id   TEXT NOT NULL,
    previous_status     TEXT,
    new_status          TEXT NOT NULL,
    transition_reason   TEXT,
    triggered_by        TEXT NOT NULL DEFAULT 'system',
    campaign_id         TEXT,
    metadata            TEXT DEFAULT '{}',
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS client_ownership (
    id                  TEXT PRIMARY KEY,
    company_domain      TEXT NOT NULL REFERENCES companies(domain) ON DELETE CASCADE,
    previous_owner_id   TEXT,
    new_owner_id        TEXT,
    change_reason       TEXT NOT NULL,
    changed_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS campaign_assignments (
    id                  TEXT PRIMARY KEY,
    contact_email       TEXT NOT NULL,
    contact_client_id   TEXT NOT NULL,
    campaign_id         TEXT NOT NULL,
    client_id           TEXT NOT NULL,
    channel             TEXT NOT NULL DEFAULT 'email',
    assigned_at         TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at        TEXT,
    outcome             TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tam_snapshots (
    id                  TEXT PRIMARY KEY,
    snapshot_date       TEXT NOT NULL,
    client_id           TEXT,
    total_universe      INTEGER NOT NULL DEFAULT 0,
    never_touched       INTEGER NOT NULL DEFAULT 0,
    in_cooldown         INTEGER NOT NULL DEFAULT 0,
    available_now       INTEGER NOT NULL DEFAULT 0,
    permanent_suppress  INTEGER NOT NULL DEFAULT 0,
    in_sequence         INTEGER NOT NULL DEFAULT 0,
    won_customer        INTEGER NOT NULL DEFAULT 0,
    burn_rate_weekly    REAL,
    exhaustion_eta_weeks REAL,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, client_id)
);

CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(company_status);
CREATE INDEX IF NOT EXISTS idx_companies_owner ON companies(client_owner_id);
CREATE INDEX IF NOT EXISTS idx_contacts_domain ON contacts(company_domain);
CREATE INDEX IF NOT EXISTS idx_contacts_status ON contacts(disposition_status);
CREATE INDEX IF NOT EXISTS idx_contacts_client ON contacts(client_id);
CREATE INDEX IF NOT EXISTS idx_contacts_enriched ON contacts(data_enriched_at);
CREATE INDEX IF NOT EXISTS idx_history_contact ON disposition_history(contact_email, contact_client_id);
CREATE INDEX IF NOT EXISTS idx_history_created ON disposition_history(created_at);
CREATE INDEX IF NOT EXISTS idx_assignments_contact ON campaign_assignments(contact_email, contact_client_id);
CREATE INDEX IF NOT EXISTS idx_assignments_campaign ON campaign_assignments(campaign_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_date ON tam_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_client ON tam_snapshots(client_id);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_str() -> str:
    return _now().isoformat()


def _dt_str(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Convert sqlite3.Row to a plain dict."""
    return {k: row[k] for k in row.keys()}


def _row_to_contact(row: sqlite3.Row) -> Contact:
    d = _row_to_dict(row)
    return Contact(**d)


def _row_to_company(row: sqlite3.Row) -> Company:
    d = _row_to_dict(row)
    return Company(**d)


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------


class Database:
    """Async SQLite database connection manager and CRUD operations."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self._conn: aiosqlite.Connection | None = None

    def _resolve_path(self) -> str:
        url = self.settings.database_url
        if url.startswith("sqlite:///"):
            return url[len("sqlite:///"):]
        if url.startswith("sqlite://"):
            return url[len("sqlite://"):]
        return url

    async def connect(self) -> None:
        path = self._resolve_path()
        self._conn = await aiosqlite.connect(path)
        self._conn.row_factory = sqlite3.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._init_schema()

    async def _init_schema(self) -> None:
        """Auto-create tables if they don't exist."""
        await self.conn.executescript(SCHEMA_SQL)
        await self.conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._conn

    # -----------------------------------------------------------------------
    # Contact CRUD
    # -----------------------------------------------------------------------

    async def create_contact(self, contact: Contact) -> Contact:
        """Insert a single contact. Auto-creates company if needed."""
        # Auto-create company (replaces PostgreSQL trigger)
        await self._ensure_company(contact.company_domain)

        now = _now_str()
        await self.conn.execute(
            """
            INSERT INTO contacts (
                email, client_id, company_domain, first_name, last_name,
                last_known_title, last_known_company, disposition_status,
                data_enriched_at, source_system, source_id,
                disposition_updated_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contact.email, contact.client_id, contact.company_domain,
                contact.first_name, contact.last_name,
                contact.last_known_title, contact.last_known_company,
                contact.disposition_status.value,
                _dt_str(contact.data_enriched_at),
                contact.source_system, contact.source_id,
                now, now, now,
            ),
        )
        # Increment company contacts_total
        await self.conn.execute(
            "UPDATE companies SET contacts_total = contacts_total + 1 WHERE domain = ?",
            (contact.company_domain,),
        )
        await self.conn.commit()
        result = await self.get_contact(contact.email, contact.client_id)
        assert result is not None
        return result

    async def bulk_create_contacts(self, contacts: list[Contact]) -> int:
        """Bulk insert contacts, skipping duplicates. Returns count inserted."""
        if not contacts:
            return 0
        inserted = 0
        now = _now_str()
        for contact in contacts:
            try:
                await self._ensure_company(contact.company_domain)
                await self.conn.execute(
                    """
                    INSERT OR IGNORE INTO contacts (
                        email, client_id, company_domain, first_name, last_name,
                        last_known_title, last_known_company, disposition_status,
                        data_enriched_at, source_system, source_id,
                        disposition_updated_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        contact.email, contact.client_id, contact.company_domain,
                        contact.first_name, contact.last_name,
                        contact.last_known_title, contact.last_known_company,
                        contact.disposition_status.value,
                        _dt_str(contact.data_enriched_at),
                        contact.source_system, contact.source_id,
                        now, now, now,
                    ),
                )
                if self.conn.total_changes:
                    # Check if the row was actually inserted
                    cursor = await self.conn.execute(
                        "SELECT changes()"
                    )
                    row = await cursor.fetchone()
                    if row and row[0] > 0:
                        await self.conn.execute(
                            "UPDATE companies SET contacts_total = contacts_total + 1 WHERE domain = ?",
                            (contact.company_domain,),
                        )
                        inserted += 1
            except sqlite3.IntegrityError:
                pass
        await self.conn.commit()
        return inserted

    async def get_contact(self, email: str, client_id: str) -> Contact | None:
        cursor = await self.conn.execute(
            "SELECT * FROM contacts WHERE email = ? AND client_id = ?",
            (email, client_id),
        )
        row = await cursor.fetchone()
        return _row_to_contact(row) if row else None

    async def get_contacts_by_domain(self, domain: str) -> list[Contact]:
        cursor = await self.conn.execute(
            "SELECT * FROM contacts WHERE company_domain = ?", (domain,)
        )
        rows = await cursor.fetchall()
        return [_row_to_contact(r) for r in rows]

    async def update_contact_fields(
        self, email: str, client_id: str, **fields: Any
    ) -> Contact | None:
        """Update arbitrary fields on a contact."""
        if not fields:
            return await self.get_contact(email, client_id)

        set_clauses = []
        values: list[Any] = []
        for key, val in fields.items():
            if isinstance(val, DispositionStatus):
                val = val.value
            elif isinstance(val, CompanyStatus):
                val = val.value
            elif isinstance(val, datetime):
                val = val.isoformat()
            elif isinstance(val, bool):
                val = int(val)
            set_clauses.append(f"{key} = ?")
            values.append(val)

        # Always bump updated_at
        set_clauses.append("updated_at = ?")
        values.append(_now_str())

        values.append(email)
        values.append(client_id)

        query = (
            f"UPDATE contacts SET {', '.join(set_clauses)} "
            f"WHERE email = ? AND client_id = ?"
        )
        await self.conn.execute(query, values)
        await self.conn.commit()
        return await self.get_contact(email, client_id)

    # -----------------------------------------------------------------------
    # Company CRUD
    # -----------------------------------------------------------------------

    async def _ensure_company(self, domain: str) -> None:
        """Create company if it doesn't exist (replaces PostgreSQL trigger)."""
        await self.conn.execute(
            "INSERT OR IGNORE INTO companies (domain) VALUES (?)",
            (domain,),
        )

    async def get_company(self, domain: str) -> Company | None:
        cursor = await self.conn.execute(
            "SELECT * FROM companies WHERE domain = ?", (domain,)
        )
        row = await cursor.fetchone()
        return _row_to_company(row) if row else None

    async def create_company(self, company: Company) -> Company:
        now = _now_str()
        await self.conn.execute(
            """
            INSERT INTO companies (domain, name, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (domain) DO UPDATE SET name = COALESCE(EXCLUDED.name, companies.name)
            """,
            (company.domain, company.name, now, now),
        )
        await self.conn.commit()
        result = await self.get_company(company.domain)
        assert result is not None
        return result

    async def update_company_fields(self, domain: str, **fields: Any) -> Company | None:
        """Update arbitrary fields on a company."""
        if not fields:
            return await self.get_company(domain)

        set_clauses = []
        values: list[Any] = []
        for key, val in fields.items():
            if isinstance(val, (CompanyStatus, DispositionStatus)):
                val = val.value
            elif isinstance(val, datetime):
                val = val.isoformat()
            elif isinstance(val, bool):
                val = int(val)
            set_clauses.append(f"{key} = ?")
            values.append(val)

        # Always bump updated_at
        set_clauses.append("updated_at = ?")
        values.append(_now_str())

        values.append(domain)

        query = f"UPDATE companies SET {', '.join(set_clauses)} WHERE domain = ?"
        await self.conn.execute(query, values)
        await self.conn.commit()
        return await self.get_company(domain)

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
        await self.conn.execute(
            """
            INSERT INTO disposition_history (
                id, contact_email, contact_client_id, previous_status, new_status,
                transition_reason, triggered_by, campaign_id, metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                contact_email,
                contact_client_id,
                previous_status.value if previous_status else None,
                new_status.value,
                reason,
                triggered_by,
                campaign_id,
                json.dumps(metadata or {}),
                _now_str(),
            ),
        )
        await self.conn.commit()

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
        await self.conn.execute(
            """
            INSERT INTO client_ownership (id, company_domain, previous_owner_id, new_owner_id, change_reason, changed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                company_domain,
                previous_owner_id,
                new_owner_id,
                reason,
                _now_str(),
            ),
        )
        await self.conn.commit()

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
        now = _now_str()
        await self.conn.execute(
            """
            INSERT INTO campaign_assignments (
                id, contact_email, contact_client_id, campaign_id, client_id, channel, assigned_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                contact_email,
                contact_client_id,
                campaign_id,
                client_id,
                channel,
                now,
                now,
            ),
        )
        await self.conn.commit()

    # -----------------------------------------------------------------------
    # TAM Queries
    # -----------------------------------------------------------------------

    async def get_tam_pools(self, client_id: str | None = None) -> dict[str, int]:
        """Get current TAM pool segmentation counts."""
        now = _now_str()
        where = "WHERE client_id = ?" if client_id else ""
        params: list[Any] = [client_id] if client_id else []

        # SQLite doesn't support FILTER, use SUM(CASE WHEN ... THEN 1 ELSE 0 END)
        cursor = await self.conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_universe,
                SUM(CASE WHEN disposition_status = 'fresh' AND sequence_count = 0
                    THEN 1 ELSE 0 END) AS never_touched,
                SUM(CASE WHEN disposition_status IN (
                        'completed_no_response', 'replied_neutral', 'replied_negative', 'lost_closed'
                    ) AND email_cooldown_until IS NOT NULL AND email_cooldown_until > ?
                    THEN 1 ELSE 0 END) AS in_cooldown,
                SUM(CASE WHEN disposition_status IN ('fresh', 'retouch_eligible')
                    AND email_suppressed = 0
                    AND (email_cooldown_until IS NULL OR email_cooldown_until <= ?)
                    THEN 1 ELSE 0 END) AS available_now,
                SUM(CASE WHEN disposition_status IN ('replied_hard_no', 'bounced', 'unsubscribed')
                    THEN 1 ELSE 0 END) AS permanent_suppress,
                SUM(CASE WHEN disposition_status = 'in_sequence'
                    THEN 1 ELSE 0 END) AS in_sequence,
                SUM(CASE WHEN disposition_status = 'won_customer'
                    THEN 1 ELSE 0 END) AS won_customer
            FROM contacts
            {where}
            """,
            [now, now] + params,
        )
        row = await cursor.fetchone()
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
        cutoff = (_now() - timedelta(days=7)).isoformat()
        if client_id:
            cursor = await self.conn.execute(
                """
                SELECT COUNT(*) AS burned
                FROM disposition_history
                WHERE new_status = 'in_sequence'
                AND created_at > ?
                AND contact_client_id = ?
                """,
                (cutoff, client_id),
            )
        else:
            cursor = await self.conn.execute(
                """
                SELECT COUNT(*) AS burned
                FROM disposition_history
                WHERE new_status = 'in_sequence'
                AND created_at > ?
                """,
                (cutoff,),
            )
        row = await cursor.fetchone()
        return float(row["burned"]) if row else 0.0

    async def insert_tam_snapshot(self, snapshot: dict, client_id: str | None = None) -> None:
        """Insert a TAM snapshot record."""
        today = date.today().isoformat()
        row_id = str(uuid.uuid4())
        await self.conn.execute(
            """
            INSERT INTO tam_snapshots (
                id, snapshot_date, client_id, total_universe, never_touched,
                in_cooldown, available_now, permanent_suppress, in_sequence,
                won_customer, burn_rate_weekly, exhaustion_eta_weeks, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            (
                row_id,
                today,
                client_id,
                snapshot.get("total_universe", 0),
                snapshot.get("never_touched", 0),
                snapshot.get("in_cooldown", 0),
                snapshot.get("available_now", 0),
                snapshot.get("permanent_suppress", 0),
                snapshot.get("in_sequence", 0),
                snapshot.get("won_customer", 0),
                snapshot.get("burn_rate_weekly"),
                snapshot.get("exhaustion_eta_weeks"),
                _now_str(),
            ),
        )
        await self.conn.commit()

    async def get_snapshots(
        self, client_id: str | None = None, days: int = 30
    ) -> list[dict]:
        """Get TAM snapshots for the last N days."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        if client_id:
            cursor = await self.conn.execute(
                """
                SELECT * FROM tam_snapshots
                WHERE client_id = ? AND snapshot_date > ?
                ORDER BY snapshot_date DESC
                """,
                (client_id, cutoff),
            )
        else:
            cursor = await self.conn.execute(
                """
                SELECT * FROM tam_snapshots
                WHERE client_id IS NULL AND snapshot_date > ?
                ORDER BY snapshot_date DESC
                """,
                (cutoff,),
            )
        rows = await cursor.fetchall()
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
        now = _now_str()
        stale_cutoff = (_now() - timedelta(days=180)).isoformat()

        cooldown_col = f"{channel}_cooldown_until"
        suppressed_col = f"{channel}_suppressed"

        # Build title filter clause
        title_clause = ""
        title_params: list[Any] = []
        if title_keywords:
            conditions = []
            for kw in title_keywords:
                conditions.append("LOWER(c.last_known_title) LIKE ?")
                title_params.append(f"%{kw.lower()}%")
            title_clause = "AND (" + " OR ".join(conditions) + ")"

        status_placeholders = ", ".join("?" for _ in statuses)

        query = f"""
            SELECT c.* FROM contacts c
            JOIN companies co ON c.company_domain = co.domain
            WHERE c.client_id = ?
            AND c.disposition_status IN ({status_placeholders})
            AND c.{suppressed_col} = 0
            AND (c.{cooldown_col} IS NULL OR c.{cooldown_col} <= ?)
            AND co.company_suppressed = 0
            AND co.is_customer = 0
            AND (co.client_owner_id = ? OR co.client_owner_id IS NULL)
            AND (c.data_enriched_at IS NULL OR c.data_enriched_at > ?)
            {title_clause}
            ORDER BY
                CASE WHEN c.disposition_status = 'fresh' THEN 0 ELSE 1 END,
                c.data_enriched_at DESC,
                c.sequence_count ASC
            LIMIT ?
        """

        # Build params in exact order of ? placeholders in the query
        params: list[Any] = []
        params.append(client_id)       # c.client_id = ?
        params.extend(statuses)        # IN (?, ?, ...)
        params.append(now)             # cooldown <= ?
        params.append(client_id)       # co.client_owner_id = ?
        params.append(stale_cutoff)    # data_enriched_at > ?
        params.extend(title_params)    # title LIKE ? (if any)
        params.append(limit)           # LIMIT ?

        cursor = await self.conn.execute(query, params)
        rows = await cursor.fetchall()
        return [_row_to_contact(r) for r in rows]

    async def count_company_in_sequence(self, domain: str) -> int:
        cursor = await self.conn.execute(
            "SELECT contacts_in_sequence FROM companies WHERE domain = ?", (domain,)
        )
        row = await cursor.fetchone()
        return row["contacts_in_sequence"] if row else 0

    async def get_distinct_clients(self) -> list[str]:
        cursor = await self.conn.execute("SELECT DISTINCT client_id FROM contacts")
        rows = await cursor.fetchall()
        return [r["client_id"] for r in rows]

    async def get_stale_contacts(self, months: int = 6) -> list[Contact]:
        """Get contacts whose enrichment data is older than N months."""
        cutoff = (_now() - timedelta(days=months * 30)).isoformat()
        cursor = await self.conn.execute(
            """
            SELECT * FROM contacts
            WHERE data_enriched_at IS NOT NULL
            AND data_enriched_at < ?
            AND disposition_status NOT IN (
                'replied_hard_no', 'bounced', 'unsubscribed', 'won_customer', 'stale_data'
            )
            """,
            (cutoff,),
        )
        rows = await cursor.fetchall()
        return [_row_to_contact(r) for r in rows]

    async def get_expired_cooldowns(self) -> list[Contact]:
        """Get contacts whose cooldown has expired and are eligible for retouch."""
        now = _now_str()
        cursor = await self.conn.execute(
            """
            SELECT * FROM contacts
            WHERE disposition_status IN (
                'completed_no_response', 'replied_neutral', 'replied_negative', 'lost_closed'
            )
            AND email_cooldown_until IS NOT NULL
            AND email_cooldown_until <= ?
            """,
            (now,),
        )
        rows = await cursor.fetchall()
        return [_row_to_contact(r) for r in rows]

    async def get_expired_ownerships(self) -> list[Company]:
        """Get companies whose ownership has expired and have no active sequences."""
        now = _now_str()
        cursor = await self.conn.execute(
            """
            SELECT * FROM companies
            WHERE client_owner_id IS NOT NULL
            AND ownership_expires_at IS NOT NULL
            AND ownership_expires_at <= ?
            AND contacts_in_sequence = 0
            """,
            (now,),
        )
        rows = await cursor.fetchall()
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

        if client_id:
            where_clauses.append("client_id = ?")
            params.append(client_id)
        if status:
            where_clauses.append("disposition_status = ?")
            params.append(status)
        if search:
            where_clauses.append(
                "(LOWER(email) LIKE ? OR LOWER(first_name) LIKE ? "
                "OR LOWER(last_name) LIKE ? OR LOWER(last_known_company) LIKE ? "
                "OR LOWER(company_domain) LIKE ?)"
            )
            term = f"%{search.lower()}%"
            params.extend([term, term, term, term, term])

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        cursor = await self.conn.execute(
            f"SELECT COUNT(*) AS total FROM contacts {where}", params
        )
        count_row = await cursor.fetchone()
        total = count_row["total"] if count_row else 0

        cursor = await self.conn.execute(
            f"SELECT * FROM contacts {where} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        )
        rows = await cursor.fetchall()
        return [_row_to_contact(r) for r in rows], total

    async def list_owned_companies(
        self, client_id: str | None = None
    ) -> list[Company]:
        """List companies with active ownership."""
        if client_id:
            cursor = await self.conn.execute(
                "SELECT * FROM companies WHERE client_owner_id = ? "
                "ORDER BY client_owned_at DESC",
                (client_id,),
            )
        else:
            cursor = await self.conn.execute(
                "SELECT * FROM companies WHERE client_owner_id IS NOT NULL "
                "ORDER BY client_owned_at DESC"
            )
        rows = await cursor.fetchall()
        return [_row_to_company(r) for r in rows]

    async def get_contact_history(
        self, email: str, client_id: str, limit: int = 50
    ) -> list[dict]:
        """Get disposition history for a contact."""
        cursor = await self.conn.execute(
            "SELECT * FROM disposition_history "
            "WHERE contact_email = ? AND contact_client_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (email, client_id, limit),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]
