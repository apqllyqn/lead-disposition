"""Database factory - selects backend based on configuration."""

from __future__ import annotations

from lead_disposition.core.config import Settings


def create_database(settings: Settings | None = None):
    """Return the appropriate database backend.

    - use_sqlite=True uses the aiosqlite backend.
    - Otherwise uses the asyncpg PostgreSQL backend (default for production).
    """
    s = settings or Settings()
    if s.use_sqlite:
        from lead_disposition.core.database import Database
        return Database(s)
    else:
        from lead_disposition.core.database_pg import PostgresDatabase
        return PostgresDatabase(s)
