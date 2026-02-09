"""Initialize the disposition database by running the migration."""

from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg

from lead_disposition.core.config import Settings


async def run_migration() -> None:
    settings = Settings()
    migration_path = Path(__file__).parent.parent.parent.parent / "migrations" / "001_disposition_schema.sql"

    if not migration_path.exists():
        print(f"Migration file not found: {migration_path}")
        return

    sql = migration_path.read_text(encoding="utf-8")

    # Connect to default database to create disposition_db if needed
    base_url = settings.database_url.rsplit("/", 1)[0]
    db_name = settings.database_url.rsplit("/", 1)[1]

    try:
        conn = await asyncpg.connect(f"{base_url}/postgres")
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        if not exists:
            await conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Created database: {db_name}")
        else:
            print(f"Database already exists: {db_name}")
        await conn.close()
    except Exception as e:
        print(f"Note: Could not create database (may already exist): {e}")

    # Run migration
    conn = await asyncpg.connect(settings.database_url)
    try:
        await conn.execute(sql)
        print("Migration completed successfully.")
    except Exception as e:
        print(f"Migration error: {e}")
        raise
    finally:
        await conn.close()


def main() -> None:
    asyncio.run(run_migration())


if __name__ == "__main__":
    main()
