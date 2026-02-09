"""Data import - CSV and charmdemon source ingestion."""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from pathlib import Path

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database
from lead_disposition.core.models import Contact, DispositionStatus


# Default column mappings for CSV import
DEFAULT_CSV_COLUMNS = {
    "email": "email",
    "first_name": "first_name",
    "last_name": "last_name",
    "company_domain": "company_domain",
    "title": "last_known_title",
    "company": "last_known_company",
}


class CSVImporter:
    """Import contacts from CSV files into the disposition system."""

    def __init__(self, db: Database, settings: Settings | None = None):
        self.db = db
        self.settings = settings or Settings()

    async def import_file(
        self,
        file_path: str | Path,
        client_id: str,
        column_map: dict[str, str] | None = None,
    ) -> ImportResult:
        """Import contacts from a CSV file.

        Args:
            file_path: Path to the CSV file.
            client_id: Client ID to assign to all imported contacts.
            column_map: Mapping from CSV column names to contact field names.
                        Keys are CSV headers, values are Contact field names.
        """
        path = Path(file_path)
        with open(path, encoding="utf-8-sig") as f:
            return await self._import_reader(f, client_id, column_map)

    async def import_csv_string(
        self,
        csv_content: str,
        client_id: str,
        column_map: dict[str, str] | None = None,
    ) -> ImportResult:
        """Import contacts from a CSV string."""
        return await self._import_reader(io.StringIO(csv_content), client_id, column_map)

    async def _import_reader(
        self,
        reader_source: io.TextIOBase | io.StringIO,
        client_id: str,
        column_map: dict[str, str] | None,
    ) -> ImportResult:
        cmap = column_map or DEFAULT_CSV_COLUMNS
        reader = csv.DictReader(reader_source)

        contacts: list[Contact] = []
        skipped = 0
        errors: list[str] = []

        for row_num, row in enumerate(reader, start=2):
            email_col = cmap.get("email", "email")
            email = row.get(email_col, "").strip().lower()
            if not email or "@" not in email:
                skipped += 1
                errors.append(f"Row {row_num}: invalid or missing email")
                continue

            domain_col = cmap.get("company_domain", "company_domain")
            domain = row.get(domain_col, "").strip().lower()
            if not domain:
                # Try to extract from email
                domain = email.split("@")[1] if "@" in email else ""
            if not domain:
                skipped += 1
                errors.append(f"Row {row_num}: no company_domain for {email}")
                continue

            contact = Contact(
                email=email,
                client_id=client_id,
                company_domain=domain,
                first_name=row.get(cmap.get("first_name", "first_name"), "").strip() or None,
                last_name=row.get(cmap.get("last_name", "last_name"), "").strip() or None,
                last_known_title=row.get(cmap.get("title", "last_known_title"), "").strip() or None,
                last_known_company=row.get(
                    cmap.get("company", "last_known_company"), ""
                ).strip() or None,
                disposition_status=DispositionStatus.FRESH,
                data_enriched_at=datetime.now(timezone.utc),
                source_system="csv",
                source_id=str(Path(reader_source.name).name) if hasattr(reader_source, "name") else "csv_string",
            )
            contacts.append(contact)

        inserted = await self.db.bulk_create_contacts(contacts)
        duplicates = len(contacts) - inserted

        return ImportResult(
            total_rows=len(contacts) + skipped,
            imported=inserted,
            duplicates=duplicates,
            skipped=skipped,
            errors=errors,
        )


class ImportResult:
    """Result of an import operation."""

    def __init__(
        self,
        total_rows: int,
        imported: int,
        duplicates: int,
        skipped: int,
        errors: list[str],
    ):
        self.total_rows = total_rows
        self.imported = imported
        self.duplicates = duplicates
        self.skipped = skipped
        self.errors = errors

    def __repr__(self) -> str:
        return (
            f"ImportResult(total={self.total_rows}, imported={self.imported}, "
            f"duplicates={self.duplicates}, skipped={self.skipped}, errors={len(self.errors)})"
        )
