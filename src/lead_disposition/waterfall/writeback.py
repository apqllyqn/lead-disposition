"""Write-back logic - persist externally-sourced leads to the internal database."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from lead_disposition.core.models import Contact, DispositionStatus
from lead_disposition.providers.base import ExternalLead

logger = logging.getLogger(__name__)


class WriteBackResult(BaseModel):
    """Result of writing external leads back to internal database."""

    total_processed: int = 0
    new_inserted: int = 0
    duplicates_skipped: int = 0
    invalid_skipped: int = 0
    errors: list[str] = Field(default_factory=list)


def external_lead_to_contact(lead: ExternalLead, client_id: str) -> Contact | None:
    """Map an ExternalLead to an internal Contact model.

    Returns None if the lead is missing required fields.
    """
    if not lead.email or "@" not in lead.email:
        return None

    # Derive company_domain from email if not provided
    company_domain = lead.company_domain
    if not company_domain:
        company_domain = lead.email.split("@")[1]

    return Contact(
        email=lead.email.lower().strip(),
        client_id=client_id,
        company_domain=company_domain.lower().strip(),
        first_name=lead.first_name,
        last_name=lead.last_name,
        last_known_title=lead.title,
        last_known_company=lead.company_name,
        disposition_status=DispositionStatus.FRESH,
        data_enriched_at=datetime.now(timezone.utc),
        source_system=lead.source_provider,
        source_id=lead.source_id,
    )


async def write_back_leads(
    db,
    leads: list[ExternalLead],
    client_id: str,
) -> WriteBackResult:
    """Persist externally-sourced leads to the internal database.

    - Maps ExternalLead fields to Contact model
    - Sets source_system to the provider name
    - Sets data_enriched_at to NOW()
    - Sets disposition_status to FRESH
    - Skips duplicates (existing email+client_id pairs)
    - Auto-creates company records
    """
    result = WriteBackResult(total_processed=len(leads))
    contacts: list[Contact] = []

    for lead in leads:
        contact = external_lead_to_contact(lead, client_id)
        if contact is None:
            result.invalid_skipped += 1
            continue
        contacts.append(contact)

    if not contacts:
        return result

    try:
        inserted = await db.bulk_create_contacts(contacts)
        result.new_inserted = inserted
        result.duplicates_skipped = len(contacts) - inserted
    except Exception as e:
        logger.error("Write-back failed: %s", e)
        result.errors.append(f"Bulk insert failed: {e}")

    return result
