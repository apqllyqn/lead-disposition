"""Cross-client deconfliction - first-mover company ownership model."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database


class Deconfliction:
    """Manages first-mover company ownership for cross-client isolation."""

    def __init__(self, db: Database, settings: Settings | None = None):
        self.db = db
        self.settings = settings or Settings()

    async def check_ownership(self, domain: str, client_id: str) -> bool:
        """Check if a client can target a company.

        Returns True if:
        - Company has no owner
        - Company is owned by this client
        - Ownership has expired and no active sequences
        """
        company = await self.db.get_company(domain)
        if company is None:
            return True
        if company.client_owner_id is None:
            return True
        if company.client_owner_id == client_id:
            return True
        # Check expiry
        if company.ownership_expires_at and company.ownership_expires_at <= datetime.now(
            timezone.utc
        ):
            if company.contacts_in_sequence == 0:
                return True
        return False

    async def claim_ownership(self, domain: str, client_id: str) -> bool:
        """Claim ownership of a company if unowned. Returns True if claimed."""
        company = await self.db.get_company(domain)
        if company is None:
            return False
        if company.client_owner_id is not None and company.client_owner_id != client_id:
            return False

        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=self.settings.ownership_duration_months * 30)

        await self.db.update_company_fields(
            domain,
            client_owner_id=client_id,
            client_owned_at=now,
            ownership_expires_at=expiry,
        )
        await self.db.insert_ownership_change(
            company_domain=domain,
            previous_owner_id=company.client_owner_id,
            new_owner_id=client_id,
            reason="first_claim",
        )
        return True

    async def release_ownership(self, domain: str) -> bool:
        """Release ownership of a company (admin action)."""
        company = await self.db.get_company(domain)
        if company is None or company.client_owner_id is None:
            return False

        previous_owner = company.client_owner_id
        await self.db.update_company_fields(
            domain,
            client_owner_id=None,
            client_owned_at=None,
            ownership_expires_at=None,
        )
        await self.db.insert_ownership_change(
            company_domain=domain,
            previous_owner_id=previous_owner,
            new_owner_id=None,
            reason="manual_release",
        )
        return True

    async def transfer_ownership(self, domain: str, new_client_id: str) -> bool:
        """Transfer ownership to a different client (admin action)."""
        company = await self.db.get_company(domain)
        if company is None:
            return False

        previous_owner = company.client_owner_id
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=self.settings.ownership_duration_months * 30)

        await self.db.update_company_fields(
            domain,
            client_owner_id=new_client_id,
            client_owned_at=now,
            ownership_expires_at=expiry,
        )
        await self.db.insert_ownership_change(
            company_domain=domain,
            previous_owner_id=previous_owner,
            new_owner_id=new_client_id,
            reason="admin_transfer",
        )
        return True

    async def process_expired_ownerships(self) -> int:
        """Release ownership for expired companies with no active sequences."""
        expired = await self.db.get_expired_ownerships()
        count = 0
        for company in expired:
            previous_owner = company.client_owner_id
            await self.db.update_company_fields(
                company.domain,
                client_owner_id=None,
                client_owned_at=None,
                ownership_expires_at=None,
            )
            await self.db.insert_ownership_change(
                company_domain=company.domain,
                previous_owner_id=previous_owner,
                new_owner_id=None,
                reason="expired",
            )
            count += 1
        return count
