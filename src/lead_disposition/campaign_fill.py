"""Campaign fill engine - selects eligible contacts respecting all disposition rules."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database
from lead_disposition.core.models import (
    CampaignFillRequest,
    CampaignFillResult,
    CompanyStatus,
    Contact,
    DispositionStatus,
)
from lead_disposition.state_machine import StateMachine


class CampaignFillEngine:
    """Fills campaigns with eligible contacts while respecting disposition rules."""

    def __init__(self, db: Database, settings: Settings | None = None):
        self.db = db
        self.settings = settings or Settings()
        self.sm = StateMachine(db, self.settings)

    async def fill(self, request: CampaignFillRequest) -> CampaignFillResult:
        """Select and assign contacts to a campaign.

        Steps:
        1. Query eligible contacts (respects suppression, cooldown, ownership, freshness)
        2. Apply velocity throttling (fresh/retouch mix ratio)
        3. Apply company contact cap
        4. Assign contacts (transition to in_sequence, update company, claim ownership)
        5. Return structured result
        """
        ratio = request.fresh_ratio if request.fresh_ratio is not None else self.settings.fresh_retouch_ratio
        max_per_co = request.max_per_company or self.settings.max_contacts_per_company
        channel = request.channel.value

        warnings: list[str] = []

        # Query fresh contacts
        fresh_target = int(request.volume * ratio)
        fresh_contacts = await self.db.query_eligible_contacts(
            client_id=request.client_id,
            channel=channel,
            title_keywords=request.title_keywords or None,
            limit=fresh_target * 2,  # over-fetch for company cap filtering
            status_filter=["fresh"],
        )

        # Query retouch contacts
        retouch_target = request.volume - fresh_target
        retouch_contacts = await self.db.query_eligible_contacts(
            client_id=request.client_id,
            channel=channel,
            title_keywords=request.title_keywords or None,
            limit=retouch_target * 2,
            status_filter=["retouch_eligible"],
        )

        if len(fresh_contacts) < fresh_target:
            warnings.append(
                f"Insufficient fresh leads: requested {fresh_target}, found {len(fresh_contacts)}"
            )

        # Apply company contact cap and build final list
        selected_fresh = self._apply_company_cap(fresh_contacts, max_per_co, {})
        company_counts = self._count_by_company(selected_fresh)
        selected_retouch = self._apply_company_cap(retouch_contacts, max_per_co, company_counts)

        # Trim to volume
        all_selected: list[Contact] = []
        all_selected.extend(selected_fresh[:fresh_target])
        remaining = request.volume - len(all_selected)
        all_selected.extend(selected_retouch[:remaining])

        # If still short, backfill with remaining fresh
        if len(all_selected) < request.volume:
            backfill_count = request.volume - len(all_selected)
            backfill = selected_fresh[fresh_target : fresh_target + backfill_count]
            all_selected.extend(backfill)

        if len(all_selected) < request.volume:
            warnings.append(
                f"Volume shortfall: requested {request.volume}, assigned {len(all_selected)}"
            )

        # Assign each selected contact
        companies_touched: set[str] = set()
        fresh_count = 0
        retouch_count = 0

        for contact in all_selected:
            await self._assign_contact(contact, request, channel)
            companies_touched.add(contact.company_domain)
            if contact.disposition_status == DispositionStatus.FRESH:
                fresh_count += 1
            else:
                retouch_count += 1

        return CampaignFillResult(
            campaign_id=request.campaign_id,
            client_id=request.client_id,
            total_requested=request.volume,
            total_assigned=len(all_selected),
            fresh_count=fresh_count,
            retouch_count=retouch_count,
            companies_touched=len(companies_touched),
            contacts=all_selected,
            warnings=warnings,
        )

    async def _assign_contact(
        self, contact: Contact, request: CampaignFillRequest, channel: str
    ) -> None:
        """Transition contact to in_sequence, update company, log assignment."""
        now = datetime.now(timezone.utc)

        # Transition state
        await self.sm.transition(
            email=contact.email,
            client_id=contact.client_id,
            new_status=DispositionStatus.IN_SEQUENCE,
            reason=f"assigned_to_campaign:{request.campaign_id}",
            triggered_by="campaign_fill",
            campaign_id=request.campaign_id,
        )

        # Update channel-specific last contacted
        channel_field = f"{channel}_last_contacted"
        await self.db.update_contact_fields(
            contact.email, contact.client_id,
            **{channel_field: now, "sequence_count": contact.sequence_count + 1},
        )

        # Insert assignment record
        await self.db.insert_assignment(
            contact_email=contact.email,
            contact_client_id=contact.client_id,
            campaign_id=request.campaign_id,
            client_id=request.client_id,
            channel=channel,
        )

        # Claim company ownership if unowned
        company = await self.db.get_company(contact.company_domain)
        if company and company.client_owner_id is None:
            ownership_expiry = now + timedelta(
                days=self.settings.ownership_duration_months * 30
            )
            await self.db.update_company_fields(
                contact.company_domain,
                client_owner_id=request.client_id,
                client_owned_at=now,
                ownership_expires_at=ownership_expiry,
            )
            await self.db.insert_ownership_change(
                company_domain=contact.company_domain,
                previous_owner_id=None,
                new_owner_id=request.client_id,
                reason="first_claim",
            )

    def _apply_company_cap(
        self,
        contacts: list[Contact],
        max_per_company: int,
        existing_counts: dict[str, int],
    ) -> list[Contact]:
        """Filter contacts to respect the per-company cap."""
        counts = dict(existing_counts)
        result: list[Contact] = []
        for c in contacts:
            current = counts.get(c.company_domain, 0)
            if current < max_per_company:
                result.append(c)
                counts[c.company_domain] = current + 1
        return result

    def _count_by_company(self, contacts: list[Contact]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for c in contacts:
            counts[c.company_domain] = counts.get(c.company_domain, 0) + 1
        return counts
