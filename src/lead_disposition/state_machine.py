"""Disposition state machine with transition validation, cooldowns, and suppression."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lead_disposition.core.config import Settings
from lead_disposition.core.database import Database
from lead_disposition.core.models import CompanyStatus, DispositionStatus


# ---------------------------------------------------------------------------
# Legal transition map
# ---------------------------------------------------------------------------

TRANSITIONS: dict[DispositionStatus, set[DispositionStatus]] = {
    DispositionStatus.FRESH: {
        DispositionStatus.IN_SEQUENCE,
        DispositionStatus.STALE_DATA,
        DispositionStatus.JOB_CHANGE_DETECTED,
    },
    DispositionStatus.IN_SEQUENCE: {
        DispositionStatus.COMPLETED_NO_RESPONSE,
        DispositionStatus.REPLIED_POSITIVE,
        DispositionStatus.REPLIED_NEUTRAL,
        DispositionStatus.REPLIED_NEGATIVE,
        DispositionStatus.REPLIED_HARD_NO,
        DispositionStatus.BOUNCED,
        DispositionStatus.UNSUBSCRIBED,
    },
    DispositionStatus.COMPLETED_NO_RESPONSE: {
        DispositionStatus.RETOUCH_ELIGIBLE,
        DispositionStatus.STALE_DATA,
        DispositionStatus.JOB_CHANGE_DETECTED,
    },
    DispositionStatus.REPLIED_POSITIVE: {
        DispositionStatus.WON_CUSTOMER,
        DispositionStatus.LOST_CLOSED,
    },
    DispositionStatus.REPLIED_NEUTRAL: {
        DispositionStatus.RETOUCH_ELIGIBLE,
        DispositionStatus.STALE_DATA,
    },
    DispositionStatus.REPLIED_NEGATIVE: {
        DispositionStatus.RETOUCH_ELIGIBLE,
        DispositionStatus.STALE_DATA,
    },
    DispositionStatus.REPLIED_HARD_NO: set(),  # terminal
    DispositionStatus.BOUNCED: set(),  # terminal
    DispositionStatus.UNSUBSCRIBED: set(),  # terminal
    DispositionStatus.RETOUCH_ELIGIBLE: {
        DispositionStatus.IN_SEQUENCE,
        DispositionStatus.STALE_DATA,
        DispositionStatus.JOB_CHANGE_DETECTED,
    },
    DispositionStatus.STALE_DATA: {
        DispositionStatus.FRESH,
        DispositionStatus.RETOUCH_ELIGIBLE,
    },
    DispositionStatus.JOB_CHANGE_DETECTED: {
        DispositionStatus.FRESH,
    },
    DispositionStatus.WON_CUSTOMER: set(),  # terminal
    DispositionStatus.LOST_CLOSED: {
        DispositionStatus.RETOUCH_ELIGIBLE,
    },
}

# Terminal states that allow no transitions out
TERMINAL_STATES = {
    DispositionStatus.REPLIED_HARD_NO,
    DispositionStatus.BOUNCED,
    DispositionStatus.UNSUBSCRIBED,
    DispositionStatus.WON_CUSTOMER,
}


class TransitionError(Exception):
    """Raised when an illegal state transition is attempted."""


class StateMachine:
    """Manages contact disposition transitions with cooldown and suppression logic."""

    def __init__(self, db: Database, settings: Settings | None = None):
        self.db = db
        self.settings = settings or Settings()

    async def transition(
        self,
        email: str,
        client_id: str,
        new_status: DispositionStatus,
        reason: str | None = None,
        triggered_by: str = "system",
        campaign_id: str | None = None,
    ) -> None:
        """Transition a contact to a new disposition status.

        Validates the transition, sets cooldowns, applies suppression,
        updates company state, and logs history.
        """
        contact = await self.db.get_contact(email, client_id)
        if contact is None:
            raise ValueError(f"Contact not found: {email} / {client_id}")

        current = contact.disposition_status
        self._validate_transition(current, new_status)

        now = datetime.now(timezone.utc)

        # Build update fields
        updates: dict = {
            "disposition_status": new_status,
            "disposition_updated_at": now,
        }

        # Set cooldowns based on the transition
        cooldown = self._get_cooldown(new_status)
        if cooldown:
            updates["email_cooldown_until"] = now + cooldown

        # Apply suppression
        suppression = self._get_suppression(new_status)
        updates.update(suppression)

        # Update contact
        await self.db.update_contact_fields(email, client_id, **updates)

        # Log history
        await self.db.insert_history(
            contact_email=email,
            contact_client_id=client_id,
            previous_status=current,
            new_status=new_status,
            reason=reason,
            triggered_by=triggered_by,
            campaign_id=campaign_id,
        )

        # Update company state
        await self._update_company_state(contact.company_domain, current, new_status, now)

        # Handle hard_no company-wide suppression
        if new_status == DispositionStatus.REPLIED_HARD_NO:
            await self._suppress_company(contact.company_domain, now)

    def _validate_transition(
        self, current: DispositionStatus, target: DispositionStatus
    ) -> None:
        """Check if the transition is legal."""
        if current == target:
            return  # no-op is always allowed
        allowed = TRANSITIONS.get(current, set())
        if target not in allowed:
            raise TransitionError(
                f"Illegal transition: {current.value} -> {target.value}. "
                f"Allowed from {current.value}: {[s.value for s in allowed]}"
            )

    def _get_cooldown(self, new_status: DispositionStatus) -> timedelta | None:
        """Return the cooldown period for a given transition target."""
        s = self.settings
        cooldowns = {
            DispositionStatus.COMPLETED_NO_RESPONSE: timedelta(days=s.cooldown_no_response_days),
            DispositionStatus.REPLIED_NEUTRAL: timedelta(days=s.cooldown_neutral_reply_days),
            DispositionStatus.REPLIED_NEGATIVE: timedelta(days=s.cooldown_negative_reply_days),
            DispositionStatus.LOST_CLOSED: timedelta(days=s.cooldown_lost_closed_days),
        }
        return cooldowns.get(new_status)

    def _get_suppression(self, new_status: DispositionStatus) -> dict:
        """Return suppression flags for a given transition target."""
        if new_status == DispositionStatus.REPLIED_HARD_NO:
            return {
                "email_suppressed": True,
                "linkedin_suppressed": True,
                "phone_suppressed": True,
            }
        elif new_status in (DispositionStatus.BOUNCED, DispositionStatus.UNSUBSCRIBED):
            return {"email_suppressed": True}
        return {}

    async def _update_company_state(
        self,
        domain: str,
        old_status: DispositionStatus,
        new_status: DispositionStatus,
        now: datetime,
    ) -> None:
        """Derive and update company status based on contact transition."""
        updates: dict = {}

        # Contact entering a sequence
        if new_status == DispositionStatus.IN_SEQUENCE:
            company = await self.db.get_company(domain)
            if company:
                updates["contacts_in_sequence"] = company.contacts_in_sequence + 1
                updates["contacts_touched"] = company.contacts_touched + 1
                updates["company_status"] = CompanyStatus.ACTIVE
                updates["last_contact_date"] = now

        # Contact leaving a sequence
        elif old_status == DispositionStatus.IN_SEQUENCE:
            company = await self.db.get_company(domain)
            if company:
                new_count = max(0, company.contacts_in_sequence - 1)
                updates["contacts_in_sequence"] = new_count
                if new_count == 0 and company.contacts_touched > 0:
                    updates["company_status"] = CompanyStatus.COOLING

        # Won customer
        if new_status == DispositionStatus.WON_CUSTOMER:
            updates["company_status"] = CompanyStatus.CUSTOMER
            updates["is_customer"] = True
            updates["customer_since"] = now

        # Hard no -> company suppressed
        if new_status == DispositionStatus.REPLIED_HARD_NO:
            updates["company_status"] = CompanyStatus.SUPPRESSED
            updates["company_suppressed"] = True
            updates["suppressed_reason"] = "hard_no_received"
            updates["suppressed_at"] = now

        if updates:
            await self.db.update_company_fields(domain, **updates)

    async def _suppress_company(self, domain: str, now: datetime) -> None:
        """Suppress all contacts at a company (hard no cascade)."""
        contacts = await self.db.get_contacts_by_domain(domain)
        for c in contacts:
            if not c.email_suppressed:
                await self.db.update_contact_fields(
                    c.email,
                    c.client_id,
                    email_suppressed=True,
                )

    async def process_expired_cooldowns(self) -> int:
        """Transition contacts with expired cooldowns to retouch_eligible."""
        contacts = await self.db.get_expired_cooldowns()
        count = 0
        for c in contacts:
            try:
                await self.transition(
                    c.email,
                    c.client_id,
                    DispositionStatus.RETOUCH_ELIGIBLE,
                    reason="cooldown_expired",
                    triggered_by="system",
                )
                count += 1
            except TransitionError:
                pass
        return count

    async def process_stale_data(self, months: int | None = None) -> int:
        """Flag contacts with old enrichment data as stale."""
        m = months or self.settings.stale_data_months
        contacts = await self.db.get_stale_contacts(m)
        count = 0
        for c in contacts:
            try:
                await self.transition(
                    c.email,
                    c.client_id,
                    DispositionStatus.STALE_DATA,
                    reason=f"data_enriched_at older than {m} months",
                    triggered_by="system",
                )
                count += 1
            except TransitionError:
                pass
        return count
