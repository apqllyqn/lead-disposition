"""Pydantic models for the disposition system."""

from __future__ import annotations

import enum
from datetime import date, datetime

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class DispositionStatus(str, enum.Enum):
    FRESH = "fresh"
    IN_SEQUENCE = "in_sequence"
    COMPLETED_NO_RESPONSE = "completed_no_response"
    REPLIED_POSITIVE = "replied_positive"
    REPLIED_NEUTRAL = "replied_neutral"
    REPLIED_NEGATIVE = "replied_negative"
    REPLIED_HARD_NO = "replied_hard_no"
    BOUNCED = "bounced"
    UNSUBSCRIBED = "unsubscribed"
    RETOUCH_ELIGIBLE = "retouch_eligible"
    STALE_DATA = "stale_data"
    JOB_CHANGE_DETECTED = "job_change_detected"
    WON_CUSTOMER = "won_customer"
    LOST_CLOSED = "lost_closed"


class CompanyStatus(str, enum.Enum):
    FRESH = "fresh"
    ACTIVE = "active"
    COOLING = "cooling"
    SUPPRESSED = "suppressed"
    CUSTOMER = "customer"


class Channel(str, enum.Enum):
    EMAIL = "email"
    LINKEDIN = "linkedin"
    PHONE = "phone"


class OwnershipChangeReason(str, enum.Enum):
    FIRST_CLAIM = "first_claim"
    EXPIRED = "expired"
    MANUAL_RELEASE = "manual_release"
    ADMIN_TRANSFER = "admin_transfer"


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

class Contact(BaseModel):
    email: str
    client_id: str
    company_domain: str
    first_name: str | None = None
    last_name: str | None = None
    last_known_title: str | None = None
    last_known_company: str | None = None
    disposition_status: DispositionStatus = DispositionStatus.FRESH
    disposition_updated_at: datetime | None = None
    email_last_contacted: datetime | None = None
    linkedin_last_contacted: datetime | None = None
    phone_last_contacted: datetime | None = None
    email_cooldown_until: datetime | None = None
    linkedin_cooldown_until: datetime | None = None
    phone_cooldown_until: datetime | None = None
    email_suppressed: bool = False
    linkedin_suppressed: bool = False
    phone_suppressed: bool = False
    data_enriched_at: datetime | None = None
    sequence_count: int = 0
    source_system: str | None = None
    source_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class Company(BaseModel):
    domain: str
    name: str | None = None
    company_status: CompanyStatus = CompanyStatus.FRESH
    company_suppressed: bool = False
    suppressed_reason: str | None = None
    suppressed_at: datetime | None = None
    contacts_total: int = 0
    contacts_in_sequence: int = 0
    contacts_touched: int = 0
    last_contact_date: datetime | None = None
    company_cooldown_until: datetime | None = None
    is_customer: bool = False
    customer_since: datetime | None = None
    client_owner_id: str | None = None
    client_owned_at: datetime | None = None
    ownership_expires_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DispositionHistory(BaseModel):
    id: str | None = None
    contact_email: str
    contact_client_id: str
    previous_status: DispositionStatus | None = None
    new_status: DispositionStatus
    transition_reason: str | None = None
    triggered_by: str = "system"
    campaign_id: str | None = None
    metadata: dict | None = None
    created_at: datetime | None = None


class CampaignAssignment(BaseModel):
    id: str | None = None
    contact_email: str
    contact_client_id: str
    campaign_id: str
    client_id: str
    channel: Channel = Channel.EMAIL
    assigned_at: datetime | None = None
    completed_at: datetime | None = None
    outcome: str | None = None
    created_at: datetime | None = None


class TAMSnapshot(BaseModel):
    id: str | None = None
    snapshot_date: date
    client_id: str | None = None
    total_universe: int = 0
    never_touched: int = 0
    in_cooldown: int = 0
    available_now: int = 0
    permanent_suppress: int = 0
    in_sequence: int = 0
    won_customer: int = 0
    burn_rate_weekly: float | None = None
    exhaustion_eta_weeks: float | None = None
    created_at: datetime | None = None


class TAMHealth(BaseModel):
    """Computed TAM health metrics (not persisted directly)."""

    total_universe: int = 0
    never_touched: int = 0
    in_cooldown: int = 0
    available_now: int = 0
    permanent_suppress: int = 0
    in_sequence: int = 0
    won_customer: int = 0
    burn_rate_weekly: float = 0.0
    exhaustion_eta_weeks: float | None = None
    health_status: str = "healthy"  # healthy, warning, critical


class CampaignFillRequest(BaseModel):
    """Input for the campaign fill engine."""

    campaign_id: str
    client_id: str
    channel: Channel = Channel.EMAIL
    volume: int
    title_keywords: list[str] = Field(default_factory=list)
    industry_keywords: list[str] = Field(default_factory=list)
    fresh_ratio: float | None = None  # Override default 0.7
    reserve_override: bool = False  # Access reserved pool
    max_per_company: int | None = None  # Override default 3


class CampaignFillResult(BaseModel):
    """Output from the campaign fill engine."""

    campaign_id: str
    client_id: str
    total_requested: int
    total_assigned: int
    fresh_count: int
    retouch_count: int
    companies_touched: int
    contacts: list[Contact]
    warnings: list[str] = Field(default_factory=list)
