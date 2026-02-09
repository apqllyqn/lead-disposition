"""Tests for Pydantic models and enums."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from lead_disposition.core.models import (
    CampaignFillRequest,
    CampaignFillResult,
    Channel,
    Company,
    CompanyStatus,
    Contact,
    DispositionStatus,
    OwnershipChangeReason,
    TAMHealth,
    TAMSnapshot,
)


class TestDispositionStatusEnum:
    def test_all_14_statuses(self):
        assert len(DispositionStatus) == 14

    def test_fresh_value(self):
        assert DispositionStatus.FRESH.value == "fresh"

    def test_hard_no_value(self):
        assert DispositionStatus.REPLIED_HARD_NO.value == "replied_hard_no"

    def test_from_string(self):
        assert DispositionStatus("in_sequence") == DispositionStatus.IN_SEQUENCE


class TestCompanyStatusEnum:
    def test_all_5_statuses(self):
        assert len(CompanyStatus) == 5

    def test_values(self):
        expected = {"fresh", "active", "cooling", "suppressed", "customer"}
        assert {s.value for s in CompanyStatus} == expected


class TestChannelEnum:
    def test_all_3_channels(self):
        assert len(Channel) == 3
        assert {c.value for c in Channel} == {"email", "linkedin", "phone"}


class TestContact:
    def test_default_values(self):
        c = Contact(email="a@b.com", client_id="c1", company_domain="b.com")
        assert c.disposition_status == DispositionStatus.FRESH
        assert c.email_suppressed is False
        assert c.linkedin_suppressed is False
        assert c.phone_suppressed is False
        assert c.sequence_count == 0

    def test_all_fields(self):
        c = Contact(
            email="john@acme.com",
            client_id="client_1",
            company_domain="acme.com",
            first_name="John",
            last_name="Doe",
            last_known_title="VP Sales",
            last_known_company="Acme Inc",
            disposition_status=DispositionStatus.IN_SEQUENCE,
            sequence_count=2,
        )
        assert c.email == "john@acme.com"
        assert c.disposition_status == DispositionStatus.IN_SEQUENCE
        assert c.sequence_count == 2


class TestCompany:
    def test_defaults(self):
        co = Company(domain="acme.com")
        assert co.company_status == CompanyStatus.FRESH
        assert co.company_suppressed is False
        assert co.is_customer is False
        assert co.contacts_total == 0

    def test_customer_fields(self):
        co = Company(
            domain="acme.com",
            is_customer=True,
            customer_since=datetime.now(timezone.utc),
            company_status=CompanyStatus.CUSTOMER,
        )
        assert co.is_customer is True
        assert co.company_status == CompanyStatus.CUSTOMER


class TestTAMHealth:
    def test_defaults(self):
        h = TAMHealth()
        assert h.health_status == "healthy"
        assert h.burn_rate_weekly == 0.0
        assert h.exhaustion_eta_weeks is None

    def test_computed(self):
        h = TAMHealth(
            total_universe=1000,
            available_now=100,
            burn_rate_weekly=25.0,
            exhaustion_eta_weeks=4.0,
            health_status="critical",
        )
        assert h.total_universe == 1000
        assert h.health_status == "critical"


class TestCampaignFillRequest:
    def test_defaults(self):
        r = CampaignFillRequest(
            campaign_id="camp_1",
            client_id="client_1",
            volume=500,
        )
        assert r.channel == Channel.EMAIL
        assert r.fresh_ratio is None
        assert r.reserve_override is False
        assert r.title_keywords == []

    def test_with_filters(self):
        r = CampaignFillRequest(
            campaign_id="camp_1",
            client_id="client_1",
            volume=200,
            title_keywords=["VP", "Director"],
            fresh_ratio=0.5,
            max_per_company=2,
        )
        assert r.fresh_ratio == 0.5
        assert len(r.title_keywords) == 2


class TestCampaignFillResult:
    def test_structure(self):
        r = CampaignFillResult(
            campaign_id="camp_1",
            client_id="client_1",
            total_requested=100,
            total_assigned=95,
            fresh_count=70,
            retouch_count=25,
            companies_touched=50,
            contacts=[],
            warnings=["Insufficient fresh leads"],
        )
        assert r.total_assigned == 95
        assert len(r.warnings) == 1
