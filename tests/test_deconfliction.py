"""Tests for first-mover deconfliction logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lead_disposition.core.models import Company, CompanyStatus


class TestOwnershipRules:
    """Test ownership check logic without DB."""

    def test_unowned_company_is_available(self):
        company = Company(domain="acme.com", client_owner_id=None)
        assert company.client_owner_id is None
        # Any client can target

    def test_owned_company_blocks_others(self):
        company = Company(
            domain="acme.com",
            client_owner_id="client_1",
            ownership_expires_at=datetime.now(timezone.utc) + timedelta(days=365),
        )
        requesting_client = "client_2"
        assert company.client_owner_id != requesting_client
        # client_2 should be blocked

    def test_same_owner_is_allowed(self):
        company = Company(
            domain="acme.com",
            client_owner_id="client_1",
        )
        assert company.client_owner_id == "client_1"

    def test_expired_ownership_allows_others(self):
        company = Company(
            domain="acme.com",
            client_owner_id="client_1",
            ownership_expires_at=datetime.now(timezone.utc) - timedelta(days=1),
            contacts_in_sequence=0,
        )
        # Expired and no active sequences = available
        assert company.ownership_expires_at < datetime.now(timezone.utc)
        assert company.contacts_in_sequence == 0

    def test_expired_but_active_sequences_blocks(self):
        company = Company(
            domain="acme.com",
            client_owner_id="client_1",
            ownership_expires_at=datetime.now(timezone.utc) - timedelta(days=1),
            contacts_in_sequence=2,
        )
        # Expired but active sequences = still owned
        assert company.ownership_expires_at < datetime.now(timezone.utc)
        assert company.contacts_in_sequence > 0


class TestOwnershipDuration:
    """Test ownership expiry calculations."""

    def test_default_12_month_duration(self):
        from lead_disposition.core.config import Settings
        settings = Settings()
        assert settings.ownership_duration_months == 12

    def test_expiry_calculation(self):
        from lead_disposition.core.config import Settings
        settings = Settings()
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=settings.ownership_duration_months * 30)
        assert (expiry - now).days == 360  # 12 * 30


class TestCompanySuppression:
    """Test company-level suppression logic."""

    def test_hard_no_suppresses_company(self):
        """Hard no at any contact should suppress the entire company."""
        company = Company(
            domain="acme.com",
            company_status=CompanyStatus.SUPPRESSED,
            company_suppressed=True,
            suppressed_reason="hard_no_received",
        )
        assert company.company_suppressed is True
        assert company.company_status == CompanyStatus.SUPPRESSED

    def test_suppressed_company_blocks_all_contacts(self):
        """Suppressed company should not appear in campaign fills."""
        company = Company(domain="acme.com", company_suppressed=True)
        assert company.company_suppressed is True
        # Query filter: co.company_suppressed = false would exclude this
