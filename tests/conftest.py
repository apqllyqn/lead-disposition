"""Shared test fixtures."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lead_disposition.core.models import Contact, DispositionStatus


@pytest.fixture
def make_contact():
    """Factory fixture for creating test contacts."""

    def _make(
        email: str = "test@example.com",
        client_id: str = "client_1",
        domain: str = "example.com",
        status: DispositionStatus = DispositionStatus.FRESH,
        **kwargs,
    ) -> Contact:
        return Contact(
            email=email,
            client_id=client_id,
            company_domain=domain,
            first_name=kwargs.get("first_name", "Test"),
            last_name=kwargs.get("last_name", "User"),
            last_known_title=kwargs.get("title", "VP Sales"),
            last_known_company=kwargs.get("company", "Example Inc"),
            disposition_status=status,
            data_enriched_at=kwargs.get("enriched_at", datetime.now(timezone.utc)),
            **{k: v for k, v in kwargs.items() if k not in ("first_name", "last_name", "title", "company", "enriched_at")},
        )

    return _make
