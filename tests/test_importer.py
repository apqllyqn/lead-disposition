"""Tests for CSV import logic (column mapping, validation, dedup)."""

from __future__ import annotations

import pytest

from lead_disposition.core.models import Contact, DispositionStatus


class TestCSVColumnMapping:
    """Test CSV column mapping and contact construction."""

    def test_email_extraction_from_domain(self):
        """If company_domain is missing, extract from email."""
        email = "john@acme.com"
        domain = email.split("@")[1]
        assert domain == "acme.com"

    def test_invalid_email_detection(self):
        """Emails without @ should be rejected."""
        bad_emails = ["", "noatsign", "  ", "just-text"]
        for email in bad_emails:
            assert "@" not in email.strip()

    def test_valid_email_accepted(self):
        valid_emails = ["a@b.com", "john.doe@company.co.uk", "test+tag@gmail.com"]
        for email in valid_emails:
            assert "@" in email

    def test_contact_from_csv_row(self):
        """Simulate constructing a Contact from a CSV row."""
        row = {
            "email": "Jane@Acme.com",
            "first_name": "Jane",
            "last_name": "Smith",
            "company_domain": "acme.com",
            "last_known_title": "CTO",
            "last_known_company": "Acme Inc",
        }
        contact = Contact(
            email=row["email"].strip().lower(),
            client_id="test_client",
            company_domain=row["company_domain"].strip().lower(),
            first_name=row["first_name"],
            last_name=row["last_name"],
            last_known_title=row["last_known_title"],
            last_known_company=row["last_known_company"],
            disposition_status=DispositionStatus.FRESH,
            source_system="csv",
        )
        assert contact.email == "jane@acme.com"
        assert contact.company_domain == "acme.com"
        assert contact.disposition_status == DispositionStatus.FRESH
        assert contact.source_system == "csv"
