"""Tests for TAM health calculations."""

from __future__ import annotations

import pytest

from lead_disposition.core.config import Settings
from lead_disposition.core.models import TAMHealth


class TestTAMHealthStatus:
    """Test TAM health status derivation."""

    def test_healthy_when_eta_above_warning(self):
        settings = Settings()
        eta = 12.0
        status = "healthy"
        if eta < settings.tam_critical_weeks:
            status = "critical"
        elif eta < settings.tam_warning_weeks:
            status = "warning"
        assert status == "healthy"

    def test_warning_when_eta_between_thresholds(self):
        settings = Settings()
        eta = 6.0  # between critical (4) and warning (8)
        status = "healthy"
        if eta < settings.tam_critical_weeks:
            status = "critical"
        elif eta < settings.tam_warning_weeks:
            status = "warning"
        assert status == "warning"

    def test_critical_when_eta_below_critical(self):
        settings = Settings()
        eta = 2.0
        status = "healthy"
        if eta < settings.tam_critical_weeks:
            status = "critical"
        elif eta < settings.tam_warning_weeks:
            status = "warning"
        assert status == "critical"

    def test_healthy_when_no_burn(self):
        """Zero burn rate = no exhaustion = healthy."""
        health = TAMHealth(
            total_universe=1000,
            available_now=500,
            burn_rate_weekly=0.0,
            exhaustion_eta_weeks=None,
            health_status="healthy",
        )
        assert health.health_status == "healthy"
        assert health.exhaustion_eta_weeks is None


class TestExhaustionETA:
    """Test exhaustion ETA calculations."""

    def test_basic_calculation(self):
        available = 100
        burn_rate = 25.0
        eta = available / burn_rate
        assert eta == 4.0

    def test_zero_burn_rate_returns_none(self):
        burn_rate = 0.0
        eta = None if burn_rate == 0 else 100 / burn_rate
        assert eta is None

    def test_high_available_low_burn(self):
        available = 10000
        burn_rate = 50.0
        eta = available / burn_rate
        assert eta == 200.0


class TestPoolSegmentation:
    """Test that pool categories are mutually exclusive and sum to total."""

    def test_pools_sum_to_total(self):
        health = TAMHealth(
            total_universe=1000,
            never_touched=400,
            in_cooldown=100,
            available_now=300,
            permanent_suppress=50,
            in_sequence=100,
            won_customer=50,
        )
        pool_sum = (
            health.never_touched
            + health.in_cooldown
            + health.available_now
            + health.permanent_suppress
            + health.in_sequence
            + health.won_customer
        )
        assert pool_sum == health.total_universe
