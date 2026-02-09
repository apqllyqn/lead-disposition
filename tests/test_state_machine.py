"""Tests for the disposition state machine - transitions, cooldowns, suppression."""

from __future__ import annotations

import pytest

from lead_disposition.core.models import DispositionStatus
from lead_disposition.state_machine import TRANSITIONS, TERMINAL_STATES, TransitionError, StateMachine


# ---------------------------------------------------------------------------
# Transition validation (unit tests - no DB required)
# ---------------------------------------------------------------------------


class TestTransitionRules:
    """Test the state transition map is correct."""

    def test_fresh_can_go_to_in_sequence(self):
        assert DispositionStatus.IN_SEQUENCE in TRANSITIONS[DispositionStatus.FRESH]

    def test_fresh_can_go_to_stale(self):
        assert DispositionStatus.STALE_DATA in TRANSITIONS[DispositionStatus.FRESH]

    def test_fresh_can_go_to_job_change(self):
        assert DispositionStatus.JOB_CHANGE_DETECTED in TRANSITIONS[DispositionStatus.FRESH]

    def test_fresh_cannot_go_to_replied(self):
        assert DispositionStatus.REPLIED_POSITIVE not in TRANSITIONS[DispositionStatus.FRESH]

    def test_in_sequence_to_completed(self):
        assert DispositionStatus.COMPLETED_NO_RESPONSE in TRANSITIONS[DispositionStatus.IN_SEQUENCE]

    def test_in_sequence_to_all_reply_types(self):
        for reply in [
            DispositionStatus.REPLIED_POSITIVE,
            DispositionStatus.REPLIED_NEUTRAL,
            DispositionStatus.REPLIED_NEGATIVE,
            DispositionStatus.REPLIED_HARD_NO,
        ]:
            assert reply in TRANSITIONS[DispositionStatus.IN_SEQUENCE]

    def test_in_sequence_to_bounce(self):
        assert DispositionStatus.BOUNCED in TRANSITIONS[DispositionStatus.IN_SEQUENCE]

    def test_in_sequence_to_unsub(self):
        assert DispositionStatus.UNSUBSCRIBED in TRANSITIONS[DispositionStatus.IN_SEQUENCE]

    def test_completed_no_response_to_retouch(self):
        assert DispositionStatus.RETOUCH_ELIGIBLE in TRANSITIONS[DispositionStatus.COMPLETED_NO_RESPONSE]

    def test_replied_positive_to_won(self):
        assert DispositionStatus.WON_CUSTOMER in TRANSITIONS[DispositionStatus.REPLIED_POSITIVE]

    def test_replied_positive_to_lost(self):
        assert DispositionStatus.LOST_CLOSED in TRANSITIONS[DispositionStatus.REPLIED_POSITIVE]

    def test_lost_closed_to_retouch(self):
        assert DispositionStatus.RETOUCH_ELIGIBLE in TRANSITIONS[DispositionStatus.LOST_CLOSED]

    def test_retouch_to_in_sequence(self):
        assert DispositionStatus.IN_SEQUENCE in TRANSITIONS[DispositionStatus.RETOUCH_ELIGIBLE]

    def test_stale_data_to_fresh(self):
        assert DispositionStatus.FRESH in TRANSITIONS[DispositionStatus.STALE_DATA]

    def test_stale_data_to_retouch(self):
        assert DispositionStatus.RETOUCH_ELIGIBLE in TRANSITIONS[DispositionStatus.STALE_DATA]

    def test_job_change_to_fresh(self):
        assert DispositionStatus.FRESH in TRANSITIONS[DispositionStatus.JOB_CHANGE_DETECTED]


class TestTerminalStates:
    """Test that terminal states have no outbound transitions."""

    def test_hard_no_is_terminal(self):
        assert DispositionStatus.REPLIED_HARD_NO in TERMINAL_STATES
        assert len(TRANSITIONS[DispositionStatus.REPLIED_HARD_NO]) == 0

    def test_bounced_is_terminal(self):
        assert DispositionStatus.BOUNCED in TERMINAL_STATES
        assert len(TRANSITIONS[DispositionStatus.BOUNCED]) == 0

    def test_unsubscribed_is_terminal(self):
        assert DispositionStatus.UNSUBSCRIBED in TERMINAL_STATES
        assert len(TRANSITIONS[DispositionStatus.UNSUBSCRIBED]) == 0

    def test_won_customer_is_terminal(self):
        assert DispositionStatus.WON_CUSTOMER in TERMINAL_STATES
        assert len(TRANSITIONS[DispositionStatus.WON_CUSTOMER]) == 0


class TestTransitionValidation:
    """Test the _validate_transition method directly."""

    def setup_method(self):
        self.sm = StateMachine.__new__(StateMachine)

    def test_valid_transition_passes(self):
        # Should not raise
        self.sm._validate_transition(
            DispositionStatus.FRESH, DispositionStatus.IN_SEQUENCE
        )

    def test_same_state_is_noop(self):
        # Same state should be allowed (no-op)
        self.sm._validate_transition(
            DispositionStatus.FRESH, DispositionStatus.FRESH
        )

    def test_invalid_transition_raises(self):
        with pytest.raises(TransitionError):
            self.sm._validate_transition(
                DispositionStatus.FRESH, DispositionStatus.REPLIED_POSITIVE
            )

    def test_terminal_state_rejects_all(self):
        for target in DispositionStatus:
            if target == DispositionStatus.REPLIED_HARD_NO:
                continue  # same-state no-op
            with pytest.raises(TransitionError):
                self.sm._validate_transition(
                    DispositionStatus.REPLIED_HARD_NO, target
                )


class TestCooldowns:
    """Test cooldown period calculations."""

    def setup_method(self):
        self.sm = StateMachine.__new__(StateMachine)
        from lead_disposition.core.config import Settings
        self.sm.settings = Settings()

    def test_no_response_cooldown(self):
        td = self.sm._get_cooldown(DispositionStatus.COMPLETED_NO_RESPONSE)
        assert td is not None
        assert td.days == 90

    def test_neutral_reply_cooldown(self):
        td = self.sm._get_cooldown(DispositionStatus.REPLIED_NEUTRAL)
        assert td is not None
        assert td.days == 45

    def test_negative_reply_cooldown(self):
        td = self.sm._get_cooldown(DispositionStatus.REPLIED_NEGATIVE)
        assert td is not None
        assert td.days == 180

    def test_lost_closed_cooldown(self):
        td = self.sm._get_cooldown(DispositionStatus.LOST_CLOSED)
        assert td is not None
        assert td.days == 90

    def test_no_cooldown_for_in_sequence(self):
        td = self.sm._get_cooldown(DispositionStatus.IN_SEQUENCE)
        assert td is None

    def test_no_cooldown_for_hard_no(self):
        td = self.sm._get_cooldown(DispositionStatus.REPLIED_HARD_NO)
        assert td is None


class TestSuppression:
    """Test suppression flag logic."""

    def setup_method(self):
        self.sm = StateMachine.__new__(StateMachine)

    def test_hard_no_suppresses_all_channels(self):
        result = self.sm._get_suppression(DispositionStatus.REPLIED_HARD_NO)
        assert result["email_suppressed"] is True
        assert result["linkedin_suppressed"] is True
        assert result["phone_suppressed"] is True

    def test_bounce_suppresses_email_only(self):
        result = self.sm._get_suppression(DispositionStatus.BOUNCED)
        assert result["email_suppressed"] is True
        assert "linkedin_suppressed" not in result
        assert "phone_suppressed" not in result

    def test_unsub_suppresses_email_only(self):
        result = self.sm._get_suppression(DispositionStatus.UNSUBSCRIBED)
        assert result["email_suppressed"] is True
        assert "linkedin_suppressed" not in result

    def test_no_suppression_for_neutral(self):
        result = self.sm._get_suppression(DispositionStatus.REPLIED_NEUTRAL)
        assert result == {}

    def test_no_suppression_for_positive(self):
        result = self.sm._get_suppression(DispositionStatus.REPLIED_POSITIVE)
        assert result == {}


class TestAllTransitionsHaveScenarios:
    """Verify every state has a defined transition set (even if empty)."""

    def test_all_statuses_in_transition_map(self):
        for status in DispositionStatus:
            assert status in TRANSITIONS, f"Missing transition definition for {status.value}"

    def test_no_transition_to_self_in_map(self):
        """No state should list itself as a valid transition target."""
        for status, targets in TRANSITIONS.items():
            assert status not in targets, f"{status.value} lists itself as a transition target"
