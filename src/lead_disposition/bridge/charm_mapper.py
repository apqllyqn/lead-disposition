"""Map Charm onboarding data to WaterfallFillRequest parameters."""

from __future__ import annotations

import logging
from typing import Any

from lead_disposition.core.models import Channel
from lead_disposition.waterfall.engine import WaterfallFillRequest

logger = logging.getLogger(__name__)


def build_waterfall_request(job_row: dict[str, Any]) -> WaterfallFillRequest:
    """Convert a lead_pull_jobs row into a WaterfallFillRequest.

    The job row contains:
      - id, client_id, suggestion_id, volume, channel,
        max_external_credits, enable_external, search_criteria (JSONB)

    search_criteria (populated by the DB trigger from onboarding data):
      - title_keywords: from client_onboarding_submissions.job_titles
      - persona_titles: from client_personas.job_title
      - industry: from client_onboarding_submissions.target_customer
      - search_keywords: from client_segments.pain_points
      - signals: from client_onboarding_submissions.signals
    """
    criteria = job_row.get("search_criteria") or {}

    # Merge title sources: onboarding job_titles + persona job_titles
    title_keywords = _flatten_strings(criteria.get("title_keywords", []))
    persona_titles = _flatten_strings(criteria.get("persona_titles", []))
    all_titles = list(dict.fromkeys(title_keywords + persona_titles))  # dedup, preserve order

    # Search keywords from pain points + signals
    search_keywords = _flatten_strings(criteria.get("search_keywords", []))
    signals = criteria.get("signals", [])
    if isinstance(signals, list):
        for sig in signals:
            if isinstance(sig, str):
                search_keywords.append(sig)
            elif isinstance(sig, dict) and sig.get("name"):
                search_keywords.append(sig["name"])

    industry = criteria.get("industry", "") or ""

    # Use suggestion_id as campaign_id (links the fill back to the approved strategy)
    campaign_id = str(job_row.get("suggestion_id") or job_row["id"])
    client_id = str(job_row["client_id"])

    channel_str = (job_row.get("channel") or "email").lower()
    try:
        channel = Channel(channel_str)
    except ValueError:
        channel = Channel.EMAIL

    return WaterfallFillRequest(
        campaign_id=campaign_id,
        client_id=client_id,
        channel=channel,
        volume=job_row.get("volume", 500),
        title_keywords=all_titles,
        industry_keywords=[industry] if industry else [],
        industry=industry if industry else None,
        search_keywords=search_keywords,
        enable_external=job_row.get("enable_external", True),
        max_external_credits=job_row.get("max_external_credits", 100.0),
    )


def _flatten_strings(val: Any) -> list[str]:
    """Normalize a value that may be a JSONB array or a plain string into a list of strings."""
    if val is None:
        return []
    if isinstance(val, str):
        return [val] if val else []
    if isinstance(val, list):
        return [str(v) for v in val if v]
    return []
