"""Waterfall engine - orchestrates multi-source lead pulling with priority order."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from lead_disposition.campaign_fill import CampaignFillEngine
from lead_disposition.core.config import Settings
from lead_disposition.core.models import CampaignFillRequest, CampaignFillResult, Channel, Contact
from lead_disposition.providers.base import LeadProvider, ProviderResult, SearchCriteria
from lead_disposition.waterfall.writeback import WriteBackResult, write_back_leads

logger = logging.getLogger(__name__)


class WaterfallFillRequest(BaseModel):
    """Extended campaign fill request with waterfall options."""

    # Core fill params (same as CampaignFillRequest)
    campaign_id: str
    client_id: str
    channel: Channel = Channel.EMAIL
    volume: int
    title_keywords: list[str] = Field(default_factory=list)
    industry_keywords: list[str] = Field(default_factory=list)
    fresh_ratio: float | None = None
    max_per_company: int | None = None

    # Waterfall-specific params
    enable_external: bool = True
    max_external_credits: float = 100.0
    providers_override: list[str] | None = None

    # External search criteria
    industry: str | None = None
    company_sizes: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    search_keywords: list[str] = Field(default_factory=list)
    company_domains: list[str] = Field(default_factory=list)


class WaterfallFillResult(BaseModel):
    """Extended fill result with waterfall metrics."""

    # Core fill results
    campaign_id: str
    client_id: str
    total_requested: int
    total_assigned: int
    fresh_count: int = 0
    retouch_count: int = 0
    companies_touched: int = 0
    contacts: list[Contact] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    # Waterfall metrics
    internal_filled: int = 0
    external_filled: int = 0
    per_provider_counts: dict[str, int] = Field(default_factory=dict)
    credits_consumed: dict[str, float] = Field(default_factory=dict)
    write_back_count: int = 0
    write_back_details: WriteBackResult | None = None


class WaterfallEngine:
    """Orchestrates multi-source lead pulling with priority-based cascading.

    Waterfall order:
    1. Internal database (existing eligible contacts)
    2. AI Ark (B2B lead database)
    3. Clay (webhook enrichment, 150+ providers)
    4. Jina AI Reader (web scraping)
    5. Spider.cloud (bulk crawling)
    """

    def __init__(
        self,
        db: Any,
        providers: list[LeadProvider],
        settings: Settings | None = None,
    ):
        self.db = db
        self.settings = settings or Settings()
        self.providers = sorted(providers, key=lambda p: p.priority)
        self.fill_engine = CampaignFillEngine(db, self.settings)

    async def fill_campaign(
        self, request: WaterfallFillRequest
    ) -> WaterfallFillResult:
        """Execute waterfall fill: internal DB first, then external providers on shortfall."""

        result = WaterfallFillResult(
            campaign_id=request.campaign_id,
            client_id=request.client_id,
            total_requested=request.volume,
            total_assigned=0,
        )

        # Step 1: Try internal database first
        internal_request = CampaignFillRequest(
            campaign_id=request.campaign_id,
            client_id=request.client_id,
            channel=request.channel,
            volume=request.volume,
            title_keywords=request.title_keywords,
            fresh_ratio=request.fresh_ratio,
            max_per_company=request.max_per_company,
        )

        internal_result = await self.fill_engine.fill(internal_request)
        result.internal_filled = internal_result.total_assigned
        result.total_assigned = internal_result.total_assigned
        result.fresh_count = internal_result.fresh_count
        result.retouch_count = internal_result.retouch_count
        result.companies_touched = internal_result.companies_touched
        result.contacts = list(internal_result.contacts)
        result.warnings.extend(internal_result.warnings)
        result.per_provider_counts["internal"] = internal_result.total_assigned

        logger.info(
            "Internal fill: %d/%d assigned",
            internal_result.total_assigned,
            request.volume,
        )

        # Step 2: Check if we need external sources
        deficit = request.volume - result.total_assigned
        if deficit <= 0 or not request.enable_external:
            return result

        if not self.settings.waterfall_enabled:
            result.warnings.append("Waterfall disabled - external sources skipped")
            return result

        logger.info("Internal shortfall: %d leads needed from external sources", deficit)

        # Step 3: Cascade through external providers
        total_credits = 0.0
        active_providers = self._get_active_providers(request.providers_override)

        search_criteria = SearchCriteria(
            client_id=request.client_id,
            industry=request.industry,
            job_titles=request.title_keywords,
            company_sizes=request.company_sizes,
            locations=request.locations,
            keywords=request.search_keywords or request.industry_keywords,
            company_domains=request.company_domains,
            limit=deficit,
        )

        all_external_leads = []

        for provider in active_providers:
            if deficit <= 0:
                break
            if total_credits >= request.max_external_credits:
                result.warnings.append(
                    f"Credit limit reached ({total_credits:.1f}/{request.max_external_credits})"
                )
                break

            logger.info(
                "Querying %s for %d leads...", provider.provider_name, deficit
            )

            # Adjust limit for remaining deficit
            search_criteria.limit = deficit

            try:
                provider_result: ProviderResult = await provider.search_leads(
                    search_criteria
                )
            except Exception as e:
                logger.error("Provider %s failed: %s", provider.provider_name, e)
                result.warnings.append(f"{provider.provider_name} error: {e}")
                continue

            if provider_result.errors:
                result.warnings.extend(provider_result.errors)

            found_count = len(provider_result.leads)
            result.per_provider_counts[provider.provider_name] = found_count
            result.credits_consumed[provider.provider_name] = (
                provider_result.credits_consumed
            )
            total_credits += provider_result.credits_consumed

            logger.info(
                "%s returned %d leads (%.1f credits)",
                provider.provider_name,
                found_count,
                provider_result.credits_consumed,
            )

            all_external_leads.extend(provider_result.leads)
            deficit -= found_count

        # Step 4: Write-back all external leads to internal database
        if all_external_leads:
            wb_result = await write_back_leads(
                self.db, all_external_leads, request.client_id
            )
            result.write_back_count = wb_result.new_inserted
            result.write_back_details = wb_result

            logger.info(
                "Write-back: %d new, %d dupes, %d invalid",
                wb_result.new_inserted,
                wb_result.duplicates_skipped,
                wb_result.invalid_skipped,
            )

            if wb_result.errors:
                result.warnings.extend(wb_result.errors)

            # Step 5: Re-run internal fill for the newly added leads
            if wb_result.new_inserted > 0:
                remaining = request.volume - result.total_assigned
                if remaining > 0:
                    refill_request = CampaignFillRequest(
                        campaign_id=request.campaign_id,
                        client_id=request.client_id,
                        channel=request.channel,
                        volume=remaining,
                        title_keywords=request.title_keywords,
                        fresh_ratio=1.0,  # All newly added leads are fresh
                        max_per_company=request.max_per_company,
                    )
                    refill_result = await self.fill_engine.fill(refill_request)

                    result.external_filled = refill_result.total_assigned
                    result.total_assigned += refill_result.total_assigned
                    result.fresh_count += refill_result.fresh_count
                    result.companies_touched += refill_result.companies_touched
                    result.contacts.extend(refill_result.contacts)
                    result.warnings.extend(refill_result.warnings)

                    logger.info(
                        "Refill from write-back: %d more assigned",
                        refill_result.total_assigned,
                    )

        if result.total_assigned < request.volume:
            result.warnings.append(
                f"Final shortfall: requested {request.volume}, "
                f"assigned {result.total_assigned} "
                f"(internal={result.internal_filled}, external={result.external_filled})"
            )

        return result

    def _get_active_providers(
        self, override: list[str] | None = None
    ) -> list[LeadProvider]:
        """Get providers in priority order, optionally filtered by override list."""
        if override:
            override_set = set(override)
            return [p for p in self.providers if p.provider_name in override_set]

        # Use configured provider order
        order = self.settings.waterfall_provider_order.split(",")
        order_map = {name.strip(): i for i, name in enumerate(order)}

        active = [p for p in self.providers if p.provider_name in order_map]
        active.sort(key=lambda p: order_map.get(p.provider_name, 999))
        return active
