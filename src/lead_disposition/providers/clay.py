"""Clay webhook-based waterfall enrichment provider adapter."""

from __future__ import annotations

import asyncio

import httpx

from lead_disposition.core.config import Settings
from lead_disposition.providers.base import (
    ExternalLead,
    LeadProvider,
    ProviderResult,
    SearchCriteria,
)


class ClayProvider(LeadProvider):
    """Clay - Webhook-based waterfall enrichment with 150+ data providers."""

    provider_name = "clay"
    priority = 2

    def __init__(self, settings: Settings):
        self.settings = settings
        self.webhook_url = settings.clay_webhook_url
        self.api_key = settings.clay_api_key
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(headers=headers, timeout=60.0)
        return self._client

    async def search_leads(self, criteria: SearchCriteria) -> ProviderResult:
        """Push search criteria to Clay webhook for waterfall enrichment.

        Clay processes asynchronously (1-2 min per batch). This adapter
        pushes the request and collects results via a callback or polling
        mechanism.
        """
        if not self.webhook_url:
            return ProviderResult(errors=["Clay webhook URL not configured"])

        payload = {
            "client_id": criteria.client_id,
            "industry": criteria.industry,
            "job_titles": criteria.job_titles,
            "locations": criteria.locations,
            "company_sizes": criteria.company_sizes,
            "keywords": criteria.keywords,
            "company_domains": criteria.company_domains,
            "limit": criteria.limit,
        }

        try:
            resp = await self.client.post(self.webhook_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            return ProviderResult(errors=[f"Clay webhook error: {e.response.status_code}"])
        except httpx.RequestError as e:
            return ProviderResult(errors=[f"Clay connection error: {e}"])

        # Clay may return results immediately (if table is pre-configured)
        # or return a job ID for polling
        if "results" in data or "rows" in data:
            return self._parse_results(data)

        # If Clay returns a job/run ID, poll for completion
        run_id = data.get("run_id") or data.get("id")
        if run_id and self.api_key:
            return await self._poll_results(run_id)

        # Webhook accepted but no immediate results
        return ProviderResult(
            leads=[],
            total_found=0,
            credits_consumed=0,
            errors=["Clay webhook accepted - results will arrive asynchronously"],
        )

    def _parse_results(self, data: dict) -> ProviderResult:
        """Parse Clay response rows into ExternalLead objects."""
        leads = []
        rows = data.get("results", data.get("rows", []))
        for row in rows:
            email = (
                row.get("email") or row.get("work_email")
                or row.get("Email") or row.get("Work Email")
            )
            if not email:
                continue
            leads.append(ExternalLead(
                email=email,
                first_name=row.get("first_name") or row.get("First Name"),
                last_name=row.get("last_name") or row.get("Last Name"),
                company_name=row.get("company") or row.get("Company"),
                company_domain=row.get("domain") or row.get("Company Domain"),
                title=row.get("title") or row.get("Title") or row.get("Job Title"),
                linkedin_url=row.get("linkedin_url") or row.get("LinkedIn URL"),
                phone=row.get("phone") or row.get("Phone"),
                location=row.get("location") or row.get("Location"),
                industry=row.get("industry") or row.get("Industry"),
                source_provider=self.provider_name,
                raw_data=row,
            ))
        return ProviderResult(
            leads=leads,
            total_found=len(leads),
            credits_consumed=len(leads) * 2.0,  # Clay averages ~2 credits per lead
        )

    async def _poll_results(self, run_id: str, max_wait: int = 180) -> ProviderResult:
        """Poll Clay API for completed enrichment results."""
        poll_url = f"https://api.clay.com/v1/runs/{run_id}"
        elapsed = 0
        interval = 10

        while elapsed < max_wait:
            await asyncio.sleep(interval)
            elapsed += interval
            try:
                resp = await self.client.get(poll_url)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                status = data.get("status", "")
                if status in ("completed", "done"):
                    return self._parse_results(data)
                if status in ("failed", "error"):
                    return ProviderResult(
                        errors=[f"Clay run {run_id} failed: {data.get('error', 'unknown')}"]
                    )
            except httpx.RequestError:
                continue

        return ProviderResult(
            errors=[f"Clay run {run_id} timed out after {max_wait}s"]
        )

    async def health_check(self) -> bool:
        return bool(self.webhook_url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
