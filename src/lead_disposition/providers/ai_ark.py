"""AI Ark B2B lead database provider adapter."""

from __future__ import annotations

import httpx

from lead_disposition.core.config import Settings
from lead_disposition.providers.base import (
    ExternalLead,
    LeadProvider,
    ProviderResult,
    SearchCriteria,
)


class AIArkProvider(LeadProvider):
    """AI Ark - B2B contact database with semantic and similarity search."""

    provider_name = "ai_ark"
    priority = 1

    def __init__(self, settings: Settings):
        self.settings = settings
        self.api_url = settings.ai_ark_api_url.rstrip("/")
        self.api_key = settings.ai_ark_api_key
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.api_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    async def search_leads(self, criteria: SearchCriteria) -> ProviderResult:
        """Search AI Ark for leads matching criteria."""
        if not self.api_key:
            return ProviderResult(errors=["AI Ark API key not configured"])

        payload: dict = {
            "limit": criteria.limit,
        }
        if criteria.job_titles:
            payload["job_titles"] = criteria.job_titles
        if criteria.industry:
            payload["industry"] = criteria.industry
        if criteria.locations:
            payload["locations"] = criteria.locations
        if criteria.company_sizes:
            payload["company_sizes"] = criteria.company_sizes
        if criteria.keywords:
            payload["keywords"] = criteria.keywords
        if criteria.company_domains:
            payload["company_domains"] = criteria.company_domains

        try:
            resp = await self.client.post("/people/search", json=payload)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            return ProviderResult(errors=[f"AI Ark API error: {e.response.status_code}"])
        except httpx.RequestError as e:
            return ProviderResult(errors=[f"AI Ark connection error: {e}"])

        leads = []
        results = data.get("results", data.get("data", []))
        for item in results:
            email = item.get("email") or item.get("work_email")
            if not email:
                continue
            leads.append(ExternalLead(
                email=email,
                first_name=item.get("first_name"),
                last_name=item.get("last_name"),
                company_name=item.get("company_name") or item.get("company"),
                company_domain=item.get("company_domain") or item.get("domain"),
                title=item.get("title") or item.get("job_title"),
                linkedin_url=item.get("linkedin_url") or item.get("linkedin"),
                phone=item.get("phone") or item.get("mobile"),
                location=item.get("location") or item.get("city"),
                industry=item.get("industry"),
                company_size=item.get("company_size") or item.get("employees"),
                source_provider=self.provider_name,
                source_id=item.get("id"),
                raw_data=item,
            ))

        return ProviderResult(
            leads=leads,
            total_found=data.get("total", len(leads)),
            credits_consumed=len(leads) * 1.0,
        )

    async def health_check(self) -> bool:
        if not self.api_key:
            return False
        try:
            resp = await self.client.get("/health")
            return resp.status_code < 500
        except httpx.RequestError:
            return False

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
