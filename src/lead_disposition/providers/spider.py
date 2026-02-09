"""Spider.cloud provider adapter - high-speed web crawling."""

from __future__ import annotations

import re

import httpx

from lead_disposition.core.config import Settings
from lead_disposition.providers.base import (
    ExternalLead,
    LeadProvider,
    ProviderResult,
    SearchCriteria,
)

EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


class SpiderProvider(LeadProvider):
    """Spider.cloud - High-speed web crawling for lead extraction."""

    provider_name = "spider"
    priority = 4

    def __init__(self, settings: Settings):
        self.settings = settings
        self.api_url = settings.spider_api_url.rstrip("/")
        self.api_key = settings.spider_api_key
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60.0,
            )
        return self._client

    async def search_leads(self, criteria: SearchCriteria) -> ProviderResult:
        """Crawl company websites to extract lead contact information."""
        if not self.api_key:
            return ProviderResult(errors=["Spider API key not configured"])

        if not criteria.company_domains:
            return ProviderResult(
                errors=["Spider requires company_domains to crawl"]
            )

        all_leads: list[ExternalLead] = []
        errors: list[str] = []
        credits = 0.0

        for domain in criteria.company_domains[: criteria.limit]:
            result = await self._crawl_company(domain)
            all_leads.extend(result.leads)
            errors.extend(result.errors)
            credits += result.credits_consumed

        return ProviderResult(
            leads=all_leads[:criteria.limit],
            total_found=len(all_leads),
            credits_consumed=credits,
            errors=errors,
        )

    async def _crawl_company(self, domain: str) -> ProviderResult:
        """Crawl a company website for team/contact pages."""
        leads: list[ExternalLead] = []
        errors: list[str] = []

        payload = {
            "url": f"https://{domain}",
            "limit": 10,  # Crawl up to 10 pages
            "return_format": "markdown",
            "request": "smart",
            "depth": 2,
        }

        try:
            resp = await self.client.post(f"{self.api_url}/crawl", json=payload)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            return ProviderResult(
                errors=[f"Spider API error for {domain}: {e.response.status_code}"],
                credits_consumed=1.0,
            )
        except httpx.RequestError as e:
            return ProviderResult(errors=[f"Spider connection error for {domain}: {e}"])

        # Spider returns a list of page results
        pages = data if isinstance(data, list) else data.get("data", [])
        seen_emails: set[str] = set()

        for page in pages:
            content = page.get("content", "") or page.get("markdown", "")
            page_url = page.get("url", "")

            # Only extract from relevant pages
            relevant_keywords = ("team", "about", "contact", "people", "staff", "leadership")
            if not any(kw in page_url.lower() for kw in relevant_keywords):
                # Also check page content for team-like sections
                if not any(kw in content.lower()[:500] for kw in relevant_keywords):
                    continue

            emails = EMAIL_PATTERN.findall(content)
            for email in emails:
                email_lower = email.lower()
                if email_lower in seen_emails:
                    continue
                local = email_lower.split("@")[0]
                if local in (
                    "info", "support", "hello", "contact", "noreply",
                    "no-reply", "admin", "sales", "marketing",
                ):
                    continue
                seen_emails.add(email_lower)

                first_name = None
                last_name = None
                if "." in local:
                    parts = local.split(".")
                    first_name = parts[0].capitalize()
                    last_name = parts[-1].capitalize()

                leads.append(ExternalLead(
                    email=email_lower,
                    first_name=first_name,
                    last_name=last_name,
                    company_domain=domain,
                    source_provider=self.provider_name,
                ))

        return ProviderResult(
            leads=leads,
            total_found=len(leads),
            credits_consumed=len(pages) * 0.5,
        )

    async def health_check(self) -> bool:
        if not self.api_key:
            return False
        try:
            resp = await self.client.post(
                f"{self.api_url}/scrape",
                json={"url": "https://example.com", "return_format": "markdown"},
            )
            return resp.status_code < 500
        except httpx.RequestError:
            return False

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
