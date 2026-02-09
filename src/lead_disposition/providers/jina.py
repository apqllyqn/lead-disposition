"""Jina AI Reader provider adapter - web scraping and content extraction."""

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

# Regex patterns for extracting contact info from scraped pages
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
LINKEDIN_PATTERN = re.compile(r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_-]+/?")


class JinaProvider(LeadProvider):
    """Jina AI Reader - Extract leads from company websites via web scraping."""

    provider_name = "jina"
    priority = 3

    def __init__(self, settings: Settings):
        self.settings = settings
        self.api_key = settings.jina_api_key
        self.reader_url = settings.jina_api_url.rstrip("/")
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            headers: dict[str, str] = {"Accept": "text/plain"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(headers=headers, timeout=30.0)
        return self._client

    async def search_leads(self, criteria: SearchCriteria) -> ProviderResult:
        """Scrape company websites for contact information.

        Uses Jina Reader to extract markdown from company team/about/contact
        pages, then parses emails and names from the content.
        """
        if not criteria.company_domains:
            # Use Jina search to find companies matching criteria
            return await self._search_by_keywords(criteria)

        all_leads: list[ExternalLead] = []
        errors: list[str] = []
        credits = 0.0

        for domain in criteria.company_domains[: criteria.limit]:
            result = await self._scrape_company(domain, criteria)
            all_leads.extend(result.leads)
            errors.extend(result.errors)
            credits += result.credits_consumed

        return ProviderResult(
            leads=all_leads[:criteria.limit],
            total_found=len(all_leads),
            credits_consumed=credits,
            errors=errors,
        )

    async def _scrape_company(
        self, domain: str, criteria: SearchCriteria
    ) -> ProviderResult:
        """Scrape a company's team/about pages for contact info."""
        leads: list[ExternalLead] = []
        errors: list[str] = []
        credits = 0.0

        # Try common team/contact page URLs
        paths = ["/team", "/about", "/about-us", "/contact", "/our-team", "/people"]
        for path in paths:
            url = f"https://{domain}{path}"
            try:
                resp = await self.client.get(f"{self.reader_url}/{url}")
                credits += 1.0
                if resp.status_code != 200:
                    continue

                content = resp.text
                extracted = self._extract_contacts(content, domain)
                leads.extend(extracted)

                if leads:
                    break  # Found contacts, no need to try more pages
            except httpx.RequestError as e:
                errors.append(f"Jina scrape error for {url}: {e}")

        return ProviderResult(
            leads=leads,
            total_found=len(leads),
            credits_consumed=credits,
            errors=errors,
        )

    async def _search_by_keywords(self, criteria: SearchCriteria) -> ProviderResult:
        """Use Jina search API to find leads by keyword."""
        query_parts = []
        if criteria.industry:
            query_parts.append(criteria.industry)
        if criteria.job_titles:
            query_parts.append(" ".join(criteria.job_titles))
        if criteria.keywords:
            query_parts.append(" ".join(criteria.keywords))

        if not query_parts:
            return ProviderResult(errors=["No search criteria provided for Jina"])

        query = " ".join(query_parts) + " team contact email"
        search_url = f"https://s.jina.ai/?q={query}"

        try:
            resp = await self.client.get(search_url)
            if resp.status_code != 200:
                return ProviderResult(
                    errors=[f"Jina search returned {resp.status_code}"],
                    credits_consumed=1.0,
                )

            content = resp.text
            leads = self._extract_contacts(content, "")
            return ProviderResult(
                leads=leads[:criteria.limit],
                total_found=len(leads),
                credits_consumed=1.0,
            )
        except httpx.RequestError as e:
            return ProviderResult(errors=[f"Jina search error: {e}"])

    def _extract_contacts(self, content: str, domain: str) -> list[ExternalLead]:
        """Extract email addresses and associated names from page content."""
        leads: list[ExternalLead] = []
        seen_emails: set[str] = set()

        emails = EMAIL_PATTERN.findall(content)
        linkedin_urls = LINKEDIN_PATTERN.findall(content)

        for email in emails:
            email_lower = email.lower()
            # Skip generic/noreply addresses
            if email_lower in seen_emails:
                continue
            local = email_lower.split("@")[0]
            if local in ("info", "support", "hello", "contact", "noreply", "no-reply", "admin"):
                continue
            seen_emails.add(email_lower)

            # Try to extract name from email (first.last@domain)
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
                company_domain=domain or email_lower.split("@")[1],
                company_name=None,
                title=None,
                source_provider=self.provider_name,
            ))

        return leads

    async def health_check(self) -> bool:
        try:
            resp = await self.client.get(f"{self.reader_url}/https://example.com")
            return resp.status_code == 200
        except httpx.RequestError:
            return False

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
