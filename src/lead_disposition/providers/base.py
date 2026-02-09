"""Abstract base class for external lead providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from pydantic import BaseModel, Field


class SearchCriteria(BaseModel):
    """Criteria for searching external lead sources."""

    client_id: str
    industry: str | None = None
    job_titles: list[str] = Field(default_factory=list)
    company_sizes: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    company_domains: list[str] = Field(default_factory=list)
    limit: int = 100


class ExternalLead(BaseModel):
    """A lead returned from an external provider before mapping to internal Contact."""

    email: str
    first_name: str | None = None
    last_name: str | None = None
    company_name: str | None = None
    company_domain: str | None = None
    title: str | None = None
    linkedin_url: str | None = None
    phone: str | None = None
    location: str | None = None
    industry: str | None = None
    company_size: str | None = None
    source_provider: str = ""
    source_id: str | None = None
    raw_data: dict = Field(default_factory=dict)


class ProviderResult(BaseModel):
    """Result from a provider search."""

    leads: list[ExternalLead] = Field(default_factory=list)
    total_found: int = 0
    credits_consumed: float = 0.0
    errors: list[str] = Field(default_factory=list)


class LeadProvider(ABC):
    """Abstract base class for external lead data providers."""

    provider_name: str = "unknown"
    priority: int = 0

    @abstractmethod
    async def search_leads(self, criteria: SearchCriteria) -> ProviderResult:
        """Search for leads matching the given criteria."""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the provider API is reachable and authenticated."""
        ...

    async def close(self) -> None:
        """Clean up any resources (HTTP sessions, etc.)."""
        pass
