"""Configuration via environment variables.

Matches Charm Email OS env var pattern: individual POSTGRES_* variables.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # --- Database (matches Charm's pattern) ---
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "postgres"
    postgres_user: str = "postgres"
    postgres_password: str = ""

    # SQLite fallback for local dev (set USE_SQLITE=true)
    use_sqlite: bool = False
    sqlite_path: str = "disposition.db"

    @property
    def database_url(self) -> str:
        if self.use_sqlite:
            return f"sqlite:///{self.sqlite_path}"
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # Cooldown defaults (days)
    cooldown_no_response_days: int = 90
    cooldown_neutral_reply_days: int = 45
    cooldown_negative_reply_days: int = 180
    cooldown_lost_closed_days: int = 90
    cooldown_linkedin_days: int = 30
    cooldown_phone_days: int = 60

    # Ownership
    ownership_duration_months: int = 12

    # Campaign fill
    max_contacts_per_company: int = 3
    fresh_retouch_ratio: float = 0.7  # 70% fresh, 30% retouch
    reserve_pool_pct: float = 0.0  # 0% reserved by default

    # Data freshness
    stale_data_months: int = 6

    # TAM health thresholds (weeks)
    tam_warning_weeks: int = 8
    tam_critical_weeks: int = 4

    # --- External Provider API Keys ---
    ai_ark_api_url: str = "https://api.ai-ark.com/v1"
    ai_ark_api_key: str = ""

    clay_webhook_url: str = ""
    clay_api_key: str = ""

    jina_api_key: str = ""
    jina_api_url: str = "https://r.jina.ai"

    spider_api_key: str = ""
    spider_api_url: str = "https://api.spider.cloud"

    # --- Waterfall settings ---
    waterfall_enabled: bool = True
    waterfall_max_credits_per_fill: float = 100.0
    waterfall_provider_order: str = "internal,ai_ark,clay,jina,spider"

    # --- Bridge worker ---
    poll_interval: int = 5
    default_volume: int = 500

    model_config = {"env_prefix": ""}
