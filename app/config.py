from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """
    Central configuration loaded from environment variables / .env file.
    All secrets must live in .env — never committed to git.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",      # silently ignore unknown env vars
    )

    # ── App ─────────────────────────────────────────────────────
    APP_NAME: str = "Bet Hero API"
    ENVIRONMENT: str = Field(default="development", pattern="^(development|staging|production)$")
    LOG_LEVEL: str = Field(default="info", pattern="^(debug|info|warning|error|critical)$")
    PORT: int = 8000

    # ── Supabase ────────────────────────────────────────────────
    SUPABASE_URL: str = Field(default="", description="https://<project>.supabase.co")
    SUPABASE_ANON_KEY: str = Field(default="", description="Public anon key (safe for frontend)")
    SUPABASE_SERVICE_ROLE_KEY: str = Field(default="", description="Secret — never expose to clients")

    # ── Upstash Redis ────────────────────────────────────────────
    UPSTASH_REDIS_REST_URL: str = Field(default="", description="https://<name>.upstash.io")
    UPSTASH_REDIS_REST_TOKEN: str = Field(default="", description="Upstash REST auth token")

    # ── Sports Data APIs ─────────────────────────────────────────
    API_SPORTS_KEY: str = Field(default="", description="api-sports.io key")
    ODDS_API_KEY: str = Field(default="", alias="ODDSPAPI_KEY", description="OddsAPI / OddsPortal key")
    THE_ODDS_API_KEY: str = Field(default="", description="the-odds-api.com key")

    # ── News & Sentiment ─────────────────────────────────────────
    NEWSAPI_KEY: str = Field(default="", description="newsapi.org key")

    # ── AI / LLM ─────────────────────────────────────────────────
    ANTHROPIC_API_KEY: str = Field(default="", description="Anthropic Claude API key")

    # ── Derived helpers ──────────────────────────────────────────
    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT == "production"

    @property
    def is_development(self) -> bool:
        return self.ENVIRONMENT == "development"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached Settings singleton.
    Use as a FastAPI dependency: settings = Depends(get_settings)
    """
    return Settings()


# Module-level singleton for non-FastAPI code (ML pipeline, scripts, etc.)
settings = get_settings()
