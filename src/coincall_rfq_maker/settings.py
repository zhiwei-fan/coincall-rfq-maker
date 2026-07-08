"""Runtime configuration loaded from the environment / .env file.

Fails fast (raises at construction time) when API_KEY or API_SECRET are
missing, since the process cannot talk to Coincall without them.
"""

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All tunables for the market maker. See README.md for the full table."""

    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", case_sensitive=False, populate_by_name=True
    )

    api_key: str = Field(alias="API_KEY")
    api_secret: SecretStr = Field(alias="API_SECRET")

    rest_base_url: str = Field(default="https://betaapi.coincall.com", alias="REST_BASE_URL")
    ws_url: str = Field(default="wss://betaws.seizeyouralpha.com/options", alias="WS_URL")

    dry_run: bool = Field(default=True, alias="DRY_RUN")
    cancel_all_on_start: bool = Field(default=True, alias="CANCEL_ALL_ON_START")

    pricing_refresh_seconds: float = Field(default=5.0, alias="PRICING_REFRESH_SECONDS")
    quote_refresh_seconds: float = Field(default=10.0, alias="QUOTE_REFRESH_SECONDS")
    price_move_threshold: float = Field(default=0.001, alias="PRICE_MOVE_THRESHOLD")

    max_quote_notional_usd: float = Field(default=1_000_000.0, alias="MAX_QUOTE_NOTIONAL_USD")
    max_leg_qty: float = Field(default=100.0, alias="MAX_LEG_QTY")
    min_time_to_expiry_hours: float = Field(default=1.0, alias="MIN_TIME_TO_EXPIRY_HOURS")
    stale_market_data_seconds: float = Field(default=30.0, alias="STALE_MARKET_DATA_SECONDS")

    bid_vol: float = Field(default=0.20, alias="BID_VOL")
    ask_vol: float = Field(default=2.00, alias="ASK_VOL")
    risk_free_rate: float = Field(default=0.05, alias="RISK_FREE_RATE")

    db_path: str = Field(default="rfq_maker.db", alias="DB_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator("api_key")
    @classmethod
    def _api_key_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("API_KEY must not be blank")
        return value

    @field_validator("api_secret")
    @classmethod
    def _api_secret_not_blank(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("API_SECRET must not be blank")
        return value
