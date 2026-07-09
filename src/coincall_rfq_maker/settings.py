"""Runtime configuration loaded from the environment / .env file.

Fails fast (raises at construction time) when API_KEY or API_SECRET are
missing, since the process cannot talk to Coincall without them.
"""

from dataclasses import dataclass

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True, slots=True)
class Credentials:
    api_key: str
    api_secret: SecretStr


class Settings(BaseSettings):
    """All tunables for the market maker. See README.md for the full table."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
        hide_input_in_errors=True,
    )

    api_key: str = Field(alias="API_KEY")
    api_secret: SecretStr = Field(alias="API_SECRET")
    taker_api_key: str | None = Field(default=None, alias="TAKER_API_KEY")
    taker_api_secret: SecretStr | None = Field(default=None, alias="TAKER_API_SECRET")

    rest_base_url: str = Field(default="https://betaapi.coincall.com", alias="REST_BASE_URL")
    ws_url: str = Field(default="wss://betaws.seizeyouralpha.com/options", alias="WS_URL")

    dry_run: bool = Field(default=True, alias="DRY_RUN")
    cancel_all_on_start: bool = Field(default=True, alias="CANCEL_ALL_ON_START")
    cancel_all_on_stop: bool = Field(default=True, alias="CANCEL_ALL_ON_STOP")

    heartbeat_interval_seconds: float = Field(default=5.0, gt=0, alias="HEARTBEAT_INTERVAL_SECONDS")
    pricing_refresh_seconds: float = Field(default=5.0, alias="PRICING_REFRESH_SECONDS")
    quote_refresh_seconds: float = Field(default=10.0, alias="QUOTE_REFRESH_SECONDS")
    price_move_threshold: float = Field(default=0.001, alias="PRICE_MOVE_THRESHOLD")

    max_quote_notional_usd: float = Field(default=1_000_000.0, alias="MAX_QUOTE_NOTIONAL_USD")
    max_leg_qty: float = Field(default=100.0, alias="MAX_LEG_QTY")
    min_time_to_expiry_hours: float = Field(default=1.0, alias="MIN_TIME_TO_EXPIRY_HOURS")
    stale_market_data_seconds: float = Field(default=30.0, alias="STALE_MARKET_DATA_SECONDS")
    taker_max_notional_usd: float = Field(default=5000.0, alias="TAKER_MAX_NOTIONAL_USD")
    taker_audit_path: str = Field(default="~/.rfq-taker/audit.jsonl", alias="TAKER_AUDIT_PATH")

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

    @field_validator("taker_api_key")
    @classmethod
    def _blank_taker_api_key_is_absent(cls, value: str | None) -> str | None:
        # An empty placeholder (TAKER_API_KEY="") must behave like an unset
        # var so maker-only configs copied from .env.example keep starting.
        if value is not None and not value.strip():
            return None
        return value

    @field_validator("taker_api_secret")
    @classmethod
    def _blank_taker_api_secret_is_absent(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and not value.get_secret_value().strip():
            return None
        return value

    @model_validator(mode="after")
    def _taker_credentials_both_or_neither(self) -> "Settings":
        if (self.taker_api_key is None) != (self.taker_api_secret is None):
            raise ValueError("TAKER_API_KEY and TAKER_API_SECRET must be set together or omitted")
        return self

    def maker_credentials(self) -> Credentials:
        return Credentials(api_key=self.api_key, api_secret=self.api_secret)

    def taker_credentials(self) -> Credentials | None:
        if self.taker_api_key is None or self.taker_api_secret is None:
            return None
        return Credentials(api_key=self.taker_api_key, api_secret=self.taker_api_secret)
