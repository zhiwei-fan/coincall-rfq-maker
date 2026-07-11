"""Maker runtime configuration loaded from the environment / .env file.

Fails fast (raises at construction time) when API_KEY or API_SECRET are
missing, since the process cannot talk to Coincall without them.
"""

from pydantic import Field, SecretStr, field_validator

from coincall_rfq_maker.core.config import CoincallClientSettings, Credentials


class MakerSettings(CoincallClientSettings):
    """All tunables for the market maker. See README.md for the full table."""

    api_key: str = Field(alias="API_KEY")
    api_secret: SecretStr = Field(alias="API_SECRET")

    ws_url: str = Field(default="wss://betaws.seizeyouralpha.com/options", alias="WS_URL")

    dry_run: bool = Field(default=True, alias="DRY_RUN")
    # Live-mode acknowledgment that position/greek limits are absent.
    allow_no_exposure_limits: bool = Field(default=False, alias="ALLOW_NO_EXPOSURE_LIMITS")
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

    bid_vol: float = Field(default=0.20, alias="BID_VOL")
    ask_vol: float = Field(default=2.00, alias="ASK_VOL")
    risk_free_rate: float = Field(default=0.05, alias="RISK_FREE_RATE")

    db_path: str = Field(default="rfq_maker.db", alias="DB_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: str = Field(default="rfq_maker.log", alias="LOG_FILE")

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

    def credentials(self) -> Credentials:
        return Credentials(api_key=self.api_key, api_secret=self.api_secret)
