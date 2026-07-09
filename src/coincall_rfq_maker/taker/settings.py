"""Taker runtime configuration loaded from the environment / .env file."""

from typing import Any

from pydantic import Field, SecretStr, field_validator, model_validator

from coincall_rfq_maker.core.config import CoincallClientSettings, Credentials


class TakerSettings(CoincallClientSettings):
    """All tunables for the RFQ taker CLI."""

    api_key: str = Field(alias="TAKER_API_KEY")
    api_secret: SecretStr = Field(alias="TAKER_API_SECRET")
    taker_max_notional_usd: float = Field(default=5000.0, alias="TAKER_MAX_NOTIONAL_USD")
    taker_audit_path: str = Field(default="~/.rfq-taker/audit.jsonl", alias="TAKER_AUDIT_PATH")

    @model_validator(mode="before")
    @classmethod
    def _ignore_maker_credential_env_names(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        cleaned = dict(data)
        if "TAKER_API_KEY" not in cleaned:
            cleaned.pop("api_key", None)
        if "TAKER_API_SECRET" not in cleaned:
            cleaned.pop("api_secret", None)
        return cleaned

    @field_validator("api_key")
    @classmethod
    def _api_key_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("TAKER_API_KEY must not be blank")
        return value

    @field_validator("api_secret")
    @classmethod
    def _api_secret_not_blank(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("TAKER_API_SECRET must not be blank")
        return value

    def credentials(self) -> Credentials:
        return Credentials(api_key=self.api_key, api_secret=self.api_secret)
