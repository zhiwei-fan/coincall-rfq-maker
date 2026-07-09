"""Shared configuration primitives for Coincall REST clients."""

from dataclasses import dataclass

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True, slots=True)
class Credentials:
    api_key: str
    api_secret: SecretStr


class CoincallClientSettings(BaseSettings):
    """Shared REST client settings read from environment or .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
        hide_input_in_errors=True,
    )

    rest_base_url: str = Field(default="https://betaapi.coincall.com", alias="REST_BASE_URL")
