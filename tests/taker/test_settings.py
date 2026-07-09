from pathlib import Path

import pytest
from pydantic import ValidationError

from coincall_rfq_maker.taker.settings import TakerSettings


def test_valid_taker_settings_require_only_taker_credentials() -> None:
    settings = TakerSettings(  # type: ignore[call-arg]
        _env_file=None,
        TAKER_API_KEY="taker-key",
        TAKER_API_SECRET="taker-secret",
    )

    assert settings.credentials().api_key == "taker-key"
    assert settings.credentials().api_secret.get_secret_value() == "taker-secret"
    assert settings.taker_max_notional_usd == 5000.0
    assert settings.taker_audit_path == "~/.rfq-taker/audit.jsonl"
    assert settings.rest_base_url == "https://betaapi.coincall.com"
    assert not hasattr(settings, "maker_credentials")


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"TAKER_API_KEY": "taker-key"},
        {"TAKER_API_SECRET": "taker-secret"},
    ],
)
def test_taker_credentials_missing_fail_fast(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValidationError) as exc_info:
        TakerSettings(_env_file=None, **kwargs)  # type: ignore[call-arg]

    message = str(exc_info.value)
    assert "TAKER_API_KEY" in message or "TAKER_API_SECRET" in message


@pytest.mark.parametrize(
    "kwargs",
    [
        {"TAKER_API_KEY": "   ", "TAKER_API_SECRET": "secret"},
        {"TAKER_API_KEY": "key", "TAKER_API_SECRET": "   "},
    ],
)
def test_blank_taker_credentials_rejected(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValidationError) as exc_info:
        TakerSettings(_env_file=None, **kwargs)  # type: ignore[call-arg]

    message = str(exc_info.value)
    assert "must not be blank" in message


def test_taker_only_env_file_keeps_working(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text('TAKER_API_KEY="taker-key"\nTAKER_API_SECRET="taker-secret"\n')

    settings = TakerSettings(_env_file=env_file)  # type: ignore[call-arg]

    assert settings.credentials().api_key == "taker-key"
    assert settings.credentials().api_secret.get_secret_value() == "taker-secret"


def test_rest_base_url_reads_shared_env_alias() -> None:
    settings = TakerSettings(  # type: ignore[call-arg]
        _env_file=None,
        TAKER_API_KEY="taker-key",
        TAKER_API_SECRET="taker-secret",
        REST_BASE_URL="https://api.coincall.com",
    )

    assert settings.rest_base_url == "https://api.coincall.com"
