from pathlib import Path

import pytest
from pydantic import ValidationError

from coincall_rfq_maker.settings import Settings


def test_missing_api_key_and_secret_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("API_SECRET", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_blank_api_key_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, API_KEY="   ", API_SECRET="secret")  # type: ignore[call-arg]


def test_valid_settings_apply_defaults() -> None:
    settings = Settings(_env_file=None, API_KEY="key", API_SECRET="secret")  # type: ignore[call-arg]
    assert settings.maker_credentials().api_key == "key"
    assert settings.maker_credentials().api_secret.get_secret_value() == "secret"
    assert settings.taker_credentials() is None
    assert settings.dry_run is True
    assert settings.cancel_all_on_start is True
    assert settings.cancel_all_on_stop is True
    assert settings.rest_base_url == "https://betaapi.coincall.com"
    assert settings.ws_url == "wss://betaws.seizeyouralpha.com/options"
    assert settings.heartbeat_interval_seconds == 5.0
    assert settings.pricing_refresh_seconds == 5.0
    assert settings.quote_refresh_seconds == 10.0
    assert settings.price_move_threshold == 0.001
    assert settings.bid_vol == 0.20
    assert settings.ask_vol == 2.00
    assert settings.risk_free_rate == 0.05
    assert settings.stale_market_data_seconds == 30.0
    assert settings.db_path == "rfq_maker.db"


def test_dry_run_defaults_true_even_when_other_fields_overridden() -> None:
    settings = Settings(_env_file=None, API_KEY="key", API_SECRET="secret", MAX_LEG_QTY=5)  # type: ignore[call-arg]
    assert settings.dry_run is True
    assert settings.max_leg_qty == 5


def test_heartbeat_interval_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, API_KEY="key", API_SECRET="secret", HEARTBEAT_INTERVAL_SECONDS=0)  # type: ignore[call-arg]


def test_taker_credentials_present_return_typed_pair() -> None:
    settings = Settings(
        _env_file=None,
        API_KEY="maker-key",
        API_SECRET="maker-secret",
        TAKER_API_KEY="taker-key",
        TAKER_API_SECRET="taker-secret",
    )  # type: ignore[call-arg]

    credentials = settings.taker_credentials()

    assert credentials is not None
    assert credentials.api_key == "taker-key"
    assert credentials.api_secret.get_secret_value() == "taker-secret"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"TAKER_API_KEY": "taker-key"},
        {"TAKER_API_SECRET": "taker-secret"},
    ],
)
def test_taker_credentials_must_be_both_or_neither(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None, API_KEY="key", API_SECRET="secret", **kwargs)  # type: ignore[call-arg]

    message = str(exc_info.value)
    assert "TAKER_API_KEY" in message
    assert "TAKER_API_SECRET" in message


def test_maker_only_env_file_keeps_working(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text('API_KEY="maker-key"\nAPI_SECRET="maker-secret"\n')

    settings = Settings(_env_file=env_file)  # type: ignore[call-arg]

    assert settings.maker_credentials().api_key == "maker-key"
    assert settings.maker_credentials().api_secret.get_secret_value() == "maker-secret"
    assert settings.taker_credentials() is None


def test_blank_taker_placeholders_behave_as_absent() -> None:
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        API_KEY="maker-key",
        API_SECRET="maker-secret",
        TAKER_API_KEY="",
        TAKER_API_SECRET="   ",
    )
    assert settings.taker_credentials() is None


def test_blank_taker_key_with_real_secret_still_both_or_neither_error() -> None:
    with pytest.raises(ValidationError, match="TAKER_API_KEY and TAKER_API_SECRET"):
        Settings(  # type: ignore[call-arg]
            _env_file=None,
            API_KEY="maker-key",
            API_SECRET="maker-secret",
            TAKER_API_KEY="",
            TAKER_API_SECRET="real-secret",
        )
