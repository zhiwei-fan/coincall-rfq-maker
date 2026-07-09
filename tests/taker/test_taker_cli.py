import asyncio
from pathlib import Path
from typing import Any

import pytest

from coincall_rfq_maker.adapters.rest import CoincallRestClient
from coincall_rfq_maker.adapters.schemas import (
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
)
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker import cli, execute
from coincall_rfq_maker.taker.audit import AuditLog
from coincall_rfq_maker.taker.client import TakerClient

BETA_URL = "https://betaapi.coincall.com"
PROD_URL = "https://api.coincall.com"

INSTRUMENT = OptionInstrument.model_validate(
    {
        "symbolName": "BTCUSD-9JUL26-56000-C",
        "baseCurrency": "BTC",
        "strike": 56000.0,
        "expirationTimestamp": 1783584000000,
        "isActive": True,
        "minQty": 0.01,
        "tickSize": 1,
    }
)

QUOTE = QuotePayload.model_validate(
    {
        "quoteId": "q1",
        "requestId": "r1",
        "state": "OPEN",
        "createTime": 1,
        "expiryTime": 3,
        "legs": [
            {
                "instrumentName": "BTCUSD-9JUL26-56000-C",
                "side": "SELL",
                "price": "0.05",
                "quantity": "0.2",
            }
        ],
    }
)


class FakeRest:
    def __init__(self) -> None:
        self.created_legs: list[dict[str, str]] | None = None
        self.cancelled: list[str] = []

    async def get_option_instruments(self, base_currency: str) -> tuple[OptionInstrument, ...]:
        assert base_currency == "BTC"
        return (INSTRUMENT,)

    async def create_rfq(self, legs: list[dict[str, str]]) -> RfqCreateResult:
        self.created_legs = legs
        return RfqCreateResult.model_validate({"requestId": "REQ123", "state": "ACTIVE"})

    async def get_quotes_received(self, request_id: str | None = None) -> tuple[QuotePayload, ...]:
        return (QUOTE,)

    async def cancel_rfq(self, request_id: str) -> dict[str, Any]:
        self.cancelled.append(request_id)
        return {"code": 0, "data": {"cancelled": True}}


def _settings(**overrides: Any) -> Settings:
    base = {
        "_env_file": None,
        "API_KEY": "maker-key",
        "API_SECRET": "maker-secret",
        "REST_BASE_URL": BETA_URL,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


# -- safety gate ------------------------------------------------------------


def test_missing_taker_creds_hard_fails_with_no_fallback_message(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("TAKER_API_KEY", raising=False)
    monkeypatch.delenv("TAKER_API_SECRET", raising=False)
    settings = _settings()

    with pytest.raises(SystemExit) as exc_info:
        cli._build_taker_client(settings, allow_prod=False)

    assert exc_info.value.code == 2
    stderr = capsys.readouterr().err
    assert "TAKER_API_KEY and TAKER_API_SECRET must be set" in stderr
    assert "NOT fall back to the maker API_KEY/API_SECRET" in stderr


def test_non_beta_host_without_allow_prod_hard_fails(
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings(
        TAKER_API_KEY="taker-key", TAKER_API_SECRET="taker-secret", REST_BASE_URL=PROD_URL
    )

    with pytest.raises(SystemExit) as exc_info:
        cli._build_taker_client(settings, allow_prod=False)

    assert exc_info.value.code == 2
    assert PROD_URL in capsys.readouterr().err


def test_non_beta_host_with_allow_prod_builds_client() -> None:
    settings = _settings(
        TAKER_API_KEY="taker-key", TAKER_API_SECRET="taker-secret", REST_BASE_URL=PROD_URL
    )

    rest = cli._build_taker_client(settings, allow_prod=True)

    assert isinstance(rest, CoincallRestClient)


def test_beta_host_with_taker_creds_builds_client() -> None:
    settings = _settings(TAKER_API_KEY="taker-key", TAKER_API_SECRET="taker-secret")

    rest = cli._build_taker_client(settings, allow_prod=False)

    assert isinstance(rest, CoincallRestClient)


# -- command wiring (no network) --------------------------------------------


def test_create_rfq_preflights_legs_and_prints_request_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fake = FakeRest()
    client = TakerClient(fake)

    asyncio.run(
        execute._cmd_create_rfq(
            client,
            AuditLog(tmp_path / "audit.jsonl"),
            ["BTCUSD-9JUL26-56000-C:BUY:0.2"],
        )
    )

    assert fake.created_legs == [
        {"instrumentName": "BTCUSD-9JUL26-56000-C", "side": "BUY", "qty": "0.2"}
    ]
    out = capsys.readouterr().out
    assert "requestId: REQ123" in out
    assert "next: rfq-taker quotes --request-id REQ123" in out
    audit_text = (tmp_path / "audit.jsonl").read_text()
    assert "create_attempt" in audit_text
    assert "create_rfq" in audit_text


def test_quotes_render_sample_payload(capsys: pytest.CaptureFixture[str]) -> None:
    client = TakerClient(FakeRest())

    asyncio.run(cli._cmd_quotes(client, "r1"))

    out = capsys.readouterr().out
    assert "quoteId=q1" in out
    assert "BTCUSD-9JUL26-56000-C" in out
    assert "SELL" in out


def test_quotes_render_empty(capsys: pytest.CaptureFixture[str]) -> None:
    class EmptyRest(FakeRest):
        async def get_quotes_received(
            self, request_id: str | None = None
        ) -> tuple[QuotePayload, ...]:
            return ()

    asyncio.run(cli._cmd_quotes(TakerClient(EmptyRest()), "r1"))

    assert "No quotes received yet." in capsys.readouterr().out


def test_cancel_rfq_confirms(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    fake = FakeRest()

    asyncio.run(
        execute._cmd_cancel_rfq(TakerClient(fake), AuditLog(tmp_path / "audit.jsonl"), "r1")
    )

    assert fake.cancelled == ["r1"]
    assert "cancelled RFQ r1" in capsys.readouterr().out
    audit_text = (tmp_path / "audit.jsonl").read_text()
    assert "cancel_attempt" in audit_text
    assert "cancel_rfq" in audit_text


def test_instruments_render_active_only(capsys: pytest.CaptureFixture[str]) -> None:
    asyncio.run(cli._cmd_instruments(TakerClient(FakeRest()), "BTC", True))

    out = capsys.readouterr().out
    assert "BTCUSD-9JUL26-56000-C" in out
    assert "symbolName" in out


def test_help_lists_full_surface_and_drops_rfqs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(["--help"])

    out = capsys.readouterr().out
    for command in ("instruments", "create-rfq", "quotes", "cancel-rfq", "execute", "trade"):
        assert command in out
    # The rfqs verb was removed (rfqList 404s for takers); the subcommand line
    # must not offer it.
    subcommands = out.split("...", 1)[0]
    assert "rfqs" not in subcommands


def test_parse_args_rejects_non_positive_poll() -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.parse_args(["trade", "--leg", "BTCUSD-9JUL26-56000-C:BUY:0.2", "--poll", "0"])

    assert exc_info.value.code == 2


def test_parse_args_rejects_non_positive_timeout() -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.parse_args(["trade", "--leg", "BTCUSD-9JUL26-56000-C:BUY:0.2", "--timeout", "-1"])

    assert exc_info.value.code == 2


def test_parse_args_accepts_positive_poll() -> None:
    args = cli.parse_args(["trade", "--leg", "BTCUSD-9JUL26-56000-C:BUY:0.2", "--poll", "0.1"])

    assert args.poll == 0.1
