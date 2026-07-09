import asyncio
from typing import Any, Literal

import pytest

from coincall_rfq_maker.adapters.rest import CoincallRestClient
from coincall_rfq_maker.adapters.schemas import (
    OptionInstrument,
    QuotePayload,
    RfqCreateResult,
    RfqListSnapshot,
    RfqPayload,
)
from coincall_rfq_maker.settings import Settings
from coincall_rfq_maker.taker import cli
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

RFQ = RfqPayload.model_validate(
    {
        "requestId": "r1",
        "state": "ACTIVE",
        "createTime": 1761650457000,
        "legs": [{"instrumentName": "BTCUSD-9JUL26-56000-C", "side": "BUY", "quantity": "0.2"}],
    }
)


class FakeRest:
    def __init__(self) -> None:
        self.created_legs: list[dict[str, str]] | None = None
        self.cancelled: list[str] = []
        self.rfq_list_calls: list[dict[str, Any]] = []

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

    async def get_rfq_list(
        self,
        request_id: str | None = None,
        state: Literal["OPEN", "CLOSED"] | None = None,
        role: Literal["TAKER", "MAKER"] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> RfqListSnapshot:
        self.rfq_list_calls.append({"state": state, "role": role})
        return RfqListSnapshot((RFQ,))


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
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake = FakeRest()
    client = TakerClient(fake)

    asyncio.run(cli._cmd_create_rfq(client, ["BTCUSD-9JUL26-56000-C:BUY:0.2"]))

    assert fake.created_legs == [
        {"instrumentName": "BTCUSD-9JUL26-56000-C", "side": "BUY", "qty": "0.2"}
    ]
    out = capsys.readouterr().out
    assert "requestId: REQ123" in out
    assert "next: rfq-taker quotes --request-id REQ123" in out


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


def test_rfqs_render_and_request_taker_role(capsys: pytest.CaptureFixture[str]) -> None:
    fake = FakeRest()

    asyncio.run(cli._cmd_rfqs(TakerClient(fake), "OPEN"))

    assert fake.rfq_list_calls == [{"state": "OPEN", "role": "TAKER"}]
    out = capsys.readouterr().out
    assert "requestId=r1" in out
    assert "BUY 0.2 BTCUSD-9JUL26-56000-C" in out


def test_cancel_rfq_confirms(capsys: pytest.CaptureFixture[str]) -> None:
    fake = FakeRest()

    asyncio.run(cli._cmd_cancel_rfq(TakerClient(fake), "r1"))

    assert fake.cancelled == ["r1"]
    assert "cancelled RFQ r1" in capsys.readouterr().out


def test_instruments_render_active_only(capsys: pytest.CaptureFixture[str]) -> None:
    asyncio.run(cli._cmd_instruments(TakerClient(FakeRest()), "BTC", True))

    out = capsys.readouterr().out
    assert "BTCUSD-9JUL26-56000-C" in out
    assert "symbolName" in out


def test_help_lists_safe_surface_and_no_trade(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        cli.parse_args(["--help"])

    out = capsys.readouterr().out
    for command in ("instruments", "create-rfq", "quotes", "cancel-rfq", "rfqs"):
        assert command in out
    assert "execute" not in out
    assert "trade" not in out
