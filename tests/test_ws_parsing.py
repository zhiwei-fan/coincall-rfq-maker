import asyncio
import json
import logging
from types import TracebackType
from typing import Any, Self
from urllib.parse import parse_qs, urlparse

import pytest

import coincall_rfq_maker.ws as ws
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import RfqStatus, Side
from coincall_rfq_maker.events import QuoteUpdated, RfqReceived, RfqTerminated, TradeExecuted
from coincall_rfq_maker.ws import CoincallWsClient, parse_ws_message


def test_dt_28_active_rfq_produces_rfq_received() -> None:
    raw = json.dumps(
        {
            "dt": 28,
            "d": {
                "requestId": "rfq-1",
                "state": "ACTIVE",
                "legs": [
                    {"instrumentName": "BTCUSD-21AUG25-120000-C", "side": "BUY", "quantity": "1"}
                ],
                "createTime": 1000,
                "expiryTime": 2000,
            },
        }
    )
    event = parse_ws_message(raw)
    assert isinstance(event, RfqReceived)
    assert event.rfq.request_id == "rfq-1"
    assert event.rfq.status is RfqStatus.ACTIVE
    assert event.rfq.legs[0].side is Side.BUY


def test_dt_28_terminal_state_produces_rfq_terminated() -> None:
    raw = json.dumps({"dt": 28, "d": {"requestId": "rfq-1", "state": "CANCELLED"}})
    event = parse_ws_message(raw)
    assert isinstance(event, RfqTerminated)
    assert event.request_id == "rfq-1"
    assert event.status is RfqStatus.CANCELLED


def test_dt_28_unknown_state_ignored() -> None:
    raw = json.dumps({"dt": 28, "d": {"requestId": "rfq-1", "state": "SOMETHING_NEW"}})
    assert parse_ws_message(raw) is None


def test_dt_20_quote_update_produces_quote_updated() -> None:
    raw = json.dumps(
        {
            "dt": 20,
            "d": {
                "quoteId": "q-1",
                "requestId": "rfq-1",
                "state": "FILLED",
                "filledPrice": 22.5,
                "filledQuantity": 1.0,
                "blockTradeId": "bt-1",
            },
        }
    )
    event = parse_ws_message(raw)
    assert isinstance(event, QuoteUpdated)
    assert event.quote_id == "q-1"
    assert event.stage is QuoteStage.FILLED
    assert event.filled_price == 22.5
    assert event.block_trade_id == "bt-1"


def test_dt_130_quote_push_matches_dt_20_and_is_not_unknown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        "quoteId": "q-1",
        "requestId": "rfq-1",
        "state": "FILLED",
        "filledPrice": 22.5,
        "filledQuantity": 1.0,
        "blockTradeId": "bt-1",
    }

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.ws"):
        dt_20 = parse_ws_message(json.dumps({"dt": 20, "d": payload}))
        dt_130 = parse_ws_message(json.dumps({"dt": 130, "d": payload}))

    assert dt_130 == dt_20
    assert isinstance(dt_130, QuoteUpdated)
    assert "Unknown WS dt code" not in caplog.text


def test_malformed_dt_20_quote_update_ignored() -> None:
    raw = json.dumps({"dt": 20, "d": {"requestId": "rfq-1", "state": "FILLED"}})

    assert parse_ws_message(raw) is None


def test_dt_130_and_dt_20_well_formed_capture_debug_frame(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        "quoteId": "q-1",
        "requestId": "rfq-1",
        "state": "FILLED",
        "filledPrice": 22.5,
        "filledQuantity": 1.0,
        "blockTradeId": "bt-1",
    }

    with caplog.at_level(logging.DEBUG, logger="coincall_rfq_maker.ws"):
        parse_ws_message(json.dumps({"dt": 20, "d": payload}))
        parse_ws_message(json.dumps({"dt": 130, "d": payload}))

    dt_20_records = [r for r in caplog.records if r.message.startswith("WS quote frame dt=20 ")]
    dt_130_records = [r for r in caplog.records if r.message.startswith("WS quote frame dt=130 ")]
    assert len(dt_20_records) == 1
    assert len(dt_130_records) == 1
    assert '"quoteId":"q-1"' in dt_20_records[0].message
    assert '"quoteId":"q-1"' in dt_130_records[0].message


def test_malformed_dt_130_quote_push_warns_with_dt_and_captures_full_frame(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = json.dumps(
        {
            "dt": 130,
            "d": {
                "requestId": "rfq-1",
                "state": "FILLED",
                "note": "x" * 300,
            },
        }
    )

    with caplog.at_level(logging.DEBUG, logger="coincall_rfq_maker.ws"):
        assert parse_ws_message(raw) is None

    assert "Malformed quote WS payload (dt=130)" in caplog.text
    assert "Unknown WS dt code" not in caplog.text

    capture_records = [r for r in caplog.records if r.message.startswith("WS quote frame dt=130")]
    assert len(capture_records) == 1
    assert ("x" * 300) in capture_records[0].message


def test_sanitizer_redacts_nested_api_key_in_captured_frame(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = json.dumps(
        {
            "dt": 130,
            "d": {
                "quoteId": "q-1",
                "requestId": "rfq-1",
                "state": "FILLED",
                "filledPrice": 22.5,
                "filledQuantity": 1.0,
                "blockTradeId": "bt-1",
                "apiKey": "secret-key",
            },
        }
    )

    with caplog.at_level(logging.DEBUG, logger="coincall_rfq_maker.ws"):
        parse_ws_message(raw)

    assert "secret-key" not in caplog.text
    assert "<redacted>" in caplog.text


def test_uuid_is_redacted_from_query_params_and_payloads() -> None:
    secret = "uuid-secret"

    assert secret not in ws._redact_ws_exception_message(f"?uuid={secret}&sign=signature-secret")
    assert ws._sanitize_payload({"uuid": secret}) == {"uuid": "<redacted>"}


def test_non_json_message_redacts_sensitive_query_params(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = "bad frame ?uuid=uuid-secret&sign=signature-secret"

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.ws"):
        assert parse_ws_message(raw) is None

    assert "uuid-secret" not in caplog.text
    assert "signature-secret" not in caplog.text
    assert "uuid=<redacted>" in caplog.text
    assert "sign=<redacted>" in caplog.text


def test_malformed_envelope_error_redacts_sensitive_query_params(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # pydantic v2 embeds input values in the error text; a bad dt carrying a signed
    # query string must not leak it into the log.
    raw = json.dumps({"dt": "?uuid=uuid-secret&sign=signature-secret"})

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.ws"):
        assert parse_ws_message(raw) is None

    assert "Malformed WS envelope" in caplog.text
    assert "uuid-secret" not in caplog.text
    assert "signature-secret" not in caplog.text


def test_no_dt_message_logs_sanitized_frame(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = json.dumps({"uuid": "uuid-secret", "other": "value"})

    with caplog.at_level(logging.DEBUG, logger="coincall_rfq_maker.ws"):
        assert parse_ws_message(raw) is None

    assert "uuid-secret" not in caplog.text
    assert '"uuid":"<redacted>"' in caplog.text
    assert "{'uuid': 'uuid-secret'" not in caplog.text


def test_captured_frame_over_cap_truncates_message_but_reports_full_length(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        "quoteId": "q-1",
        "requestId": "rfq-1",
        "state": "FILLED",
        "filledPrice": 22.5,
        "filledQuantity": 1.0,
        "blockTradeId": "bt-1",
        "note": "y" * 9000,
    }
    raw = json.dumps({"dt": 130, "d": payload})

    with caplog.at_level(logging.DEBUG, logger="coincall_rfq_maker.ws"):
        parse_ws_message(raw)

    capture_records = [r for r in caplog.records if r.message.startswith("WS quote frame dt=130")]
    assert len(capture_records) == 1
    message = capture_records[0].message

    prefix = "WS quote frame dt=130 ("
    assert message.startswith(prefix)
    size_str, rendered = message[len(prefix) :].split(" chars): ", 1)
    reported_size = int(size_str)

    assert reported_size > ws._MAX_FRAME_CHARS
    assert len(rendered) <= ws._MAX_FRAME_CHARS
    assert ("y" * 9000) not in rendered


def test_dt_23_block_trade_public_does_not_emit_quote_frame_capture(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = json.dumps({"dt": 23, "d": {"instrumentName": "BTCUSD-21AUG25-120000-C"}})

    with caplog.at_level(logging.DEBUG, logger="coincall_rfq_maker.ws"):
        assert parse_ws_message(raw) is None

    assert not any(record.message.startswith("WS quote frame") for record in caplog.records)


def test_dt_22_block_trade_detail_produces_trade_executed() -> None:
    raw = json.dumps(
        {
            "dt": 22,
            "d": {
                "blockTradeId": "bt-1",
                "quoteId": "q-1",
                "requestId": "rfq-1",
                "filledPrice": 22.5,
                "filledQuantity": 1.0,
                "fillTime": 123456,
            },
        }
    )
    event = parse_ws_message(raw)
    assert isinstance(event, TradeExecuted)
    assert event.block_trade_id == "bt-1"
    assert event.quote_id == "q-1"
    assert event.filled_price == 22.5
    assert event.filled_quantity == 1.0
    assert event.fill_time_ms == 123456


def test_dt_22_block_trade_detail_parses_integer_ids() -> None:
    raw = json.dumps(
        {
            "dt": 22,
            "d": {
                "blockTradeId": 2075207494989787138,
                "quoteId": 2075207494989787139,
                "requestId": 2075207481654804480,
                "filledPrice": 8467.04,
                "filledQuantity": 0.01,
                "fillTime": 1783602996739,
            },
        }
    )

    event = parse_ws_message(raw)

    assert isinstance(event, TradeExecuted)
    assert event.block_trade_id == "2075207494989787138"
    assert event.quote_id == "2075207494989787139"
    assert event.request_id == "2075207481654804480"
    assert event.filled_price == 8467.04
    assert event.filled_quantity == 0.01
    assert event.fill_time_ms == 1783602996739


def test_malformed_dt_22_block_trade_detail_ignored() -> None:
    raw = json.dumps({"dt": 22, "d": {"quoteId": "q-1", "requestId": "rfq-1"}})

    assert parse_ws_message(raw) is None


def test_dt_23_block_trade_public_ignored() -> None:
    raw = json.dumps({"dt": 23, "d": {"instrumentName": "BTCUSD-21AUG25-120000-C"}})
    assert parse_ws_message(raw) is None


def test_unknown_dt_code_ignored_but_payload_is_captured_at_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = json.dumps({"dt": 9999, "d": {"foo": "bar"}})

    with caplog.at_level(logging.WARNING, logger="coincall_rfq_maker.ws"):
        assert parse_ws_message(raw) is None

    assert "Unknown WS dt code 9999" in caplog.text
    assert '"foo":"bar"' in caplog.text


def test_subscription_ack_ignored() -> None:
    raw = json.dumps({"action": "SUBSCRIBE", "result": "success"})
    assert parse_ws_message(raw) is None


def test_heartbeat_ignored() -> None:
    raw = json.dumps({"c": 11, "rc": 1})
    assert parse_ws_message(raw) is None


def test_non_json_message_ignored() -> None:
    assert parse_ws_message("not json{{{") is None


def test_non_object_json_message_ignored() -> None:
    assert parse_ws_message(json.dumps([])) is None


class ShutdownQueue:
    async def put(self, event: object) -> None:
        raise asyncio.QueueShutDown


class FakeConnection:
    def __init__(self) -> None:
        self.recv_calls = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass

    async def send(self, message: str) -> None:
        pass

    async def recv(self) -> str:
        self.recv_calls += 1
        return json.dumps({"dt": 28, "d": {"requestId": "rfq-1", "state": "CANCELLED"}})


class BlockingConnection:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        await asyncio.sleep(3600)
        return ""


class StopAfterSubscribeConnection:
    def __init__(self, shutdown: asyncio.Event) -> None:
        self._shutdown = shutdown
        self.sent: list[str] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        self._shutdown.set()
        raise RuntimeError("second connection closed")


async def wait_until(predicate: Any, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_queue_shutdown_while_putting_event_exits_read_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection()

    async def fake_connect(*args: Any, **kwargs: Any) -> FakeConnection:
        return connection

    monkeypatch.setattr(ws.websockets, "connect", fake_connect)
    client = CoincallWsClient("wss://example.test/ws", "key", "secret", ShutdownQueue())  # type: ignore[arg-type]

    await client._connect_and_read(asyncio.Event())

    assert connection.recv_calls == 1


@pytest.mark.asyncio
async def test_heartbeat_sent_after_subscribe_and_stops_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = BlockingConnection()

    async def fake_connect(*args: Any, **kwargs: Any) -> BlockingConnection:
        return connection

    monkeypatch.setattr(ws.websockets, "connect", fake_connect)
    client = CoincallWsClient(
        "wss://example.test/ws",
        "key",
        "secret",
        asyncio.Queue(),
        heartbeat_interval_seconds=0.01,
    )
    shutdown = asyncio.Event()

    task = asyncio.create_task(client._connect_and_read(shutdown))
    await wait_until(lambda: len(connection.sent) >= len(ws.SUBSCRIPTIONS) + 2)

    sent = [json.loads(message) for message in connection.sent]
    assert sent[: len(ws.SUBSCRIPTIONS)] == [
        {"action": "subscribe", "dataType": data_type} for data_type in ws.SUBSCRIPTIONS
    ]
    heartbeat_frames = sent[len(ws.SUBSCRIPTIONS) :]
    assert len(heartbeat_frames) >= 2
    assert all(frame == {"action": "heartbeat"} for frame in heartbeat_frames)

    shutdown.set()
    await asyncio.wait_for(task, timeout=1.0)
    stopped_at = len(connection.sent)
    await asyncio.sleep(0.03)

    assert len(connection.sent) == stopped_at


@pytest.mark.asyncio
async def test_run_redacts_signed_url_from_connection_error_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    shutdown = asyncio.Event()
    captured_url = ""

    async def fake_connect(url: str, *args: Any, **kwargs: Any) -> BlockingConnection:
        nonlocal captured_url
        captured_url = url
        shutdown.set()
        raise RuntimeError(f"bad signed URL: {url}")

    monkeypatch.setattr(ws.websockets, "connect", fake_connect)
    client = CoincallWsClient("wss://example.test/ws", "maker-key", "secret", asyncio.Queue())

    with caplog.at_level(logging.ERROR, logger="coincall_rfq_maker.ws"):
        await client.run(shutdown)

    signed_query = urlparse(captured_url).query
    values = parse_qs(signed_query)
    assert captured_url
    assert captured_url not in caplog.text
    assert signed_query not in caplog.text
    assert "maker-key" not in caplog.text
    assert values["sign"][0] not in caplog.text
    assert "apiKey=maker-key" not in caplog.text


@pytest.mark.asyncio
async def test_ws_disconnect_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shutdown = asyncio.Event()
    connect_attempts = 0

    async def fake_connect(*args: Any, **kwargs: Any) -> StopAfterSubscribeConnection:
        nonlocal connect_attempts
        connect_attempts += 1
        if connect_attempts == 1:
            raise RuntimeError("first connection dropped")
        return StopAfterSubscribeConnection(shutdown)

    monkeypatch.setattr(ws.websockets, "connect", fake_connect)
    monkeypatch.setattr(ws, "_INITIAL_RECONNECT_DELAY_SECONDS", 0.001)
    monkeypatch.setattr(ws, "_MAX_RECONNECT_DELAY_SECONDS", 0.001)
    client = CoincallWsClient("wss://example.test/ws", "key", "secret", asyncio.Queue())

    await client.run(shutdown)

    assert connect_attempts == 2


@pytest.mark.asyncio
async def test_supervisor_cancellation_cancels_child_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = CoincallWsClient(
        "wss://example.test/ws",
        "key",
        "secret",
        asyncio.Queue(),
        heartbeat_interval_seconds=0.01,
    )
    connection = BlockingConnection()
    shutdown = asyncio.Event()
    read_started = asyncio.Event()
    heartbeat_started = asyncio.Event()
    child_tasks: list[asyncio.Task[Any]] = []

    async def fake_read_loop(connection: BlockingConnection, shutdown: asyncio.Event) -> None:
        task = asyncio.current_task()
        assert task is not None
        child_tasks.append(task)
        read_started.set()
        await asyncio.sleep(3600)

    async def fake_heartbeat_loop(connection: BlockingConnection, shutdown: asyncio.Event) -> None:
        task = asyncio.current_task()
        assert task is not None
        child_tasks.append(task)
        heartbeat_started.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(client, "_read_loop", fake_read_loop)
    monkeypatch.setattr(client, "_heartbeat_loop", fake_heartbeat_loop)

    supervisor = asyncio.create_task(client._supervise_connection(connection, shutdown))
    await asyncio.wait_for(read_started.wait(), timeout=1.0)
    await asyncio.wait_for(heartbeat_started.wait(), timeout=1.0)

    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(supervisor, timeout=1.0)

    assert len(child_tasks) == 2
    assert all(task.done() for task in child_tasks)
