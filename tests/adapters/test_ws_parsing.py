import json

from coincall_rfq_maker.adapters.ws import parse_ws_message
from coincall_rfq_maker.domain.quote import QuoteStage
from coincall_rfq_maker.domain.rfq import RfqStatus, Side
from coincall_rfq_maker.events import QuoteUpdated, RfqReceived, RfqTerminated, TradeExecuted


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


def test_dt_22_block_trade_detail_produces_trade_executed() -> None:
    raw = json.dumps(
        {"dt": 22, "d": {"blockTradeId": "bt-1", "quoteId": "q-1", "requestId": "rfq-1"}}
    )
    event = parse_ws_message(raw)
    assert isinstance(event, TradeExecuted)
    assert event.block_trade_id == "bt-1"
    assert event.quote_id == "q-1"


def test_dt_23_block_trade_public_ignored() -> None:
    raw = json.dumps({"dt": 23, "d": {"instrumentName": "BTCUSD-21AUG25-120000-C"}})
    assert parse_ws_message(raw) is None


def test_unknown_dt_code_ignored() -> None:
    raw = json.dumps({"dt": 9999, "d": {}})
    assert parse_ws_message(raw) is None


def test_subscription_ack_ignored() -> None:
    raw = json.dumps({"action": "SUBSCRIBE", "result": "success"})
    assert parse_ws_message(raw) is None


def test_heartbeat_ignored() -> None:
    raw = json.dumps({"c": 11, "rc": 1})
    assert parse_ws_message(raw) is None


def test_non_json_message_ignored() -> None:
    assert parse_ws_message("not json{{{") is None
