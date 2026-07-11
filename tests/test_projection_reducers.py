from coincall_rfq_maker.domain.quote import Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.orchestration import RFQ_TOMBSTONE_CAPACITY, RfqStore
from coincall_rfq_maker.quoting.store import QuoteStore


def make_rfq(request_id: str, instruments: tuple[str, ...] = ("A",)) -> Rfq:
    return Rfq(
        request_id=request_id,
        status=RfqStatus.ACTIVE,
        legs=tuple(RfqLeg(name, Side.BUY, "1") for name in instruments),
        create_time_ms=0,
        expiry_time_ms=1,
    )


def make_quote(quote_id: str, stage: QuoteStage = QuoteStage.OPEN) -> Quote:
    return Quote(
        request_id="rfq-1",
        stage=stage,
        legs=(QuoteLeg("A", 1.0),),
        create_time_ms=0,
        quote_id=quote_id,
    )


def test_rfq_store_refuses_in_place_resurrection() -> None:
    store = RfqStore()
    store.upsert(make_rfq("rfq-1"))
    terminal = store.mark_terminal("rfq-1", RfqStatus.CANCELLED, now_ms=1)

    store.upsert(make_rfq("rfq-1"))

    assert store.get("rfq-1") is terminal
    assert store.active() == []


def test_rfq_store_refuses_resurrection_after_eviction() -> None:
    store = RfqStore()
    store.upsert(make_rfq("rfq-1"))
    store.mark_terminal("rfq-1", RfqStatus.CANCELLED, now_ms=1)
    store.evict("rfq-1")

    store.upsert(make_rfq("rfq-1"))

    assert store.get("rfq-1") is None


def test_rfq_store_tombstones_are_fifo_bounded() -> None:
    assert RFQ_TOMBSTONE_CAPACITY == 10_000
    store = RfqStore(tombstone_capacity=3)
    for request_id in ("oldest", "second", "third", "newest"):
        store.evict(request_id)

    store.upsert(make_rfq("oldest"))
    for request_id in ("second", "third", "newest"):
        store.upsert(make_rfq(request_id))

    assert store.get("oldest") is not None
    assert [store.get(request_id) for request_id in ("second", "third", "newest")] == [
        None,
        None,
        None,
    ]


def test_rfq_store_removes_stale_instrument_holders() -> None:
    store = RfqStore()
    store.upsert(make_rfq("rfq-1", ("A", "B")))

    store.upsert(make_rfq("rfq-1", ("A", "C")))

    assert store._instrument_holders["A"] == {"rfq-1"}
    assert "B" not in store._instrument_holders
    assert store._instrument_holders["C"] == {"rfq-1"}


def test_quote_store_refuses_historical_pointer_theft() -> None:
    store = QuoteStore()
    first = make_quote("Q1")
    second = make_quote("Q2")
    cancelled_first = make_quote("Q1", QuoteStage.CANCELLED)
    store.store(first)
    store.store(second)

    store.store(cancelled_first)

    assert store.get_for_rfq("rfq-1") is second
    assert store.get_by_quote_id("Q1") is cancelled_first


def test_quote_store_new_generation_takes_current_pointer() -> None:
    store = QuoteStore()
    store.store(make_quote("Q1"))
    store.store(make_quote("Q2"))
    third = make_quote("Q3")

    store.store(third)

    assert store.get_for_rfq("rfq-1") is third


def test_quote_store_terminal_current_quote_is_absorbing() -> None:
    store = QuoteStore()
    filled = make_quote("Q2", QuoteStage.FILLED)
    store.store(filled)

    store.store(make_quote("Q2", QuoteStage.OPEN))

    assert store.get_for_rfq("rfq-1") is filled
    assert store.get_by_quote_id("Q2") is filled
