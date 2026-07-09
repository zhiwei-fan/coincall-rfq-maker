import aiosqlite
import pytest

from coincall_rfq_maker.domain.quote import Quote, QuoteLeg, QuoteStage
from coincall_rfq_maker.domain.rfq import Rfq, RfqLeg, RfqStatus, Side
from coincall_rfq_maker.events import TradeExecuted
from coincall_rfq_maker.persistence.store import PersistenceStore

INSTRUMENT = "BTCUSD-21AUG25-120000-C"


@pytest.mark.asyncio
async def test_rfq_round_trip(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = str(tmp_path / "test.db")
    rfq = Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(RfqLeg(instrument_name=INSTRUMENT, side=Side.BUY, quantity="1"),),
        create_time_ms=0,
        expiry_time_ms=1_000,
    )
    async with PersistenceStore(db_path) as store:
        await store.record_rfq(rfq, now_ms=100)
        history = await store.fetch_rfq_history("rfq-1")
    assert len(history) == 1
    assert history[0]["request_id"] == "rfq-1"
    assert history[0]["status"] == "ACTIVE"
    assert history[0]["legs"][0]["instrumentName"] == INSTRUMENT


@pytest.mark.asyncio
async def test_quote_round_trip_with_market_snapshot(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = str(tmp_path / "test.db")
    quote = Quote(
        request_id="rfq-1",
        stage=QuoteStage.OPEN,
        legs=(QuoteLeg(instrument_name=INSTRUMENT, price=22.5),),
        create_time_ms=0,
        quote_id="q-1",
        filled_price=22.5,
        filled_quantity=1.0,
        fill_time_ms=123456,
    )
    async with PersistenceStore(db_path) as store:
        await store.record_quote(quote, {"BTCUSD": 50_000.0}, now_ms=100)
        history = await store.fetch_quote_history("rfq-1")
    assert len(history) == 1
    assert history[0]["quote_id"] == "q-1"
    assert history[0]["market_snapshot"] == {"BTCUSD": 50_000.0}
    assert history[0]["filled_price"] == 22.5
    assert history[0]["filled_quantity"] == 1.0
    assert history[0]["fill_time_ms"] == 123456


@pytest.mark.asyncio
async def test_fill_round_trip(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = str(tmp_path / "test.db")
    event = TradeExecuted(
        block_trade_id="bt-1",
        quote_id="q-1",
        request_id="rfq-1",
        filled_price=22.5,
        filled_quantity=1.0,
        fill_time_ms=123456,
    )
    async with PersistenceStore(db_path) as store:
        await store.record_fill(event, now_ms=100)
        fills = await store.fetch_fills()
    assert len(fills) == 1
    assert fills[0]["block_trade_id"] == "bt-1"
    assert fills[0]["filled_price"] == 22.5
    assert fills[0]["filled_quantity"] == 1.0
    assert fills[0]["fill_time_ms"] == 123456


@pytest.mark.asyncio
async def test_existing_database_missing_fill_columns_is_migrated(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = str(tmp_path / "test.db")
    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(
            """
            CREATE TABLE quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id TEXT,
                request_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                legs_json TEXT NOT NULL,
                market_snapshot_json TEXT,
                recorded_at_ms INTEGER NOT NULL
            );
            CREATE TABLE fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_trade_id TEXT NOT NULL,
                quote_id TEXT NOT NULL,
                request_id TEXT,
                recorded_at_ms INTEGER NOT NULL
            );
            """
        )
        await conn.commit()

    quote = Quote(
        request_id="rfq-1",
        stage=QuoteStage.FILLED,
        legs=(QuoteLeg(instrument_name=INSTRUMENT, price=22.5),),
        create_time_ms=0,
        quote_id="q-1",
        filled_price=22.5,
        filled_quantity=1.0,
        fill_time_ms=123456,
    )
    fill = TradeExecuted(
        block_trade_id="bt-1",
        quote_id="q-1",
        request_id="rfq-1",
        filled_price=22.5,
        filled_quantity=1.0,
        fill_time_ms=123456,
    )

    async with PersistenceStore(db_path) as store:
        await store.record_quote(quote, None, now_ms=100)
        await store.record_fill(fill, now_ms=101)
        history = await store.fetch_quote_history("rfq-1")
        fills = await store.fetch_fills()

    assert history[0]["filled_price"] == 22.5
    assert fills[0]["filled_quantity"] == 1.0


@pytest.mark.asyncio
async def test_append_style_keeps_full_history(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = str(tmp_path / "test.db")
    rfq = Rfq(
        request_id="rfq-1",
        status=RfqStatus.ACTIVE,
        legs=(),
        create_time_ms=0,
        expiry_time_ms=1_000,
    )
    async with PersistenceStore(db_path) as store:
        await store.record_rfq(rfq, now_ms=100)
        terminal = rfq.with_status(RfqStatus.FILLED, last_update_time_ms=200)
        await store.record_rfq(terminal, now_ms=200)
        history = await store.fetch_rfq_history("rfq-1")
    assert len(history) == 2
    assert history[0]["status"] == "ACTIVE"
    assert history[1]["status"] == "FILLED"
