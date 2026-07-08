"""Append-style audit persistence.

Every call inserts a new row rather than updating in place, so the tables
are a full history of RFQ/quote/fill state changes, not just current state.
Quotes are stored together with the market snapshot that priced them.
"""

import json
from types import TracebackType
from typing import Any, Self

import aiosqlite

from coincall_rfq_maker.domain.quote import Quote
from coincall_rfq_maker.domain.rfq import Rfq
from coincall_rfq_maker.events import TradeExecuted

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rfqs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    status TEXT NOT NULL,
    stage TEXT NOT NULL,
    legs_json TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS quotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_id TEXT,
    request_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    legs_json TEXT NOT NULL,
    market_snapshot_json TEXT,
    recorded_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    block_trade_id TEXT NOT NULL,
    quote_id TEXT NOT NULL,
    request_id TEXT,
    recorded_at_ms INTEGER NOT NULL
);
"""


class PersistenceStore:
    """Async aiosqlite-backed audit log. Use as an async context manager."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> Self:
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError(
                "PersistenceStore not started; use 'async with PersistenceStore(...)'"
            )
        return self._conn

    async def record_rfq(self, rfq: Rfq, now_ms: int) -> None:
        legs = [
            {
                "instrumentName": leg.instrument_name,
                "side": leg.side.value,
                "quantity": leg.quantity,
            }
            for leg in rfq.legs
        ]
        await self.connection.execute(
            "INSERT INTO rfqs (request_id, status, stage, legs_json, recorded_at_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (rfq.request_id, rfq.status.value, rfq.stage.value, json.dumps(legs), now_ms),
        )
        await self.connection.commit()

    async def record_quote(
        self, quote: Quote, market_snapshot: dict[str, float] | None, now_ms: int
    ) -> None:
        legs = [{"instrumentName": leg.instrument_name, "price": leg.price} for leg in quote.legs]
        await self.connection.execute(
            "INSERT INTO quotes "
            "(quote_id, request_id, stage, legs_json, market_snapshot_json, recorded_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                quote.quote_id,
                quote.request_id,
                quote.stage.value,
                json.dumps(legs),
                json.dumps(market_snapshot) if market_snapshot is not None else None,
                now_ms,
            ),
        )
        await self.connection.commit()

    async def record_fill(self, event: TradeExecuted, now_ms: int) -> None:
        await self.connection.execute(
            "INSERT INTO fills (block_trade_id, quote_id, request_id, recorded_at_ms) "
            "VALUES (?, ?, ?, ?)",
            (event.block_trade_id, event.quote_id, event.request_id, now_ms),
        )
        await self.connection.commit()

    async def fetch_rfq_history(self, request_id: str) -> list[dict[str, Any]]:
        cursor = await self.connection.execute(
            "SELECT request_id, status, stage, legs_json, recorded_at_ms FROM rfqs "
            "WHERE request_id = ? ORDER BY id",
            (request_id,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "request_id": row[0],
                "status": row[1],
                "stage": row[2],
                "legs": json.loads(row[3]),
                "recorded_at_ms": row[4],
            }
            for row in rows
        ]

    async def fetch_quote_history(self, request_id: str) -> list[dict[str, Any]]:
        cursor = await self.connection.execute(
            "SELECT quote_id, request_id, stage, legs_json, market_snapshot_json, recorded_at_ms "
            "FROM quotes WHERE request_id = ? ORDER BY id",
            (request_id,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "quote_id": row[0],
                "request_id": row[1],
                "stage": row[2],
                "legs": json.loads(row[3]),
                "market_snapshot": json.loads(row[4]) if row[4] else None,
                "recorded_at_ms": row[5],
            }
            for row in rows
        ]

    async def fetch_fills(self) -> list[dict[str, Any]]:
        cursor = await self.connection.execute(
            "SELECT block_trade_id, quote_id, request_id, recorded_at_ms FROM fills ORDER BY id"
        )
        rows = await cursor.fetchall()
        return [
            {
                "block_trade_id": row[0],
                "quote_id": row[1],
                "request_id": row[2],
                "recorded_at_ms": row[3],
            }
            for row in rows
        ]
