"""Append-only JSONL audit log for the taker TRADE path.

A taker cannot list its own RFQs via the API (rfqList 404s for the taker
account), so this local log is the orphan-recovery and observation mechanism:
every RFQ created, quote executed, RFQ cancelled, or execution refused/ambiguous
is recorded as one JSON line. An unaudited trade is a real problem, so a write
failure is loud (WARNING to stderr) but never crashes a decision already made.
"""

import json
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any

from coincall_rfq_maker.core.clock import get_timestamp_ms


class AuditLog:
    """Appends one JSON line per recorded action to ``path``."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def record(self, action: str, payload: dict[str, Any]) -> bool:
        """Append ``{"ts", "action", **payload}`` as one JSON line.

        Creates the parent directory if missing. On any write failure, prints a
        WARNING to stderr rather than raising — the trade decision is already
        made and must not be undone by a logging error.
        Returns ``True`` when the line was persisted, otherwise ``False``.
        """
        entry = {"ts": get_timestamp_ms(), "action": action, **payload}
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry) + "\n")
        except OSError as exc:
            with suppress(OSError):
                sys.stderr.write(
                    f"WARNING: failed to write audit record {action!r} to {self._path}: {exc}\n"
                )
            return False
        return True
