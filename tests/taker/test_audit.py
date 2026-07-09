import json
import sys
from pathlib import Path

import pytest

from coincall_rfq_maker.taker.audit import AuditLog


def _read(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def test_record_appends_one_json_line_per_action(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    audit = AuditLog(path)

    audit.record("create_rfq", {"requestId": "r1", "legs": [{"instrumentName": "X"}]})
    audit.record("execute", {"requestId": "r1", "quoteId": "q1", "blockTradeId": "b1"})

    records = _read(path)
    assert [r["action"] for r in records] == ["create_rfq", "execute"]
    assert records[0]["requestId"] == "r1"
    assert records[0]["legs"] == [{"instrumentName": "X"}]
    assert records[1]["blockTradeId"] == "b1"
    # Every record carries a millisecond timestamp.
    assert all(isinstance(r["ts"], int) and r["ts"] > 0 for r in records)


def test_record_creates_missing_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "audit.jsonl"
    assert not path.parent.exists()

    assert AuditLog(path).record("cancel_rfq", {"requestId": "r1"}) is True

    assert path.exists()
    assert _read(path)[0]["action"] == "cancel_rfq"


def test_record_warns_on_write_failure_but_does_not_raise(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Make the intended parent an existing FILE so mkdir/open cannot succeed.
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    audit = AuditLog(blocker / "audit.jsonl")

    # Must not raise — a decision already made must not be undone by a log error.
    assert (
        audit.record("execute", {"requestId": "r1", "quoteId": "q1", "blockTradeId": "b1"}) is False
    )

    err = capsys.readouterr().err
    assert "WARNING" in err
    assert "execute" in err


def test_record_still_never_raises_when_stderr_is_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ClosedStderr:
        def write(self, _text: str) -> None:
            raise OSError("stderr closed")

    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    monkeypatch.setattr(sys, "stderr", ClosedStderr())

    assert AuditLog(blocker / "audit.jsonl").record("execute_attempt", {"requestId": "r1"}) is False
