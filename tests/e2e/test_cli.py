from pathlib import Path

import pytest

from coincall_rfq_maker.e2e import cli
from coincall_rfq_maker.settings import MakerSettings


def test_help_needs_no_credentials(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.run(["--help"])

    assert exc_info.value.code == 0
    help_text = capsys.readouterr().out
    assert "Beta-only live RFQ maker" in help_text
    assert "at most one LIVE quote" in help_text
    assert "cancel-and-replace" in help_text
    assert "never trades" in help_text
    assert "flattens on shutdown" in help_text


def test_reports_are_json_and_human_readable(tmp_path: Path) -> None:
    result = cli.E2eResult(True, "proven", "r1", "q1", (), tmp_path, "BTC", "0.01")
    cli._write_reports(result)

    assert '"passed": true' in (tmp_path / "report.json").read_text()
    assert "rfq-e2e: PASS" in (tmp_path / "summary.txt").read_text()


def test_non_default_risk_knob_refuses_live_e2e() -> None:
    settings = MakerSettings.model_construct(max_quote_notional_usd=1.0)

    with pytest.raises(cli.E2eFailure, match="MAX_QUOTE_NOTIONAL_USD"):
        cli._require_default_risk_settings(settings)
