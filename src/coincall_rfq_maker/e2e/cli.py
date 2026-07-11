"""Composition root for the beta-only ``rfq-e2e`` console script."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path

from pydantic import ValidationError

from coincall_rfq_maker.core.adapters.rest import CoincallRestClient
from coincall_rfq_maker.e2e.core import (
    E2eFailure,
    E2eResult,
    maker_env,
    run_e2e,
    validate_beta_urls,
)
from coincall_rfq_maker.settings import MakerSettings
from coincall_rfq_maker.taker.client import TakerClient
from coincall_rfq_maker.taker.settings import TakerSettings

_DEFAULT_RISK_FIELDS = (
    "max_quote_notional_usd",
    "max_leg_qty",
    "min_time_to_expiry_hours",
    "stale_market_data_seconds",
)


class AsyncClock:
    def now(self) -> float:
        return time.monotonic()

    def epoch_ms(self) -> int:
        return time.time_ns() // 1_000_000

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SubprocessMaker:
    def __init__(self, cwd: Path) -> None:
        self._cwd = cwd
        self._process: asyncio.subprocess.Process | None = None

    async def start(self, env: Mapping[str, str]) -> None:
        self._process = await asyncio.create_subprocess_exec(
            "rfq-maker", "--no-dry-run", cwd=self._cwd, env=dict(env)
        )

    async def interrupt(self) -> None:
        if self._process is not None and self._process.returncode is None:
            self._process.send_signal(signal.SIGINT)

    async def kill(self) -> None:
        if self._process is not None and self._process.returncode is None:
            self._process.kill()

    async def wait(self, timeout_seconds: float) -> int | None:
        if self._process is None:
            return 0
        try:
            return await asyncio.wait_for(self._process.wait(), timeout_seconds)
        except TimeoutError:
            return None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rfq-e2e",
        description=(
            "Beta-only live RFQ maker end-to-end harness; maintains at most one LIVE quote, "
            "may cancel-and-replace it, never trades, and flattens on shutdown."
        ),
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Directory for this run (default: e2e-runs/<generated-runid>/).",
    )
    return parser.parse_args(argv)


def _load_settings() -> tuple[MakerSettings, TakerSettings]:
    try:
        return (
            MakerSettings(),  # type: ignore[call-arg]
            TakerSettings(),  # type: ignore[call-arg]
        )
    except ValidationError as exc:
        raise E2eFailure(f"configuration error: {exc.errors(include_input=False)}") from exc


def _default_run_dir(cwd: Path) -> Path:
    run_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    return cwd / "e2e-runs" / run_id


def _require_default_risk_settings(settings: MakerSettings) -> None:
    """This test proves the stock risk configuration, never an operator's relaxed one."""
    changed = [
        name
        for name in _DEFAULT_RISK_FIELDS
        if getattr(settings, name) != MakerSettings.model_fields[name].default
    ]
    if changed:
        names = ", ".join(MakerSettings.model_fields[name].alias or name for name in changed)
        raise E2eFailure(f"refusing live e2e with non-default maker risk setting(s): {names}")


def _read_log(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _write_reports(result: E2eResult) -> None:
    result.run_dir.mkdir(parents=True, exist_ok=True)
    (result.run_dir / "report.json").write_text(
        json.dumps(result.report(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (result.run_dir / "summary.txt").write_text(result.summary() + "\n", encoding="utf-8")


async def _run(args: argparse.Namespace, cwd: Path) -> E2eResult:
    run_dir = (args.run_dir or _default_run_dir(cwd)).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    try:
        maker_settings, taker_settings = _load_settings()
        validate_beta_urls(
            maker_rest_url=maker_settings.rest_base_url,
            maker_ws_url=maker_settings.ws_url,
            taker_rest_url=taker_settings.rest_base_url,
        )
        _require_default_risk_settings(maker_settings)
    except E2eFailure as exc:
        return E2eResult(False, str(exc), None, None, (), run_dir, None, None)

    taker_credentials = taker_settings.credentials()
    rest = CoincallRestClient(
        taker_credentials.api_key,
        taker_credentials.api_secret.get_secret_value(),
        taker_settings.rest_base_url,
    )
    try:
        async with rest:
            return await run_e2e(
                taker=TakerClient(rest),
                maker=SubprocessMaker(cwd),
                clock=AsyncClock(),
                read_log=lambda: _read_log(run_dir / "maker.log"),
                maker_environment=maker_env(os.environ, run_dir),
                run_dir=run_dir,
            )
    except Exception as exc:
        return E2eResult(
            False,
            f"unexpected {type(exc).__name__}: {exc}",
            None,
            None,
            (),
            run_dir,
            None,
            None,
        )


def run(argv: Sequence[str] | None = None) -> None:
    """Console-script entry point. ``--help`` intentionally requires no credentials."""
    args = parse_args(argv)
    cwd = Path.cwd()
    try:
        result = asyncio.run(_run(args, cwd))
    except FileExistsError:
        sys.stderr.write(f"rfq-e2e: run directory already exists: {args.run_dir}\n")
        raise SystemExit(2) from None
    _write_reports(result)
    print(result.summary())
    if not result.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    run()
