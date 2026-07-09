"""Console entry point for maker-vs-taker scenarios."""

import argparse
import asyncio
import logging
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from coincall_rfq_maker.taker.scenarios import (
    SCENARIO_NAMES,
    fake_harness,
    run_scenario,
    scenario_names,
)

_LIVE_UNAVAILABLE = (
    "live taker transport not yet available (needs Coincall taker API endpoints + TAKER_API_KEY)"
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rfq-taker",
        description="Run maker-vs-taker RFQ scenarios.",
    )
    parser.add_argument(
        "--scenario",
        choices=(*SCENARIO_NAMES, "all"),
        default="all",
        help="Scenario to run.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use live taker transport when Coincall taker endpoints are available.",
    )
    return parser.parse_args(argv)


async def run_async(selected: str) -> int:
    failures = 0
    with tempfile.TemporaryDirectory(prefix="rfq-taker-") as tmpdir:
        for name in scenario_names(selected):
            db_path = str(Path(tmpdir) / f"{name}.db")
            try:
                async with fake_harness(db_path) as harness:
                    await run_scenario(name, harness.exchange, harness.context)
            except Exception as exc:
                failures += 1
                print(f"[FAIL] {name}: {exc}", file=sys.stderr)
            else:
                print(f"[PASS] {name}")
    return 1 if failures else 0


def run(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.live:
        print(_LIVE_UNAVAILABLE, file=sys.stderr)
        raise SystemExit(1)
    logging.basicConfig(level=logging.ERROR)
    raise SystemExit(asyncio.run(run_async(args.scenario)))


if __name__ == "__main__":
    run()
