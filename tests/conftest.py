"""Shared pytest fixtures.

`observability.setup_logging()` clears root-logger handlers and attaches a
`RotatingFileHandler("rfq_maker.log")`. Any test that drives `cli.run_async`
(directly or indirectly) triggers that call, and every subsequent test's log
records then land in the real, production `rfq_maker.log` in the repo root.
This autouse fixture snapshots and restores the root logger's handlers/level
around every test so pytest never writes into that file.
"""

import logging
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _restore_root_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)
