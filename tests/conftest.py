"""Shared pytest fixtures.

`observability.setup_logging()` clears root-logger handlers and attaches a
`RotatingFileHandler`. Any test that drives `cli.run_async` triggers that call, and
before `LOG_FILE` existed it targeted the real, production `rfq_maker.log` in the repo
root — which holds irreplaceable live WS evidence. This autouse fixture points
`LOG_FILE` at a per-test tmp path, hard-fails any attempt to open the repo log anyway,
and restores the root logger's handlers/level afterwards.
"""

import logging
import logging.handlers  # `import logging` alone does not bind the `handlers` submodule
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_root_logging(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Keep test logging out of the repository's live production log."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    repo_log = Path("rfq_maker.log").resolve()
    test_log = tmp_path / "rfq_maker.log"
    original_file_handler = logging.handlers.RotatingFileHandler

    class GuardedRotatingFileHandler(original_file_handler):
        def __init__(self, filename: str | Path, *args: object, **kwargs: object) -> None:
            if Path(filename).resolve() == repo_log:
                raise AssertionError(f"tests must not open the repository log: {repo_log}")
            super().__init__(filename, *args, **kwargs)

    monkeypatch.setenv("LOG_FILE", str(test_log))
    monkeypatch.setattr(logging.handlers, "RotatingFileHandler", GuardedRotatingFileHandler)
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)
