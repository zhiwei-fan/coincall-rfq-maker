import logging
import logging.handlers
from pathlib import Path

from coincall_rfq_maker.observability import setup_logging


def test_setup_logging_uses_supplied_log_file(tmp_path: Path) -> None:
    log_file = tmp_path / "configured.log"

    setup_logging("DEBUG", str(log_file))

    file_handlers = [
        handler
        for handler in logging.getLogger().handlers
        if isinstance(handler, logging.handlers.RotatingFileHandler)
    ]
    assert len(file_handlers) == 1
    assert Path(file_handlers[0].baseFilename) == log_file
    assert Path(file_handlers[0].baseFilename) != Path("rfq_maker.log").resolve()
