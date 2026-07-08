"""Logging setup: console + rotating file, with structured RFQ/quote/instrument context.

Call sites pass `extra={"rfq_id": ..., "quote_id": ..., "instrument": ...}` on
individual log calls to populate the context fields; a filter fills in "-"
for any that are omitted so the format string never breaks.
"""

import logging
import logging.handlers
import sys

_LOG_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s "
    "[rfq=%(rfq_id)s quote=%(quote_id)s instrument=%(instrument)s] "
    "%(message)s"
)
_CONTEXT_FIELDS = ("rfq_id", "quote_id", "instrument")


class _ContextDefaultsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for field in _CONTEXT_FIELDS:
            if not hasattr(record, field):
                setattr(record, field, "-")
        return True


def setup_logging(log_level: str = "INFO", log_file: str = "rfq_maker.log") -> None:
    """Configure root logging once at process startup."""
    formatter = logging.Formatter(_LOG_FORMAT)
    context_filter = _ContextDefaultsFilter()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(context_filter)

    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(context_filter)

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Quiet noisy third-party loggers.
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
