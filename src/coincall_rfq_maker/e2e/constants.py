"""Frozen live-e2e safety bounds and production-log evidence markers."""

READINESS_TIMEOUT_SECONDS = 30.0
QUOTE_WAIT_TIMEOUT_SECONDS = 120.0
SHUTDOWN_TIMEOUT_SECONDS = 30.0
OVERALL_TIMEOUT_SECONDS = 300.0
POLL_INTERVAL_SECONDS = 3.0
MIN_INSTRUMENT_TTE_HOURS = 24.0

BETA_REST_HOST = "betaapi.coincall.com"
BETA_WS_HOST = "betaws.seizeyouralpha.com"

# These prefixes are emitted by production code. tests/e2e/test_markers.py drives
# the real emit sites so changing a production line breaks the harness test.
WS_SUBSCRIBED_MARKER = "Subscribed to "
QUOTE_CREATED_MARKER = "Created quote "
RFQ_NOT_QUOTED_MARKER = "Not quoting RFQ "
