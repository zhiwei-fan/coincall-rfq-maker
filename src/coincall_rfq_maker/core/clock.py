"""Neutral wall-clock helpers."""

import time


def get_timestamp_ms() -> int:
    """Current UNIX timestamp in milliseconds."""
    return int(time.time() * 1000)


current_time_ms = get_timestamp_ms
