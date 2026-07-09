from types import TracebackType
from typing import Any

import pytest

from coincall_rfq_maker.adapters.rest import CoincallAmbiguousError, CoincallRestClient


class TimeoutContext:
    async def __aenter__(self) -> object:
        raise TimeoutError("timed out after server receipt might have happened")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class TimeoutPostSession:
    def __init__(self) -> None:
        self.post_attempts = 0

    def post(self, *args: Any, **kwargs: Any) -> TimeoutContext:
        self.post_attempts += 1
        return TimeoutContext()


@pytest.mark.asyncio
async def test_state_changing_post_timeout_is_not_retried_and_is_ambiguous() -> None:
    session = TimeoutPostSession()
    client = CoincallRestClient("key", "secret")
    client._session = session  # type: ignore[assignment]

    with pytest.raises(CoincallAmbiguousError):
        await client.create_quote("rfq-1", [{"instrumentName": "BTCUSD-21AUG25-120000-C"}])

    assert session.post_attempts == 1
