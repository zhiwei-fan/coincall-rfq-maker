from types import TracebackType
from typing import Any, Self

import pytest

from coincall_rfq_maker import cli
from coincall_rfq_maker.adapters.rest import CoincallRequestError
from coincall_rfq_maker.settings import Settings


class FakeRestContext:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass


class FakePersistenceContext:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass


class FailingQuoteLifecycle:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def cancel_all(self) -> None:
        raise CoincallRequestError("cancel-all unavailable")


@pytest.mark.asyncio
async def test_startup_cancel_all_failure_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "CoincallRestClient", FakeRestContext)
    monkeypatch.setattr(cli, "PersistenceStore", FakePersistenceContext)
    monkeypatch.setattr(cli, "QuoteLifecycle", FailingQuoteLifecycle)
    settings = Settings(API_KEY="key", API_SECRET="secret", CANCEL_ALL_ON_START=True)

    with pytest.raises(SystemExit) as exc_info:
        await cli.run_async(settings)

    assert exc_info.value.code == 1
    assert capsys.readouterr().err == (
        "Startup error: failed to cancel all quotes: cancel-all unavailable\n"
    )
