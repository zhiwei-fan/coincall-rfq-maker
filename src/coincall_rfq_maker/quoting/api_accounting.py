"""Operation-boundary accounting for quote REST API outcomes."""

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Protocol

from coincall_rfq_maker.core.adapters.rest import (
    ApiFailureKind,
    CoincallError,
    classify_api_failure,
)


class ApiOutcomeReporter(Protocol):
    def record_api_failure(self) -> None: ...

    def record_api_success(self) -> None: ...


class _NullApiOutcomeReporter:
    def record_api_failure(self) -> None:
        pass

    def record_api_success(self) -> None:
        pass


class ApiOutcomeBoundary:
    def __init__(self, reporter: ApiOutcomeReporter | None = None) -> None:
        self._reporter = reporter if reporter is not None else _NullApiOutcomeReporter()
        self._api_operation: ContextVar[bool | None] = ContextVar(
            "quote_lifecycle_api_operation", default=None
        )

    async def run[T](self, operation: Callable[[], Awaitable[T]]) -> T:
        if self._api_operation.get():
            return await operation()

        token = self._api_operation.set(True)
        try:
            result = await operation()
        except CoincallError as exc:
            if classify_api_failure(exc) is ApiFailureKind.PERSISTENT:
                self._reporter.record_api_failure()
            raise
        else:
            self._reporter.record_api_success()
            return result
        finally:
            self._api_operation.reset(token)
