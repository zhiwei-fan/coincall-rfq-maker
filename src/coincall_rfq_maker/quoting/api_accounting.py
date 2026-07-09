"""Operation-boundary accounting for quote REST API outcomes."""

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol

from coincall_rfq_maker.core.adapters.rest import CoincallError


@dataclass
class _ApiOperationAccounting:
    ambiguous_failures: int = 0


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
        self._api_operation: ContextVar[_ApiOperationAccounting | None] = ContextVar(
            "quote_lifecycle_api_operation", default=None
        )

    async def run[T](self, operation: Callable[[], Awaitable[T]]) -> T:
        accounting = self._api_operation.get()
        if accounting is not None:
            return await operation()

        accounting = _ApiOperationAccounting()
        token = self._api_operation.set(accounting)
        try:
            result = await operation()
        except CoincallError:
            for _ in range(accounting.ambiguous_failures or 1):
                self._reporter.record_api_failure()
            raise
        else:
            self._reporter.record_api_success()
            return result
        finally:
            self._api_operation.reset(token)

    def note_ambiguous_failure(self) -> None:
        accounting = self._api_operation.get()
        if accounting is not None:
            accounting.ambiguous_failures += 1
