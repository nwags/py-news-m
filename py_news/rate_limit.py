"""Simple shared in-process request rate limiting."""

from __future__ import annotations

from threading import Lock
from typing import Callable
import time


class SharedRateLimiter:
    """A blocking shared limiter with deterministic test hooks."""

    def __init__(
        self,
        max_requests_per_second: float,
        *,
        time_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.max_requests_per_second = max_requests_per_second
        self._time_fn = time_fn or time.monotonic
        self._sleep_fn = sleep_fn or time.sleep
        self._lock = Lock()
        self._last_request_at: float | None = None

    def wait_for_slot(self) -> None:
        if self.max_requests_per_second <= 0:
            return

        min_interval = 1.0 / self.max_requests_per_second

        with self._lock:
            now = self._time_fn()
            if self._last_request_at is None:
                self._last_request_at = now
                return

            elapsed = now - self._last_request_at
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                self._sleep_fn(wait_seconds)
                now = self._time_fn()

            self._last_request_at = now
