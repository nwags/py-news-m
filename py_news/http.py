"""Bounded reusable HTTP client for provider adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
import time

import requests

from py_news.config import AppConfig
from py_news.rate_limit import SharedRateLimiter

TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class HttpFailure(Exception):
    method: str
    url: str
    reason: str
    attempts: int
    status_code: int | None = None
    is_transient: bool = False

    def __str__(self) -> str:
        status = f" status={self.status_code}" if self.status_code is not None else ""
        transient = " transient=true" if self.is_transient else ""
        return f"HTTP failure method={self.method} url={self.url}{status} attempts={self.attempts}{transient} reason={self.reason}"


class HttpClient:
    """Sync requests-based HTTP client with retries and structured errors."""

    def __init__(
        self,
        config: AppConfig,
        *,
        rate_limiter: SharedRateLimiter | None = None,
        session: requests.Session | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.config = config
        self.rate_limiter = rate_limiter
        self.session = session or requests.Session()
        self.max_attempts = max(1, max_attempts)
        self.backoff_seconds = max(0.0, backoff_seconds)
        self.sleep_fn = sleep_fn or time.sleep
        self.last_attempts: int = 0
        self.last_status_code: int | None = None
        self.last_rate_limited: bool = False

    def request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response, attempts = self._request(method=method, url=url, params=params, headers=headers)
        try:
            payload = response.json()
        except ValueError as exc:
            raise HttpFailure(
                method=method,
                url=url,
                reason="invalid_json_response",
                attempts=attempts,
                status_code=response.status_code,
                is_transient=False,
            ) from exc

        if isinstance(payload, dict):
            return payload
        return {"data": payload}

    def request_text(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        response, _ = self._request(method=method, url=url, params=params, headers=headers)
        return response.text

    def request_response(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        response, _ = self._request(method=method, url=url, params=params, headers=headers)
        return response

    def _request(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
    ) -> tuple[requests.Response, int]:
        merged_headers = {"User-Agent": self.config.user_agent}
        if headers:
            merged_headers.update(headers)

        attempt = 0
        while attempt < self.max_attempts:
            attempt += 1
            if self.rate_limiter is not None:
                self.rate_limiter.wait_for_slot()

            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=merged_headers,
                    timeout=(self.config.connect_timeout_seconds, self.config.read_timeout_seconds),
                )
            except requests.RequestException as exc:
                is_transient = isinstance(exc, (requests.Timeout, requests.ConnectionError))
                if is_transient and attempt < self.max_attempts:
                    self.sleep_fn(self.backoff_seconds * attempt)
                    continue
                self.last_attempts = attempt
                self.last_status_code = None
                self.last_rate_limited = False
                raise HttpFailure(
                    method=method,
                    url=url,
                    reason=str(exc),
                    attempts=attempt,
                    status_code=None,
                    is_transient=is_transient,
                ) from exc

            if response.status_code >= 400:
                is_transient = response.status_code in TRANSIENT_STATUS_CODES
                if is_transient and attempt < self.max_attempts:
                    self.sleep_fn(self.backoff_seconds * attempt)
                    continue

                self.last_attempts = attempt
                self.last_status_code = response.status_code
                self.last_rate_limited = response.status_code == 429
                raise HttpFailure(
                    method=method,
                    url=url,
                    reason=response.text[:300] if response.text else response.reason,
                    attempts=attempt,
                    status_code=response.status_code,
                    is_transient=is_transient,
                )

            self.last_attempts = attempt
            self.last_status_code = response.status_code
            self.last_rate_limited = False
            return response, attempt

        self.last_attempts = attempt
        self.last_status_code = None
        self.last_rate_limited = False
        raise HttpFailure(
            method=method,
            url=url,
            reason="max_attempts_exhausted",
            attempts=attempt,
            status_code=None,
            is_transient=True,
        )
