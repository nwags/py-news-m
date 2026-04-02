from pathlib import Path

import requests

from py_news.config import load_config
from py_news.http import HttpClient, HttpFailure


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        text: str = "",
        payload: dict | None = None,
        json_exc: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.reason = text or "reason"
        self._payload = payload if payload is not None else {}
        self._json_exc = json_exc

    def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._payload


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def request(self, **kwargs):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_http_client_retries_transient_status():
    config = load_config(project_root=Path.cwd())
    session = FakeSession([
        FakeResponse(503, "temporary"),
        FakeResponse(200, payload={"ok": True}),
    ])

    client = HttpClient(config, session=session, backoff_seconds=0.0)
    payload = client.request_json("GET", "https://example.test")

    assert payload["ok"] is True
    assert session.calls == 2


def test_http_client_raises_structured_failure_on_non_transient():
    config = load_config(project_root=Path.cwd())
    session = FakeSession([FakeResponse(404, "not found")])

    client = HttpClient(config, session=session, backoff_seconds=0.0)

    try:
        client.request_json("GET", "https://example.test/missing")
        assert False, "expected HttpFailure"
    except HttpFailure as exc:
        assert exc.status_code == 404
        assert exc.is_transient is False
        assert exc.attempts == 1


def test_http_client_retries_timeout_then_fails_structured():
    config = load_config(project_root=Path.cwd())
    session = FakeSession([
        requests.Timeout("slow"),
        requests.Timeout("still slow"),
        requests.Timeout("slowest"),
    ])

    client = HttpClient(config, session=session, backoff_seconds=0.0)

    try:
        client.request_text("GET", "https://example.test/slow")
        assert False, "expected HttpFailure"
    except HttpFailure as exc:
        assert exc.status_code is None
        assert exc.is_transient is True
        assert exc.attempts == 3


def test_http_client_invalid_json_reports_real_attempt_count():
    config = load_config(project_root=Path.cwd())
    session = FakeSession(
        [
            FakeResponse(503, "temporary"),
            FakeResponse(200, text="{bad json", json_exc=ValueError("bad json")),
        ]
    )

    client = HttpClient(config, session=session, backoff_seconds=0.0)

    try:
        client.request_json("GET", "https://example.test/json")
        assert False, "expected HttpFailure"
    except HttpFailure as exc:
        assert exc.reason == "invalid_json_response"
        assert exc.attempts == 2
