"""Canonical runtime summary and progress emitters for m-cache commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sys
import time
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class RuntimeContext:
    domain: str
    command_path: list[str]
    summary_json: bool
    progress_json: bool
    progress_heartbeat_seconds: float
    quiet: bool
    verbose: bool
    log_level: str | None
    log_file: str | None
    resolution_mode: str | None = None
    provider_requested: str | None = None


class ProgressEmitter:
    def __init__(self, context: RuntimeContext) -> None:
        self.context = context
        self.started = time.monotonic()

    def emit(
        self,
        *,
        event: str,
        phase: str,
        counters: dict[str, int | float] | None = None,
        detail: str | None = None,
        provider: str | None = None,
        canonical_key: str | None = None,
        rate_limit_state: str | None = None,
    ) -> None:
        if not self.context.progress_json:
            return
        payload: dict[str, Any] = {
            "event": event,
            "domain": self.context.domain,
            "command_path": self.context.command_path,
            "phase": phase,
            "elapsed_seconds": round(max(0.0, time.monotonic() - self.started), 6),
            "counters": counters or {},
        }
        if detail is not None:
            payload["detail"] = detail
        if provider is not None:
            payload["provider"] = provider
        if canonical_key is not None:
            payload["canonical_key"] = canonical_key
        if rate_limit_state is not None:
            payload["rate_limit_state"] = rate_limit_state
        sys.stderr.write(json.dumps(payload, sort_keys=True) + "\n")
        sys.stderr.flush()


def render_runtime_summary(
    *,
    context: RuntimeContext,
    started_at: str,
    status: str,
    remote_attempted: bool,
    provider_used: str | None,
    rate_limited: bool,
    retry_count: int,
    persisted_locally: bool | None,
    counters: dict[str, int | float],
    warnings: list[str] | None = None,
    errors: list[str] | None = None,
    effective_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    finished_at = utc_now_iso()
    started_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    finished_dt = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    elapsed = max(0.0, (finished_dt - started_dt).total_seconds())

    payload: dict[str, Any] = {
        "status": status,
        "domain": context.domain,
        "command_path": context.command_path,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round(elapsed, 6),
        "resolution_mode": context.resolution_mode,
        "remote_attempted": bool(remote_attempted),
        "provider_requested": context.provider_requested,
        "provider_used": provider_used,
        "rate_limited": bool(rate_limited),
        "retry_count": int(retry_count),
        "persisted_locally": persisted_locally,
        "counters": counters,
        "warnings": warnings or [],
        "errors": errors or [],
    }
    if effective_config is not None:
        payload["effective_config"] = effective_config
    return payload
