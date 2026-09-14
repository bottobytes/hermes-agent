"""Retry utilities — jittered backoff for decorrelated retries.

Jittered delays (vs. fixed exponential) prevent thundering-herd retry spikes
when many sessions hit the same rate-limited provider concurrently.
"""

import random
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional

# Monotonic counter for jitter-seed uniqueness within a process; locked
# because concurrent gateway sessions retry simultaneously.
_jitter_counter = 0
_jitter_lock = threading.Lock()

# Z.AI Coding Plan's GLM-5.2 endpoint often returns 429 code 1305 ("service may be
# temporarily overloaded"). Short retries hammer the same window, so after
# ``_ZAI_CODING_OVERLOAD_SHORT_ATTEMPTS`` normal retries the wait widens progressively;
# the cap stays interactive-friendly (a TUI message should fail visibly in minutes).
# The short count is shared by ``adaptive_rate_limit_backoff`` and
# ``zai_coding_overload_retry_ceiling`` so the two cannot silently desync.
_ZAI_CODING_OVERLOAD_LONG_BACKOFF = (30.0, 60.0, 90.0, 120.0)
_ZAI_CODING_OVERLOAD_SHORT_ATTEMPTS = 3

# Generic transient-throttle long backoff: applies to the rate-limit /
# overload / upstream-rate-limit failure family on ANY provider. The
# default single-query budget (3) dies inside a ~90s provider overload
# burst; these waits (15/30/60/60 + light jitter) ride out typical
# overload windows. The 60s base ceiling keeps the schedule
# interactive-friendly — worst case ≈ 2.75 min of waiting across the
# full table, after which the turn fails and (for kanban workers) the
# exit-75 sentinel requeues the task instead of crashing it.
_TRANSIENT_THROTTLE_LONG_BACKOFF = (15.0, 30.0, 60.0, 60.0)
_TRANSIENT_THROTTLE_SHORT_ATTEMPTS = 3

# Failure-reason values (``FailoverReason``) that represent a transient
# provider-side throttle: the credential is fine, the task is fine, the
# server is just busy or the account hit a momentary quota window.
# Consumed by the conversation loop (widen the retry ceiling) and by the
# CLI/worker exit-code mapping (EX_TEMPFAIL requeue instead of crash).
TRANSIENT_THROTTLE_FAILURE_REASONS = frozenset({
    "rate_limit",
    "upstream_rate_limit",
    "overloaded",
})


def parse_retry_after_seconds(value_or_headers: Any) -> Optional[float]:
    """Parse a ``Retry-After`` value (numeric / HTTP-date) or a headers mapping (both casings tried) into
    seconds, clamped at 0.0; None when absent / unparseable."""
    raw = value_or_headers
    if raw is not None and not isinstance(raw, (str, int, float)):
        getter = getattr(raw, "get", None)
        if not callable(getter):
            return None
        try:
            raw = getter("Retry-After")
            if raw is None:
                raw = getter("retry-after")
        except Exception:
            return None
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return max(0.0, float(raw))
    text = str(raw).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except (TypeError, ValueError):
        pass
    # HTTP-date form (RFC 7231): seconds until that instant, clamped at 0.
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when is None:  # older stdlib returns None instead of raising
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def jittered_backoff(attempt: int, *, base_delay: float = 5.0, max_delay: float = 120.0, jitter_ratio: float = 0.5) -> float:
    """min(base * 2^(attempt-1), max_delay) + uniform jitter in
    [0, jitter_ratio * delay]. ``attempt`` is 1-based."""
    global _jitter_counter
    with _jitter_lock:
        _jitter_counter += 1
        tick = _jitter_counter

    exponent = max(0, attempt - 1)
    delay = max_delay if (exponent >= 63 or base_delay <= 0) else min(base_delay * (2 ** exponent), max_delay)

    # Seed from time + counter so coarse clocks still decorrelate.
    seed = (time.time_ns() ^ (tick * 0x9E3779B9)) & 0xFFFFFFFF
    return delay + random.Random(seed).uniform(0, jitter_ratio * delay)


def _error_text(error: Any) -> str:
    """Best-effort flattened provider error text for retry classification."""
    parts = [error, getattr(error, "message", None), getattr(error, "body", None), getattr(error, "response", None)]
    return " ".join(str(part) for part in parts if part is not None).lower()


def is_zai_coding_overload_error(*, base_url: str | None, model: str | None, error: Any) -> bool:
    """Return True for Z.AI Coding Plan transient overload 429s.

    The coding-plan endpoint reports overload as HTTP 429 with body code 1305
    and message "The service may be temporarily overloaded...". Treat only
    that narrow shape specially so ordinary quota/billing 429s still fail fast
    through the existing classifier. The model gate matches the whole GLM
    coding family (``glm-5.2``, ``glm-5.3``, ...) via the ``glm-`` prefix:
    restricting it to one version silently drops every newer model from the
    adaptive backoff (the 2026-09-02 incident: glm-5.3 workers exhausted
    their 3-retry budget inside a ~90s overload burst and crashed).
    """
    base = (base_url or "").lower()
    model_name = (model or "").lower()
    status = getattr(error, "status_code", None)
    text = _error_text(error)
    return (
        status == 429
        and "api.z.ai/api/coding/paas/v4" in base
        and "glm-" in model_name
        and ("1305" in text or "temporarily overloaded" in text)
    )


def is_transient_throttle_reason(reason: Any) -> bool:
    """True when a ``FailoverReason`` (or its string value) is a transient
    provider-side throttle: rate limit, upstream rate limit, or overload.

    These deserve a deeper retry budget and a requeue (not a crash) when the
    budget exhausts — the credential and the task are both fine; the server
    was merely busy for a burst.
    """
    value = getattr(reason, "value", reason)
    return str(value) in TRANSIENT_THROTTLE_FAILURE_REASONS


def adaptive_rate_limit_backoff(
    attempt: int, *, base_url: str | None, model: str | None, error: Any, default_wait: float,
    short_attempts: int = _ZAI_CODING_OVERLOAD_SHORT_ATTEMPTS,
    failure_reason: Any = None,
) -> tuple[float, str | None]:
    """Provider-aware rate-limit backoff.

    For most providers this returns ``default_wait`` unchanged. For Z.AI
    Coding Plan GLM overloads, keep the first ``short_attempts`` retries on
    the normal short exponential schedule, then switch to progressively longer
    waits (30s → 60s → 90s → 120s, capped) plus light jitter. For every other
    provider whose error classified as a transient throttle
    (``rate_limit`` / ``upstream_rate_limit`` / ``overloaded``), the same
    structure applies with a gentler table (15/30/60/60) — deep enough to
    ride out a ~90s overload burst, capped low enough to stay
    interactive-friendly.

    ``attempt`` is 1-based, matching the retry loop's logged attempt number.
    Returns ``(wait_seconds, reason_label)`` where ``reason_label`` is suitable
    for status/log decoration when a provider-specific policy fired.
    """
    if is_zai_coding_overload_error(base_url=base_url, model=model, error=error):
        if attempt <= short_attempts:
            return default_wait, "zai_coding_overload_short"
        idx = min(attempt - short_attempts - 1, len(_ZAI_CODING_OVERLOAD_LONG_BACKOFF) - 1)
        base_delay = _ZAI_CODING_OVERLOAD_LONG_BACKOFF[idx]
        # A smaller jitter ratio keeps long waits readable while still avoiding
        # synchronized retry storms across concurrent Hermes sessions.
        return jittered_backoff(1, base_delay=base_delay, max_delay=base_delay, jitter_ratio=0.2), "zai_coding_overload_long"

    if failure_reason is not None and is_transient_throttle_reason(failure_reason):
        if attempt <= _TRANSIENT_THROTTLE_SHORT_ATTEMPTS:
            return default_wait, "transient_throttle_short"
        idx = min(
            attempt - _TRANSIENT_THROTTLE_SHORT_ATTEMPTS - 1,
            len(_TRANSIENT_THROTTLE_LONG_BACKOFF) - 1,
        )
        base_delay = _TRANSIENT_THROTTLE_LONG_BACKOFF[idx]
        return jittered_backoff(1, base_delay=base_delay, max_delay=base_delay, jitter_ratio=0.2), "transient_throttle_long"

    return default_wait, None


def zai_coding_overload_retry_ceiling(short_attempts: int = _ZAI_CODING_OVERLOAD_SHORT_ATTEMPTS) -> int:
    """Retry-loop ceiling for the full Z.AI overload schedule: one past the last long entry,
    because the loop gives up when ``retry_count >= ceiling`` BEFORE computing the attempt's
    backoff (the default ``api_max_retries`` of 3 equals ``short_attempts``)."""
    return short_attempts + len(_ZAI_CODING_OVERLOAD_LONG_BACKOFF) + 1


def transient_throttle_retry_ceiling(
    short_attempts: int = _TRANSIENT_THROTTLE_SHORT_ATTEMPTS,
) -> int:
    """Retry-loop ceiling for the generic transient-throttle schedule.

    Same sizing rationale as :func:`zai_coding_overload_retry_ceiling`: the
    loop's exhaustion check precedes the backoff computation, so the ceiling
    must sit one past the final long-tier entry. With the default single-query
    budget of 3, none of the 15/30/60/60 long waits were ever reachable —
    a ~90s overload burst killed the worker mid-table.
    """
    return short_attempts + len(_TRANSIENT_THROTTLE_LONG_BACKOFF) + 1
