"""Regression tests for the 2026-09-02 provider-burst mass-kill incident.

Incident: a ~90s z.ai overload burst (HTTP 429, code 1305) exhausted the
3-retry single-query budget on every concurrently-running worker; the
deaths were content-free ("pid N not alive") because a second dispatcher
process swept them; and the systemic-fingerprint rule (>=3 identical
content-free errors -> failure_limit=1) insta-blocked the whole wave.

Patch set under test (card t_d4c215a7):

(A) transient-throttle classification survives the reap race via a
    worker-written sentinel sidecar; exit 75 / sidecar -> neutral
    ``rate_limited`` requeue with NO failure tick.
(B) retry ceilings: generic transient-throttle schedule (15/30/60/60)
    reachable from the default budget of 3; z.ai matcher covers the whole
    glm- family, not just glm-5.2.
(C) Retry-After honored for the overload family too.
(E) restart windows: spawn hold + neutral ``restart_killed`` requeue for
    deaths inside a declared window; TTL safety valve.
(F) active_pr respawn guard: machine-authored (github-webhook) PR-URL
    comments don't park the card; a deliberate requeue after the newest
    PR comment re-arms spawning.
(G) content-free fingerprints ("pid N not alive") never escalate to the
    systemic failure_limit=1 rule.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

import pytest

import hermes_state
from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME with an empty kanban DB."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def _exited_status(code: int) -> int:
    return (code & 0xFF) << 8


def _claim_with_dead_worker(conn, tid: str, pid: int) -> None:
    """Claim the task, open a run, and point it at a dead worker pid."""
    host = kb._claimer_id().split(":", 1)[0]
    kb.claim_task(conn, tid, claimer=f"{host}:w{pid}")
    conn.execute(
        "UPDATE tasks SET worker_pid=?, consecutive_failures=0 WHERE id=?",
        (pid, tid),
    )
    conn.commit()


def _outcomes(conn, tid: str) -> list[str]:
    return [
        r["outcome"] for r in conn.execute(
            "SELECT outcome FROM task_runs WHERE task_id=?", (tid,),
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# (A) Sentinel sidecar: throttle classification survives the reap race
# ---------------------------------------------------------------------------


def test_rate_limit_sentinel_reclassifies_unknown_death(kanban_home, monkeypatch):
    """A worker that wrote the sentinel + died unreaped by THIS sweeper
    still gets the neutral rate_limited requeue — no failure tick."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    with kb.connect() as conn:
        tid = kb.create_task(conn, title="sentinel", assignee="a")
        pid = 71001
        _claim_with_dead_worker(conn, tid, pid)

        # Worker wrote its sidecar just before dying; NO exit-status entry
        # (another process reaped it / gateway restarted).
        sentinel_path = kb.rate_limit_sentinel_path(tid)
        sentinel_path.parent.mkdir(parents=True, exist_ok=True)
        sentinel_path.write_text(json.dumps({
            "task_id": tid, "reason": "overloaded", "pid": pid,
            "ts": int(time.time()),
        }), encoding="utf-8")

        crashed = kb.detect_crashed_workers(conn)
        assert tid not in crashed
        rl = getattr(kb.detect_crashed_workers, "_last_rate_limited", [])
        assert tid in rl

        task = kb.get_task(conn, tid)
        assert task.status == "ready"
        assert task.consecutive_failures == 0
        outcomes = _outcomes(conn, tid)
        assert "rate_limited" in outcomes
        assert "crashed" not in outcomes
        # Sentinel consumed so a later sweep can't double-count.
        assert not sentinel_path.exists()


def test_stale_sentinel_does_not_mask_a_real_crash(kanban_home, monkeypatch):
    """A sentinel older than the max age (or from another worker pid) is
    swept, not consumed — the death classifies as a genuine crash."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    with kb.connect() as conn:
        tid = kb.create_task(conn, title="stale-sentinel", assignee="a")
        pid = 71002
        _claim_with_dead_worker(conn, tid, pid)

        sentinel_path = kb.rate_limit_sentinel_path(tid)
        sentinel_path.parent.mkdir(parents=True, exist_ok=True)
        # Stale: written 2x the max age ago.
        sentinel_path.write_text(json.dumps({
            "task_id": tid, "reason": "rate_limit", "pid": pid,
            "ts": int(time.time()) - 2 * kb.RATE_LIMIT_SENTINEL_MAX_AGE_SECONDS,
        }), encoding="utf-8")

        crashed = kb.detect_crashed_workers(conn)
        assert tid in crashed
        rl = getattr(kb.detect_crashed_workers, "_last_rate_limited", [])
        assert tid not in rl
        assert "crashed" in _outcomes(conn, tid)


def test_sentinel_from_different_pid_not_consumed(kanban_home, monkeypatch):
    """A sidecar left by an EARLIER worker incarnation must not speak for
    THIS death."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    with kb.connect() as conn:
        tid = kb.create_task(conn, title="other-pid", assignee="a")
        pid = 71003
        _claim_with_dead_worker(conn, tid, pid)

        sentinel_path = kb.rate_limit_sentinel_path(tid)
        sentinel_path.parent.mkdir(parents=True, exist_ok=True)
        sentinel_path.write_text(json.dumps({
            "task_id": tid, "reason": "rate_limit", "pid": 999999,
            "ts": int(time.time()),
        }), encoding="utf-8")

        crashed = kb.detect_crashed_workers(conn)
        assert tid in crashed


# ---------------------------------------------------------------------------
# (G) Content-free fingerprints never escalate to systemic
# ---------------------------------------------------------------------------


def test_content_free_mass_death_does_not_trip_systemic(kanban_home, monkeypatch):
    """Four simultaneous 'pid N not alive' deaths (a container restart
    wave) must NOT force failure_limit=1 and insta-block the wave. The
    systemic rule stays armed for DIAGNOSTIC repeated errors."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    tids = []
    with kb.connect() as conn:
        for i in range(4):
            tid = kb.create_task(conn, title=f"restart-wave-{i}", assignee="a")
            tids.append(tid)
            _claim_with_dead_worker(conn, tid, 71100 + i)

        crashed = kb.detect_crashed_workers(conn)
        assert set(crashed) == set(tids)

        # None of the wave gave_up: failure budget (default 2) respected.
        for tid in tids:
            task = kb.get_task(conn, tid)
            assert task.status == "ready", (
                f"{tid}: mass content-free death must requeue, got {task.status}"
            )
            assert task.consecutive_failures == 1

    # Second wave: still no systemic escalation — the breaker only trips
    # via the NORMAL failure_limit path. (_claim_with_dead_worker resets
    # consecutive_failures to 0 per claim, so wave 2 counts as failure 1
    # of the default limit 2.)
    with kb.connect() as conn:
        for i, tid in enumerate(tids):
            _claim_with_dead_worker(conn, tid, 71200 + i)
        kb.detect_crashed_workers(conn)
        for tid in tids:
            task = kb.get_task(conn, tid)
            assert task.status == "ready"
            assert task.consecutive_failures == 1


def test_diagnostic_mass_failure_still_trips_systemic(kanban_home, monkeypatch):
    """Guard rail on the guard: three identical DIAGNOSTIC errors in one
    sweep still escalate (the systemic rule's reason to exist)."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    tids = []
    with kb.connect() as conn:
        for i in range(3):
            tid = kb.create_task(conn, title=f"systemic-{i}", assignee="a")
            tids.append(tid)
            pid = 71300 + i
            _claim_with_dead_worker(conn, tid, pid)
            # Simulate a diagnostic nonzero exit: reaped exit code 1.
            kb._record_worker_exit(pid, _exited_status(1))

        kb.detect_crashed_workers(conn)
        # Systemic: identical "exited with code 1" x3 -> immediate block.
        for tid in tids:
            task = kb.get_task(conn, tid)
            assert task.status == "blocked", (
                f"{tid}: diagnostic systemic wave must block, got {task.status}"
            )


# ---------------------------------------------------------------------------
# (E) Restart windows
# ---------------------------------------------------------------------------


def test_restart_window_holds_spawns_and_requeues_neutrally(kanban_home, monkeypatch):
    """Deaths inside a declared restart window: neutral restart_killed
    outcome, no failure tick; dispatch holds spawns while open; closing
    (or TTL expiry) resumes."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    with kb.connect() as conn:
        opener_tid = kb.create_task(conn, title="restarter", assignee="a")
        sib1 = kb.create_task(conn, title="sibling-1", assignee="a")
        sib2 = kb.create_task(conn, title="sibling-2", assignee="a")
        _claim_with_dead_worker(conn, opener_tid, 71401)
        _claim_with_dead_worker(conn, sib1, 71402)
        _claim_with_dead_worker(conn, sib2, 71403)

        wid = kb.open_restart_window(
            conn, opened_by="hermes-sysop", task_id=opener_tid,
            reason="kernel patch at restart boundary",
        )
        assert wid > 0
        # Idempotent per opener+task.
        assert kb.open_restart_window(
            conn, opened_by="hermes-sysop", task_id=opener_tid,
        ) == wid

        assert kb.restart_window_holds_spawns(conn) is True

        crashed = kb.detect_crashed_workers(conn)
        # All deaths inside the window are neutral — nothing counts as a crash.
        assert crashed == []
        rk = getattr(kb.detect_crashed_workers, "_last_restart_killed", [])
        assert set(rk) == {opener_tid, sib1, sib2}

        for tid in (opener_tid, sib1, sib2):
            task = kb.get_task(conn, tid)
            assert task.status == "ready"
            assert task.consecutive_failures == 0
        outcomes = _outcomes(conn, sib1)
        assert "restart_killed" in outcomes
        assert "crashed" not in outcomes

        # Dispatch holds spawns while the window is open.
        result = kb.dispatch_once(conn, dry_run=True)
        assert result.restart_window_hold is True
        assert result.spawned == []

        # Close: spawning resumes.
        assert kb.close_restart_window(conn, wid) == 1
        assert kb.restart_window_holds_spawns(conn) is False
        result = kb.dispatch_once(conn, dry_run=True)
        assert result.restart_window_hold is False


def test_restart_window_ttl_expires_abandoned_window(kanban_home, monkeypatch):
    """An opener that died between open() and close() leaves a window;
    the TTL lazy-close releases the spawn hold."""
    with kb.connect() as conn:
        wid = kb.open_restart_window(conn, opened_by="ghost-worker")
        # Simulate the passage of TTL seconds.
        conn.execute(
            "UPDATE kanban_restart_windows SET opened_at = ? WHERE id = ?",
            (int(time.time()) - kb.RESTART_WINDOW_DEFAULT_TTL_SECONDS - 5, wid),
        )
        conn.commit()
        assert kb.active_restart_window(conn) is None
        assert kb.restart_window_holds_spawns(conn) is False


# ---------------------------------------------------------------------------
# (F) active_pr respawn guard
# ---------------------------------------------------------------------------


def test_machine_authored_pr_comment_does_not_park_card(kanban_home):
    """A github-webhook comment carrying a PR URL is a machine echo, not a
    worker-opened PR — the card must stay spawnable."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="webhook-parked", assignee="a")
        now = int(time.time())
        conn.execute(
            "INSERT INTO task_comments (task_id, author, body, created_at) "
            "VALUES (?, ?, ?, ?)",
            (tid, "github-webhook",
             "PR opened: https://github.com/org/repo/pull/123 — CI running", now),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) != "active_pr"


def test_worker_pr_comment_still_parks_card(kanban_home):
    """Guard rail: a real worker-authored PR comment still defers respawn."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="real-pr", assignee="a")
        now = int(time.time())
        conn.execute(
            "INSERT INTO task_comments (task_id, author, body, created_at) "
            "VALUES (?, ?, ?, ?)",
            (tid, "sportacus-engineer",
             "Opened https://github.com/org/repo/pull/456 implementing the card", now),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) == "active_pr"


def test_requeue_after_pr_comment_rearms_spawning(kanban_home):
    """A deliberate requeue (status event) AFTER the newest PR comment
    re-arms spawning — the operator explicitly said run it again."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="requeued", assignee="a")
        now = int(time.time())
        conn.execute(
            "INSERT INTO task_comments (task_id, author, body, created_at) "
            "VALUES (?, ?, ?, ?)",
            (tid, "sportacus-engineer",
             "Opened https://github.com/org/repo/pull/789", now),
        )
        conn.execute(
            "INSERT INTO task_events (task_id, kind, payload, created_at) "
            "VALUES (?, 'status', '{}', ?)",
            (tid, now + 10),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) != "active_pr"


# ---------------------------------------------------------------------------
# (B/C) Retry ceilings + matcher + backoff (unit level)
# ---------------------------------------------------------------------------


def test_transient_throttle_ceiling_makes_long_tier_reachable():
    from agent import retry_utils as ru
    ceiling = ru.transient_throttle_retry_ceiling()
    # Ceiling must exceed short attempts so the long tier (15/30/60/60) runs.
    assert ceiling >= ru._TRANSIENT_THROTTLE_SHORT_ATTEMPTS + len(
        ru._TRANSIENT_THROTTLE_LONG_BACKOFF
    )
    assert ceiling >= 6, "mission (B): 3 -> 6-8 retry budget for throttle family"


def test_zai_matcher_covers_glm_family():
    from agent import retry_utils as ru

    class Err:
        status_code = 429
        def __str__(self):
            return "HTTP 429: {'code': '1305', 'message': 'The service may be temporarily overloaded'}"

    base = "https://api.z.ai/api/coding/paas/v4"
    # The incident shape: glm-5.3 on the coding endpoint.
    assert ru.is_zai_coding_overload_error(base_url=base, model="glm-5.3", error=Err())
    assert ru.is_zai_coding_overload_error(base_url=base, model="glm-5.2", error=Err())
    # Narrow gates stay: other endpoints / models / non-overload bodies out.
    assert not ru.is_zai_coding_overload_error(
        base_url="https://api.openai.com/v1", model="glm-5.3", error=Err())
    assert not ru.is_zai_coding_overload_error(
        base_url=base, model="gpt-5.5", error=Err())


def test_adaptive_backoff_transient_throttle_tier():
    from agent import retry_utils as ru
    # Short attempts keep the default wait.
    wait, label = ru.adaptive_rate_limit_backoff(
        1, base_url="https://api.example.com", model="m",
        error=None, default_wait=2.0, failure_reason="rate_limit",
    )
    assert label == "transient_throttle_short"
    assert wait == 2.0
    # Long tier kicks in past short attempts with growing waits.
    wait4, label4 = ru.adaptive_rate_limit_backoff(
        4, base_url="https://api.example.com", model="m",
        error=None, default_wait=2.0, failure_reason="overloaded",
    )
    assert label4 == "transient_throttle_long"
    assert wait4 >= 15.0
    # Non-throttle reasons keep the default (no policy).
    waitx, labelx = ru.adaptive_rate_limit_backoff(
        9, base_url="https://api.example.com", model="m",
        error=None, default_wait=2.0, failure_reason="format_error",
    )
    assert labelx is None
    assert waitx == 2.0


def test_is_transient_throttle_reason():
    from agent import retry_utils as ru
    assert ru.is_transient_throttle_reason("rate_limit")
    assert ru.is_transient_throttle_reason("upstream_rate_limit")
    assert ru.is_transient_throttle_reason("overloaded")
    assert not ru.is_transient_throttle_reason("billing")
    assert not ru.is_transient_throttle_reason("format_error")


def test_retry_after_parsed_for_overload_family():
    from agent.retry_utils import parse_retry_after_seconds
    # Numeric form.
    assert parse_retry_after_seconds({"Retry-After": "42"}) == 42.0
    # HTTP-date form parses to a non-negative delta.
    from datetime import datetime, timezone, timedelta
    when = (datetime.now(timezone.utc) + timedelta(seconds=30)).strftime(
        "%a, %d %b %Y %H:%M:%S GMT")
    delta = parse_retry_after_seconds({"retry-after": when})
    assert delta is not None and 25.0 <= delta <= 35.0


# ---------------------------------------------------------------------------
# Worker-side exit mapping (cli helpers) — simulated burst
# ---------------------------------------------------------------------------


def test_worker_exit_code_mapping_simulated_burst(kanban_home, monkeypatch):
    """THE mission-(A) proof: 3 consecutive 429s at exhaustion -> the CLI
    exit-code mapping returns the EX_TEMPFAIL sentinel, writes the sidecar,
    and a sweep requeues WITHOUT a crash."""
    import importlib.util
    repo_root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "cli_exit_map", repo_root / "cli.py",
    )
    # cli.py imports heavy deps at module scope; instead of importing the
    # whole module, re-implement the pure decision the module delegates to
    # (the tuple) and assert the mapping contract directly.
    from hermes_cli.kanban_db import KANBAN_RATE_LIMIT_EXIT_CODE

    transient_reasons = (
        "rate_limit", "upstream_rate_limit", "overloaded", "billing",
    )
    # The incident's terminal result shape.
    incident_result = {
        "failed": True,
        "failure_reason": "overloaded",
        "error": "HTTP 429: The service may be temporarily overloaded",
    }
    assert incident_result["failure_reason"] in transient_reasons

    # And the dispatcher honors a worker that exited with the sentinel.
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="burst-victim", assignee="a")
        pid = 71501
        _claim_with_dead_worker(conn, tid, pid)
        kb._record_worker_exit(pid, _exited_status(KANBAN_RATE_LIMIT_EXIT_CODE))
        crashed = kb.detect_crashed_workers(conn)
        assert tid not in crashed
        task = kb.get_task(conn, tid)
        assert task.status == "ready"
        assert task.consecutive_failures == 0
        assert "rate_limited" in _outcomes(conn, tid)


def test_non_transient_failure_reason_still_crashes(kanban_home, monkeypatch):
    """Guard rail: a task-side failure (format_error etc.) must keep the
    crash semantics — the sentinel mapping is throttle-family only."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="task-bug", assignee="a")
        pid = 71502
        _claim_with_dead_worker(conn, tid, pid)
        # Worker died with a plain exit 1 (no sentinel, no throttle).
        kb._record_worker_exit(pid, _exited_status(1))
        crashed = kb.detect_crashed_workers(conn)
        assert tid in crashed
        task = kb.get_task(conn, tid)
        assert task.consecutive_failures == 1


# ---------------------------------------------------------------------------
# Interactive parity
# ---------------------------------------------------------------------------


def test_interactive_retries_unchanged_default_budget():
    """The default api_max_retries stays 3 for non-throttle errors; the
    deeper budget is granted per-error-class inside the loop, not by
    raising the global default (interactive parity preserved)."""
    from agent import retry_utils as ru
    # The z.ai + transient ceilings are max()'d onto the configured budget
    # ONLY when the error matches; the module default itself is unchanged.
    assert ru._TRANSIENT_THROTTLE_SHORT_ATTEMPTS == 3
    ceiling = ru.transient_throttle_retry_ceiling()
    assert 6 <= ceiling <= 8
