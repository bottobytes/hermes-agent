"""Regression tests for the 2026-09-09 review-lane starvation incident (t_3371481a).

Incident: quota walls stamped quota-flavored ``last_failure_error`` strings
onto 13 review cards. The recovery cron re-armed only the ENGINEER cohort
(``hermes kanban reassign`` clears the string); the REVIEW cohort's frozen
strings kept re-matching the respawn guard's ``blocker_auth`` regex on every
tick — 4.3h of silent starvation while the reviewer's credentials were
verified healthy by live probe.

Patch under test (kernel card t_3371481a):

1. **Staleness gate** — a ``blocker_auth``-matching error string whose latest
   run ended more than ``HERMES_KANBAN_RESPAWN_AUTH_STALENESS_SECONDS``
   (default 1800) ago no longer holds the card: the guard releases and the
   next tick spawns (worst case: one fresh rate-limited exit that re-stamps
   a fresh string).
2. **Live-credential gate** — when the error names a provider (kernel-stamped
   ``provider-quota-exhausted (zai)`` shape) and the assignee profile's
   credential pool for that provider has at least one non-DEAD key with no
   ACTIVE exhausted mark, the string is stale by definition and the guard
   releases — exactly the incident's live-probe-healthy / string-frozen
   state. All-keys-actively-exhausted pools still HOLD.
3. **Rate-limit backoff** — the ``rate_limit_cooldown`` reason grows with the
   card's trailing run of consecutive ``rate_limited`` outcomes (base x2 per
   bounce, capped at 30 min) so burst/concurrency 429s space probes out
   instead of bouncing every flat window. Key marks are never touched.
4. **Fail-safe defaults** — unreadable pool, absent profile, and providerless
   error text never release the guard on their own (the staleness gate
   carries those releases).
"""

from __future__ import annotations

import json
import os
import time

import pytest

from hermes_cli import kanban_db as kb

# t_3371481a: a kanban WORKER runs this suite with HERMES_KANBAN_* pinned at
# the LIVE board. Worker env beats HERMES_HOME in the board resolver, so any
# leaked var sends fixture writes to production. Strip at import time —
# before any fixture can open a connection (root conftest also strips these
# in its sandbox fixture, but import order must not be load-bearing).
for _var in (
    "HERMES_KANBAN_DB",
    "HERMES_KANBAN_BOARD",
    "HERMES_KANBAN_HOME",
    "HERMES_KANBAN_WORKSPACES_ROOT",
    "HERMES_KANBAN_LOGS_ROOT",
    "HERMES_KANBAN_TASK",
    "HERMES_KANBAN_WORKSPACE",
    "HERMES_KANBAN_RUN_ID",
    "HERMES_KANBAN_CLAIM_LOCK",
):
    os.environ.pop(_var, None)


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME with an empty kanban DB."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    kb.init_db()
    return home


@pytest.fixture(autouse=True)
def _clear_pool_cache():
    """The pool-state cache is module-global; isolate every test."""
    kb._auth_pool_state_cache.clear()
    yield
    kb._auth_pool_state_cache.clear()


QUOTA_ERR = (
    "pid 4321 exited rate-limited — provider-quota-exhausted (zai): "
    "provider quota/rate wall, requeued without counting a failure "
    "(respawn deferred until the window clears)"
)


def _seed_card_with_error(conn, *, error, ended_at, outcome="crashed"):
    """Create a ready card carrying a stamped failure error + ended run."""
    tid = kb.create_task(conn, title="starved-card", assignee="a")
    kb.claim_task(conn, tid)
    run_id = kb.get_task(conn, tid).current_run_id
    conn.execute(
        "UPDATE task_runs SET outcome=?, status=?, ended_at=? WHERE id=?",
        (outcome, outcome, ended_at, run_id),
    )
    conn.execute(
        "UPDATE tasks SET status='ready', current_run_id=NULL, "
        "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL, "
        "last_failure_error=? WHERE id=?",
        (error, tid),
    )
    conn.commit()
    return tid


def _write_pool_auth(home, entries):
    """Write a credential_pool auth.json into a profile home."""
    (home / "auth.json").write_text(
        json.dumps({"credential_pool": {"zai": entries}}), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# 1. Staleness gate
# ---------------------------------------------------------------------------


def test_stale_error_string_releases_guard(kanban_home, monkeypatch):
    """The incident's frozen string, 45 min old, no longer holds the card."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    with kb.connect() as conn:
        tid = _seed_card_with_error(
            conn, error=QUOTA_ERR, ended_at=now - 45 * 60
        )
        # Pre-patch behavior: pure regex match → blocker_auth forever.
        assert kb.check_respawn_guard(conn, tid) is None  # released (stale)


def test_fresh_error_string_still_guards(kanban_home, monkeypatch):
    """Inside the staleness window (and no clean pool to vouch) the guard holds."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    with kb.connect() as conn:
        tid = _seed_card_with_error(
            conn, error=QUOTA_ERR, ended_at=now - 60
        )
        assert kb.check_respawn_guard(conn, tid) == "blocker_auth"


def test_staleness_window_is_configurable(kanban_home, monkeypatch):
    """A tight window releases what the default window holds."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    monkeypatch.setenv("HERMES_KANBAN_RESPAWN_AUTH_STALENESS_SECONDS", "30")
    with kb.connect() as conn:
        tid = _seed_card_with_error(
            conn, error=QUOTA_ERR, ended_at=now - 60
        )
        assert kb.check_respawn_guard(conn, tid) is None


def test_staleness_disabled_by_zero(kanban_home, monkeypatch):
    """staleness=0 disables the gate; a fresh string without pool info holds."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    monkeypatch.setenv("HERMES_KANBAN_RESPAWN_AUTH_STALENESS_SECONDS", "0")
    with kb.connect() as conn:
        tid = _seed_card_with_error(
            conn, error=QUOTA_ERR, ended_at=now - 45 * 60
        )
        assert kb.check_respawn_guard(conn, tid) == "blocker_auth"


def test_no_ended_run_unknown_age_holds(kanban_home, monkeypatch):
    """Unknown age must NOT release a guard the credential gate can't vouch for."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="legacy", assignee="a")
        conn.execute(
            "UPDATE tasks SET last_failure_error=? WHERE id=?",
            (QUOTA_ERR, tid),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) == "blocker_auth"


# ---------------------------------------------------------------------------
# 2. Live-credential gate
# ---------------------------------------------------------------------------


def test_clean_pool_releases_guard_with_fresh_string(kanban_home, monkeypatch, tmp_path):
    """The incident's exact state: fresh string + live-probe-healthy pool."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    # Build a profile home with a clean zai pool.
    profiles = tmp_path / ".hermes" / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    prof = profiles / "rv"
    prof.mkdir()
    _write_pool_auth(prof, [
        {"label": "key0", "last_status": "ok"},
        {"label": "key1", "last_status": None},
    ])
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="review-card", assignee="rv")
        conn.execute(
            "UPDATE tasks SET last_failure_error=? WHERE id=?",
            (QUOTA_ERR, tid),
        )
        # A fresh crashed run (ended 60s ago) — NOT rate_limited, so the
        # cooldown path is inert and blocker_auth is the live question.
        kb.claim_task(conn, tid)
        run_id = kb.get_task(conn, tid).current_run_id
        conn.execute(
            "UPDATE task_runs SET outcome='crashed', status='crashed', "
            "ended_at=? WHERE id=?",
            (now - 60, run_id),
        )
        conn.execute(
            "UPDATE tasks SET status='ready', current_run_id=NULL, "
            "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL "
            "WHERE id=?",
            (tid,),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) is None  # clean pool → release


def test_all_keys_exhausted_pool_holds_guard(kanban_home, monkeypatch, tmp_path):
    """Genuine quota wall: every non-DEAD key actively exhausted → HOLD."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    profiles = tmp_path / ".hermes" / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    prof = profiles / "rv"
    prof.mkdir()
    _write_pool_auth(prof, [
        {
            "label": "key0",
            "last_status": "exhausted",
            "last_status_at": now - 60,
            # No reset_at → status_at + 1h fallback TTL → active until now+3540.
        },
        {
            "label": "key1",
            "last_status": "exhausted",
            "last_status_at": now - 60,
            "last_error_reset_at": now + 3_600,
        },
    ])
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="review-card", assignee="rv")
        conn.execute(
            "UPDATE tasks SET last_failure_error=? WHERE id=?",
            (QUOTA_ERR, tid),
        )
        kb.claim_task(conn, tid)
        run_id = kb.get_task(conn, tid).current_run_id
        conn.execute(
            "UPDATE task_runs SET outcome='crashed', status='crashed', "
            "ended_at=? WHERE id=?",
            (now - 60, run_id),
        )
        conn.execute(
            "UPDATE tasks SET status='ready', current_run_id=NULL, "
            "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL "
            "WHERE id=?",
            (tid,),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) == "blocker_auth"  # HOLD


def test_expired_marks_release_guard(kanban_home, monkeypatch, tmp_path):
    """All keys exhausted but every mark's recovery time has passed → release."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    profiles = tmp_path / ".hermes" / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    prof = profiles / "rv"
    prof.mkdir()
    _write_pool_auth(prof, [
        {
            "label": "key0",
            "last_status": "exhausted",
            "last_status_at": now - 7_200,
            # No reset_at → fallback TTL 1h → expired an hour ago.
        },
    ])
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="review-card", assignee="rv")
        conn.execute(
            "UPDATE tasks SET last_failure_error=? WHERE id=?",
            (QUOTA_ERR, tid),
        )
        kb.claim_task(conn, tid)
        run_id = kb.get_task(conn, tid).current_run_id
        conn.execute(
            "UPDATE task_runs SET outcome='crashed', status='crashed', "
            "ended_at=? WHERE id=?",
            (now - 60, run_id),
        )
        conn.execute(
            "UPDATE tasks SET status='ready', current_run_id=NULL, "
            "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL "
            "WHERE id=?",
            (tid,),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) is None  # expired marks → release


def test_unreadable_pool_holds_guard(kanban_home, monkeypatch, tmp_path):
    """Fail-safe: profile exists but auth.json unparsable → NOT clean → hold."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    profiles = tmp_path / ".hermes" / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    prof = profiles / "rv"
    prof.mkdir()
    (prof / "auth.json").write_text("{not json", encoding="utf-8")
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="review-card", assignee="rv")
        conn.execute(
            "UPDATE tasks SET last_failure_error=? WHERE id=?",
            (QUOTA_ERR, tid),
        )
        kb.claim_task(conn, tid)
        run_id = kb.get_task(conn, tid).current_run_id
        conn.execute(
            "UPDATE task_runs SET outcome='crashed', status='crashed', "
            "ended_at=? WHERE id=?",
            (now - 60, run_id),
        )
        conn.execute(
            "UPDATE tasks SET status='ready', current_run_id=NULL, "
            "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL "
            "WHERE id=?",
            (tid,),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) == "blocker_auth"


# ---------------------------------------------------------------------------
# 3. Rate-limit backoff growth
# ---------------------------------------------------------------------------


def _seed_rate_limited_card(conn, now, outcomes):
    """Create a ready card whose trailing runs are the given outcomes."""
    tid = kb.create_task(conn, title="burst-card", assignee="a")
    for outcome in outcomes:
        kb.claim_task(conn, tid)
        run_id = kb.get_task(conn, tid).current_run_id
        conn.execute(
            "UPDATE task_runs SET outcome=?, status=?, ended_at=? WHERE id=?",
            (outcome, outcome, now - 100, run_id),
        )
        conn.execute(
            "UPDATE tasks SET status='ready', current_run_id=NULL, "
            "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL "
            "WHERE id=?",
            (tid,),
        )
        conn.commit()
    conn.execute(
        "UPDATE tasks SET last_failure_error=? WHERE id=?",
        (QUOTA_ERR, tid),
    )
    conn.commit()
    return tid


def test_burst_backoff_doubles_cooldown(kanban_home, monkeypatch):
    """One bounce → flat cooldown; three bounces → 4x (capped later)."""
    now = 5_000_000
    monkeypatch.setenv("HERMES_KANBAN_RATE_LIMIT_COOLDOWN_SECONDS", "300")
    with kb.connect() as conn:
        # Single bounce: inside flat 300s → guarded.
        tid = _seed_rate_limited_card(conn, now, ["rate_limited"])
        monkeypatch.setattr(kb.time, "time", lambda: now + 100)
        assert kb.check_respawn_guard(conn, tid) == "rate_limit_cooldown"
        monkeypatch.setattr(kb.time, "time", lambda: now + 400)
        assert kb.check_respawn_guard(conn, tid) is None  # flat window elapsed

        # Three bounces: 300 * 2^2 = 1200s — still guarded at +400s...
        tid3 = _seed_rate_limited_card(
            conn, now, ["rate_limited", "rate_limited", "rate_limited"]
        )
        monkeypatch.setattr(kb.time, "time", lambda: now + 400)
        assert kb.check_respawn_guard(conn, tid3) == "rate_limit_cooldown"
        # ...released past 1200s.
        monkeypatch.setattr(kb.time, "time", lambda: now + 1_300)
        assert kb.check_respawn_guard(conn, tid3) is None


def test_backoff_streak_broken_by_completion(kanban_home, monkeypatch):
    """A completed run between bounces resets the streak to the base window."""
    now = 5_000_000
    monkeypatch.setenv("HERMES_KANBAN_RATE_LIMIT_COOLDOWN_SECONDS", "300")
    with kb.connect() as conn:
        tid = _seed_rate_limited_card(
            conn, now, ["rate_limited", "completed", "rate_limited"]
        )
        monkeypatch.setattr(kb.time, "time", lambda: now + 400)
        # Trailing streak is 1 (the completion broke it): flat window gone.
        assert kb.check_respawn_guard(conn, tid) is None


def test_backoff_caps_at_30_minutes(kanban_home, monkeypatch):
    """Deep bounce streaks never exceed the 30-minute cap."""
    now = 5_000_000
    monkeypatch.setenv("HERMES_KANBAN_RATE_LIMIT_COOLDOWN_SECONDS", "300")
    with kb.connect() as conn:
        tid = _seed_rate_limited_card(conn, now, ["rate_limited"] * 8)
        # Runs ended at now-100 → cap window elapses at (now-100)+1800.
        monkeypatch.setattr(kb.time, "time", lambda: now + 1_800 - 101)
        assert kb.check_respawn_guard(conn, tid) == "rate_limit_cooldown"
        monkeypatch.setattr(kb.time, "time", lambda: now + 1_800 - 99)
        assert kb.check_respawn_guard(conn, tid) is None


# ---------------------------------------------------------------------------
# 4. Interactions
# ---------------------------------------------------------------------------


def test_release_survives_pool_cache_cold_and_warm(kanban_home, monkeypatch, tmp_path):
    """The mtime cache must not flip a release into a hold across reads."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    profiles = tmp_path / ".hermes" / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    prof = profiles / "rv"
    prof.mkdir()
    _write_pool_auth(prof, [{"label": "key0", "last_status": "ok"}])
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="cache-card", assignee="rv")
        conn.execute(
            "UPDATE tasks SET last_failure_error=? WHERE id=?",
            (QUOTA_ERR, tid),
        )
        kb.claim_task(conn, tid)
        run_id = kb.get_task(conn, tid).current_run_id
        conn.execute(
            "UPDATE task_runs SET outcome='crashed', status='crashed', "
            "ended_at=? WHERE id=?",
            (now - 60, run_id),
        )
        conn.execute(
            "UPDATE tasks SET status='ready', current_run_id=NULL, "
            "claim_lock=NULL, claim_expires=NULL, worker_pid=NULL "
            "WHERE id=?",
            (tid,),
        )
        conn.commit()
        assert kb.check_respawn_guard(conn, tid) is None  # cold read
        assert kb.check_respawn_guard(conn, tid) is None  # warm cache read
        # A different provider asked from the same unchanged file must not
        # be masked by the cached entry (per-provider isolation).
        state = kb._pool_state_for_provider(prof / "auth.json", "zai")
        assert state is not None and len(state["entries"]) == 1
        assert kb._pool_state_for_provider(prof / "auth.json", "openrouter") is None


def test_review_lane_uses_same_gates(kanban_home, monkeypatch):
    """The review lane (where the incident starved) releases identically."""
    now = 5_000_000
    monkeypatch.setattr(kb.time, "time", lambda: now)
    with kb.connect() as conn:
        tid = _seed_card_with_error(
            conn, error=QUOTA_ERR, ended_at=now - 45 * 60
        )
        assert kb.check_respawn_guard(conn, tid, lane="review") is None
