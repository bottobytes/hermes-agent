"""t_f0393d9f — frozen-review wake gap tests.

Covers the three kernel changes:
  1. Status-aware unreachable grace (review lane short, others 2h default)
  2. review_spawn_starved detector: fire-once idempotency + re-arm
  3. Regression: ready/todo/triage keep the 2h generic grace
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

import pytest

import hermes_state
from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME with an empty kanban DB + stubbed profiles."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


@pytest.fixture
def stub_profiles(monkeypatch):
    """Deterministic profile resolution: 'riker-2' real, everything else miss.

    Returns a set so tests can add/remove profiles dynamically.
    """
    real = {"riker-2"}
    monkeypatch.setattr(
        kb, "_dispatch_profile_exists", lambda name: (name in real)
    )
    return real


def _conn(kanban_home):
    conn = kb.connect(Path(str(kanban_home)) / "kanban.db")
    conn.row_factory = sqlite3.Row
    return conn


def _mk_card(conn, task_id, *, status, assignee, age_s):
    """Insert a card directly with a backdated last event."""
    now = int(time.time())
    conn.execute(
        "INSERT INTO tasks (id, title, assignee, status, priority, created_by,"
        " created_at, workspace_kind) VALUES (?, ?, ?, ?, 5, 'worker', ?, 'scratch')",
        (task_id, f"title {task_id}", assignee, status, now - age_s - 10),
    )
    # backdated lifecycle event so the sweep's age math works
    conn.execute(
        "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) "
        "VALUES (?, NULL, 'status', NULL, ?)",
        (task_id, now - age_s),
    )
    conn.commit()


def _kinds(conn, task_id):
    return [
        r["kind"]
        for r in conn.execute(
            "SELECT kind FROM task_events WHERE task_id=? ORDER BY id", (task_id,)
        )
    ]


def _backdate_last_event(conn, task_id, age_s):
    conn.execute(
        "UPDATE task_events SET created_at = ? WHERE task_id = ? AND id = "
        "(SELECT MAX(id) FROM task_events WHERE task_id = ?)",
        (int(time.time()) - age_s, task_id, task_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# 1. Status-aware unreachable grace
# ---------------------------------------------------------------------------


def test_review_lane_uses_short_grace(kanban_home, stub_profiles, monkeypatch):
    """Review card under phantom assignee flags at the SHORT review grace.

    Acceptance 1a: park a card in review under a fake assignee with review
    grace = 5s via env → sweep fires assignee_unreachable + comment.
    """
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", "5")
    # generic grace left at default 2h to prove lane separation
    conn = _conn(kanban_home)
    _mk_card(conn, "t_rev1", status="review", assignee="sdlc-review", age_s=30)
    flagged = kb.reconcile_stuck_assignees(conn)
    assert flagged == ["t_rev1"]
    assert "assignee_unreachable" in _kinds(conn, "t_rev1")
    comment = conn.execute(
        "SELECT body FROM task_comments WHERE task_id='t_rev1'"
    ).fetchone()
    assert comment and "sdlc-review" in comment["body"]


def test_review_lane_below_short_grace_stays_silent(kanban_home, stub_profiles, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", "300")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_rev2", status="review", assignee="sdlc-review", age_s=30)
    assert kb.reconcile_stuck_assignees(conn) == []
    assert "assignee_unreachable" not in _kinds(conn, "t_rev2")


def test_review_lane_falls_back_to_generic_knob(kanban_home, stub_profiles, monkeypatch):
    """No review-specific knob → generic env/config steers BOTH lanes."""
    monkeypatch.delenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", raising=False)
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS", "10")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_rev3", status="review", assignee="ghost-reviewer", age_s=30)
    assert kb.reconcile_stuck_assignees(conn) == ["t_rev3"]


def test_generic_lanes_keep_2h_default(kanban_home, stub_profiles):
    """Acceptance 2 (regression): ready/todo/triage phantom cards under the
    2h default do NOT flag (30 min old → silent; 3h old → flags)."""
    conn = _conn(kanban_home)
    _mk_card(conn, "t_ready_young", status="ready", assignee="phantom-a", age_s=1800)
    _mk_card(conn, "t_todo_young", status="todo", assignee="phantom-b", age_s=1800)
    _mk_card(conn, "t_triage_young", status="triage", assignee="phantom-c", age_s=1800)
    assert kb.reconcile_stuck_assignees(conn) == []
    _mk_card(conn, "t_ready_old", status="ready", assignee="phantom-d", age_s=3 * 3600)
    assert kb.reconcile_stuck_assignees(conn) == ["t_ready_old"]


def test_explicit_grace_param_wins_all_lanes(kanban_home, stub_profiles):
    """Caller-supplied grace_seconds (tests/CLI) overrides per-lane resolution."""
    conn = _conn(kanban_home)
    _mk_card(conn, "t_rev_x", status="review", assignee="phantom-e", age_s=60)
    # explicit 5s grace flags the 60s-old card even though default review
    # grace is 15 min
    assert kb.reconcile_stuck_assignees(conn, grace_seconds=5) == ["t_rev_x"]


# ---------------------------------------------------------------------------
# 2. review_spawn_starved detector
# ---------------------------------------------------------------------------


def test_starved_fires_once_and_rearms(kanban_home, stub_profiles, monkeypatch):
    """Acceptance 1b core: real-profile reviewer never spawns → fires once at
    threshold; does NOT repeat; re-arms after a new review_requested."""
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "5")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_st1", status="review", assignee="riker-2", age_s=30)
    # simulate the real handoff: backdated review_requested
    conn.execute(
        "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) "
        "VALUES (?, NULL, 'review_requested', NULL, ?)",
        ("t_st1", int(time.time()) - 30),
    )
    conn.commit()

    first = kb.detect_review_spawn_starved(conn)
    assert first == ["t_st1"], "detector must fire at threshold"
    assert "review_spawn_starved" in _kinds(conn, "t_st1")
    comment = conn.execute(
        "SELECT body FROM task_comments WHERE task_id='t_st1'"
    ).fetchone()
    assert comment and "riker-2" in comment["body"]

    # Subsequent ticks: NO repeat (idempotent per episode)
    for _ in range(3):
        assert kb.detect_review_spawn_starved(conn) == []

    # Fresh review_requested re-arms the episode (new handoff after
    # changes_requested) → backdate it past threshold → fires again
    _backdate_last_event(conn, "t_st1", 30)
    conn.execute(
        "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) "
        "VALUES (?, NULL, 'review_requested', NULL, ?)",
        ("t_st1", int(time.time()) - 30),
    )
    conn.commit()
    assert kb.detect_review_spawn_starved(conn) == ["t_st1"]
    # ...and goes quiet again
    assert kb.detect_review_spawn_starved(conn) == []


def test_starved_ignores_phantom_assignee(kanban_home, stub_profiles, monkeypatch):
    """Phantom assignee in review is the unreachable sweep's job, not this
    detector's (verified miss → skip)."""
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "0")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_st2", status="review", assignee="sdlc-review", age_s=9999)
    assert kb.detect_review_spawn_starved(conn) == []


def test_starved_ignores_claimed_and_running(kanban_home, stub_profiles, monkeypatch):
    """A reviewer that claimed the card (claim_lock set / status running) is
    NOT starved."""
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "0")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_st3", status="review", assignee="riker-2", age_s=9999)
    conn.execute(
        "UPDATE tasks SET claim_lock='x:1' WHERE id='t_st3'"
    )
    conn.commit()
    assert kb.detect_review_spawn_starved(conn) == []
    conn.execute("UPDATE tasks SET claim_lock=NULL, status='running' WHERE id='t_st3'")
    conn.commit()
    assert kb.detect_review_spawn_starved(conn) == []


def test_starved_below_threshold_silent(kanban_home, stub_profiles, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_SECONDS_UNUSED", "0")
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "3600")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_st4", status="review", assignee="riker-2", age_s=60)
    assert kb.detect_review_spawn_starved(conn) == []


def test_starved_wired_into_dispatch_tick(kanban_home, stub_profiles, monkeypatch):
    """The detector rides the dispatch tick: result.review_spawn_starved is
    populated via dispatch_once, same as flagged_unreachable."""
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "0")
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS", "0")
    conn = _conn(kanban_home)
    _mk_card(conn, "t_st5", status="review", assignee="riker-2", age_s=60)
    result = kb.dispatch_once(conn)
    assert getattr(result, "review_spawn_starved", None) == ["t_st5"]
    # second tick: idempotent
    result2 = kb.dispatch_once(conn)
    assert result2.review_spawn_starved == []


def test_dispatch_result_field_exists():
    """DispatchResult carries the new field (dataclass surface)."""
    r = kb.DispatchResult()
    assert r.review_spawn_starved == []


# ---------------------------------------------------------------------------
# 3. Grace resolver unit checks
# ---------------------------------------------------------------------------


def test_resolver_lanes(monkeypatch):
    monkeypatch.delenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", raising=False)
    assert kb._resolve_unreachable_grace_seconds() == 7200
    assert kb._resolve_unreachable_grace_seconds(status="review") == 900
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", "42")
    assert kb._resolve_unreachable_grace_seconds(status="review") == 42
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS", "100")
    assert kb._resolve_unreachable_grace_seconds(status="ready") == 100
    # review with only the generic knob set: falls back to generic
    monkeypatch.delenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", raising=False)
    assert kb._resolve_unreachable_grace_seconds(status="review") == 100
    # invalid review knob (negative) falls through to generic too
    monkeypatch.setenv("HERMES_KANBAN_UNREACHABLE_GRACE_SECONDS_REVIEW", "-1")
    assert kb._resolve_unreachable_grace_seconds(status="review") == 100


def test_starve_resolver(monkeypatch):
    monkeypatch.delenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", raising=False)
    assert kb._resolve_review_spawn_starved_seconds() == 1800
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "7")
    assert kb._resolve_review_spawn_starved_seconds() == 7
    monkeypatch.setenv("HERMES_KANBAN_REVIEW_SPAWN_STARVED_SECONDS", "junk")
    assert kb._resolve_review_spawn_starved_seconds() == 1800
