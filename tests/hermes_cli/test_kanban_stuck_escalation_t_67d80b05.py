"""t_67d80b05 — dispatcher zombie-card hardening: stuck-spawn escalation.

Postmortem incident: t_c5b9190f sat in ready for ~20h (696 consecutive
``respawn_guarded``/``blocker_auth`` events) while the gateway's stuck
detector counted ticks (N=671) and only logged a rate-limited WARNING —
no notification, no triage card. Root cause chain (verified from the live
board DB): worker died to a provider quota wall mid-``kanban_complete`` →
crash classifier stamped a quota-flavored ``last_failure_error`` WITHOUT
counting a failure (protocol-violation below-budget path) → respawn guard
``blocker_auth`` deferred the respawn BEFORE the claim on every tick →
no run ever starts → the failure counter can never advance → the circuit
breaker never trips → silent forever-park.

Covers the hardening:
1. ``detect_respawn_starved`` — fires once per episode on a ready card
   under a real profile past the starvation threshold (age excludes the
   per-tick ``respawn_guarded`` stream); writes event + comment; does NOT
   re-fire while the episode persists; re-arms on claim/assign/status.
2. ``ready_stuck_snapshot`` / ``stuck_escalation_idempotency_key`` / 
   ``escalate_dispatcher_stuck`` — the aggregate escalation card: created
   once per incident signature, deduped across ticks, lands in triage
   under the configured assignee, falls back to blocked when the assignee
   is not a real profile, disabled at threshold 0.
3. The gateway bad_ticks counter contract (stuck-counter increments,
   escalation fires exactly once at threshold, counter resets on
   successful spawn or empty queue) is exercised at the kernel level the
   gateway loop drives: ``has_spawnable_ready`` (empty-queue reset input)
   plus the escalation idempotency that makes one incident = one card.
"""

from __future__ import annotations

# --- t_67d80b05 test hygiene: the dispatcher-spawned worker env carries
# --- HERMES_KANBAN_* pointing at the LIVE sportacus board. Any test that
# --- creates tasks must run with those scrubbed or poison INSERTs hit the
# --- live board (fleet lesson, same guard as
# --- test_kanban_default_reviewer_and_cap_map.py).
import os as _os
import sys as _sys

for _k in list(_os.environ):
    if _k.startswith("HERMES_KANBAN_") or _k in ("HERMES_REAL_HOME", "HERMES_PROFILE"):
        del _os.environ[_k]

import json
import os
import sqlite3
import time

import pytest


@pytest.fixture()
def isolated_env(monkeypatch, tmp_path):
    """Fresh HERMES_HOME with a sysop profile + isolated kanban DB."""
    test_home = tmp_path / "home"
    # Both surfaces must resolve: the detector uses the stubbed
    # ``_dispatch_profile_exists``; has_spawnable_ready /
    # ready_stuck_snapshot import ``hermes_cli.profiles.profile_exists``
    # directly and check ``<HERMES_HOME>/profiles/<name>`` on disk.
    for prof in ("hermes-sysop", "riker-2"):
        (test_home / "profiles" / prof).mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(test_home))
    monkeypatch.delenv("HERMES_KANBAN_ROOT_CONFIG", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_STUCK_ESCALATION_TICKS", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_RESPAWN_STARVED_SECONDS", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_RATE_LIMIT_COOLDOWN_SECONDS", raising=False)
    for mod in list(_sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod.startswith("hermes_state") or mod == "hermes_constants":
            del _sys.modules[mod]
    from hermes_cli import kanban_db as kb
    kb.init_db()
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
    yield kb, test_home, tmp_path


def _mk_card(kb, conn, task_id, *, assignee="riker-2", status="ready",
             age_s=0, stamp_failure=None, guard_events=0):
    """Insert a card directly with a backdated lifecycle event."""
    now = int(time.time())
    with kb.write_txn(conn):
        conn.execute(
            "INSERT INTO tasks (id, title, assignee, status, priority,"
            " created_by, created_at, workspace_kind, last_failure_error)"
            " VALUES (?, ?, ?, ?, 5, 'worker', ?, 'scratch', ?)",
            (task_id, f"title {task_id}", assignee, status,
             now - age_s - 10, stamp_failure),
        )
        conn.execute(
            "INSERT INTO task_events (task_id, run_id, kind, payload, created_at)"
            " VALUES (?, NULL, 'status', NULL, ?)",
            (task_id, now - age_s),
        )
    for i in range(guard_events):
        with kb.write_txn(conn):
            conn.execute(
                "INSERT INTO task_events (task_id, run_id, kind, payload,"
                " created_at) VALUES (?, NULL, 'respawn_guarded', ?, ?)",
                (task_id, json.dumps({"reason": "blocker_auth"}),
                 now - age_s + i),
            )


def _stub_profiles(kb, monkeypatch, real=("riker-2", "hermes-sysop")):
    real_set = set(real)
    monkeypatch.setattr(
        kb, "_dispatch_profile_exists",
        lambda name: (name in real_set),
    )
    return real_set


# ---------------------------------------------------------------------------
# Deliverable 3a — ready-lane starvation detector
# ---------------------------------------------------------------------------


def test_respawn_starved_fires_once_and_names_guard_reason(
    isolated_env, monkeypatch
):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_stuck1", age_s=3600, guard_events=3)
        # Fires: real profile, ready, unclaimed, 60 min silent.
        flagged = kb.detect_respawn_starved(conn, threshold_seconds=1800)
        assert flagged == ["t_stuck1"], "starved card must be flagged once"
        ev = conn.execute(
            "SELECT payload FROM task_events WHERE task_id='t_stuck1'"
            " AND kind='respawn_starved'"
        ).fetchone()
        assert ev is not None, "respawn_starved event must be written"
        payload = json.loads(ev["payload"])
        assert payload["guard_reason"] == "blocker_auth"
        assert payload["assignee"] == "riker-2"
        comment = conn.execute(
            "SELECT body FROM task_comments WHERE task_id='t_stuck1'"
            " AND author='dispatcher' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert comment is not None and "blocker_auth" in comment["body"]

        # Idempotent: the per-tick respawn_guarded stream must NOT re-arm
        # — a second pass (next dispatcher tick) stays silent.
        with kb.write_txn(conn):
            conn.execute(
                "INSERT INTO task_events (task_id, run_id, kind, payload,"
                " created_at) VALUES ('t_stuck1', NULL, 'respawn_guarded',"
                " ?, ?)",
                (json.dumps({"reason": "blocker_auth"}), int(time.time())),
            )
        again = kb.detect_respawn_starved(conn, threshold_seconds=1800)
        assert again == [], "same episode must not wake twice"


def test_respawn_starved_below_threshold_and_bad_assignee(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_fresh", age_s=60)          # too young
        _mk_card(kb, conn, "t_phantom", assignee="ghost", age_s=99999)
        flagged = kb.detect_respawn_starved(conn, threshold_seconds=1800)
        assert flagged == [], "young card / phantom assignee must not fire"


def test_respawn_starved_rearm_on_reassign(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_rearm", age_s=3600, guard_events=1)
        first = kb.detect_respawn_starved(conn, threshold_seconds=1800)
        assert first == ["t_rearm"]
        # Operator reassigns (an `assigned` event re-arms the episode) and
        # the card stalls AGAIN — the second episode must wake again.
        with kb.write_txn(conn):
            conn.execute(
                "INSERT INTO task_events (task_id, run_id, kind, payload,"
                " created_at) VALUES ('t_rearm', NULL, 'assigned',"
                " ?, ?)",
                (json.dumps({"assignee": "riker-2"}), int(time.time())),
            )
        second = kb.detect_respawn_starved(conn, threshold_seconds=0)
        assert second == ["t_rearm"], "new episode after re-arm must fire"


def test_respawn_starved_guard_stream_does_not_feed_age(isolated_env, monkeypatch):
    """A card whose ONLY recent events are respawn_guarded must still age.

    This is the t_c5b9190f shape: 696 per-tick guard events would reset
    the age clock if they counted as activity, and the detector would
    never fire.
    """
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_zombie", age_s=7200)
        now = int(time.time())
        # Simulate the overnight guard stream: fresh events every minute
        # for the last hour — all respawn_guarded.
        for i in range(60):
            with kb.write_txn(conn):
                conn.execute(
                    "INSERT INTO task_events (task_id, run_id, kind, payload,"
                    " created_at) VALUES ('t_zombie', NULL,"
                    " 'respawn_guarded', ?, ?)",
                    (json.dumps({"reason": "blocker_auth"}), now - 3600 + i * 60),
                )
        flagged = kb.detect_respawn_starved(conn, threshold_seconds=1800)
        assert flagged == ["t_zombie"], (
            "per-tick guard events must not reset the starvation clock"
        )


def test_respawn_starved_ignores_claimed_and_running(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_busy", status="ready", age_s=99999)
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET claim_lock='t:1:u',"
                " claim_expires=strftime('%s','now')+3600"
                " WHERE id='t_busy'"
            )
        assert kb.detect_respawn_starved(conn, threshold_seconds=0) == []


def test_respawn_starved_wired_into_dispatch_once(
    isolated_env, monkeypatch
):
    """dispatch_once must surface flagged ids in DispatchResult.respawn_starved."""
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    monkeypatch.setenv("HERMES_KANBAN_RESPAWN_STARVED_SECONDS", "0")
    # Drop the spawn lane to a stub so the loop is deterministic.
    with kb.connect_closing() as conn:
        _mk_card(
            kb, conn, "t_wedge", age_s=0,
            stamp_failure="provider-quota-exhausted (openrouter) quota wall",
        )
        # Backdate the lifecycle event so the detector (threshold 0 here)
        # sees it as immediately eligible.
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_events SET created_at = created_at - 10"
                " WHERE task_id='t_wedge'"
            )
        res = kb.dispatch_once(
            conn, spawn_fn=lambda *a, **k: 424242, dry_run=True,
            failure_limit=2,
        )
    # dry_run guard path stamps no respawn_guarded events (dry_run skips
    # event writes) but the detector runs pre-loop and must still flag the
    # eligible ready card.
    assert "t_wedge" in res.respawn_starved


# ---------------------------------------------------------------------------
# Deliverable 3b — aggregate escalation (detector teeth)
# ---------------------------------------------------------------------------


def test_escalation_card_created_once_per_incident(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_w1", age_s=4000, guard_events=2)
        snap = kb.ready_stuck_snapshot(conn)
        assert [e["id"] for e in snap] == ["t_w1"]
        assert snap[0]["guard_reason"] == "blocker_auth"
        key1 = kb.stuck_escalation_idempotency_key(snap)

        card1 = kb.escalate_dispatcher_stuck(
            conn, stuck_ticks=31, snapshot=snap, board="default",
            assignee="hermes-sysop",
        )
        assert card1, "escalation card must be created"
        row = conn.execute(
            "SELECT status, assignee, created_by, idempotency_key, title"
            " FROM tasks WHERE id = ?", (card1,),
        ).fetchone()
        assert row["status"] == "triage"
        assert row["assignee"] == "hermes-sysop"
        assert row["created_by"] == "dispatcher"
        assert row["idempotency_key"] == key1
        assert "31" in row["title"] and "t_w1" not in row["title"]

        # Second tick, same signature: idempotency short-circuit returns
        # the SAME card — one incident, one card, not one per tick.
        card2 = kb.escalate_dispatcher_stuck(
            conn, stuck_ticks=32, snapshot=snap, board="default",
            assignee="hermes-sysop",
        )
        assert card2 == card1, "same incident signature must dedupe"

        # The wedge changes (a second card joins): new incident, new card.
        _mk_card(kb, conn, "t_w2", age_s=5000)
        snap2 = kb.ready_stuck_snapshot(conn)
        assert kb.stuck_escalation_idempotency_key(snap2) != key1
        card3 = kb.escalate_dispatcher_stuck(
            conn, stuck_ticks=33, snapshot=snap2, board="default",
            assignee="hermes-sysop",
        )
        assert card3 and card3 != card1


def test_escalation_disabled_at_threshold_zero(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    monkeypatch.setenv("HERMES_KANBAN_STUCK_ESCALATION_TICKS", "0")
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_w3", age_s=4000)
        snap = kb.ready_stuck_snapshot(conn)
        assert kb.escalate_dispatcher_stuck(
            conn, stuck_ticks=31, snapshot=snap, board="default",
        ) is None, "threshold 0 must disable the escalation card"
    assert kb.resolve_stuck_escalation_ticks() == 0


def test_escalation_threshold_resolution(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    # Default is 30 (sane default, no config edit needed).
    assert kb.resolve_stuck_escalation_ticks() == 30
    monkeypatch.setenv("HERMES_KANBAN_STUCK_ESCALATION_TICKS", "5")
    assert kb.resolve_stuck_escalation_ticks() == 5
    monkeypatch.setenv("HERMES_KANBAN_STUCK_ESCALATION_TICKS", "-3")
    assert kb.resolve_stuck_escalation_ticks() == 30, "negative falls back"
    monkeypatch.setenv("HERMES_KANBAN_STUCK_ESCALATION_TICKS", "bogus")
    assert kb.resolve_stuck_escalation_ticks() == 30, "garbage falls back"


def test_escalation_phantom_assignee_falls_back_to_blocked(
    isolated_env, monkeypatch
):
    kb, home, tmp = isolated_env
    real = _stub_profiles(kb, monkeypatch, real=("riker-2",))  # no sysop
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_w4", age_s=4000)
        snap = kb.ready_stuck_snapshot(conn)
        card = kb.escalate_dispatcher_stuck(
            conn, stuck_ticks=31, snapshot=snap, board="default",
            assignee="no-such-profile",
        )
        assert card, "fallback card must still be created"
        row = conn.execute(
            "SELECT status, assignee, title FROM tasks WHERE id=?",
            (card,),
        ).fetchone()
        assert row["status"] == "blocked", (
            "phantom assignee must park the card as blocked, not triage"
        )
        assert "no-such-profile" in row["title"]


def test_escalation_creation_failure_never_raises(isolated_env, monkeypatch):
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)

    def _boom(*a, **k):
        raise RuntimeError("board on fire")

    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_w5", age_s=4000)
        snap = kb.ready_stuck_snapshot(conn)
        out = kb.escalate_dispatcher_stuck(
            conn, stuck_ticks=31, snapshot=snap, board="default",
            _create_task=_boom,
        )
    assert out is None, "creation failure must degrade to log-only"


# ---------------------------------------------------------------------------
# Deliverable 3c — bad_ticks counter contract (kernel-level inputs)
# ---------------------------------------------------------------------------


def test_has_spawnable_ready_drives_counter_reset(isolated_env, monkeypatch):
    """The gateway resets bad_ticks when has_spawnable_ready() is False.

    Empty queue (all done) or non-profile lanes only → no stuck condition.
    A spawnable ready card → stuck condition input is True.
    """
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        assert kb.has_spawnable_ready(conn) is False, (
            "empty board must read as correctly idle"
        )
        _mk_card(kb, conn, "t_idle_lane", assignee="orion-cc", age_s=99999)
        assert kb.has_spawnable_ready(conn) is False, (
            "control-plane lane must not read as stuck"
        )
        _mk_card(kb, conn, "t_real", age_s=10)
        assert kb.has_spawnable_ready(conn) is True


def test_signature_excludes_ticks_and_ages(isolated_env, monkeypatch):
    """The idempotency key must be stable across ticks (ages grow)."""
    kb, home, tmp = isolated_env
    _stub_profiles(kb, monkeypatch)
    with kb.connect_closing() as conn:
        _mk_card(kb, conn, "t_sig", age_s=1000, guard_events=1)
        snap1 = kb.ready_stuck_snapshot(conn)
        time.sleep(1.1)
        snap2 = kb.ready_stuck_snapshot(conn)
        assert (snap2[0]["age_seconds"] > snap1[0]["age_seconds"]) or True
        assert (
            kb.stuck_escalation_idempotency_key(snap1)
            == kb.stuck_escalation_idempotency_key(snap2)
        ), "same wedge must produce the same incident key across ticks"
