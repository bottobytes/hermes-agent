"""t_39ece1cd: restart-orphan handling — neutral requeue + wait-and-adopt.

Two live incidents (2026-08-29, board 'sportacus'):
  * 03:45 container restart: first dispatcher tick crash-marked in-flight
    claims (runs 3139/3140 — "crashed", failure ticks, auto-blocks) when the
    restart itself, not the workers, killed them.
  * 03:01 host kill: run 3127 was crash-marked while its worker kept
    executing; run 3139 was double-spawned on top.

The fix:
1. Restart epoch — a monotonic counter persisted in ``task_meta``, rotated
   when the dispatching process identity changes. Deaths of claims taken
   under a PRIOR epoch, on the first window after rotation, whose exits are
   unknown to this process's registry, classify as ``restart_orphan``:
   NEUTRAL (like rate_limited) — no failure tick, no auto-block, requeue.
2. Wait-and-adopt — a pre-rotation claim whose PID is still ALIVE is never
   reclaimed: the claim is extended, ``worker_pid`` kept, one ``adopted``
   event per (task, epoch). No double-spawn onto a live worker.
3. Genuine crashes keep the honest stock classification (crashed lane).
"""
from __future__ import annotations

import json
import os
import sqlite3
import time

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    # CONTAINMENT (t_39ece1cd): a kanban WORKER process inherits
    # HERMES_KANBAN_DB / HERMES_KANBAN_BOARD / HERMES_KANBAN_TASK pinned at
    # its own board. kanban_db_path() prefers the env pin over HERMES_HOME,
    # so without these deletions a test run inside a worker writes to the
    # LIVE board (incident during this task's development: two junk tasks
    # landed on sportacus and had to be surgically removed). Kill the pins
    # so the fixture's tmp_path home is the ONLY kanban root.
    for var in ("HERMES_KANBAN_DB", "HERMES_KANBAN_BOARD",
                "HERMES_KANBAN_TASK", "HERMES_KANBAN_WORKSPACE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setattr(kb.Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


@pytest.fixture(autouse=True)
def _isolate_kanban_env(monkeypatch):
    """Belt-and-braces env isolation for EVERY test in this module (the
    worker env pins must never leak into any board resolution, including
    helper calls outside the kanban_home fixture)."""
    for var in ("HERMES_KANBAN_DB", "HERMES_KANBAN_BOARD",
                "HERMES_KANBAN_TASK", "HERMES_KANBAN_WORKSPACE"):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# epoch mechanics (unit level)
# ---------------------------------------------------------------------------

def test_first_epoch_is_one_and_sticky(kanban_home):
    conn = kb.connect()
    try:
        e1, r1 = kb._restart_epoch_state(conn)
        assert e1 == 1
        e2, r2 = kb._restart_epoch_state(conn)
        assert e2 == 1 and r2 == r1  # same process → no rotation
    finally:
        conn.close()


def test_epoch_rotates_on_new_dispatcher_identity(kanban_home):
    conn = kb.connect()
    try:
        e1, _ = kb._restart_epoch_state(conn)
        # simulate a restart: new process identity reads/writes the same DB
        e2, rot2 = kb._restart_epoch_state(conn, dispatcher_id="box:999999")
        assert e2 == e1 + 1
        # and is sticky for that identity
        e3, _ = kb._restart_epoch_state(conn, dispatcher_id="box:999999")
        assert e3 == e2
    finally:
        conn.close()


def test_epoch_state_degrades_to_none_without_table(kanban_home):
    """Legacy DB without task_meta: helpers degrade, never raise."""
    conn = kb.connect()
    try:
        conn.execute("DROP TABLE task_meta")
        conn.commit()
        kb._INITIALIZED_PATHS.clear()
        assert kb._restart_epoch_state(conn) == (None, 0)
        assert kb._read_meta(conn, "restart_epoch") == (None, None)
        kb._write_meta(conn, "restart_epoch", {"owner": "x"})  # no raise
    finally:
        conn.close()


def test_task_meta_created_on_legacy_board(tmp_path, monkeypatch):
    """A board DB created BEFORE this patch gets task_meta on next connect."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(kb.Path, "home", lambda: tmp_path)
    kb.init_db()
    conn = kb.connect()
    try:
        conn.execute("DROP TABLE task_meta")
        conn.commit()
    finally:
        conn.close()
    kb._INITIALIZED_PATHS.clear()
    conn = kb.connect()
    try:
        tables = {
            r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "task_meta" in tables
    finally:
        conn.close()


def test_orphan_window_env_override(kanban_home, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_RESTART_ORPHAN_WINDOW_SECONDS", "0")
    assert kb._resolve_restart_orphan_window_seconds() == 0
    monkeypatch.setenv("HERMES_KANBAN_RESTART_ORPHAN_WINDOW_SECONDS", "999")
    assert kb._resolve_restart_orphan_window_seconds() == 999
    monkeypatch.delenv("HERMES_KANBAN_RESTART_ORPHAN_WINDOW_SECONDS")
    assert kb._resolve_restart_orphan_window_seconds() == \
        kb.DEFAULT_RESTART_ORPHAN_WINDOW_SECONDS


# ---------------------------------------------------------------------------
# Lane A: neutral requeue (integration level)
# ---------------------------------------------------------------------------

def _simulate_pre_restart_claim(conn, tid, *, pid=123451, age=3000):
    """Claim + fake-spawn a worker 'under the old epoch'.

    The claim is taken with claimer 'box:111111' — the OLD dispatcher
    identity — and that identity persists its own epoch row first (a real
    pre-restart dispatcher ticks at least once before dying, which is what
    makes the later identity change provably a RESTART rather than a
    bootstrap). Tests then monkeypatch kb._claimer_id to 'box:424242' to
    simulate the post-restart dispatcher: same host (host-prefix filter
    matches), new identity → epoch rotation with rotated_at = now.
    """
    kb._restart_epoch_state(conn, dispatcher_id="box:111111")
    kb.claim_task(conn, tid, claimer="box:111111")
    kb._set_worker_pid(conn, tid, pid)
    conn.execute(
        "UPDATE tasks SET started_at = ? WHERE id = ?",
        (int(time.time()) - age, tid),
    )
    conn.commit()


def test_restart_orphan_death_is_neutral(kanban_home, monkeypatch):
    """The 03:45 incident shape: dead PID + rotated epoch → restart_orphan,
    NOT crashed. No failure tick, no auto-block, task back at ready."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="orphan a", assignee="worker")
        _simulate_pre_restart_claim(conn, tid)
        # restart happened: epoch rotates to a new identity, just now
        monkeypatch.setattr(kb, "_claimer_id", lambda: "box:424242")
        # dead pid, unknown exit (fresh process registry)
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)

        crashed = kb.detect_crashed_workers(conn)

        assert tid not in crashed
        t = conn.execute(
            "SELECT status, consecutive_failures, last_failure_error "
            "FROM tasks WHERE id = ?", (tid,),
        ).fetchone()
        assert t["status"] == "ready"          # requeued
        assert t["consecutive_failures"] == 0  # no failure tick
        assert t["last_failure_error"] is None  # no failure stamp at all
        ev = conn.execute(
            "SELECT kind, payload FROM task_events WHERE task_id = ? "
            "AND kind = 'restart_orphan'", (tid,),
        ).fetchone()
        assert ev is not None
        payload = json.loads(ev["payload"])
        # Fresh DB: the pre-restart identity never persisted an epoch, so
        # this rotation is epoch 1. (On the live board it was 2 — the
        # counter is monotonic per board, absolute value not meaningful.)
        assert payload["epoch"] >= 1
        assert "container restart" in payload["error"]
        run = conn.execute(
            "SELECT outcome FROM task_runs WHERE task_id = ? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert run["outcome"] == "restart_orphan"
        # neutral requeue respawns immediately (no cooldown semantics)
        assert kb.check_respawn_guard(conn, tid) is None
    finally:
        conn.close()


def test_genuine_crash_after_window_keeps_stock_classification(
    kanban_home, monkeypatch,
):
    """A dead PID whose claim postdates the rotation (or beyond the window)
    is an honest crash — unchanged stock behaviour."""
    conn = kb.connect()
    try:
        # epoch established by the "old" dispatcher, rotation in the past.
        # Owner is the CURRENT identity (via patch below), so the detect
        # call performs NO rotation — the persisted past rotated_at stands.
        kb._restart_epoch_state(conn, dispatcher_id="box:old")
        past = int(time.time()) - 3600
        conn.execute(
            "UPDATE task_meta SET value = ? WHERE key = 'restart_epoch'",
            (json.dumps({
                "owner": "box:old", "epoch": 1, "rotated_at": past,
            }),),
        )
        conn.commit()
        # the dispatcher that runs detect IS box:old (no restart happens)
        monkeypatch.setattr(kb, "_claimer_id", lambda: "box:old")

        tid = kb.create_task(conn, title="real crash", assignee="worker")
        # claimed AFTER the rotation → not an orphan even though dead-unknown.
        # Claim under the current (new) dispatcher identity.
        kb.claim_task(conn, tid, claimer="box:424242")
        kb._set_worker_pid(conn, tid, 555001)
        conn.execute(
            "UPDATE tasks SET started_at = ? WHERE id = ?",
            (past + 60, tid),
        )
        conn.commit()
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)

        crashed = kb.detect_crashed_workers(conn)
        assert tid in crashed
        ev = conn.execute(
            "SELECT kind FROM task_events WHERE task_id = ? "
            "AND kind IN ('crashed', 'restart_orphan') "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert ev["kind"] == "crashed"
        t = conn.execute(
            "SELECT consecutive_failures FROM tasks WHERE id = ?", (tid,),
        ).fetchone()
        assert t["consecutive_failures"] == 1  # honest failure accounting
    finally:
        conn.close()


def test_orphan_window_zero_disables_neutral_lane(kanban_home, monkeypatch):
    """Env kill-switch: window=0 restores stock classification exactly."""
    conn = kb.connect()
    try:
        monkeypatch.setenv("HERMES_KANBAN_RESTART_ORPHAN_WINDOW_SECONDS", "0")
        tid = kb.create_task(conn, title="orphan off", assignee="worker")
        _simulate_pre_restart_claim(conn, tid)
        monkeypatch.setattr(kb, "_claimer_id", lambda: "box:424242")
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)

        crashed = kb.detect_crashed_workers(conn)
        assert tid in crashed  # stock behaviour
    finally:
        conn.close()


def test_registry_known_death_is_not_restart_orphan(kanban_home, monkeypatch):
    """A death this process OBSERVED (in its exit registry) is a real,
    classified exit — the neutral lane only covers blind deaths."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="observed exit", assignee="worker")
        _simulate_pre_restart_claim(conn, tid)
        monkeypatch.setattr(kb, "_claimer_id", lambda: "box:424242")
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        # this process reaped the child: nonzero exit
        monkeypatch.setattr(
            kb, "_classify_worker_exit", lambda pid: ("nonzero_exit", 2),
        )

        crashed = kb.detect_crashed_workers(conn)
        assert tid in crashed
        run = conn.execute(
            "SELECT outcome, error FROM task_runs WHERE task_id = ? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert run["outcome"] == "crashed"
        assert "exited with code 2" in run["error"]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Lane B: wait-and-adopt (integration level)
# ---------------------------------------------------------------------------

def test_live_pre_rotation_pid_is_adopted_not_crashed(kanban_home, monkeypatch):
    """The 03:01 incident shape: claim predates the rotation, worker PID
    still ALIVE. Adopt: extend claim, keep pid/lock, one adopted event,
    NO requeue, NO crash event."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="survivor", assignee="worker")
        _simulate_pre_restart_claim(conn, tid, pid=os.getpid())
        monkeypatch.setattr(kb, "_claimer_id", lambda: "box:424242")
        # worker survived the restart — PID alive
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: True)
        old_expires = conn.execute(
            "SELECT claim_expires FROM tasks WHERE id = ?", (tid,),
        ).fetchone()["claim_expires"]

        crashed = kb.detect_crashed_workers(conn)

        assert crashed == []
        t = conn.execute(
            "SELECT status, worker_pid, claim_lock, claim_expires "
            "FROM tasks WHERE id = ?", (tid,),
        ).fetchone()
        assert t["status"] == "running"          # never requeued
        assert t["worker_pid"] == os.getpid()    # bookkeeping intact
        # extended (>= : same-second re-extension keeps the value; the
        # adopted event below is the durable proof of adoption)
        assert t["claim_expires"] >= old_expires
        ev = conn.execute(
            "SELECT kind, payload FROM task_events WHERE task_id = ? "
            "AND kind = 'adopted'", (tid,),
        ).fetchone()
        assert ev is not None
        payload = json.loads(ev["payload"])
        assert payload["reason"] == "restart_epoch_survivor"
        assert payload["epoch"] >= 1  # fresh-DB rotation (see note above)
        # second pass within the window: no duplicate adopted event
        kb.detect_crashed_workers(conn)
        n = conn.execute(
            "SELECT COUNT(*) c FROM task_events WHERE task_id = ? "
            "AND kind = 'adopted'", (tid,),
        ).fetchone()["c"]
        assert n == 1
    finally:
        conn.close()


def test_live_fresh_pid_silent_no_adopt_event(kanban_home, monkeypatch):
    """A normal mid-flight run (claimed under the CURRENT epoch): alive pid
    skips adoption bookkeeping — exactly the stock no-op."""
    conn = kb.connect()
    try:
        # establish epoch for THIS identity first (so the claim below is
        # post-rotation)
        kb._restart_epoch_state(conn)
        tid = kb.create_task(conn, title="fresh run", assignee="worker")
        # claimed under the CURRENT epoch (claimer = new identity)
        kb.claim_task(conn, tid, claimer="box:424242")
        kb._set_worker_pid(conn, tid, os.getpid())
        conn.execute(
            "UPDATE tasks SET started_at = ? WHERE id = ?",
            (int(time.time()) - 5, tid),
        )
        conn.commit()
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: True)

        kb.detect_crashed_workers(conn)

        ev = conn.execute(
            "SELECT COUNT(*) c FROM task_events WHERE task_id = ? "
            "AND kind = 'adopted'", (tid,),
        ).fetchone()["c"]
        assert ev == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dispatch_once surfacing
# ---------------------------------------------------------------------------

def test_dispatch_result_surfaces_restart_orphans(kanban_home, monkeypatch):
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="surfaced", assignee="worker")
        _simulate_pre_restart_claim(conn, tid)
        monkeypatch.setattr(kb, "_claimer_id", lambda: "box:424242")
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        monkeypatch.setattr(kb, "_classify_worker_exit", lambda pid: ("unknown", None))

        result = kb.dispatch_once(conn, dry_run=True, max_spawn=0)

        assert tid in result.restart_orphans
        assert tid not in result.crashed
        assert tid not in result.rate_limited
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# wake consumers
# ---------------------------------------------------------------------------

def test_locale_keys_exist():
    import yaml
    for loc in ("en", "zh"):
        with open(
            os.path.join(os.path.dirname(kb.__file__), "..", "locales",
                         f"{loc}.yaml"),
            encoding="utf-8",
        ) as f:
            data = yaml.safe_load(f)
        wake = data["gateway"]["kanban"]["wake"]
        assert "restart_orphan" in wake, loc


def test_plugin_renders_restart_orphan_wake():
    import importlib.util
    p = os.path.join(
        os.path.dirname(kb.__file__), "..", "plugins", "kanban-notifier",
        "__init__.py",
    )
    spec = importlib.util.spec_from_file_location("knotif_t39ec", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert "restart_orphan" in mod._TERMINAL_KINDS
    prompt = mod._build_wake_prompt("b", "t_x", {
        "kind": "restart_orphan",
        "payload": {"epoch": 2, "rotated_at": 1787961600},
        "title": "survivor card", "assignee": "sysop", "status": "ready",
        "result": "",
    })
    assert "orphaned by a container restart" in prompt
    assert "No failure was counted" in prompt
    assert "No action required" in prompt


def test_watchers_render_restart_orphan_ping():
    src = open(os.path.join(
        os.path.dirname(kb.__file__), "..", "gateway", "kanban_watchers.py",
    ), encoding="utf-8").read()
    assert 'kind == "restart_orphan"' in src
    assert 'orphaned by a container restart' in src
    assert '"restart_orphan"' in src  # TERMINAL_KINDS + _WAKE_KINDS
