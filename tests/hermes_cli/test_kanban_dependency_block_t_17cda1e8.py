"""Regression tests for t_17cda1e8 — vacuous dependency blocks refused.

Footgun (observed live on t_52135e4c, 2026-09-07 21:06 CEST):
``block_task(kind='dependency')`` parked the card in ``todo`` (kanban_db.py
"Dependency blocks never enter the human blocked bucket"), but
``recompute_ready`` promotes EVERY todo card whose parents are all done or
archived — the sticky guard (``_has_sticky_block``) only applies to
``blocked`` cards. A dependency block on a card whose parents were already
terminal was therefore a vacuous wait: the card auto-promoted 24 s later
(gateway log ``promoted=1``) and the dispatcher respawned the worker, once
per dispatch tick pair, forever (~1 V4-pro spawn per cycle). The only manual
escape was re-blocking with ``needs_input``.

Contract under test (kernel fix, option (a) of the task card):

* ``block_task(kind='dependency')`` on a card whose parents are ALL
  terminal (done/archived) — or which has NO parents — is REFUSED:
  returns ``False`` (legacy bool shape), or ``(False, reason)`` with
  ``with_reason=True``; the reason names the loop mechanism and tells the
  caller to use ``kind='needs_input'``. The card keeps its current status,
  nothing is written, no ``dependency_wait`` event fires.
* A card with at least one non-terminal parent blocks to ``todo``
  exactly as before (regression pin), emits ``dependency_wait``, and
  still auto-promotes when the parent completes.
* The live incident shape (single done parent) is asserted end-to-end:
  refused AND ``recompute_ready`` cannot re-promote it — no respawn loop.
* Tool surface: ``_handle_block`` returns the kernel reason verbatim in
  the error payload (the worker reads the reason and corrects the call).
* CLI surface: ``_cmd_block`` exits non-zero and prints the reason.

The t_fbd0fb38 assignee-validation suite pins the adjacent tool-layer
patterns; this file mirrors its env hygiene so the LIVE board can never be
touched from a dispatcher-spawned worker session.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# t_17cda1e8: this file may run inside a kanban worker session whose env
# pins the LIVE board (HERMES_KANBAN_* beats HERMES_HOME in kb.connect()
# resolution). Strip at module level (not a fixture) so every import order
# and the per-file subprocess conftest pattern are both protected — fleet
# lesson from the e2e fixtures (poison INSERTs hit the live board).
for _var in (
    "HERMES_KANBAN_DB",
    "HERMES_KANBAN_BOARD",
    "HERMES_KANBAN_TASK",
    "HERMES_KANBAN_RUN_ID",
    "HERMES_KANBAN_CLAIM_LOCK",
    "HERMES_KANBAN_WORKSPACE",
    "HERMES_KANBAN_WORKSPACES_ROOT",
    "HERMES_REAL_HOME",
    "HERMES_PROFILE",
):
    os.environ.pop(_var, None)

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Isolated HERMES_HOME with an empty kanban DB."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    yield home
    kb._INITIALIZED_PATHS.clear()


def _running_task(conn, title="t"):
    """Create a task and drive it to ``running`` so block_task can act."""
    tid = kb.create_task(conn, title=title, assignee="worker")
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (tid,))
    claimed = kb.claim_task(conn, tid, claimer="worker")
    assert claimed is not None
    return tid


def _finish(conn, tid):
    """Force a task row to ``done`` (bypass claim machinery)."""
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET status='done' WHERE id=?", (tid,))


def _events(conn, tid, kind=None):
    rows = conn.execute(
        "SELECT kind, payload FROM task_events WHERE task_id = ? ORDER BY id",
        (tid,),
    ).fetchall()
    out = [
        (r["kind"], json.loads(r["payload"]) if r["payload"] else None)
        for r in rows
    ]
    if kind is not None:
        out = [e for e in out if e[0] == kind]
    return out


# ---------------------------------------------------------------------------
# Kernel: vacuous dependency blocks are refused
# ---------------------------------------------------------------------------


def test_dependency_block_all_parents_done_refused(kanban_home: Path) -> None:
    """The live t_52135e4c shape: single parent already done → refuse."""
    with kb.connect_closing() as conn:
        parent = kb.create_task(conn, title="parent", assignee="worker")
        _finish(conn, parent)
        child = _running_task(conn, title="child")
        kb.link_tasks(conn, parent_id=parent, child_id=child)

        ok = kb.block_task(conn, child, reason="wait restart", kind="dependency")
        assert ok is False, "vacuous dependency block must be refused"
        # Card untouched: still running, claim intact, no todo parking.
        task = kb.get_task(conn, child)
        assert task.status == "running"
        assert task.claim_lock is not None
        # No dependency_wait event — the wait never started.
        assert _events(conn, child, kind="dependency_wait") == []


def test_dependency_block_no_parents_refused(kanban_home: Path) -> None:
    """Parentless card: the wait is just as vacuous — refuse."""
    with kb.connect_closing() as conn:
        tid = _running_task(conn)
        ok, why = kb.block_task(
            conn, tid, reason="wait", kind="dependency", with_reason=True,
        )
        assert ok is False
        assert why and "needs_input" in why
        assert kb.get_task(conn, tid).status == "running"
        assert _events(conn, tid, kind="dependency_wait") == []


def test_refusal_reason_is_actionable(kanban_home: Path) -> None:
    """with_reason=True names the loop mechanism and the fix."""
    with kb.connect_closing() as conn:
        tid = _running_task(conn)
        ok, why = kb.block_task(
            conn, tid, reason="wait", kind="dependency", with_reason=True,
        )
        assert ok is False
        assert "recompute_ready" in why
        assert "needs_input" in why


def test_dependency_block_all_parents_archived_refused(kanban_home: Path) -> None:
    """archived is terminal too — same refusal as done."""
    with kb.connect_closing() as conn:
        parent = kb.create_task(conn, title="parent", assignee="worker")
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status='archived' WHERE id=?", (parent,),
            )
        child = _running_task(conn, title="child")
        kb.link_tasks(conn, parent_id=parent, child_id=child)
        ok = kb.block_task(conn, child, reason="wait", kind="dependency")
        assert ok is False
        assert kb.get_task(conn, child).status == "running"


def test_mixed_parents_one_pending_still_parks_in_todo(kanban_home: Path) -> None:
    """Two parents, one done + one pending → legal wait, todo parking."""
    with kb.connect_closing() as conn:
        done_parent = kb.create_task(conn, title="dp", assignee="worker")
        _finish(conn, done_parent)
        pending_parent = kb.create_task(conn, title="pp", assignee="worker")
        child = _running_task(conn, title="child")
        kb.link_tasks(conn, parent_id=done_parent, child_id=child)
        kb.link_tasks(conn, parent_id=pending_parent, child_id=child)

        ok = kb.block_task(conn, child, reason="wait", kind="dependency")
        assert ok is True
        assert kb.get_task(conn, child).status == "todo"
        assert _events(conn, child, kind="dependency_wait")


# ---------------------------------------------------------------------------
# Kernel: no respawn loop end-to-end (the incident shape)
# ---------------------------------------------------------------------------


def test_refused_card_not_promoted_by_recompute_ready(kanban_home: Path) -> None:
    """After a refusal, recompute_ready must not manufacture a promotion.

    On the live board the loop was: block(dependency) → todo →
    recompute_ready promotes (parents done) → dispatcher spawns worker →
    repeat. With the refusal the card stays running under its claim, so
    there is no todo row for recompute_ready to act on at all. To make the
    invariant explicit even for a stale-state card (e.g. a pre-fix todo
    row), driving recompute_ready over it still must not loop: a refused
    card that somehow sits in todo is promoted ONCE (legacy behaviour for
    todo cards is out of scope to change) — the fix guarantees no NEW
    vacuous todo parking is ever created.
    """
    with kb.connect_closing() as conn:
        parent = kb.create_task(conn, title="parent", assignee="worker")
        _finish(conn, parent)
        child = _running_task(conn, title="child")
        kb.link_tasks(conn, parent_id=parent, child_id=child)

        ok = kb.block_task(conn, child, reason="wait", kind="dependency")
        assert ok is False
        assert kb.get_task(conn, child).status == "running"
        # The guard means there is no todo parking to promote — the card is
        # still running under its live claim, so the dispatcher cannot
        # respawn it. recompute_ready is a no-op here.
        promoted = kb.recompute_ready(conn)
        assert promoted == 0
        assert kb.get_task(conn, child).status == "running"


def test_parked_child_promotes_when_parent_completes(kanban_home: Path) -> None:
    """Regression pin: the legitimate dependency flow still works."""
    with kb.connect_closing() as conn:
        parent = kb.create_task(conn, title="parent", assignee="worker")
        child = _running_task(conn, title="child")
        kb.link_tasks(conn, parent_id=parent, child_id=child)
        kb.block_task(conn, child, reason="wait", kind="dependency")
        assert kb.get_task(conn, child).status == "todo"

        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (parent,))
        kb.claim_task(conn, parent, claimer="worker")
        kb.complete_task(conn, parent, result="done")
        kb.recompute_ready(conn)
        assert kb.get_task(conn, child).status == "ready"


def test_sticky_kinds_unaffected_by_guard(kanban_home: Path) -> None:
    """needs_input on a parentless card still blocks (sticky bucket)."""
    with kb.connect_closing() as conn:
        tid = _running_task(conn)
        ok = kb.block_task(conn, tid, reason="human needed", kind="needs_input")
        assert ok is True
        assert kb.get_task(conn, tid).status == "blocked"
        # And recompute_ready must NOT promote it (sticky, #28712).
        kb.recompute_ready(conn)
        assert kb.get_task(conn, tid).status == "blocked"


# ---------------------------------------------------------------------------
# Tool surface: the worker sees the kernel reason verbatim
# ---------------------------------------------------------------------------


def test_handle_block_surfaces_refusal_reason(
    kanban_home: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import kanban_tools as kt

    with kb.connect() as conn:
        tid = kb.create_task(conn, title="impl", assignee="worker")
        task = kb.claim_task(conn, tid, claimer="worker:1")
        assert task is not None
    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))

    out = json.loads(
        kt._handle_block({"reason": "wait for restart", "kind": "dependency"})
    )
    # tool_error payload: {"error": "..."} — the actionable kernel reason
    # rides verbatim so the worker corrects the call instead of retrying.
    assert "error" in out, out
    assert "dependency block refused" in out["error"]
    assert "needs_input" in out["error"]
    # Card untouched: still running under claim.
    with kb.connect_closing() as conn:
        assert kb.get_task(conn, tid).status == "running"


def test_handle_block_legitimate_dependency_still_parks(
    kanban_home: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import kanban_tools as kt

    with kb.connect() as conn:
        parent = kb.create_task(conn, title="parent", assignee="worker")
        child = kb.create_task(conn, title="child", assignee="worker")
        # Claim BEFORE linking: claim_task gates on parent deps, so a child
        # linked to a pending parent is not claimable (dependency gating).
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (child,))
        task = kb.claim_task(conn, child, claimer="worker:1")
        assert task is not None
        kb.link_tasks(conn, parent_id=parent, child_id=child)
    monkeypatch.setenv("HERMES_KANBAN_TASK", child)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))

    out = json.loads(
        kt._handle_block({"reason": "wait for parent", "kind": "dependency"})
    )
    assert not (out.get("error") or out.get("isError")), out
    assert out.get("status") == "todo"
    with kb.connect_closing() as conn:
        assert kb.get_task(conn, child).status == "todo"


# ---------------------------------------------------------------------------
# CLI surface: operator sees the reason, non-zero exit
# ---------------------------------------------------------------------------


def test_cmd_block_prints_refusal_reason(
    kanban_home: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operator path: /kanban block … --kind dependency surfaces the reason."""
    from hermes_cli import kanban as kc

    with kb.connect_closing() as conn:
        tid = _running_task(conn)

    raw = kc.run_slash(f"block {tid} --kind dependency wait for restart")
    assert "cannot block" in raw
    assert "needs_input" in raw
    # Card untouched.
    with kb.connect_closing() as conn:
        assert kb.get_task(conn, tid).status == "running"


# ---------------------------------------------------------------------------
# Back-compat: legacy bool shape untouched
# ---------------------------------------------------------------------------


def test_with_reason_false_returns_plain_bool(kanban_home: Path) -> None:
    """Default call shape still returns a bare bool for old callers."""
    with kb.connect_closing() as conn:
        tid = _running_task(conn)
        ok = kb.block_task(conn, tid, reason="r", kind="needs_input")
        assert ok is True
        unknown = kb.block_task(conn, "t_nope", reason="r", kind="dependency")
        assert unknown is False
