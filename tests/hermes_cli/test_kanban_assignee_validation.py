"""Write-time phantom assignee/reviewer validation (t_fbd0fb38).

Contract under test — the Captain-approved design:

* ``kanban_request_review(reviewer=X)`` where X names NO real Hermes
  profile → the tool call returns an immediate ERROR (the worker reads it
  and retries), the card is left COMPLETELY untouched (still running,
  claim intact, no review park, no wake), and a durable per-card strike
  counter is bumped (``assignee_invalid_attempt`` task event).
* 3 strikes on the same card → stop rejecting; auto-fall back to the
  reviewer=None routing (standard self/FO review), post a plain-English
  board comment, and proceed. Validation can never wedge a card.
* Any SUCCESSFUL request_review writes the reset marker
  (``assignee_write_validated``) — one good write clears the strikes.
* ``reviewer=None`` / omitted: unchanged behaviour (legal self-review
  path, proven by t_44867a2e).
* ``kanban_create(assignee=X)`` with a phantom X → immediate ERROR, the
  task is never created (the dispatcher otherwise SILENTLY DROPS unknown
  assignee cards — they sit in ready forever).
* ``HERMES_KANBAN_ASSIGNEE_VALIDATION=off`` (or
  ``kanban.assignee_validation: false``) disables the gate cleanly.
* An unresolvable profiles module (``profile_exists`` → None) fails OPEN.
* The counter/fallback event kinds are NOT notifier terminal kinds —
  counting an invalid attempt must never itself wake anyone (the wake tax
  this task exists to end).

The heavy lifecycle semantics stay pinned by test_kanban_review_lifecycle.py
(DB layer, unvalidated by design — validation lives at the tool surface).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# t_fbd0fb38: this file may run inside a kanban worker session whose env
# pins the LIVE board (HERMES_KANBAN_DB/BOARD/TASK). Those env vars BEAT
# HERMES_HOME in kb.connect() resolution, so without this strip every
# fixture write would poison the live board. Module-level (not a fixture)
# so it also protects the per-file subprocess conftest pattern.
for _var in (
    "HERMES_KANBAN_DB",
    "HERMES_KANBAN_BOARD",
    "HERMES_KANBAN_TASK",
    "HERMES_KANBAN_RUN_ID",
    "HERMES_KANBAN_CLAIM_LOCK",
    "HERMES_KANBAN_WORKSPACE",
    "HERMES_KANBAN_WORKSPACES_ROOT",
):
    os.environ.pop(_var, None)

import pytest

from hermes_cli import kanban_db as kb


REAL_PROFILES = {"alice", "worker"}


@pytest.fixture
def kanban_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Isolated HERMES_HOME with an empty kanban DB and real profile dirs.

    Profile dirs are created ON DISK (not monkeypatched) so every surface
    agrees: ``profile_exists`` (the gate the dispatcher uses), the TTL
    cache, and ``list_profiles`` (the error hint) all read the same truth.
    "alice"/"worker" exist; "fo-reviewer"/"ghost"/anything else is a
    phantom — exactly like the live incidents.
    """
    home = tmp_path / ".hermes"
    home.mkdir()
    profiles_root = home / "profiles"
    for real in ("alice", "worker"):
        (profiles_root / real).mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.delenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", raising=False)
    # Some sibling suites (test_kanban_per_profile_cap) purge hermes_cli*
    # from sys.modules and re-import; the module-level ``kb`` binding this
    # file captured at collection can then be a STALE object while the tool
    # handlers resolve the FRESH module at call time. Always clear the
    # exists-cache on the CURRENT sys.modules entry so no cached verdict
    # from another test's home leaks into this one.
    import sys as _sys

    _kb_now = _sys.modules.get("hermes_cli.kanban_db")
    _kb_target = _kb_now if _kb_now is not None else kb
    _kb_target._profile_exists_cache.clear()
    _kb_target._INITIALIZED_PATHS.clear()
    _kb_target.init_db()
    yield home
    _kb_target._profile_exists_cache.clear()


@pytest.fixture
def review_worker(kanban_home: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """A claimed running task owned by this "worker" session."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="impl", assignee="worker")
        task = kb.claim_task(conn, tid, claimer="worker:1")
        assert task is not None
    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))
    return tid


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
# request_review: phantom reviewer rejected, card untouched
# ---------------------------------------------------------------------------


def test_phantom_reviewer_rejected_card_untouched(review_worker: str) -> None:
    from tools import kanban_tools as kt

    out = json.loads(
        kt._handle_request_review({"summary": "done", "reviewer": "fo-reviewer"})
    )
    assert "error" in out
    assert "not a real Hermes profile" in out["error"]
    # valid-profile hint surfaces in the error (Captain-readable)
    assert "alice" in out["error"]
    # attempt accounting visible to the agent
    assert "1 of 3" in out["error"]

    with kb.connect() as conn:
        task = kb.get_task(conn, review_worker)
        assert task is not None
        assert task.status == "running"          # NOT parked in review
        assert task.assignee == "worker"          # NOT reassigned
        assert task.claim_lock is not None        # claim intact
        assert _events(conn, review_worker, "review_requested") == []
        kinds = [k for k, _ in _events(conn, review_worker)]
        assert "assignee_invalid_attempt" in kinds  # strike recorded durably


def test_second_phantom_attempt_counts_up(review_worker: str) -> None:
    from tools import kanban_tools as kt

    for expected_attempt in (1, 2):
        out = json.loads(
            kt._handle_request_review(
                {"summary": "done", "reviewer": "ghost-reviewer"}
            )
        )
        assert "error" in out
        assert f"{expected_attempt} of 3" in out["error"]
    with kb.connect() as conn:
        task = kb.get_task(conn, review_worker)
        assert task is not None
        assert task.status == "running"
        assert len(_events(conn, review_worker, "assignee_invalid_attempt")) == 2


def test_valid_reviewer_passes_unchanged(review_worker: str) -> None:
    from tools import kanban_tools as kt

    out = json.loads(
        kt._handle_request_review({"summary": "done", "reviewer": "alice"})
    )
    assert out.get("ok") is True, out
    with kb.connect() as conn:
        task = kb.get_task(conn, review_worker)
        assert task is not None
        assert task.status == "review"
        assert task.assignee == "alice"
        requested = _events(conn, review_worker, "review_requested")
        assert len(requested) == 1
        assert requested[0][1]["reviewer"] == "alice"
        assert _events(conn, review_worker, "assignee_write_validated")


def test_leading_at_prefix_is_tolerated(review_worker: str) -> None:
    from tools import kanban_tools as kt

    out = json.loads(
        kt._handle_request_review({"summary": "done", "reviewer": "@alice"})
    )
    assert out.get("ok") is True, out
    with kb.connect() as conn:
        assert kb.get_task(conn, review_worker).assignee == "alice"


def test_omitted_reviewer_unchanged(review_worker: str) -> None:
    from tools import kanban_tools as kt

    out = json.loads(kt._handle_request_review({"summary": "done"}))
    assert out.get("ok") is True, out
    with kb.connect() as conn:
        task = kb.get_task(conn, review_worker)
        assert task.status == "review"
        assert task.assignee == "worker"  # implementer keeps the card
        kinds = [k for k, _ in _events(conn, review_worker)]
        assert "assignee_invalid_attempt" not in kinds


# ---------------------------------------------------------------------------
# 3-strike auto-fallback
# ---------------------------------------------------------------------------


def test_third_strike_auto_falls_back(review_worker: str) -> None:
    from tools import kanban_tools as kt

    # Strikes 1 and 2: rejected.
    for _ in range(2):
        out = json.loads(
            kt._handle_request_review({"summary": "done", "reviewer": "ghost"})
        )
        assert "error" in out

    # Strike 3: proceeds with reviewer=None routing + visible comment.
    out = json.loads(
        kt._handle_request_review({"summary": "done", "reviewer": "ghost"})
    )
    assert out.get("ok") is True, out
    assert "reviewer_fallback" in out
    assert "not a real profile" in out["reviewer_fallback"]

    with kb.connect() as conn:
        task = kb.get_task(conn, review_worker)
        assert task is not None
        assert task.status == "review"
        # reviewer=None → standard routing: assignee stays the implementer
        assert task.assignee == "worker"
        attempts = _events(conn, review_worker, "assignee_invalid_attempt")
        assert len(attempts) == 3
        assert attempts[-1][1] == {"field": "reviewer", "name": "ghost"}
        fallbacks = _events(conn, review_worker, "assignee_validation_fallback")
        assert len(fallbacks) == 1
        assert fallbacks[0][1]["attempts"] == 3
        comments = kb.list_comments(conn, review_worker)
        assert any(
            "not a real Hermes profile" in c.body and "ghost" in c.body
            for c in comments
        ), [c.body for c in comments]


def test_counter_resets_after_success(review_worker: str) -> None:
    from tools import kanban_tools as kt

    # Two strikes...
    for _ in range(2):
        out = json.loads(
            kt._handle_request_review({"summary": "v", "reviewer": "ghost"})
        )
        assert "error" in out
    # ...then the worker learns the right name and succeeds.
    ok = json.loads(
        kt._handle_request_review({"summary": "v", "reviewer": "alice"})
    )
    assert ok.get("ok") is True, ok
    # Review comes back with changes; the worker retries a phantom again.
    with kb.connect() as conn:
        task = kb.claim_review_task(conn, review_worker, claimer="alice:1")
        assert task is not None
        assert kb.request_changes(
            conn, review_worker, reason="redo",
            expected_run_id=task.current_run_id,
        ) == (True, "worker")
    # The re-run: strike counter must have RESET — this is attempt 1, not 3.
    out = json.loads(
        kt._handle_request_review({"summary": "v2", "reviewer": "ghost"})
    )
    assert "error" in out
    assert "1 of 3" in out["error"], "counter did not reset after success"


def test_fallback_never_rejects_forever_after(review_worker: str) -> None:
    """After a fallback the NEXT phantom attempt is strike 1 again (the
    successful fallback write is itself a validated write)."""
    from tools import kanban_tools as kt

    out = None
    for _ in range(3):
        out = json.loads(
            kt._handle_request_review({"summary": "v", "reviewer": "ghost"})
        )
    assert out is not None and out.get("ok") is True, out  # 3rd strike fell back

    with kb.connect() as conn:
        task = kb.claim_review_task(conn, review_worker, claimer="alice:1")
        assert task is not None
        assert kb.request_changes(
            conn, review_worker, reason="redo",
            expected_run_id=task.current_run_id,
        )[0]

    out = json.loads(
        kt._handle_request_review({"summary": "v2", "reviewer": "ghost"})
    )
    assert "error" in out
    assert "1 of 3" in out["error"]


# ---------------------------------------------------------------------------
# Escape hatch + fail-open
# ---------------------------------------------------------------------------


def test_validation_off_env_disables_gate(
    review_worker: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools import kanban_tools as kt

    monkeypatch.setenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", "off")
    out = json.loads(
        kt._handle_request_review({"summary": "done", "reviewer": "ghost"})
    )
    assert out.get("ok") is True, out  # legacy behaviour restored
    with kb.connect() as conn:
        task = kb.get_task(conn, review_worker)
        assert task is not None
        assert task.assignee == "ghost"
        kinds = [k for k, _ in _events(conn, review_worker)]
        assert "assignee_invalid_attempt" not in kinds


def test_validation_off_config_mirror(
    review_worker: str, monkeypatch: pytest.MonkeyPatch, kanban_home: Path
) -> None:
    import hermes_cli.config as cfgmod
    from tools import kanban_tools as kt

    monkeypatch.delenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", raising=False)
    monkeypatch.setattr(
        cfgmod,
        "load_config",
        lambda *a, **k: {"kanban": {"assignee_validation": False}},
    )
    assert kb.assignee_validation_enabled() is False
    out = json.loads(
        kt._handle_request_review({"summary": "done", "reviewer": "ghost"})
    )
    assert out.get("ok") is True, out


def test_unresolvable_profiles_module_fails_open(
    review_worker: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """profile_exists → None (unknown) must never reject a real profile."""
    import sys as _sys
    from tools import kanban_tools as kt

    _kb_now = _sys.modules["hermes_cli.kanban_db"]
    monkeypatch.setattr(_kb_now, "_dispatch_profile_exists", lambda name: None)
    _kb_now._profile_exists_cache.clear()
    try:
        out = json.loads(
            kt._handle_request_review({"summary": "done", "reviewer": "ghost"})
        )
        assert out.get("ok") is True, out
    finally:
        _kb_now._profile_exists_cache.clear()


# ---------------------------------------------------------------------------
# kanban_create: phantom assignee rejected statelessly
# ---------------------------------------------------------------------------


def test_create_phantom_assignee_rejected(kanban_home: Path) -> None:
    from tools import kanban_tools as kt

    out = json.loads(
        kt._handle_create({"title": "child", "assignee": "phantom-crew"})
    )
    assert "error" in out
    assert "not a real Hermes profile" in out["error"]
    assert "task was NOT created" in out["error"]
    with kb.connect() as conn:
        rows = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()
        assert rows["n"] == 0  # nothing entered the board


def test_create_valid_assignee_passes(kanban_home: Path) -> None:
    from tools import kanban_tools as kt

    out = json.loads(
        kt._handle_create({"title": "child", "assignee": "alice"})
    )
    assert out.get("ok") is True, out
    with kb.connect() as conn:
        task = kb.get_task(conn, out["task_id"])
        assert task is not None and task.assignee == "alice"


def test_create_validation_off_env(
    kanban_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools import kanban_tools as kt

    monkeypatch.setenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", "off")
    out = json.loads(
        kt._handle_create({"title": "child", "assignee": "phantom-crew"})
    )
    assert out.get("ok") is True, out


def test_create_at_prefix_stripped(kanban_home: Path) -> None:
    from tools import kanban_tools as kt

    out = json.loads(kt._handle_create({"title": "child", "assignee": "@alice"}))
    assert out.get("ok") is True, out


# ---------------------------------------------------------------------------
# Kernel helpers: config resolution + counter semantics
# ---------------------------------------------------------------------------


def test_assignee_validation_enabled_matrix(
    kanban_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hermes_cli.config as cfgmod

    # default ON
    monkeypatch.delenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", raising=False)
    monkeypatch.setattr(cfgmod, "load_config", lambda *a, **k: {})
    assert kb.assignee_validation_enabled() is True
    # env off-switch (all spellings)
    for spelling in ("0", "false", "no", "off", "OFF", "Off"):
        monkeypatch.setenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", spelling)
        assert kb.assignee_validation_enabled() is False, spelling
    # env wins over config
    monkeypatch.setenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", "off")
    monkeypatch.setattr(
        cfgmod, "load_config",
        lambda *a, **k: {"kanban": {"assignee_validation": True}},
    )
    assert kb.assignee_validation_enabled() is False
    # config mirror
    monkeypatch.delenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", raising=False)
    monkeypatch.setattr(
        cfgmod, "load_config",
        lambda *a, **k: {"kanban": {"assignee_validation": False}},
    )
    assert kb.assignee_validation_enabled() is False
    # garbage env value fails SAFE (gate stays on)
    monkeypatch.setenv("HERMES_KANBAN_ASSIGNEE_VALIDATION", "banana")
    assert kb.assignee_validation_enabled() is True


def test_counter_reset_semantics_direct(kanban_home: Path) -> None:
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="counter", assignee="worker")
        assert kb.count_invalid_assignee_attempts(conn, tid) == 0
        with kb.write_txn(conn):
            n = kb.record_invalid_assignee_attempt(
                conn, tid, field="reviewer", name="ghost"
            )
        assert n == 1
        assert kb.count_invalid_assignee_attempts(conn, tid) == 1
        with kb.write_txn(conn):
            kb.record_invalid_assignee_attempt(
                conn, tid, field="reviewer", name="ghost"
            )
        assert kb.count_invalid_assignee_attempts(conn, tid) == 2
        # reset marker clears the live count without rewriting history
        with kb.write_txn(conn):
            kb.record_assignee_write_validated(
                conn, tid, field="reviewer", name="alice"
            )
        assert kb.count_invalid_assignee_attempts(conn, tid) == 0
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM task_events WHERE kind = ?",
            (kb.EVENT_KIND_INVALID_ATTEMPT,),
        ).fetchone()
        assert total["n"] == 2  # audit rows preserved


def test_valid_assignee_hint_names_real_profiles(kanban_home: Path) -> None:
    hint = kb.valid_assignee_hint()
    assert "alice" in hint and "worker" in hint


# ---------------------------------------------------------------------------
# No-wake guarantee: the new event kinds are not notifier terminal kinds
# ---------------------------------------------------------------------------


def test_new_event_kinds_are_not_terminal_wake_kinds() -> None:
    """Counting an invalid attempt / recording a fallback must never wake
    anyone (t_fbd0fb38 exists to END the phantom-name wake tax). Source-
    scan the gateway watcher's terminal set so the guarantee travels with
    the code even when the watcher module is too heavy to import here."""
    src = Path(__file__).resolve().parents[2] / "gateway" / "kanban_watchers.py"
    text = src.read_text(encoding="utf-8")
    for kind in (
        "assignee_invalid_attempt",
        "assignee_write_validated",
        "assignee_validation_fallback",
    ):
        assert f'"{kind}"' not in text, (
            f"{kind} must never join TERMINAL_KINDS — it would re-create "
            "the wake tax this task removes"
        )
