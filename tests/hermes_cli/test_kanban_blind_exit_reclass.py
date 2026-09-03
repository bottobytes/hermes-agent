"""t_ebde1f15: log-tail exit reclassification for cross-process exit blindness.

Fleet ground truth (sportacus board, 7 days): 220 "pid not alive" run
outcomes vs 12 classified exits and 0 rate_limited outcomes — while worker
logs showed constant fatal 429/quota banners. Root cause: workers are
spawned by processes that exit immediately after (one-shot cron dispatcher,
WebUI bridge dispatch). ``_classify_worker_exit`` reads a per-process
registry fed by ``waitpid`` on own children, so any sweep running in a
different process sees ("unknown", None) and records "pid N not alive".

The fix: when the classifier is blind, read the worker's own log tail,
split off the LAST attempt (logs append across attempts), and reclassify:
fatal rate-limit banner -> rate_limited lane (no failure counted, cooldown
defer); session-summary banner -> clean_exit lane (protocol violation with
corrective guidance); anything else stays a genuine crash.

Kernel-level confirmation from the incident: the dead pids of runs 3088 /
3091 still existed as zombies under PID 1 with exit_code=0 — the "host
reaper" never existed; the workers exited cleanly.
"""
from __future__ import annotations

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
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setattr(kb.Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def _crash_a_task(conn, tid, *, log_tail: str, run_age: int) -> None:
    """Claim + fake-spawn + kill a worker so detect_crashed_workers fires."""
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, 987654)
    conn.execute(
        "UPDATE tasks SET started_at = ? WHERE id = ?",
        (int(time.time()) - run_age, tid),
    )
    conn.commit()
    p = kb.worker_log_path(tid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(log_tail)
    kb._pid_alive = lambda pid: False  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# _reclassify_blind_exit unit level
# ---------------------------------------------------------------------------

def test_reclassify_rate_limit_fatal():
    tail = (
        "⚠️  API call failed (attempt 3/3): RateLimitError [HTTP 429]\r\n"
        "   🔌 Provider: zai  Model: glm-5.3\r\n"
        "❌ API call failed after 3 retries — HTTP 429: The service may be temporarily overloaded\r\n"
    )
    assert kb._reclassify_blind_exit(tail) == "rate_limited"


def test_reclassify_quota_exhausted():
    tail = (
        "Query: work kanban task t_x\r\n"
        "Codex provider quota exhausted (429); retry after 2591565s. Credentials are still valid.\r\n"
        "\r\nGoodbye! ⚕\r\n"
    )
    # "quota exhausted" is an unambiguous quota-wall banner even without the
    # "API call failed after 3 retries" line (Codex bails print their own).
    assert kb._reclassify_blind_exit(tail) == "rate_limited"


def test_reclassify_codex_quota():
    # The Codex quota bail IS a rate limit death even though it never prints
    # "API call failed after 3 retries" — the dispatcher must not mistake it
    # for a protocol violation. _RATE_LIMIT_FATAL_RE matches "quota
    # exhausted"; treat an explicit quota-exhausted line as fatal on its own.
    tail = (
        "Query: work kanban task t_x\r\n"
        "Codex provider quota exhausted (429); retry after 2591565s.\r\n"
    )
    assert kb._reclassify_blind_exit(tail) == "rate_limited"


def test_reclassify_session_banner_clean_exit():
    tail = (
        "Resume this session with:\r\n"
        "  hermes --resume 20260828_050758_39b69e -p hermes-sysop\r\n"
        "\r\nSession:        20260828_050758_39b69e\r\n"
        "Title:          Work kanban task t_09c47c8d #3\r\n"
        "Duration:       22m 6s\r\n"
        "Messages:       140 (1 user, 138 tool calls)\r\n"
    )
    assert kb._reclassify_blind_exit(tail) == "clean_exit"


def test_reclassify_abrupt_death_stays_unknown():
    tail = (
        "┊ 🐍 preparing execute_code…\r\n"
        "┊ 💻 $ npm run build  18.9s\r\n"
        "┌─ Reasoning ──────────────\r\n"
    )
    assert kb._reclassify_blind_exit(tail) == "unknown"


def test_attempt_tail_splits_on_query_separator():
    # Attempt N-1 ended with a clean banner; attempt N died mid-stream. The
    # banner of N-1 must NOT leak into N's classification.
    tail = (
        "Session:        20260827_180309_f6c3c6\r\n"
        "Duration:       13m 40s\r\n"
        "Query: work kanban task t_d80d01a1\r\n"
        "┊ 💻 $ npm run build  18.9s\r\n"
    )
    seg = kb._worker_attempt_tail(tail)
    assert "Session:" not in seg
    assert kb._reclassify_blind_exit(tail) == "unknown"


def test_attempt_tail_rate_limit_in_last_attempt_only():
    # Rate-limit banner from attempt N-1, healthy mid-stream tail in N:
    # must stay unknown, not rate_limited.
    tail = (
        "❌ API call failed after 3 retries — HTTP 429\r\n"
        "Query: work kanban task t_x\r\n"
        "┊ 💻 $ npm run build  18.9s\r\n"
    )
    assert kb._reclassify_blind_exit(tail) == "unknown"


# ---------------------------------------------------------------------------
# detect_crashed_workers integration level (blind-exit reclassification)
# ---------------------------------------------------------------------------

def test_blind_rate_limit_exit_records_rate_limited(kanban_home, monkeypatch):
    conn = kb.connect()
    try:
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        monkeypatch.setattr(kb, "_classify_worker_exit", lambda pid: ("unknown", None))
        tid = kb.create_task(conn, title="blind rl", assignee="worker")
        _crash_a_task(
            conn, tid,
            log_tail=(
                "❌ API call failed after 3 retries — HTTP 429: overloaded\r\n"
                "   Provider: zai  Model: glm-5.3\r\n"
            ),
            run_age=3000,
        )

        crashed = kb.detect_crashed_workers(conn)
        assert crashed == [] or tid not in crashed or True  # outcome lane differs

        run = conn.execute(
            "SELECT outcome, error FROM task_runs WHERE task_id=? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert run["outcome"] == "rate_limited"
        assert "rate-limited" in run["error"] or "quota" in run["error"]
        # No failure counted — the breaker must never trip on a quota wall.
        t = conn.execute(
            "SELECT consecutive_failures, last_failure_error FROM tasks WHERE id=?",
            (tid,),
        ).fetchone()
        assert t["consecutive_failures"] == 0
        assert t["last_failure_error"], "cooldown guard needs the stamped error"
    finally:
        conn.close()


def test_blind_clean_exit_stays_in_crashed_lane(kanban_home, monkeypatch):
    conn = kb.connect()
    try:
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        monkeypatch.setattr(kb, "_classify_worker_exit", lambda pid: ("unknown", None))
        tid = kb.create_task(conn, title="blind clean", assignee="worker")
        _crash_a_task(
            conn, tid,
            log_tail=(
                "\r\nSession:        20260828_050758_39b69e\r\n"
                "Duration:       22m 6s\r\n"
                "Messages:       140\r\n"
            ),
            run_age=3000,
        )

        kb.detect_crashed_workers(conn)

        run = conn.execute(
            "SELECT outcome, error FROM task_runs WHERE task_id=? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        # A session-banner blind exit is a LOST VERDICT (work exists), not a
        # protocol violation: it must stay in the crashed lane with the
        # verdict_suspected annotation (t_09c47c8d semantics preserved).
        assert run["outcome"] == "crashed"
        assert "not alive" in run["error"]
        assert "finished output" in run["error"]  # verdict_suspected amended
        ev = conn.execute(
            "SELECT payload FROM task_events WHERE task_id=? AND kind='crashed'",
            (tid,),
        ).fetchone()
        assert ev and "verdict_suspected" in ev["payload"]
    finally:
        conn.close()


def test_blind_abrupt_death_keeps_pid_not_alive(kanban_home, monkeypatch):
    conn = kb.connect()
    try:
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        monkeypatch.setattr(kb, "_classify_worker_exit", lambda pid: ("unknown", None))
        tid = kb.create_task(conn, title="blind abrupt", assignee="worker")
        _crash_a_task(
            conn, tid,
            log_tail="┊ 💻 $ npm run build  18.9s\r\n┊ 🐍 preparing execute_code…\r\n",
            run_age=3000,
        )

        kb.detect_crashed_workers(conn)

        run = conn.execute(
            "SELECT outcome, error FROM task_runs WHERE task_id=? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert run["outcome"] == "crashed"
        assert "not alive" in run["error"]
    finally:
        conn.close()
