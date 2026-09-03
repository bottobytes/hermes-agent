"""t_ebde1f15 addendum: the -q rc=0 quota trap.

The plain ``-q`` single-query CLI path (used by every non-goal kanban
worker) never maps API failures to exit codes — only ``-Q`` does. A worker
that dies to a fatal 429/quota wall therefore exits rc=0, the spawner's
``waitpid`` classifies ``clean_exit``, and ``detect_crashed_workers``
counts a PROTOCOL VIOLATION (auto-block lane) for what is really a quota
wall. The fix reroutes ``clean_exit`` + fatal-quota tail to the
rate_limited lane (no failure counted, cooldown defer).
"""
from __future__ import annotations

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


def test_clean_exit_with_quota_tail_reroutes_to_rate_limited(
    kanban_home, monkeypatch
):
    conn = kb.connect()
    try:
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        # The spawner DID observe the exit: rc=0 → clean_exit classification.
        monkeypatch.setattr(kb, "_classify_worker_exit", lambda pid: ("clean_exit", 0))
        tid = kb.create_task(conn, title="rc0 quota trap", assignee="worker")
        _crash_a_task(
            conn, tid,
            log_tail=(
                "❌ API call failed after 3 retries — HTTP 429: The service may be temporarily overloaded\r\n"
                "   🔌 Provider: zai  Model: glm-5.3\r\n"
                "\r\nSession:        20260828_040226_65d98e\r\n"
                "Duration:       57m 55s\r\n"
            ),
            run_age=3420,
        )

        kb.detect_crashed_workers(conn)

        run = conn.execute(
            "SELECT outcome, error FROM task_runs WHERE task_id=? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert run["outcome"] == "rate_limited", (
            "rc=0 + fatal quota banner must be a quota wall, not a protocol violation"
        )
        t = conn.execute(
            "SELECT consecutive_failures FROM tasks WHERE id=?", (tid,),
        ).fetchone()
        assert t["consecutive_failures"] == 0
    finally:
        conn.close()


def test_clean_exit_without_quota_tail_stays_protocol_violation(
    kanban_home, monkeypatch
):
    conn = kb.connect()
    try:
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        monkeypatch.setattr(kb, "_classify_worker_exit", lambda pid: ("clean_exit", 0))
        tid = kb.create_task(conn, title="rc0 plain", assignee="worker")
        _crash_a_task(
            conn, tid,
            log_tail=(
                "The fix is complete and verified.\r\n"
                "\r\nSession:        20260828_050758_39b69e\r\n"
                "Duration:       22m 6s\r\n"
            ),
            run_age=3420,
        )

        kb.detect_crashed_workers(conn)

        run = conn.execute(
            "SELECT outcome, error FROM task_runs WHERE task_id=? "
            "ORDER BY id DESC LIMIT 1", (tid,),
        ).fetchone()
        assert run["outcome"] == "crashed"
        assert "protocol violation" in run["error"]
    finally:
        conn.close()
