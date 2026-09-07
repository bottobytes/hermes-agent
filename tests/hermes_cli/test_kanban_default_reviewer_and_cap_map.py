"""Regression tests for t_3c8f043d — default-reviewer routing +
per-profile concurrency-cap overrides.

Covers:
1. ``kanban.default_reviewer`` routes a reviewer-less ``review_requested``
   to the configured profile (kernel-level, root-config resolution via
   ``HERMES_KANBAN_ROOT_CONFIG``).
2. Explicit ``reviewer=`` still wins over the default.
3. Re-review provenance still wins over the default (durable reviewer
   from the latest changes_requested event is reused).
4. A configured default reviewer that is NOT a real profile is ignored
   (fail-open to the legacy implementer-profile behaviour).
5. ``kanban.max_in_progress_per_profile_map`` overrides the scalar cap
   per profile (ready lane + review lane).
6. Root-config resolution: the knob is read from the ROOT config even
   when HERMES_HOME points at a profile home (the worker-process shape).
"""
from __future__ import annotations

# --- t_3c8f043d test hygiene: the dispatcher-spawned worker env carries
# --- HERMES_KANBAN_* pointing at the LIVE sportacus board. Any test that
# --- creates tasks must run with those scrubbed or poison INSERTs hit the
# --- live board (fleet lesson from the e2e fixtures).
import os as _os
import sys as _sys

for _k in list(_os.environ):
    if _k.startswith("HERMES_KANBAN_") or _k in ("HERMES_REAL_HOME", "HERMES_PROFILE"):
        del _os.environ[_k]

import json
import os
import sqlite3
import sys
import tempfile

import pytest


@pytest.fixture()
def isolated_env(monkeypatch, tmp_path):
    """Fresh HERMES_HOME with alpha/beta/gamma profiles + a ROOT config."""
    test_home = tmp_path / "home"
    for prof in ("alpha", "beta", "gamma", "default"):
        (test_home / "profiles" / prof).mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(test_home))
    # No HERMES_KANBAN_ROOT_CONFIG: root cfg resolution falls through to
    # the current config path (the profile home) — tests that exercise the
    # root-config split set it explicitly.
    monkeypatch.delenv("HERMES_KANBAN_ROOT_CONFIG", raising=False)
    monkeypatch.delenv("HERMES_REAL_HOME", raising=False)
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod.startswith("hermes_state") or mod == "hermes_constants":
            del sys.modules[mod]
    from hermes_cli import kanban_db
    yield kanban_db, test_home, tmp_path


def _fake_spawn(*args, **kwargs):
    return 424242


def _create_running(conn, kb, tid):
    """Force a task row into running/claimed state (bypass claim machinery)."""
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET status='running', claim_lock='t:1:u', "
            "claim_expires=strftime('%s','now')+3600 WHERE id=?",
            (tid,),
        )


# ---------------------------------------------------------------------------
# Deliverable 1 — default reviewer routing
# ---------------------------------------------------------------------------


def test_default_reviewer_routes_when_no_explicit_reviewer(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n  default_reviewer: beta\n", encoding="utf-8"
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            tid = kb.create_task(conn, title="work", assignee="alpha")
            _create_running(conn, kb, tid)
            ok = kb.request_review(conn, tid, summary="ready for review", force=True)
            assert ok, "request_review should succeed"
            row = conn.execute(
                "SELECT assignee, status FROM tasks WHERE id=?", (tid,)
            ).fetchone()
            assert row["assignee"] == "beta", (
                f"reviewer-less request must route to default_reviewer, "
                f"got {row['assignee']!r}"
            )
            assert row["status"] == "review"
            ev = conn.execute(
                "SELECT payload FROM task_events WHERE task_id=? "
                "AND kind='review_requested' ORDER BY id DESC LIMIT 1",
                (tid,),
            ).fetchone()
            payload = json.loads(ev["payload"])
            assert payload.get("reviewer") == "beta"
            assert payload.get("implementer") == "alpha"
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_explicit_reviewer_wins_over_default(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n  default_reviewer: beta\n", encoding="utf-8"
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            tid = kb.create_task(conn, title="work", assignee="alpha")
            _create_running(conn, kb, tid)
            ok = kb.request_review(
                conn, tid, summary="s", reviewer="gamma", force=True
            )
            assert ok
            row = conn.execute(
                "SELECT assignee FROM tasks WHERE id=?", (tid,)
            ).fetchone()
            assert row["assignee"] == "gamma"
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_reeview_provenance_wins_over_default(isolated_env):
    """Re-review (no explicit reviewer) reuses the durable reviewer from the
    latest changes_requested event, NOT the configured default."""
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n  default_reviewer: beta\n", encoding="utf-8"
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            tid = kb.create_task(conn, title="work", assignee="alpha")
            _create_running(conn, kb, tid)
            ok = kb.request_review(conn, tid, summary="s", reviewer="gamma", force=True)
            assert ok
            # Simulate the reviewer requesting changes: latest run outcome
            # changes_requested + event payload carrying reviewer=gamma.
            with kb.write_txn(conn):
                conn.execute(
                    "UPDATE tasks SET status='ready' WHERE id=?", (tid,)
                )
            run = conn.execute(
                "SELECT id FROM task_runs WHERE task_id=? ORDER BY id DESC LIMIT 1",
                (tid,),
            ).fetchone()
            with kb.write_txn(conn):
                conn.execute(
                    "UPDATE task_runs SET outcome='changes_requested' WHERE id=?",
                    (run["id"],),
                )
                kb._append_event(
                    conn, tid, "changes_requested",
                    {"reviewer": "gamma", "reason": "fix"},
                    run_id=run["id"],
                )
            _create_running(conn, kb, tid)
            ok = kb.request_review(conn, tid, summary="fixed", force=True)
            assert ok
            row = conn.execute(
                "SELECT assignee FROM tasks WHERE id=?", (tid,)
            ).fetchone()
            assert row["assignee"] == "gamma", (
                "re-review must reuse provenance reviewer, not the default"
            )
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_phantom_default_reviewer_fails_open(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n  default_reviewer: no-such-profile\n", encoding="utf-8"
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            tid = kb.create_task(conn, title="work", assignee="alpha")
            _create_running(conn, kb, tid)
            ok = kb.request_review(conn, tid, summary="s", force=True)
            assert ok
            row = conn.execute(
                "SELECT assignee FROM tasks WHERE id=?", (tid,)
            ).fetchone()
            assert row["assignee"] == "alpha", (
                "unresolvable default_reviewer must fall back to the "
                "implementer-profile legacy behaviour"
            )
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_no_default_reviewer_keeps_legacy_behaviour(isolated_env):
    kb, home, tmp = isolated_env
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
        tid = kb.create_task(conn, title="work", assignee="alpha")
        _create_running(conn, kb, tid)
        ok = kb.request_review(conn, tid, summary="s", force=True)
        assert ok
        row = conn.execute(
            "SELECT assignee FROM tasks WHERE id=?", (tid,)
        ).fetchone()
        assert row["assignee"] == "alpha"


def test_root_config_resolution_from_profile_home(isolated_env):
    """The knob lives in the ROOT config; the worker process runs under a
    PROFILE home. Resolution must find the root config via
    HERMES_REAL_HOME even though load_config() would read the profile's
    own (knob-less) config."""
    kb, home, tmp = isolated_env
    real_home = tmp / "realhome"
    (real_home / ".hermes").mkdir(parents=True, exist_ok=True)
    (real_home / ".hermes" / "config.yaml").write_text(
        "kanban:\n  default_reviewer: beta\n", encoding="utf-8"
    )
    os.environ["HERMES_REAL_HOME"] = str(real_home)
    try:
        assert kb.default_reviewer_profile() == "beta"
    finally:
        os.environ.pop("HERMES_REAL_HOME", None)


# ---------------------------------------------------------------------------
# Deliverable 2 — per-profile cap map
# ---------------------------------------------------------------------------


def test_cap_map_overrides_scalar(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n"
        "  max_in_progress_per_profile_map:\n"
        "    alpha: 2\n"
        "    beta: 4\n",
        encoding="utf-8",
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            for i in range(5):
                kb.create_task(conn, title=f"a{i}", assignee="alpha")
            for i in range(5):
                kb.create_task(conn, title=f"b{i}", assignee="beta")
        with kb.connect_closing() as conn:
            res = kb.dispatch_once(
                conn, spawn_fn=_fake_spawn, dry_run=True,
                max_in_progress_per_profile=3,  # scalar; map overrides
            )
        spawn_counts = {}
        for _tid, who, _ws in res.spawned:
            spawn_counts[who] = spawn_counts.get(who, 0) + 1
        assert spawn_counts.get("alpha") == 2, (
            f"map cap 2 must override scalar 3 for alpha, got {spawn_counts}"
        )
        assert spawn_counts.get("beta") == 4, (
            f"map cap 4 must override scalar 3 for beta, got {spawn_counts}"
        )
        capped = [c[1] for c in res.skipped_per_profile_capped]
        assert capped.count("alpha") == 3
        assert capped.count("beta") == 1
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_cap_map_lifts_reviewer_lane(isolated_env):
    """The Sep-7 incident shape: implementer at scalar cap, reviewer-less
    review routed (by default_reviewer) to a profile whose map cap is
    HIGHER — the review spawn must go through."""
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n"
        "  default_reviewer: gamma\n"
        "  max_in_progress_per_profile_map:\n"
        "    gamma: 4\n",
        encoding="utf-8",
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            # implementer lanes saturate the scalar cap
            tids = [kb.create_task(conn, title=f"e{i}", assignee="alpha")
                    for i in range(3)]
            work = kb.create_task(conn, title="w", assignee="alpha")
            # put the three e-tasks into running
            for t in tids:
                _create_running(conn, kb, t)
            # request review with no explicit reviewer -> routes to gamma
            _create_running(conn, kb, work)
            ok = kb.request_review(conn, work, summary="s", force=True)
            assert ok
            row = conn.execute(
                "SELECT assignee, status FROM tasks WHERE id=?", (work,)
            ).fetchone()
            assert row["assignee"] == "gamma"
        # dispatch tick: scalar cap 1 would block alpha (3 running) AND any
        # second alpha spawn; gamma's map cap 4 must let the review spawn.
        with kb.connect_closing() as conn:
            res = kb.dispatch_once(
                conn, spawn_fn=_fake_spawn, dry_run=True,
                max_in_progress_per_profile=1,
            )
        spawned_ids = [s[0] for s in res.spawned]
        assert work in spawned_ids, (
            f"review for gamma (map cap 4) must spawn; spawned={res.spawned}, "
            f"capped={res.skipped_per_profile_capped}"
        )
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_cap_map_invalid_entries_dropped(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(
        "kanban:\n"
        "  max_in_progress_per_profile_map:\n"
        "    alpha: 2\n"
        "    bad-zero: 0\n"
        "    bad-neg: -3\n"
        "    bad-str: three\n"
        "    bad-bool: true\n"
        "    '': 5\n",
        encoding="utf-8",
    )
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        m = kb.per_profile_cap_map()
        assert m == {"alpha": 2}, f"invalid entries must be dropped, got {m}"
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_cap_map_scalar_only_when_map_empty(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text("kanban:\n  max_in_progress: 8\n", encoding="utf-8")
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        assert kb.per_profile_cap_map() == {}
        assert kb.default_reviewer_profile() is None
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            for i in range(4):
                kb.create_task(conn, title=f"a{i}", assignee="alpha")
        with kb.connect_closing() as conn:
            res = kb.dispatch_once(
                conn, spawn_fn=_fake_spawn, dry_run=True,
                max_in_progress_per_profile=3,
            )
        spawn_counts = [s[1] for s in res.spawned].count("alpha")
        assert spawn_counts == 3  # scalar still binds
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_broken_root_config_fails_open(isolated_env):
    kb, home, tmp = isolated_env
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text("kanban: [broken: {{", encoding="utf-8")
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)
    try:
        assert kb.per_profile_cap_map() == {}
        assert kb.default_reviewer_profile() is None
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            tid = kb.create_task(conn, title="work", assignee="alpha")
            _create_running(conn, kb, tid)
            ok = kb.request_review(conn, tid, summary="s", force=True)
            assert ok  # legacy behaviour preserved
            row = conn.execute(
                "SELECT assignee FROM tasks WHERE id=?", (tid,)
            ).fetchone()
            assert row["assignee"] == "alpha"
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)
