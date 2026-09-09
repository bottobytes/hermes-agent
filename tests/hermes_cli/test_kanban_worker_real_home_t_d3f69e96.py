"""Tests: worker-side root-config visibility (t_d3f69e96).

Two complementary kernel fixes, one contract: a dispatcher-spawned kanban
worker (``HERMES_HOME=<root>/profiles/<name>``, historically NO
``HERMES_REAL_HOME``) must be able to read the ROOT config's ``kanban:``
block — ``default_reviewer``, per-profile cap map — so worker-originated
``kanban_request_review`` routes to the operator-configured reviewer
instead of starving on the implementer's congested profile (live incident
Sep 9: t_f0e998f6 / t_456600d2 starved 30+ min while sportacus-reviewer
sat idle).

Fix surface under test:

1. ``_default_spawn`` injects ``HERMES_REAL_HOME`` into the worker env via
   ``hermes_constants.get_real_home`` (same resolver the terminal subprocess
   sanitizer uses). Additive: never overwrites an inherited value, never
   touches ``HOME``.
2. ``_root_kanban_cfg_candidates`` derives an additional root-config
   candidate from a profile-shaped ``HERMES_HOME`` via
   ``hermes_constants.get_default_hermes_root`` (<root>/profiles/<name> →
   <root>), so even a hand-launched ``hermes -p <profile>`` session (no
   env pin at all) resolves the root config.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


@pytest.fixture()
def worker_env(monkeypatch, tmp_path):
    """A dispatch-shaped sandbox: root home, profiles, and an OS home.

    Layout (mirrors the live gateway container):

        <tmp>/hermesroot/                 — the .hermes root (kanban knobs)
            config.yaml                   — ROOT config with the knob
            profiles/alpha/               — implementer profile home
            profiles/beta/                — reviewer profile home
        <tmp>/oshome/                     — the OS user home (HOME)
    """
    root = tmp_path / "hermesroot"
    for prof in ("alpha", "beta"):
        (root / "profiles" / prof).mkdir(parents=True, exist_ok=True)
    os_home = tmp_path / "oshome"
    os_home.mkdir()
    (os_home / ".hermes").mkdir()  # real-home candidate target must not exist here

    monkeypatch.setenv("HERMES_HOME", str(root / "profiles" / "alpha"))
    monkeypatch.setenv("HOME", str(os_home))
    monkeypatch.delenv("HERMES_REAL_HOME", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_ROOT_CONFIG", raising=False)
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod.startswith("hermes_state") or mod == "hermes_constants":
            del sys.modules[mod]
    from hermes_cli import kanban_db

    kanban_db._root_kanban_cfg_cache.clear()
    yield kanban_db, root, os_home, tmp_path
    kanban_db._root_kanban_cfg_cache.clear()


def _write_root_cfg(root, kanban_yaml: str) -> None:
    (root / "config.yaml").write_text(kanban_yaml, encoding="utf-8")


def _make_task(kb, *, assignee: str = "alpha"):
    return kb.Task(
        id="t_rh",
        title="real-home pin",
        body=None,
        assignee=assignee,
        status="running",
        priority=0,
        created_by="test",
        created_at=1,
        started_at=None,
        completed_at=None,
        workspace_kind="dir",
        workspace_path=None,
        claim_lock="lock",
        claim_expires=None,
        tenant=None,
        current_run_id=1,
    )


def _capture_spawn_env(kb, monkeypatch, workspace: str) -> dict:
    monkeypatch.setattr(kb, "_resolve_hermes_argv", lambda: ["hermes"])

    captured: dict = {}

    class FakeProc:
        pid = 4242

    def fake_popen(cmd, *args, **kwargs):
        captured["cmd"] = list(cmd)
        captured["env"] = dict(kwargs.get("env") or {})
        captured["cwd"] = kwargs.get("cwd")
        return FakeProc()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    kb._default_spawn(_make_task(kb), workspace)
    return captured


# ---------------------------------------------------------------------------
# Fix 1 — spawn-side env pin
# ---------------------------------------------------------------------------


def test_spawn_env_pins_real_home(worker_env, monkeypatch, tmp_path):
    """_default_spawn injects HERMES_REAL_HOME pointing at the OS home."""
    kb, root, os_home, _ = worker_env
    workspace = tmp_path / "ws"
    workspace.mkdir()

    captured = _capture_spawn_env(kb, monkeypatch, str(workspace))

    assert captured["env"].get("HERMES_REAL_HOME") == str(os_home), (
        "spawned worker env must carry HERMES_REAL_HOME so the root kanban "
        "config (default_reviewer, cap map) is visible worker-side"
    )
    # The pin is additive — HOME itself must not be rewritten.
    assert captured["env"].get("HOME") == str(os_home)
    # Profile home still pinned as the active home.
    assert captured["env"].get("HERMES_HOME") == str(root / "profiles" / "alpha")


def test_spawn_env_never_overrides_inherited_real_home(worker_env, monkeypatch, tmp_path):
    """An explicit operator/test HERMES_REAL_HOME survives the spawn."""
    kb, root, os_home, _ = worker_env
    explicit = tmp_path / "explicit-home"
    explicit.mkdir()
    monkeypatch.setenv("HERMES_REAL_HOME", str(explicit))

    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _capture_spawn_env(kb, monkeypatch, str(workspace))

    assert captured["env"].get("HERMES_REAL_HOME") == str(explicit)


# ---------------------------------------------------------------------------
# Fix 2 — candidate derivation from a profile-shaped HERMES_HOME
# ---------------------------------------------------------------------------


def test_default_reviewer_resolved_from_profile_home_without_env_pin(worker_env):
    """The card's VERIFY shape: `-p <profile>` run, no HERMES_REAL_HOME —
    the root config must still be found via the derived-root candidate."""
    kb, root, os_home, _ = worker_env
    _write_root_cfg(root, "kanban:\n  default_reviewer: beta\n")

    assert kb.default_reviewer_profile() == "beta"

    labels = [label for label, _ in kb._root_kanban_cfg_candidates()]
    assert "derived_root" in labels


def test_cap_map_resolved_from_profile_home(worker_env):
    """The per-profile cap map is read from the same derived root config."""
    kb, root, os_home, _ = worker_env
    _write_root_cfg(
        root,
        "kanban:\n"
        "  default_reviewer: beta\n"
        "  max_in_progress_per_profile_map:\n"
        "    alpha: 2\n"
        "    beta: 5\n",
    )

    assert kb.per_profile_cap_map() == {"alpha": 2, "beta": 5}


def test_profile_config_still_shadowed_knobless(worker_env):
    """Fail-open regression: a knob-less root config yields None (legacy
    behaviour), never an error and never a value from elsewhere."""
    kb, root, os_home, _ = worker_env
    (root / "config.yaml").write_text("logging:\n  level: INFO\n", encoding="utf-8")

    assert kb.default_reviewer_profile() is None
    assert kb.per_profile_cap_map() == {}


def test_vanilla_root_home_has_no_derived_candidate(worker_env, monkeypatch, tmp_path):
    """A non-profile HERMES_HOME (root == active home) adds no derived
    candidate — vanilla installs keep the exact legacy resolution."""
    kb, _, _, _ = worker_env
    vanilla = tmp_path / "vanilla-root"
    vanilla.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(vanilla))
    kb._root_kanban_cfg_cache.clear()

    labels = [label for label, _ in kb._root_kanban_cfg_candidates()]
    assert "derived_root" not in labels


# ---------------------------------------------------------------------------
# End-to-end — the incident chain: spawn env → candidates → request_review
# ---------------------------------------------------------------------------


def test_spawned_worker_env_routes_default_reviewer(worker_env, monkeypatch, tmp_path):
    """Apply the env _default_spawn would hand a worker, then run a
    reviewer-less request_review: the card must land on the configured
    default reviewer ('beta'), with correct event provenance."""
    kb, root, os_home, _ = worker_env
    _write_root_cfg(
        root,
        "kanban:\n  default_reviewer: beta\n  review_dispatch: true\n",
    )

    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _capture_spawn_env(kb, monkeypatch, str(workspace))
    worker_env_vars = captured["env"]
    assert worker_env_vars.get("HERMES_REAL_HOME") == str(os_home)

    # Simulate the worker process: adopt the spawned env, re-resolve.
    for key in ("HERMES_HOME", "HOME", "HERMES_REAL_HOME"):
        monkeypatch.setenv(key, worker_env_vars[key])
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod == "hermes_constants":
            del sys.modules[mod]
    from hermes_cli import kanban_db as worker_kb

    worker_kb._root_kanban_cfg_cache.clear()
    assert worker_kb.default_reviewer_profile() == "beta"

    with worker_kb.connect_closing() as conn:
        worker_kb.create_board(slug="default", name="T")
        tid = worker_kb.create_task(conn, title="work", assignee="alpha")
        with worker_kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status='running', claim_lock='t:1:u', "
                "claim_expires=strftime('%s','now')+3600 WHERE id=?",
                (tid,),
            )
        ok = worker_kb.request_review(conn, tid, summary="ready", force=True)
        assert ok, "request_review should succeed"
        row = conn.execute(
            "SELECT assignee, status FROM tasks WHERE id=?", (tid,)
        ).fetchone()
        assert row["assignee"] == "beta", (
            f"worker-originated review must auto-route to default_reviewer, "
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


def test_explicit_reviewer_still_wins_worker_side(worker_env, monkeypatch, tmp_path):
    """Regression guard from the card: explicit reviewer= beats the knob."""
    kb, root, os_home, _ = worker_env
    _write_root_cfg(root, "kanban:\n  default_reviewer: beta\n")

    # Worker-shaped env: profile home + real home, no explicit root-config pin.
    monkeypatch.setenv("HERMES_REAL_HOME", str(os_home))
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod == "hermes_constants":
            del sys.modules[mod]
    from hermes_cli import kanban_db as worker_kb

    worker_kb._root_kanban_cfg_cache.clear()
    with worker_kb.connect_closing() as conn:
        worker_kb.create_board(slug="default", name="T")
        tid = worker_kb.create_task(conn, title="work", assignee="alpha")
        with worker_kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status='running', claim_lock='t:1:u', "
                "claim_expires=strftime('%s','now')+3600 WHERE id=?",
                (tid,),
            )
        ok = worker_kb.request_review(
            conn, tid, summary="s", reviewer="gamma", force=True
        )
        # 'gamma' does not exist as a profile dir here — routing keeps the
        # explicit name verbatim (profile resolution is dispatch-side).
        assert ok
        row = conn.execute(
            "SELECT assignee FROM tasks WHERE id=?", (tid,)
        ).fetchone()
        assert row["assignee"] == "gamma"
