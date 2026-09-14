"""Tests for coolify-deploy-watch CLI + coolify-deploy-notifier scan hook
(card t_9b07e08e — deploy-wake coverage gap for API/script-created deploys).

Two units under test, both loaded by file path (no package install needed):

  1. scripts/coolify-deploy-watch — the script-friendly deploy_subs
     registration hook. The daemon (already running, polls every ~5s) picks
     rows up with no restart; these tests verify the CLI's contract against
     a temp subs DB and a fake Coolify API:
       - trigger-response parsing (all payload shapes)
       - session resolution (explicit / newest-sidecar / missing refused)
       - done=1 dedup refusal and --refire override
       - --dry-run writes nothing
       - --adopt-recent filtering (window, non-terminal, unknown-to-DB,
         already-watched-app exclusion)
       - app name/uuid resolution and latest-deployment selection
  2. plugin __init__.py — _scan_for_unregistered: the daemon-side visibility
     scanner (loud warning for non-terminal deployments with no row).
     Scanner-only tests; the wake path itself is unchanged and was verified
     live (probe r13gyt9zgs6jw3zd48wqflyb, 2026-09-08 13:28).

Conventions follow tests/hermes_cli/test_kanban_*: importlib load, fakes
instead of network, tmp_path DBs, module-scope env scrubbing where needed.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

HERE = Path(__file__).resolve().parent
TERMINAL = ("finished", "failed", "cancelled", "cancelled-by-user")


def _first_existing(candidates: list[Path]) -> Path | None:
    for c in candidates:
        if c.exists():
            return c
    return None


def _first_plugin_file(candidates: list[Path]) -> Path | None:
    """First candidate that IS the plugin (content-marked) — a bare existing
    path is not enough: tests/plugins/__init__.py (package marker) shadows
    the plugin when this suite runs from the repo layout."""
    for c in candidates:
        try:
            if c.is_file() and "fire-and-be-woken" in c.read_text(encoding="utf-8", errors="replace")[:2000]:
                return c
        except Exception:
            continue
    return None


# Resolution order: bundle dir (workspace copy, ships with kernel-restore
# Bundle 7) -> repo layout (kernel-hardening branch: scripts/ + plugins/ +
# tests/plugins/) -> live volume install (~/.hermes). On a pristine CI box
# without any of these, the suite skips — these units are fleet plugins, not
# hermes-core.
CLI_PATH = _first_existing([
    HERE / "scripts" / "coolify-deploy-watch",
    Path(__file__).resolve().parents[2] / "scripts" / "coolify-deploy-watch",
    Path("/home/hermeswebui/.hermes/scripts/coolify-deploy-watch"),
])
PLUGIN_PATH = _first_plugin_file([
    HERE / "__init__.py",
    Path(__file__).resolve().parents[2] / "plugins" / "coolify-deploy-notifier" / "__init__.py",
    Path("/home/hermeswebui/.hermes/plugins/coolify-deploy-notifier/__init__.py"),
])

pytestmark = pytest.mark.skipif(
    CLI_PATH is None or PLUGIN_PATH is None,
    reason="coolify-deploy-watch CLI / coolify-deploy-notifier plugin not present "
           "(fleet plugin; see kernel-restore Bundle 7, card t_9b07e08e)",
)

# Worker-env poison guard (house lesson from t_f5d5809b): the kanban worker
# exports HERMES_KANBAN_* / HERMES_SESSION_ID / HERMES_HOME, which this CLI
# legitimately honors at runtime — but in tests they must NOT leak into
# session resolution or path defaults. Scrub at module import (conftest-style)
# before the module fixtures load.
for _var in ("HERMES_SESSION_ID", "HERMES_HOME",
             "COOLIFY_DEPLOY_WATCH_SESSION", "COOLIFY_DEPLOY_SUBS_DB",
             "COOLIFY_BASE_URL", "COOLIFY_ACCESS_TOKEN"):
    os.environ.pop(_var, None)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def _load(name: str, path: Path):
    from importlib.machinery import SourceFileLoader
    loader = SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def cli():
    return _load("coolify_deploy_watch_test", CLI_PATH)


@pytest.fixture(scope="module")
def plugin():
    return _load("coolify_deploy_notifier_test", PLUGIN_PATH)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def subs_db(tmp_path):
    db = tmp_path / "subs.db"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS deploy_subs (
            deployment_uuid TEXT PRIMARY KEY,
            app_uuid        TEXT NOT NULL,
            app_name        TEXT,
            session_id      TEXT NOT NULL,
            created_at      REAL NOT NULL,
            auto_diagnose   INTEGER NOT NULL DEFAULT 1,
            done            INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()
    conn.close()
    return str(db)


@pytest.fixture()
def sessions_dir(tmp_path):
    sdir = tmp_path / "webui" / "sessions"
    sdir.mkdir(parents=True)
    return sdir


def _mk_session(sessions_dir, sid, archived=False, age=0):
    p = sessions_dir / f"{sid}.json"
    p.write_text(json.dumps({"session_id": sid, "archived": archived}))
    if age:
        os.utime(p, (time.time() - age, time.time() - age))
    return p


def _row(conn, dep_uuid):
    return conn.execute(
        "SELECT deployment_uuid, app_uuid, app_name, session_id, created_at, "
        "auto_diagnose, done FROM deploy_subs WHERE deployment_uuid = ?",
        (dep_uuid,),
    ).fetchone()


# ---------------------------------------------------------------------------
# Trigger-response parsing
# ---------------------------------------------------------------------------
class TestPayloadParsing:
    @pytest.mark.parametrize("payload,expected", [
        ({"deployments": [{"deployment_uuid": "aaaaaaaaaaaaaaaaaaaaaaaaa"}]},
         ["aaaaaaaaaaaaaaaaaaaaaaaaa"]),
        ({"data": [{"deployment_uuid": "bbbbbbbbbbbbbbbbbbbbbbbbb"}]},
         ["bbbbbbbbbbbbbbbbbbbbbbbbb"]),
        ({"deployment_uuid": "ccccccccccccccccccccccccc"}, ["ccccccccccccccccccccccccc"]),
        ({"uuid": "ddddddddddddddddddddddddd"}, ["ddddddddddddddddddddddddd"]),
        ({"deployments": ["eeeeeeeeeeeeeeeeeeeeeeeee"]}, ["eeeeeeeeeeeeeeeeeeeeeeeee"]),
        ("fffffffffffffffffffffffff", ["fffffffffffffffffffffffff"]),
        ('{"deployments": [{"deployment_uuid": "ggggggggggggggggggggggggg"}]}',
         ["ggggggggggggggggggggggggg"]),
        ({}, []),
        ("not json at all", []),
        ({"message": "Not found."}, []),
        ({"deployments": [{"no_uuid": 1}]}, []),
    ])
    def test_shapes(self, cli, payload, expected):
        assert cli._deployment_uuids_from_payload(payload) == expected


# ---------------------------------------------------------------------------
# Session resolution
# ---------------------------------------------------------------------------
class TestSessionResolution:
    def test_explicit_ok(self, cli, sessions_dir):
        _mk_session(sessions_dir, "s1")
        with patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)):
            sid, err = cli._resolve_session(SimpleNamespace(session_id="s1", force=False))
        assert sid == "s1" and err == ""

    def test_explicit_missing_refused(self, cli, sessions_dir):
        with patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)):
            sid, err = cli._resolve_session(SimpleNamespace(session_id="nope", force=False))
        assert sid == "" and "sidecar not found" in err

    def test_force_bypasses_check(self, cli, sessions_dir):
        with patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)):
            sid, err = cli._resolve_session(SimpleNamespace(session_id="nope", force=True))
        assert sid == "nope" and err == ""

    def test_newest_wins_and_archived_skipped(self, cli, sessions_dir):
        _mk_session(sessions_dir, "old", age=600)
        _mk_session(sessions_dir, "arch", archived=True, age=60)
        _mk_session(sessions_dir, "new", age=5)
        with patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)):
            sid, err = cli._resolve_session(SimpleNamespace(session_id=None, force=False))
        assert sid == "new" and err == "auto: newest session"

    def test_no_sessions(self, cli, sessions_dir):
        with patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)):
            sid, err = cli._resolve_session(SimpleNamespace(session_id=None, force=False))
        assert sid == "" and "--session-id" in err


# ---------------------------------------------------------------------------
# DB write path (registration + dedup + dry-run)
# ---------------------------------------------------------------------------
class TestRegistration:
    def _args(self, **kw):
        base = dict(session_id="s1", no_diagnose=False, dry_run=False,
                    refire=False, force=True)
        base.update(kw)
        return SimpleNamespace(**base)

    def test_register_and_refuse_refire(self, cli, subs_db):
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu", "app-uuid-1", "wiki",
                      "s1", True)
        row = _row(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu")
        assert row[3] == "s1" and row[6] == 0 and row[5] == 1
        # done=1 row => CLI must refuse re-registration
        conn.execute("UPDATE deploy_subs SET done = 1 WHERE deployment_uuid = ?",
                     ("uuuuuuuuuuuuuuuuuuuuuuuuu",))
        conn.commit()
        prev = cli._existing_row(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu")
        assert prev is not None and prev[6] == 1
        conn.close()

    def test_cli_refuses_done_row(self, cli, subs_db, sessions_dir, capsys):
        _mk_session(sessions_dir, "s1")
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu", "app-uuid-1", "wiki",
                      "s1", True)
        conn.execute("UPDATE deploy_subs SET done = 1 WHERE deployment_uuid = ?",
                     ("uuuuuuuuuuuuuuuuuuuuuuuuu",))
        conn.commit()
        conn.close()
        with patch.object(cli, "_subs_db", return_value=subs_db), \
             patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)), \
             patch.object(cli, "_creds", return_value=("http://x", "tok")), \
             patch.object(cli, "_registration_plan", return_value=(
                 [{"deployment_uuid": "uuuuuuuuuuuuuuuuuuuuuuuuu", "app_uuid": "a",
                   "app_name": "wiki", "status": "finished"}], "test")):
            rc = cli.main(["--deployment-id", "uuuuuuuuuuuuuuuuuuuuuuuuu",
                           "--session-id", "s1", "--json"])
        out = json.loads(capsys.readouterr().out)
        assert rc == 0
        assert out["status"] == "refused"
        assert any("done=1" in r["reason"] for r in out["refused"])
        # row untouched
        conn = sqlite3.connect(subs_db)
        assert _row(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu")[6] == 1
        conn.close()

    def test_cli_refire_overrides(self, cli, subs_db, sessions_dir, capsys):
        _mk_session(sessions_dir, "s1")
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu", "a", "wiki", "s1", True)
        conn.execute("UPDATE deploy_subs SET done = 1 WHERE deployment_uuid = ?",
                     ("uuuuuuuuuuuuuuuuuuuuuuuuu",))
        conn.commit()
        conn.close()
        with patch.object(cli, "_subs_db", return_value=subs_db), \
             patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)), \
             patch.object(cli, "_creds", return_value=("http://x", "tok")), \
             patch.object(cli, "_registration_plan", return_value=(
                 [{"deployment_uuid": "uuuuuuuuuuuuuuuuuuuuuuuuu", "app_uuid": "a",
                   "app_name": "wiki", "status": "finished"}], "test")):
            rc = cli.main(["--deployment-id", "uuuuuuuuuuuuuuuuuuuuuuuuu",
                           "--session-id", "s1", "--refire", "--json"])
        out = json.loads(capsys.readouterr().out)
        assert rc == 0 and out["status"] == "ok"
        conn = sqlite3.connect(subs_db)
        assert _row(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu")[6] == 0   # re-armed
        conn.close()

    def test_dry_run_writes_nothing(self, cli, subs_db, sessions_dir, capsys):
        _mk_session(sessions_dir, "s1")
        with patch.object(cli, "_subs_db", return_value=subs_db), \
             patch.object(cli, "_sessions_dir", return_value=str(sessions_dir)), \
             patch.object(cli, "_creds", return_value=("http://x", "tok")), \
             patch.object(cli, "_registration_plan", return_value=(
                 [{"deployment_uuid": "uuuuuuuuuuuuuuuuuuuuuuuuu", "app_uuid": "a",
                   "app_name": "wiki", "status": "in_progress"}], "test")):
            rc = cli.main(["--deployment-id", "uuuuuuuuuuuuuuuuuuuuuuuuu",
                           "--session-id", "s1", "--dry-run", "--json"])
        out = json.loads(capsys.readouterr().out)
        assert rc == 0 and out["status"] == "ok" and out["dry_run"] is True
        conn = sqlite3.connect(subs_db)
        n = conn.execute("SELECT COUNT(*) FROM deploy_subs").fetchone()[0]
        conn.close()
        assert n == 0

    def test_auto_diagnose_flag_off(self, cli, subs_db):
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu", "a", "wiki", "s1", False)
        assert _row(conn, "uuuuuuuuuuuuuuuuuuuuuuuuu")[5] == 0
        conn.close()


# ---------------------------------------------------------------------------
# adopt-recent plan
# ---------------------------------------------------------------------------
def _dep(du, app_id="40", status="in_progress", created=None):
    created = created or time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
    return {"deployment_uuid": du, "application_id": app_id,
            "application_name": "workspace", "status": status,
            "created_at": created}


class TestAdoptRecent:
    def _args(self):
        return SimpleNamespace(
            from_stdin=False, deployment_id=None, adopt_recent=True, window=30,
            app=None, app_uuid=None,
        )

    def test_adopts_only_fresh_nonterminal_unknown(self, cli, subs_db):
        old_ts = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z",
                               time.gmtime(time.time() - 3600))
        recent = [
            _dep("aaaaaaaaaaaaaaaaaaaaaaaaa"),                      # adopt
            _dep("bbbbbbbbbbbbbbbbbbbbbbbbb", status="finished"),   # terminal
            _dep("ccccccccccccccccccccccccc", created=old_ts),      # too old
        ]
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "ddddddddddddddddddddddddd", "app-w", "wiki", "s", True)
        known = [d for d in recent]
        known.append(_dep("ddddddddddddddddddddddddd"))
        conn.close()
        with patch.object(cli, "_subs_db", return_value=subs_db), \
             patch.object(cli, "_recent_deployments", return_value=known), \
             patch.object(cli, "_list_apps", return_value=[
                 {"id": 40, "uuid": "ws-uuid", "name": "workspace"}]):
            plan, note = cli._registration_plan(self._args(), "http://x", "tok")
        assert [p["deployment_uuid"] for p in plan] == ["aaaaaaaaaaaaaaaaaaaaaaaaa"]
        assert plan[0]["app_uuid"] == "ws-uuid" and plan[0]["app_name"] == "workspace"

    def test_excludes_apps_with_active_watch(self, cli, subs_db):
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "eeeeeeeeeeeeeeeeeeeeeeee", "watched-uuid", "chat", "s", True)
        conn.close()
        with patch.object(cli, "_subs_db", return_value=subs_db), \
             patch.object(cli, "_recent_deployments", return_value=[
                 _dep("fffffffffffffffffffffffff", app_id="7")]), \
             patch.object(cli, "_list_apps", return_value=[
                 {"id": 7, "uuid": "watched-uuid", "name": "chat"}]):
            plan, _ = cli._registration_plan(self._args(), "http://x", "tok")
        assert plan == []

    def test_adopt_known_done_app_new_deploy_adopts(self, cli, subs_db):
        # App previously watched (done=1) and a NEW deployment appears — adopt
        # (the daemon's dedup is per-deployment; done rows don't watch anymore)
        conn = sqlite3.connect(subs_db)
        cli._register(conn, "1111111111111111111111111", "done-uuid", "chat", "s", True)
        conn.execute("UPDATE deploy_subs SET done = 1 WHERE deployment_uuid = ?",
                     ("1111111111111111111111111",))
        conn.commit()
        conn.close()
        with patch.object(cli, "_subs_db", return_value=subs_db), \
             patch.object(cli, "_recent_deployments", return_value=[
                 _dep("2222222222222222222222222", app_id="7")]), \
             patch.object(cli, "_list_apps", return_value=[
                 {"id": 7, "uuid": "done-uuid", "name": "chat"}]):
            plan, _ = cli._registration_plan(self._args(), "http://x", "tok")
        assert [p["deployment_uuid"] for p in plan] == ["2222222222222222222222222"]


# ---------------------------------------------------------------------------
# App resolution + latest deployment
# ---------------------------------------------------------------------------
class TestAppLatest:
    def test_latest_of_app(self, cli):
        args = SimpleNamespace(from_stdin=False, deployment_id=None,
                               adopt_recent=False, app="wiki", app_uuid=None)
        with patch.object(cli, "_resolve_app", return_value=("wiki-uuid", "wiki")), \
             patch.object(cli, "_app_deployments", return_value=[
                 {"deployment_uuid": "3333333333333333333333333", "status": "in_progress"},
                 {"deployment_uuid": "4444444444444444444444444", "status": "finished"},
             ]):
            plan, note = cli._registration_plan(args, "http://x", "tok")
        assert plan[0]["deployment_uuid"] == "3333333333333333333333333"
        assert "wiki" in note

    def test_app_not_found(self, cli):
        args = SimpleNamespace(from_stdin=False, deployment_id=None,
                               adopt_recent=False, app="ghost", app_uuid=None)
        with patch.object(cli, "_resolve_app", return_value=("", "")):
            with pytest.raises(SystemExit):
                cli._registration_plan(args, "http://x", "tok")


# ---------------------------------------------------------------------------
# ISO timestamp parsing
# ---------------------------------------------------------------------------
class TestTimestamps:
    def test_parse_iso(self, cli):
        from datetime import datetime, timezone

        expected = datetime(2026, 9, 8, 11, 47, 55, tzinfo=timezone.utc).timestamp()
        assert cli._parse_iso("2026-09-08T11:47:55.000000Z") == pytest.approx(
            expected, abs=1)
        assert cli._parse_iso("garbage") == 0.0
        assert cli._parse_iso("") == 0.0


# ---------------------------------------------------------------------------
# Plugin scanner
# ---------------------------------------------------------------------------
class TestPluginScanner:
    def _scan(self, plugin, subs_db, api_rows, monkeypatch):
        plugin._last_scan_at = 0.0
        conn = sqlite3.connect(subs_db)
        captured = []
        monkeypatch.setattr(plugin, "_coolify_get",
                            lambda path: captured.append(path) or api_rows)
        monkeypatch.setattr(plugin, "_coolify_creds", lambda: ("http://x", "tok"))
        records = []
        monkeypatch.setattr(plugin.logger, "warning", lambda *a, **k: records.append(a))
        try:
            plugin._scan_for_unregistered(conn, "/tmp/whatever")
        finally:
            conn.close()
        return captured, records

    def test_warns_on_unregistered_nonterminal(self, plugin, subs_db, monkeypatch):
        rows = [_dep("aaaaaaaaaaaaaaaaaaaaaaaaa"), _dep("bbbbbbbbbbbbbbbbbbbbbbbbb")]
        captured, records = self._scan(plugin, subs_db, rows, monkeypatch)
        texts = [" ".join(str(a) for a in r) for r in records]
        assert any("UNREGISTERED" in t for t in texts)
        assert any("2" in t.split()[0] or "UNREGISTERED" in t for t in texts)
        assert captured and "per_page=25" in captured[0]

    def test_silent_when_all_registered(self, plugin, subs_db, monkeypatch):
        conn = sqlite3.connect(subs_db)
        conn.execute(
            "INSERT INTO deploy_subs (deployment_uuid, app_uuid, app_name, "
            "session_id, created_at, auto_diagnose, done) VALUES "
            "('aaaaaaaaaaaaaaaaaaaaaaaaa','a','wiki','s',1,1,0)")
        conn.commit()
        _, records = self._scan(plugin, subs_db, [_dep("aaaaaaaaaaaaaaaaaaaaaaaaa")],
                                monkeypatch)
        assert not records

    def test_silent_when_terminal(self, plugin, subs_db, monkeypatch):
        _, records = self._scan(plugin, subs_db,
                                [_dep("aaaaaaaaaaaaaaaaaaaaaaaaa", status="finished")],
                                monkeypatch)
        assert not records

    def test_silent_when_old(self, plugin, subs_db, monkeypatch):
        old_ts = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z",
                               time.gmtime(time.time() - 3600))
        _, records = self._scan(plugin, subs_db,
                                [_dep("aaaaaaaaaaaaaaaaaaaaaaaaa", created=old_ts)],
                                monkeypatch)
        assert not records

    def test_throttled_by_scan_period(self, plugin, subs_db, monkeypatch):
        plugin._last_scan_at = 0.0
        calls = []

        def fake_get(path):
            calls.append(path)
            return [_dep("aaaaaaaaaaaaaaaaaaaaaaaaa")]

        monkeypatch.setattr(plugin, "_coolify_get", fake_get)
        monkeypatch.setattr(plugin, "_coolify_creds", lambda: ("http://x", "tok"))
        monkeypatch.setattr(plugin.logger, "warning", lambda *a, **k: None)
        conn = sqlite3.connect(subs_db)
        try:
            plugin._scan_for_unregistered(conn, "/tmp/x")   # first -> scans
            plugin._scan_for_unregistered(conn, "/tmp/x")   # second -> throttled
        finally:
            conn.close()
        assert len(calls) == 1

    def test_api_error_swallowed(self, plugin, subs_db, monkeypatch):
        plugin._last_scan_at = 0.0
        monkeypatch.setattr(plugin, "_coolify_get",
                            lambda path: (_ for _ in ()).throw(RuntimeError("boom")))
        monkeypatch.setattr(plugin, "_coolify_creds", lambda: ("http://x", "tok"))
        conn = sqlite3.connect(subs_db)
        try:
            plugin._scan_for_unregistered(conn, "/tmp/x")   # must not raise
        finally:
            conn.close()

    def test_iso_helper(self, plugin):
        assert plugin._parse_iso_ts("2026-09-08T11:47:55.000000Z") > 0
        assert plugin._parse_iso_ts("") == 0.0

    def test_non_terminal_tuple(self, plugin):
        assert plugin._NON_TERMINAL == ("queued", "in_progress")
        assert set(plugin._TERMINAL) == set(TERMINAL)
