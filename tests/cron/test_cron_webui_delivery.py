"""webui cron delivery lane (kernel t_993b18df).

The Hermes WebUI binds HERMES_SESSION_PLATFORM=webui /
HERMES_SESSION_CHAT_ID=<webui session id> for browser sessions, so a
deliver=origin job created there carries origin {"platform": "webui"}.
Before this bundle every such delivery died at Platform("webui") with
"unknown platform 'webui'" and the job's report was lost (live incidents
2026-09-09: jobs 4227b6af5f2e / 85b2e33b3b67 — last_status ok, report
never reached the session).

The lane: webui targets resolve like bot-chat (out-of-band), preflight
skips the gateway-credential check, and _deliver_to_webui POSTs the report
to the WebUI's own /api/chat/start (login via HERMES_WEBUI_PASSWORD when
set), which starts a server-side [CRON DELIVERY] agent turn in the target
session.
"""

import json
import urllib.error

import pytest

import cron.scheduler as sched


# --------------------------------------------------------------------------
# Target resolution
# --------------------------------------------------------------------------

class TestWebuiTargetResolution:
    def test_origin_webui_resolves(self):
        """The exact production shape: deliver=origin + webui origin."""
        job = {
            "id": "j1",
            "deliver": "origin",
            "origin": {"platform": "webui", "chat_id": "90fa91617882"},
        }
        targets = sched._resolve_delivery_targets(job)
        assert targets == [{
            "platform": "webui",
            "chat_id": "90fa91617882",
            "thread_id": None,
            "_resolved_from": "origin",
        }]

    def test_explicit_webui_hex_session_id(self):
        """webui:<sid> with letters (non-numeric sids) must not bounce off
        the channel directory — the pre-fix behavior dropped them."""
        job = {"id": "j1", "deliver": "webui:1fdd3e955707"}
        targets = sched._resolve_delivery_targets(job)
        assert len(targets) == 1
        assert targets[0]["platform"] == "webui"
        assert targets[0]["chat_id"] == "1fdd3e955707"

    def test_explicit_webui_numeric_session_id(self):
        job = {"id": "j1", "deliver": "webui:90fa91617882"}
        targets = sched._resolve_delivery_targets(job)
        assert len(targets) == 1
        assert targets[0]["chat_id"] == "90fa91617882"

    def test_explicit_webui_rejects_non_session_id(self):
        job = {"id": "j1", "deliver": "webui:not-a-session-id"}
        assert sched._resolve_delivery_targets(job) == []

    def test_bare_webui_without_origin_drops_honestly(self):
        """Bare 'webui' has no home-channel concept: no target (the honest
        fire-time error names the deliver value)."""
        assert sched._resolve_delivery_targets({"id": "j1", "deliver": "webui"}) == []


# --------------------------------------------------------------------------
# Preflight
# --------------------------------------------------------------------------

class TestWebuiPreflight:
    def test_preflight_allows_origin_webui(self):
        job = {
            "id": "j1",
            "deliver": "origin",
            "origin": {"platform": "webui", "chat_id": "90fa91617882"},
        }
        assert sched._preflight_check_delivery(job) is None

    def test_preflight_allows_explicit_webui(self):
        """webui needs no gateway credentials — must not be blocked by the
        connected-platforms check (bot-chat-style carve-out)."""
        job = {"id": "j1", "deliver": "webui:90fa91617882"}
        assert sched._preflight_check_delivery(job) is None

    def test_webui_is_known_delivery_platform(self):
        assert sched._is_known_delivery_platform("webui")


# --------------------------------------------------------------------------
# HTTP delivery (urllib monkeypatched)
# --------------------------------------------------------------------------

class _Resp:
    def __init__(self, status=200, headers=None):
        self.status = status
        self._headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        return b'{"ok": true}'

    def headers_get(self, k, default=""):
        return self._headers.get(k, default)


class _HeadersDict(dict):
    def get(self, k, default=None):
        return super().get(k, default)


class _URLOpenSpy:
    """Records urlopen calls; replays scripted responses/exceptions."""

    def __init__(self, script):
        self.script = list(script)  # list of (callable(req, timeout) -> resp) or exceptions
        self.calls = []

    def __call__(self, req, timeout=None):
        self.calls.append(req)
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step(req, timeout)


def _login_resp(req, timeout=None):
    r = _Resp(200)
    r.headers = _HeadersDict({
        "Set-Cookie": "hermes_session=tok123.sigabc; Path=/; HttpOnly",
    })
    return r


def _chat_ok(req, timeout=None):
    return _Resp(200)


@pytest.fixture
def webui_env(monkeypatch):
    monkeypatch.setenv("HERMES_WEBUI_PASSWORD", "sekrit")
    monkeypatch.setenv("HERMES_WEBUI_PORT", "8787")
    monkeypatch.delenv("HERMES_CRON_WEBUI_DELIVERY_URL", raising=False)
    monkeypatch.setenv("HERMES_CRON_WEBUI_BUSY_RETRIES", "2")
    monkeypatch.setenv("HERMES_CRON_WEBUI_BUSY_RETRY_DELAY", "0")
    monkeypatch.setenv("HERMES_CRON_WEBUI_DELIVERY_TIMEOUT", "5")


class TestDeliverToWebui:
    def _job(self):
        return {"id": "j1", "name": "test job"}

    def test_success_sends_login_then_chat_start(self, webui_env, monkeypatch):
        spy = _URLOpenSpy([_login_resp, _chat_ok])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "90fa91617882", "report body")
        assert err is None
        assert len(spy.calls) == 2
        login_req, chat_req = spy.calls
        assert login_req.full_url.endswith("/api/auth/login")
        assert json.loads(login_req.data) == {"password": "sekrit"}
        assert chat_req.full_url.endswith("/api/chat/start")
        # no Origin/Referer: stays a non-browser request (CSRF gate exempt)
        assert chat_req.get_header("Origin") is None
        payload = json.loads(chat_req.data)
        assert payload["session_id"] == "90fa91617882"
        assert payload["message"].startswith("[CRON DELIVERY]")
        assert "test job" in payload["message"]
        assert "report body" in payload["message"]
        assert "cronjob(action='list')" in payload["message"]

    def test_no_password_skips_login(self, webui_env, monkeypatch):
        monkeypatch.delenv("HERMES_WEBUI_PASSWORD", raising=False)
        spy = _URLOpenSpy([_chat_ok])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "sid", "body")
        assert err is None
        assert len(spy.calls) == 1  # straight to chat/start

    def test_busy_409_retries_then_succeeds(self, webui_env, monkeypatch):
        monkeypatch.setattr(sched.time, "sleep", lambda s: None)

        def busy(req, timeout=None):
            raise urllib.error.HTTPError(
                req.full_url, 409, "Conflict", {}, None  # type: ignore[arg-type]
            )

        spy = _URLOpenSpy([_login_resp, busy, busy, _chat_ok])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "sid", "body")
        assert err is None
        assert len(spy.calls) == 4  # login + 2 busy + ok

    def test_busy_409_exhausts_honestly(self, webui_env, monkeypatch):
        monkeypatch.setattr(sched.time, "sleep", lambda s: None)

        def busy(req, timeout=None):
            raise urllib.error.HTTPError(
                req.full_url, 409, "Conflict", {}, None  # type: ignore[arg-type]
            )

        spy = _URLOpenSpy([_login_resp, busy, busy, busy])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "sid", "body")
        assert err is not None and "HTTP 409" in err

    def test_busy_409_exhaust_makes_no_scheduling_claim(self, webui_env, monkeypatch, caplog):
        """t_70dd9cc7: _deliver_to_webui cannot know whether the caller will
        spool (one-shot) or not (recurring) — so neither its WARNING nor the
        sentinel string may claim a redelivery was scheduled. The claim lives
        exclusively on the spool path (_spool_pending_webui_delivery)."""
        monkeypatch.setattr(sched.time, "sleep", lambda s: None)

        def busy(req, timeout=None):
            raise urllib.error.HTTPError(
                req.full_url, 409, "Conflict", {}, None  # type: ignore[arg-type]
            )

        spy = _URLOpenSpy([_login_resp, busy, busy, busy])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        with caplog.at_level("INFO", logger="cron.scheduler"):
            err = sched._deliver_to_webui(self._job(), "sid", "body")
        assert isinstance(err, sched._WebuiBusyError)
        # Honest busy error, no scheduling promise…
        assert "HTTP 409" in err and "session busy" in err
        assert "persistent redelivery" not in err
        assert "scheduled" not in err
        # …and the log says the session stayed busy WITHOUT claiming a
        # redelivery was queued.
        msgs = [r.getMessage() for r in caplog.records]
        assert any("stayed busy through" in m for m in msgs)
        assert not any("scheduling persistent redelivery" in m for m in msgs)

    def test_404_surfaces_immediately(self, webui_env, monkeypatch):

        def gone(req, timeout=None):
            raise urllib.error.HTTPError(
                req.full_url, 404, "Not Found", {}, None  # type: ignore[arg-type]
            )

        spy = _URLOpenSpy([_login_resp, gone])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "deadbeef", "body")
        assert err is not None and "404" in err
        assert len(spy.calls) == 2  # no retries on 404

    def test_login_failure_surfaces(self, webui_env, monkeypatch):

        def unauthorized(req, timeout=None):
            raise urllib.error.HTTPError(
                req.full_url, 401, "Unauthorized", {}, None  # type: ignore[arg-type]
            )

        spy = _URLOpenSpy([unauthorized])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "sid", "body")
        assert err is not None and "login failed" in err

    def test_connection_error_surfaces(self, webui_env, monkeypatch):
        spy = _URLOpenSpy([ConnectionError("refused")])
        monkeypatch.setattr("urllib.request.urlopen", spy)
        err = sched._deliver_to_webui(self._job(), "sid", "body")
        assert err is not None and "refused" in err


# --------------------------------------------------------------------------
# _deliver_result integration (production-shaped job)
# --------------------------------------------------------------------------

class TestDeliverResultWebui:
    def test_production_shaped_job_reaches_webui_lane(
        self, webui_env, monkeypatch, tmp_path
    ):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "4227b6af5f2e",
                "name": "zai key2 reset",
                "deliver": "origin",
                "origin": {
                    "platform": "webui",
                    "chat_id": "90fa91617882",
                    "thread_id": None,
                },
            }
            sent = {}

            def fake_deliver(j, chat_id, content):
                sent["chat_id"] = chat_id
                sent["content"] = content
                return None

            monkeypatch.setattr(sched, "_deliver_to_webui", fake_deliver)
            err = sched._deliver_result(job, "the report")
            assert err is None
            assert sent["chat_id"] == "90fa91617882"
            assert "the report" in sent["content"]

    def test_delivery_error_recorded(
        self, webui_env, monkeypatch, tmp_path
    ):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "j9",
                "name": "broken",
                "deliver": "origin",
                "origin": {"platform": "webui", "chat_id": "abc123def456"},
            }
            monkeypatch.setattr(
                sched, "_deliver_to_webui",
                lambda j, c, content: "webui delivery failed (HTTP 500)",
            )
            err = sched._deliver_result(job, "the report")
            assert err is not None and "HTTP 500" in err


# --------------------------------------------------------------------------
# Persistent redelivery for busy sessions (t_ee4b2f97)
# --------------------------------------------------------------------------

class TestWebuiBusySpool:
    """Fire-time spooling: a one-shot firing into a busy session must not
    lose its report — the content is persisted to the spool dir and the job
    record carries a pending_webui_delivery marker."""

    def _oneshot_job(self):
        return {
            "id": "deadbeef1234",
            "name": "one-shot report",
            "deliver": "origin",
            "origin": {"platform": "webui", "chat_id": "90fa91617882"},
            "schedule": {"kind": "once"},
            "repeat": {"times": 1, "completed": 1},
        }

    def test_busy_oneshot_spools_report(self, webui_env, monkeypatch, tmp_path):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            # Persist the job so the spooler's marker update finds a record.
            cron_jobs.save_jobs([self._oneshot_job()])

            def busy(j, chat_id, content, **kw):
                return sched._WebuiBusyError(
                    f"webui delivery to session {chat_id} failed (HTTP 409) — busy"
                )

            monkeypatch.setattr(sched, "_deliver_to_webui", busy)
            err = sched._deliver_result(self._oneshot_job(), "THE LOST REPORT")
            assert err is not None
            assert "spooled" in err and "redelivered" in err
            spools = sched._load_pending_webui_spools()
            assert len(spools) == 1
            _path, record = spools[0]
            assert record["job_id"] == "deadbeef1234"
            assert record["session_id"] == "90fa91617882"
            assert "THE LOST REPORT" in record["message"]
            assert record["attempts"] == 1

    def test_busy_recurring_job_does_not_spool(self, webui_env, monkeypatch, tmp_path):
        """Recurring jobs self-heal on the next fire — plain honest error,
        no spool (otherwise every busy recurring fire would double-deliver)."""
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "rec1",
                "name": "recurring",
                "deliver": "origin",
                "origin": {"platform": "webui", "chat_id": "90fa91617882"},
                "schedule": {"kind": "interval", "minutes": 5},
            }
            cron_jobs.save_jobs([job])

            def busy(j, chat_id, content, **kw):
                return sched._WebuiBusyError("webui delivery busy")

            monkeypatch.setattr(sched, "_deliver_to_webui", busy)
            err = sched._deliver_result(job, "recurring report")
            assert err is not None
            assert "spooled" not in err
            assert sched._load_pending_webui_spools() == []

    def test_busy_recurring_job_does_not_log_redelivery_scheduling(
        self, webui_env, monkeypatch, tmp_path, caplog
    ):
        """t_70dd9cc7: the recurring busy-exhaust path must not log any claim
        that a redelivery was scheduled — nothing is (the job self-heals on
        its next fire). One-shots DO get the claim, via the spool WARNING."""
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "rec2",
                "name": "recurring ctrl",
                "deliver": "origin",
                "origin": {"platform": "webui", "chat_id": "90fa91617882"},
                "schedule": {"kind": "interval", "minutes": 3},
            }
            cron_jobs.save_jobs([job])

            def busy(j, chat_id, content, **kw):
                return sched._WebuiBusyError("webui delivery busy")

            monkeypatch.setattr(sched, "_deliver_to_webui", busy)
            with caplog.at_level("INFO", logger="cron.scheduler"):
                err = sched._deliver_result(job, "recurring report")
            assert err is not None
            assert "spooled" not in err
            assert sched._load_pending_webui_spools() == []
            msgs = [r.getMessage() for r in caplog.records]
            # No scheduling claim anywhere in the recurring path…
            assert not any(
                "scheduling persistent redelivery" in m
                or "spooled for redelivery" in m
                for m in msgs
            )
            # …but the self-heal explanation IS present (t_70dd9cc7 INFO).
            assert any(
                "recurring job busy-exhausted" in m
                and "next fire will retry delivery" in m
                for m in msgs
            )

    def test_terminal_error_does_not_spool(self, webui_env, monkeypatch, tmp_path):
        """404/401/5xx are not busy — no spool, honest error unchanged."""
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            cron_jobs.save_jobs([self._oneshot_job()])
            monkeypatch.setattr(
                sched, "_deliver_to_webui",
                lambda j, c, content, **kw: "webui delivery failed (HTTP 500)",
            )
            err = sched._deliver_result(self._oneshot_job(), "the report")
            assert "HTTP 500" in err
            assert sched._load_pending_webui_spools() == []

    def test_busy_marker_is_str_and_busy(self):
        sentinel = sched._WebuiBusyError("busy (HTTP 409)")
        assert isinstance(sentinel, str)
        assert sentinel.busy is True
        assert "HTTP 409" in sentinel


class TestWebuiRedeliveryPass:
    """Tick-time redelivery over the spool: busy→idle delivers, persistent
    busy backs off, horizon gives up honestly, terminal errors park."""

    def _spool(self, tmp_path, cron_jobs, **overrides):
        from datetime import datetime, timezone

        record = {
            "job_id": "aaa111bbb222",
            "name": "spooled job",
            "session_id": "90fa91617882",
            "message": "DEFERRED REPORT",
            # Fresh by default: inside the redelivery horizon. Tests that
            # need staleness override first_try/last_try explicitly.
            "first_try": datetime.now(timezone.utc).isoformat(),
            "attempts": 1,
            "error": "busy",
        }
        record.update(overrides)
        base = sched._webui_pending_dir()
        base.mkdir(parents=True, exist_ok=True)
        path = base / "aaa111bbb222__90fa91617882__20260909T140000.json"
        path.write_text(json.dumps(record), encoding="utf-8")
        return path, record

    def test_idle_session_redelivers_and_cleans_up(
        self, webui_env, monkeypatch, tmp_path
    ):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "aaa111bbb222",
                "name": "spooled job",
                "schedule": {"kind": "once"},
                "repeat": {"times": 1, "completed": 1},
                "state": "completed",
                "enabled": False,
                "last_delivery_error": "old busy error",
            }
            cron_jobs.save_jobs([job])
            path, _ = self._spool(tmp_path, cron_jobs)

            delivered = {}
            monkeypatch.setattr(
                sched, "_deliver_to_webui",
                lambda j, sid, content, **kw: delivered.update(
                    sid=sid, content=content
                ) or None,
            )
            n = sched._deliver_pending_webui_reports()
            assert n == 1
            assert delivered["sid"] == "90fa91617882"
            assert "DEFERRED REPORT" in delivered["content"]
            assert not path.exists()
            jobs_after = cron_jobs.load_jobs()
            assert jobs_after[0].get("pending_webui_delivery") in (None,)
            assert jobs_after[0].get("last_delivery_error") is None

    def test_still_busy_backs_off_without_giving_up(
        self, webui_env, monkeypatch, tmp_path
    ):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            path, _ = self._spool(tmp_path, cron_jobs)

            def busy(j, sid, content, **kw):
                return sched._WebuiBusyError("still busy (HTTP 409)")

            monkeypatch.setattr(sched, "_deliver_to_webui", busy)
            n = sched._deliver_pending_webui_reports()
            assert n == 0
            # Spool still there, attempts bumped, last_try recorded.
            spools = sched._load_pending_webui_spools()
            assert len(spools) == 1
            _p, record = spools[0]
            assert record["attempts"] == 2
            assert record.get("last_try")

    def test_horizon_gives_up_preserving_content(
        self, webui_env, monkeypatch, tmp_path
    ):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "aaa111bbb222",
                "name": "spooled job",
                "schedule": {"kind": "once"},
                "state": "completed",
                "enabled": False,
            }
            cron_jobs.save_jobs([job])
            # first_try two hours ago — past the 1h default horizon.
            self._spool(
                tmp_path, cron_jobs,
                first_try="2026-09-09T12:00:00+00:00",
                last_try="2026-09-09T13:59:00+00:00",
            )

            def busy(j, sid, content, **kw):
                return sched._WebuiBusyError("busy")

            monkeypatch.setattr(sched, "_deliver_to_webui", busy)
            n = sched._deliver_pending_webui_reports()
            assert n == 0
            assert sched._load_pending_webui_spools() == []
            gaveup = list(sched._webui_pending_dir().glob("*.gaveup*"))
            assert len(gaveup) == 1
            jobs_after = cron_jobs.load_jobs()
            err = jobs_after[0].get("last_delivery_error") or ""
            assert "abandoned" in err and "gaveup" in jobs_after[0]["last_delivery_error"]

    def test_terminal_error_parks_with_content(
        self, webui_env, monkeypatch, tmp_path
    ):
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            job = {
                "id": "aaa111bbb222",
                "name": "spooled job",
                "schedule": {"kind": "once"},
                "state": "completed",
                "enabled": False,
            }
            cron_jobs.save_jobs([job])
            self._spool(tmp_path, cron_jobs)

            monkeypatch.setattr(
                sched, "_deliver_to_webui",
                lambda j, sid, content, **kw: "webui delivery failed (HTTP 404)",
            )
            n = sched._deliver_pending_webui_reports()
            assert n == 0
            assert sched._load_pending_webui_spools() == []
            assert list(sched._webui_pending_dir().glob("*.gaveup*"))
            jobs_after = cron_jobs.load_jobs()
            assert "HTTP 404" in (jobs_after[0].get("last_delivery_error") or "")

    def test_backoff_window_respected(self, webui_env, monkeypatch, tmp_path):
        """A spool poked seconds ago must not be re-poked immediately."""
        from cron import jobs as cron_jobs
        from datetime import datetime, timezone

        with cron_jobs.use_cron_store(tmp_path):
            now_iso = datetime.now(timezone.utc).isoformat()
            self._spool(
                tmp_path, cron_jobs,
                first_try=now_iso, last_try=now_iso, attempts=3,
            )
            calls = []
            monkeypatch.setattr(
                sched, "_deliver_to_webui",
                lambda j, sid, content, **kw: calls.append(sid) or None,
            )
            n = sched._deliver_pending_webui_reports()
            assert n == 0
            assert calls == []  # backoff (30s * 2^2 = 120s) not elapsed


class TestTickRunsRedelivery:
    def test_idle_tick_still_runs_redelivery_pass(self, webui_env, monkeypatch, tmp_path):
        """The redelivery hook must run on IDLE ticks too — that's when the
        operator has stopped typing and the session goes idle."""
        from cron import jobs as cron_jobs

        with cron_jobs.use_cron_store(tmp_path):
            calls = []
            monkeypatch.setattr(
                sched, "_deliver_pending_webui_reports",
                lambda: calls.append(1) or 0,
            )
            monkeypatch.setattr(
                sched, "get_due_jobs", lambda: [],
            )
            n = sched.tick(verbose=False, sync=True)
            assert n == 0
            assert calls == [1]
