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
