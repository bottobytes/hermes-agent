"""t_fc76cfa8 — idempotent fire-claim handoff: concurrent daemon-fire vs
gateway-ticker double-claim must never corrupt the execution ledger.

Reproduces the two production corruptions (executions.db, 2026-09-09/10):
  1. t_1074c7a2 22:17:46 — the WINNING firer's row flipped to
     ``failed: Interrupted by shutdown`` ~127ms after "completed successfully"
     because a single transient fire-claim probe failure declared ownership
     lost even though the immediate re-probe confirmed ownership.
  2. t_4a15ee2a phase 2 (row c93b66f2) + the two ``Fire claim lost; execution
     was not started.`` rows — the LOSING firer recorded ``failed`` on its own
     row; the fire itself succeeded under the winner.

Fix surface (kernel Bundle 15, t_fc76cfa8):
  * cron/executions.py — new terminal state ``relinquished`` +
    relinquish_execution(); in-place CHECK-constraint migration; terminal
    sets widened.
  * cron/scheduler.py — _process_job loser path relinquishes;
    _fire_claim_ownership_lost() re-probes once before declaring loss.
  * cron/scheduler_provider.py — claim_fire distinguishes lost-race
    (relinquish) from genuine rejection (failed).
  * agent/monitoring/cron_health.py — status projection knows the new state.

These tests exercise the real sqlite ledger + the real store CAS against a
temp HERMES_HOME (E2E-over-mocks discipline), mirroring
test_claim_job_for_fire.py / test_execution_ledger.py conventions.
"""
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME so jobs.json / executions.db never touch prod."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


@pytest.fixture
def ledger(monkeypatch, tmp_path):
    import cron.executions as executions

    monkeypatch.setattr(
        executions, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db"
    )
    return executions


# --------------------------------------------------------------------------
# 1. the new terminal state itself
# --------------------------------------------------------------------------

def test_relinquish_transitions_claimed_row(ledger):
    row = ledger.create_execution("job-race", source="builtin")
    out = ledger.relinquish_execution(row["id"], reason="lost race")
    assert out["status"] == "relinquished"
    assert out["finished_at"] is not None
    assert "lost race" in out["error"]


def test_relinquish_is_idempotent_and_never_overwrites_terminal(ledger):
    """Terminal states stay immutable: a late loser re-check cannot flip a
    row the winner already completed."""
    row = ledger.create_execution("job-race", source="builtin")
    assert ledger.finish_execution(row["id"], success=True) is not None
    # the loser's write must be a no-op on the completed row
    assert ledger.relinquish_execution(row["id"]) is None
    final = ledger.list_executions(job_id="job-race")[0]
    assert final["status"] == "completed"
    assert final["error"] is None


def test_relinquish_after_running_still_terminal(ledger):
    """A running row (claimed → running transition raced) also relinquishes
    cleanly rather than being stranded non-terminal."""
    row = ledger.create_execution("job-race", source="builtin")
    assert ledger.mark_execution_running(row["id"]) is not None
    out = ledger.relinquish_execution(row["id"])
    assert out["status"] == "relinquished"


def test_relinquished_counts_as_terminal_for_pruning(ledger, monkeypatch):
    monkeypatch.setattr(ledger, "MAX_TERMINAL_EXECUTIONS", 3)
    for i in range(5):
        row = ledger.create_execution(f"job-{i}", source="builtin")
        ledger.relinquish_execution(row["id"])
    # cap enforced — only the newest 3 terminal rows survive
    assert len(ledger.list_executions(limit=500)) == 3


def test_pre_migration_database_is_upgraded_in_place(ledger, tmp_path, monkeypatch):
    """A ledger DB created by the OLD 5-state constraint must accept the new
    status after init — the in-place rebuild widens the CHECK."""
    import sqlite3

    legacy = tmp_path / "legacy.db"
    conn = sqlite3.connect(legacy)
    conn.execute(
        """CREATE TABLE executions (
             id TEXT PRIMARY KEY, job_id TEXT NOT NULL, source TEXT NOT NULL,
             process_id TEXT NOT NULL, pid INTEGER NOT NULL,
             process_started_at INTEGER,
             status TEXT NOT NULL CHECK(status IN
               ('claimed','running','completed','failed','unknown')),
             claimed_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
             error TEXT)"""
    )
    conn.execute(
        "INSERT INTO executions VALUES ('legacy-1','j','builtin','p',1,NULL,"
        "'completed','2026-09-09T00:00:00+00:00',NULL,"
        "'2026-09-09T00:00:01+00:00',NULL)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ledger, "EXECUTIONS_FILE", legacy)
    # any transaction triggers _initialize_schema → the migration
    row = ledger.create_execution("fresh-job", source="builtin")
    out = ledger.relinquish_execution(row["id"])
    assert out["status"] == "relinquished"
    # legacy rows survived the rebuild verbatim
    survived = [r for r in ledger.list_executions(limit=500)
                if r["id"] == "legacy-1"]
    assert survived and survived[0]["status"] == "completed"


# --------------------------------------------------------------------------
# 2. the kernel loser path: ticker _process_job vs a concurrent fire
# --------------------------------------------------------------------------

def test_ticker_loser_relinquishes_instead_of_failing(temp_home, ledger, monkeypatch):
    """THE regression (row 466bf94a / 527d699b): when claim_job_for_fire
    returns False under _process_job, the execution row must be retired as
    relinquished — not recorded as failed."""
    import cron.scheduler as s

    run_spy = MagicMock(return_value=True)
    monkeypatch.setattr(s, "claim_job_for_fire", lambda _jid, **_kw: False)
    monkeypatch.setattr(s, "run_one_job", run_spy)
    monkeypatch.setattr(
        s, "advance_next_runs", lambda _ids: 0
    )
    monkeypatch.setattr(s, "get_due_jobs", lambda: [
        {"id": "race-job", "name": "race"},
    ])

    executed = s.tick(verbose=False, sync=True)
    assert executed >= 1

    run_spy.assert_not_called()
    rows = ledger.list_executions(job_id="race-job")
    assert rows, "ticker must have created its execution row"
    assert rows[0]["status"] == "relinquished"
    assert rows[0]["source"] == "builtin"
    assert "concurrent firer" in rows[0]["error"]


def test_concurrent_claim_two_firers_one_row_survives(temp_home, ledger):
    """THE card-level scenario, end-to-end against the real store CAS:
    a WS-daemon-style firer and the ticker race claim_job_for_fire on the
    same job; exactly one wins; the loser's ledger row must end
    relinquished while the winner's row reaches completed."""
    from cron.jobs import create_job, claim_job_for_fire, get_job
    import cron.scheduler as s

    job = create_job(prompt="x", schedule="every 5m", name="race-target")
    jid = job["id"]

    daemon_row = ledger.create_execution(jid, source="buzz-ws-daemon")
    ticker_row = ledger.create_execution(jid, source="builtin")

    # Fire both claims the way production does: daemon first, ticker right
    # behind it — the store CAS must serialize them.
    daemon_won = claim_job_for_fire(jid, return_job=True)
    ticker_won = claim_job_for_fire(jid, return_job=True)

    winners = [bool(isinstance(w, dict)) for w in (daemon_won, ticker_won)]
    assert winners == [True, False], "CAS must pick exactly one firer"

    # loser retires its row through the SAME kernel helper the patched
    # _process_job uses
    ledger.relinquish_execution(
        ticker_row["id"],
        reason="Fire claim lost to a concurrent firer; execution was not started.",
    )

    # winner's bookkeeping proceeds normally
    winner_claim = daemon_won
    winner_claim["execution_id"] = daemon_row["id"]
    ledger.mark_execution_running(daemon_row["id"])
    ledger.finish_execution(daemon_row["id"], success=True)

    rows = ledger.list_executions(job_id=jid)
    by_source = {r["source"]: r for r in rows}
    assert by_source["builtin"]["status"] == "relinquished"
    assert by_source["buzz-ws-daemon"]["status"] == "completed"
    # and the durable job record is healthy — no phantom failure anywhere
    rec = get_job(jid)
    assert rec.get("last_status") in (None, "ok")


def test_true_threaded_concurrent_claim_single_winner(temp_home, ledger):
    """Hammer the CAS from N threads at once (simulated concurrent claim):
    exactly one claim wins and no loser write can corrupt the winner's row."""
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="hammer")
    jid = job["id"]

    rows = [ledger.create_execution(jid, source=f"firer-{i}") for i in range(8)]
    results = [None] * 8
    barrier = threading.Barrier(8)

    def firer(i):
        try:
            barrier.wait(timeout=5)
            results[i] = bool(isinstance(claim_job_for_fire(jid, return_job=True), dict))
        except Exception as e:  # noqa: BLE001
            results[i] = f"err:{e}"

    threads = [threading.Thread(target=firer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert all(r is True or r is False for r in results), results
    assert sum(1 for r in results if r is True) == 1
    # every loser retires cleanly; the winner's row is untouchable
    for i, row in enumerate(rows):
        if results[i] is False:
            out = ledger.relinquish_execution(row["id"])
            assert out is None or out["status"] == "relinquished"
        else:
            assert ledger.finish_execution(row["id"], success=True) is not None
    statuses = sorted(r["status"] for r in ledger.list_executions(job_id=jid))
    assert statuses == sorted(["relinquished"] * 7 + ["completed"])


# --------------------------------------------------------------------------
# 3. the winner-flip: transient probe must not fail a completed run
# --------------------------------------------------------------------------

def test_transient_probe_failure_does_not_interrupt_completed_run(
    temp_home, ledger, monkeypatch
):
    """THE t_1074c7a2 regression: _fire_claim_ownership_lost() returning
    False-once (transient) must NOT mark a finishing run interrupted when
    the immediate re-probe confirms ownership. Drives _run_one_job_body with
    the real store CAS + a flaky first probe."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="probe-flake")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    # Flaky probe: fails exactly once (the transient), succeeds after.
    real_heartbeat = s.heartbeat_fire_claim
    state = {"calls": 0}

    def flaky_probe(job_id, *, expected_owner):
        state["calls"] += 1
        if state["calls"] == 1:
            return False  # the transient lie
        return real_heartbeat(job_id, expected_owner=expected_owner)

    monkeypatch.setattr(s, "heartbeat_fire_claim", flaky_probe)

    calls = []

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        calls.append("run")
        return (True, "out", "final response", None)

    def fake_save(jid_, out):
        return f"/tmp/{jid_}.txt"

    def fake_deliver(job, content, adapters=None, loop=None, **_k):
        calls.append("deliver")
        return None

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", fake_deliver)

    # silence job-record writes that would need the real mark pipeline
    monkeypatch.setattr(
        s, "mark_job_run",
        lambda jid_, ok, err=None, delivery_error=None, **_kw: True,
    )

    ok = s._run_one_job_body(claimed, verbose=False)
    assert ok is True
    assert calls == ["run", "deliver"]

    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", (
        f"transient probe must not flip a completed run: {final}"
    )
    assert final["error"] is None


def test_completed_run_survives_persistent_double_false_probe(
    temp_home, ledger, monkeypatch
):
    """THE 2026-09-10 03:04:58 regression (round 2): even when the outer
    ownership probes BOTH return False (persistent-looking transient), a
    COMPLETED run whose claim is provably still owned must fall through to
    normal bookkeeping — completed row, delivery attempted — instead of
    being written failed 'Interrupted by shutdown before terminal
    completion.' 144ms after success. The card's literal deliverable:
    claim token checked before final status write."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="winner-flip")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    real_heartbeat = s.heartbeat_fire_claim
    probe_log = []

    def double_false_then_true(job_id, *, expected_owner):
        # _fire_claim_ownership_lost probes twice, then the terminal-write
        # gate probes once — all three see False in the 03:04:58 shape is
        # IMPOSSIBLE (the third returned True, that's how 'Interrupted by
        # shutdown' got written). Simulate exactly that: probes 1-2 False,
        # probe 3 (the owner gate) True.
        probe_log.append(True)
        if len(probe_log) <= 2:
            return False
        return real_heartbeat(job_id, expected_owner=expected_owner)

    monkeypatch.setattr(s, "heartbeat_fire_claim", double_false_then_true)

    calls = []

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        calls.append("run")
        return (True, "out", "final response", None)

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", lambda j, o, **_k: "/tmp/x.txt")
    monkeypatch.setattr(
        s, "_deliver_result",
        lambda job, content, adapters=None, loop=None, **_k: calls.append("deliver"),
    )
    monkeypatch.setattr(
        s, "mark_job_run",
        lambda jid_, ok, err=None, delivery_error=None, **_kw: True,
    )

    ok = s._run_one_job_body(claimed, verbose=False)
    assert ok is True
    assert calls == ["run", "deliver"], calls

    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", final
    assert final["error"] is None


def test_interrupted_owned_run_still_records_interruption(
    temp_home, ledger, monkeypatch
):
    """Guard rail for round 2: an owned claim on a FAILED/aborted run
    (success=False) still records 'Interrupted by shutdown...' — the
    completed-run guard must not swallow genuinely interrupted runs."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="owned-interrupted")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    real_heartbeat = s.heartbeat_fire_claim
    probe_log = []

    def double_false_then_true(job_id, *, expected_owner):
        # Round 3 (t_e7356d78): the ownership probe is a 3-step ladder
        # (probe + 0.25s + 0.5s re-probes); the terminal-write gate then
        # probes once more. For the guard rail — owned + genuinely
        # interrupted run still records the interruption — the ladder
        # must confirm loss (calls 1-3 False) while the gate's own probe
        # (call 4) recovers True: ownership provably still ours.
        probe_log.append(True)
        if len(probe_log) <= 3:
            return False
        return real_heartbeat(job_id, expected_owner=expected_owner)

    monkeypatch.setattr(s, "heartbeat_fire_claim", double_false_then_true)

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        # the agent was interrupted mid-flight: success=False
        return (False, "out", "", "RuntimeError: interrupted")

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", lambda j, o, **_k: "/tmp/x.txt")
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **k: None)
    monkeypatch.setattr(
        s, "mark_job_run",
        lambda jid_, ok, err=None, delivery_error=None, **_kw: True,
    )

    ok = s._run_one_job_body(claimed, verbose=False)
    assert ok is True
    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "failed"
    assert "Interrupted by shutdown" in (final["error"] or "")


def test_persistent_ownership_loss_still_interrupts(temp_home, ledger, monkeypatch):
    """Guard rail: a REAL takeover (probe False twice) still fences the stale
    runner — the double-probe must not mask genuine ownership loss."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="real-takeover")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    # A replacement firer takes the claim over durably (TTL-bounded reclaim
    # happens in production when the owner crashed). Simulate by force.
    from cron.jobs import claim_job_for_fire as _cjf

    monkeypatch.setattr(s, "heartbeat_fire_claim", lambda *_a, **_k: False)

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        return (True, "out", "final response", None)

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", lambda j, o, **_k: "/tmp/x.txt")
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **k: None)
    monkeypatch.setattr(
        s, "mark_job_run", lambda *a, **k: True
    )

    ok = s._run_one_job_body(claimed, verbose=False)
    assert ok is True
    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "failed"
    assert "ownership" in (final["error"] or "").lower()


# --------------------------------------------------------------------------
# 4. provider surface
# --------------------------------------------------------------------------

def test_provider_claim_fire_lost_race_relinquishes(temp_home, ledger):
    """scheduler_provider.claim_fire on a lost race must retire the row as
    relinquished; only genuine rejections (missing/paused job) stay failed."""
    from cron.jobs import create_job, claim_job_for_fire
    from cron.scheduler_provider import CronScheduler

    job = create_job(prompt="x", schedule="every 5m", name="provider-race")
    jid = job["id"]
    # the ticker already claimed → provider loses
    assert isinstance(claim_job_for_fire(jid, return_job=True), dict)

    class _Prov(CronScheduler):
        @property
        def name(self):
            return "stub"

        def start(self, stop_event, **kw):
            pass

        def register_job(self, job):
            return None

    prov = _Prov()
    # claim_fire creates its own execution row via self.name source
    out = prov.claim_fire(jid)
    assert out is None  # lost the race
    rows = ledger.list_executions(job_id=jid, limit=10)
    provider_rows = [r for r in rows if r["source"] == "stub"]
    assert provider_rows, rows
    assert provider_rows[0]["status"] == "relinquished"


def test_provider_claim_fire_missing_job_records_failure(temp_home, ledger):
    """A claim on a nonexistent job is a genuine rejection — stays failed."""
    from cron.scheduler_provider import CronScheduler

    class _Prov(CronScheduler):
        @property
        def name(self):
            return "stub"

        def start(self, stop_event, **kw):
            pass

        def register_job(self, job):
            return None

    prov = _Prov()
    out = prov.claim_fire("no-such-job-anywhere")
    assert out is None
    rows = ledger.list_executions(job_id="no-such-job-anywhere")
    assert rows and rows[0]["status"] == "failed"
    assert "no longer exists" in rows[0]["error"]


# --------------------------------------------------------------------------
# 5. monitoring projection
# --------------------------------------------------------------------------

def test_monitoring_projection_accepts_relinquished(ledger):
    from agent.monitoring.cron_health import project_execution_event

    row = ledger.create_execution("job-mon", source="builtin")
    out = ledger.relinquish_execution(row["id"])
    event = project_execution_event(out)
    assert event.status == "relinquished"
    assert event.error_class is None  # not classified as a failure


# --------------------------------------------------------------------------
# 6. round 3 (t_e7356d78): boot-contention transient must not fence an
#    in-flight run; a durable takeover still must.
# --------------------------------------------------------------------------

def _round3_patches(monkeypatch, s):
    calls = []

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        calls.append("run")
        return (True, "out", "final response", None)

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", lambda j, o, **_k: "/tmp/x.txt")
    monkeypatch.setattr(
        s, "_deliver_result",
        lambda job, content, adapters=None, loop=None, **_k: calls.append("deliver"),
    )
    monkeypatch.setattr(
        s, "mark_job_run",
        lambda jid_, ok, err=None, delivery_error=None, **_kw: True,
    )
    return calls


def test_boot_contention_transient_does_not_fence_inflight_run(
    temp_home, ledger, monkeypatch
):
    """THE row-2bd51483 regression (round 3): during dual-scheduler boot
    catch-up, the first 60s heartbeat can return False across the whole
    re-probe ladder while ownership provably never changed. The run must
    NOT be fenced — the loss signal defers to the next beat, which
    (transient over) re-confirms ownership and the run completes."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="boot-burst")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    real_heartbeat = s.heartbeat_fire_claim
    # Call 1 is the ENTRY gate probe (must tell the truth: owned). Calls
    # 2-5 are beat-1's probe + the 3-probe ladder — all lie False, a
    # contention window outlasting round 2's 0.25s re-probe while
    # ownership provably never changed. Every call from 6 on (beat 2,
    # ~60s later in prod, one interval in the test) tells the truth.
    state = {"calls": 0}

    def contention_transient(job_id, *, expected_owner):
        state["calls"] += 1
        if 2 <= state["calls"] <= 5:
            return False
        return real_heartbeat(job_id, expected_owner=expected_owner)

    monkeypatch.setattr(s, "heartbeat_fire_claim", contention_transient)
    calls = _round3_patches(monkeypatch, s)

    # Drive the full run_one_job wrapper: the heartbeat thread runs with
    # a short interval so two beats fire inside the run's lifetime; the
    # age gate must survive beat 1's confirmed-looking loss.
    monkeypatch.setattr(s, "_RUN_CLAIM_HEARTBEAT_SECONDS", 0.05)
    monkeypatch.setattr(s, "_FIRE_CLAIM_REPROBE_LADDER_SECONDS", (0.01, 0.01))

    def body_no_cancel(job, *, defer_agent_teardown=None, extra_prompt=None,
                       cancel_event=None, execution_id=None, **kw):
        calls.append("body")
        deadline = time.monotonic() + 0.4
        while time.monotonic() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                calls.append("FENCED")
                return (False, "out", "", "interrupted")
            time.sleep(0.01)
        return (True, "out", "final response", None)

    monkeypatch.setattr(s, "run_job", body_no_cancel)

    with patch_scope_secrets():
        ok = s.run_one_job(claimed)
    assert ok is True
    assert "FENCED" not in calls, calls
    assert calls.count("body") == 1

    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", final
    assert final["error"] is None


def test_persistent_takeover_fences_on_second_beat(
    temp_home, ledger, monkeypatch
):
    """Guard rail (round 3): a REAL takeover (every probe False forever)
    must still fence the stale runner — now on the SECOND consecutive
    confirmed-loss beat (~one heartbeat interval after detection)."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="real-takeover-r3")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    monkeypatch.setattr(s, "_RUN_CLAIM_HEARTBEAT_SECONDS", 0.05)
    monkeypatch.setattr(s, "_FIRE_CLAIM_REPROBE_LADDER_SECONDS", (0.01, 0.01))

    # Call 1 = entry gate (truth: owned). Every probe after that lies
    # False — a durable takeover.
    state = {"calls": 0}

    def entry_true_then_lost(job_id, *, expected_owner):
        state["calls"] += 1
        if state["calls"] == 1:
            return True
        return False

    monkeypatch.setattr(s, "heartbeat_fire_claim", entry_true_then_lost)

    fenced = threading.Event()

    def body_no_cancel(job, *, defer_agent_teardown=None, extra_prompt=None,
                       cancel_event=None, execution_id=None, **kw):
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                fenced.set()
                return (False, "out", "", "interrupted")
            time.sleep(0.01)
        return (True, "out", "final response", None)

    monkeypatch.setattr(s, "run_job", body_no_cancel)

    with patch_scope_secrets():
        ok = s.run_one_job(claimed)

    assert fenced.wait(timeout=1) is True or fenced.is_set()
    assert ok is True
    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "failed"
    assert (
        "Interrupted by shutdown" in (final["error"] or "")
        or "ownership" in (final["error"] or "").lower()
    )


def test_unknown_probe_beats_never_fence(temp_home, ledger, monkeypatch):
    """Fence-unavailable (UNKNOWN/None) beats must never fence a run —
    the row-2bd51483 class where the fire fence flock times out under
    contention. Bounded only by the last-confirmed grace window."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="fence-timeout")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    # Entry gate tolerates None; beats return None forever.
    monkeypatch.setattr(s, "heartbeat_fire_claim", lambda *_a, **_k: None)

    monkeypatch.setattr(s, "_RUN_CLAIM_HEARTBEAT_SECONDS", 0.05)
    monkeypatch.setattr(s, "_FIRE_CLAIM_REPROBE_LADDER_SECONDS", (0.01, 0.01))
    monkeypatch.setattr(s, "_FIRE_CLAIM_HEARTBEAT_GRACE_SECONDS", 10.0)

    fenced = threading.Event()

    def body_no_cancel(job, *, defer_agent_teardown=None, extra_prompt=None,
                       cancel_event=None, execution_id=None, **kw):
        deadline = time.monotonic() + 0.6
        while time.monotonic() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                fenced.set()
                return (False, "out", "", "interrupted")
            time.sleep(0.01)
        return (True, "out", "final response", None)

    monkeypatch.setattr(s, "run_job", body_no_cancel)

    with patch_scope_secrets():
        ok = s.run_one_job(claimed)

    assert not fenced.is_set(), "UNKNOWN beats must not fence"
    assert ok is True
    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", final


def test_jobs_heartbeat_returns_none_when_fence_unavailable(
    temp_home, ledger, monkeypatch
):
    """The store primitive itself: an unacquirable fire fence yields
    None (UNKNOWN), not False (ownership-lost)."""
    import cron.jobs as jobs
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="fence-none")
    jid = job["id"]
    assert claim_job_for_fire(jid) is True
    claimed = jobs.get_job(jid)
    owner = claimed["fire_claim"]["by"]

    # Hold the cross-process fire fence from this process so the probe
    # cannot acquire it (flock times out after _JOBS_LOCK_TIMEOUT_SECONDS).
    # Instead of the 30s wait, shrink the timeout for the test.
    monkeypatch.setattr(jobs, "_JOBS_LOCK_TIMEOUT_SECONDS", 0.2)

    import contextlib

    @contextlib.contextmanager
    def stuck_fence(_job_id):
        yield False  # fence unacquirable — e.g. flock timeout

    monkeypatch.setattr(jobs, "_fire_job_lock", stuck_fence)
    assert jobs.heartbeat_fire_claim(jid, expected_owner=owner) is None


def test_terminal_gate_none_probe_does_not_discard_completed_run(
    temp_home, ledger, monkeypatch
):
    """Round-3 terminal gate: a completed run whose owner probe returns
    UNKNOWN (None) must fall through to normal bookkeeping — the durable
    expected_fire_owner CAS inside mark_job_run stays the final arbiter,
    so optimism cannot corrupt a real takeover."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="gate-none")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    # _fire_claim_ownership_lost routes through _confirm_fire_claim_loss;
    # a None first probe → "cannot prove loss" → ownership NOT lost →
    # normal path. The terminal gate also probes once (None → not False
    # → _still_owner True → success falls through normally).
    monkeypatch.setattr(s, "heartbeat_fire_claim", lambda *_a, **_k: None)

    calls = _round3_patches(monkeypatch, s)

    with patch_scope_secrets():
        ok = s._run_one_job_body(claimed, verbose=False)
    assert ok is True
    assert calls == ["run", "deliver"], calls

    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", final
    assert final["error"] is None


def patch_scope_secrets():
    """Silence the profile secret-scope setup around real body runs."""
    from contextlib import ExitStack
    from unittest.mock import patch

    stack = ExitStack()
    stack.enter_context(
        patch("agent.secret_scope.set_secret_scope", return_value=None)
    )
    stack.enter_context(
        patch(
            "agent.secret_scope.build_profile_secret_scope",
            return_value=None,
        )
    )
    stack.enter_context(patch("agent.secret_scope.reset_secret_scope"))
    return stack


# ==========================================================================
# t_c3f925f6 — phantom fire-claim fences from profile-scoped store flips
# ==========================================================================
# The WebUI host process flips process-global HERMES_HOME + cron.jobs module
# constants for the duration of any profile-scoped request (api/profiles.py
# legacy context managers; api/streaming.py turn env). A fire-claim
# heartbeat beat resolving the store inside that window found the root job
# ABSENT in the profile's jobs.json and returned False (provable loss)
# through the round-3 ladder — killing healthy runs (2026-09-10 rows
# 2bd51483, 07e495b4/2855, d90d235c/2890). Fix: absent = None (UNKNOWN),
# never False; run_one_job store-pins the whole run at dispatch origin.


def test_heartbeat_absent_from_flipped_store_returns_unknown_not_false(
    temp_home, monkeypatch
):
    """THE t_c3f925f6 regression: a beat resolving the WRONG (flipped) store
    must return None (UNKNOWN), never False (provable loss). Round-3's ladder
    then treats it as cannot-prove-loss and the run survives."""
    import cron.jobs as cj

    job = cj.create_job(prompt="x", schedule="every 5m", name="flip-target")
    jid = job["id"]
    claimed = cj.claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    owner = claimed["fire_claim"]["by"]

    orig = (cj.HERMES_DIR, cj.CRON_DIR, cj.JOBS_FILE, cj.OUTPUT_DIR)
    orig_env = os.environ.get("HERMES_HOME", "")
    profile_home = Path(temp_home) / "profile-home"
    (profile_home / "cron").mkdir(parents=True, exist_ok=True)
    (profile_home / "cron" / "jobs.json").write_text("[]")

    cj.HERMES_DIR = profile_home
    cj.CRON_DIR = profile_home / "cron"
    cj.JOBS_FILE = cj.CRON_DIR / "jobs.json"
    cj.OUTPUT_DIR = cj.CRON_DIR / "output"
    os.environ["HERMES_HOME"] = str(profile_home)
    try:
        result = cj.heartbeat_fire_claim(jid, expected_owner=owner)
        # THE FIX: absent from the resolved store = UNKNOWN, not loss.
        assert result is None, (
            f"absent-from-store must be None (UNKNOWN), got {result!r} — "
            "a False here is the phantom fence that killed rows "
            "2bd51483/07e495b4/2855/d90d235c/2890"
        )
    finally:
        cj.HERMES_DIR, cj.CRON_DIR, cj.JOBS_FILE, cj.OUTPUT_DIR = orig
        if orig_env:
            os.environ["HERMES_HOME"] = orig_env
        else:
            os.environ.pop("HERMES_HOME", None)
        # sanity: after restore, the same beat confirms ownership.
        assert cj.heartbeat_fire_claim(jid, expected_owner=owner) is True


def test_run_claim_heartbeat_absent_from_store_returns_unknown(temp_home):
    """Symmetric guard for the one-shot run_claim heartbeat: absent from the
    resolved store is UNKNOWN, not False."""
    import cron.jobs as cj

    job = cj.create_job(prompt="x", schedule="30m", name="oneshot-flip")
    jid = job["id"]
    claimed = cj.claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)

    orig = (cj.HERMES_DIR, cj.CRON_DIR, cj.JOBS_FILE, cj.OUTPUT_DIR)
    orig_env = os.environ.get("HERMES_HOME", "")
    profile_home = Path(temp_home) / "profile-home2"
    (profile_home / "cron").mkdir(parents=True, exist_ok=True)
    (profile_home / "cron" / "jobs.json").write_text("[]")
    cj.HERMES_DIR = profile_home
    cj.CRON_DIR = profile_home / "cron"
    cj.JOBS_FILE = cj.CRON_DIR / "jobs.json"
    cj.OUTPUT_DIR = cj.CRON_DIR / "output"
    os.environ["HERMES_HOME"] = str(profile_home)
    try:
        # no run_claim stamped; job absent from flipped store → None
        result = cj.heartbeat_run_claim(
            jid, expected_owner="whatever-owner"
        )
        assert result is None
    finally:
        cj.HERMES_DIR, cj.CRON_DIR, cj.JOBS_FILE, cj.OUTPUT_DIR = orig
        if orig_env:
            os.environ["HERMES_HOME"] = orig_env
        else:
            os.environ.pop("HERMES_HOME", None)


def test_run_one_job_store_pin_survives_concurrent_flip(
    temp_home, ledger, monkeypatch
):
    """The dispatch-origin store pin (run_one_job) must make the whole run —
    entry gate, fenced output save, delivery, terminal mark — resolve the
    ORIGINAL store even while a concurrent thread flip-flops the process
    globals the way a profile-scoped request does. No UnboundLocalError, no
    phantom fence, row completed."""
    import cron.jobs as cj
    import cron.scheduler as s

    job = cj.create_job(prompt="x", schedule="every 5m", name="pinned-run")
    jid = job["id"]
    claimed = cj.claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    _orig = (cj.HERMES_DIR, cj.CRON_DIR, cj.JOBS_FILE, cj.OUTPUT_DIR)
    _orig_env = os.environ.get("HERMES_HOME", "")
    profile_home = Path(temp_home) / "prof-flip"
    (profile_home / "cron").mkdir(parents=True, exist_ok=True)
    (profile_home / "cron" / "jobs.json").write_text("[]")

    stop_flip = threading.Event()
    flip_count = {"n": 0}

    def flipper():
        while not stop_flip.wait(0.02):
            cj.HERMES_DIR = profile_home
            cj.CRON_DIR = profile_home / "cron"
            cj.JOBS_FILE = cj.CRON_DIR / "jobs.json"
            cj.OUTPUT_DIR = cj.CRON_DIR / "output"
            os.environ["HERMES_HOME"] = str(profile_home)
            flip_count["n"] += 1
            time.sleep(0.02)
            cj.HERMES_DIR, cj.CRON_DIR, cj.JOBS_FILE, cj.OUTPUT_DIR = _orig
            os.environ["HERMES_HOME"] = _orig_env

    events = []

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        events.append("run")
        # Hold the run open long enough for several flip cycles AND a
        # heartbeat beat to land inside a flipped window.
        time.sleep(0.3)
        return (True, "out", "final response", None)

    def fake_save(jid_, out):
        events.append("save")
        return f"/tmp/{jid_}.txt"

    def fake_deliver(job, content, adapters=None, loop=None, **_k):
        events.append("deliver")
        return None

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", fake_deliver)
    monkeypatch.setattr(
        s, "mark_job_run",
        lambda jid_, ok, err=None, delivery_error=None, **_kw: True,
    )
    monkeypatch.setattr(s, "_RUN_CLAIM_HEARTBEAT_SECONDS", 0.05)
    monkeypatch.setattr(s, "_FIRE_CLAIM_REPROBE_LADDER_SECONDS", (0.01, 0.01))

    t = threading.Thread(target=flipper, daemon=True)
    t.start()
    try:
        with patch_scope_secrets():
            ok = s.run_one_job(claimed)
    finally:
        stop_flip.set()
        t.join(timeout=2.0)

    assert ok is True, "pinned run must survive a concurrent store flip"
    assert events == ["run", "save", "deliver"], (
        f"pinned run must complete full bookkeeping, got {events}"
    )
    assert flip_count["n"] >= 1, "flipper must actually have flipped"

    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", (
        f"pinned run must complete, not fence: {final}"
    )


def test_output_fence_trip_recovery_fallthrough_no_unbound_local(
    temp_home, ledger, monkeypatch
):
    """THE 15:11:20 live hit (exec af574a14): when the OUTPUT side-effect
    fence trips on a flipped store and the recovery probe then sees ownership
    back, the fall-through must do honest bookkeeping — NOT raise
    UnboundLocalError on should_deliver/unresolved_origin. With the
    t_c3f925f6 pre-initialized defaults the row closes normally; the
    UnboundLocalError path is gone."""
    import cron.scheduler as s
    from cron.jobs import create_job, claim_job_for_fire

    job = create_job(prompt="x", schedule="every 5m", name="fence-trip")
    jid = job["id"]
    claimed = claim_job_for_fire(jid, return_job=True)
    assert isinstance(claimed, dict)
    row = ledger.create_execution(jid, source="builtin")
    claimed["execution_id"] = row["id"]

    events = []

    def fake_run_job(job, *, defer_agent_teardown=None, execution_id=None, **kw):
        events.append("run")
        return (True, "out", "final response", None)

    def fake_save(jid_, out):
        raise AssertionError("save must be skipped when the fence yields False")

    class _FenceFalse:
        def __enter__(self):
            return False

        def __exit__(self, *a):
            return False

    real_fence = s.fire_claim_fence

    fence_state = {"trips": 0}

    def fence_false_then_true(job_id, *, expected_owner):
        fence_state["trips"] += 1
        if fence_state["trips"] == 1:
            return _FenceFalse()
        return real_fence(job_id, expected_owner=expected_owner)

    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **k: None)
    monkeypatch.setattr(
        s, "mark_job_run",
        lambda jid_, ok, err=None, delivery_error=None, **_kw: True,
    )
    monkeypatch.setattr(s, "fire_claim_fence", fence_false_then_true)

    with patch_scope_secrets():
        ok = s._run_one_job_body(claimed, verbose=False)
    assert ok is True
    # Pre-fix this raised UnboundLocalError, escaped through the outer
    # BaseException handler, and left a failed row with the UnboundLocal
    # text. Post-fix the recovery probe sees ownership back (flip ended),
    # falls through, and the row closes completed with delivery suppressed.
    final = ledger.list_executions(job_id=jid)[0]
    assert final["status"] == "completed", (
        f"recovery fall-through must complete the row: {final}"
    )
    assert final["error"] is None
    assert "unbound" not in (final.get("error") or "").lower()
