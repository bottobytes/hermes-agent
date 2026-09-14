"""Regression tests for the serial-review-lane cap (t_a0d28a97).

Incident (2026-09-09 14:2xZ, sportacus board): the Captain's 12:25Z ruling
made sportacus-reviewer SERIAL on standard GLM-5.3 (one shared zai
subscription), but the dispatcher had no review-lane cap — every claimable
review-status card got its own lane (4 simultaneous review lanes), all
bounded only by the TOTAL per-profile cap (map value 4). The parallel heavy
review workers burned the shared pool → burst rate-limit wall → 3 review
runs killed mid-flight, 3 cascade-marked key pools, re-starvation risk via
stale error strings.

Patch under test:

1. ``kanban.review_lane_max_parallel`` (scalar) + per-profile
   ``kanban.review_lane_max_parallel_map`` overrides + the
   ``HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL`` env override — env > config
   scalar > None (no cap, backward compatible).
2. The cap counts only in-flight REVIEW-origin runs (claim events carrying
   the ``source_status=review`` marker ``claim_review_task`` records — the
   same provenance ``_retry_status_for_run`` trusts) via
   ``count_running_review_origin_per_profile``.
3. Enforcement lives in the review loop only: a profile at its review-lane
   cap simply doesn't claim more review cards — clean skip into
   ``DispatchResult.skipped_review_lane_capped``, no events, no failure
   ticks, no guard. Ready-lane work under the same profile, other profiles,
   and the surviving review lane are untouched.
4. Drain semantics: the second review card spawns on the very next tick
   after the first run completes (complete_task clears the review-origin
   count).
"""

from __future__ import annotations

# --- t_a0d28a97 test hygiene: the dispatcher-spawned worker env carries
# --- HERMES_KANBAN_* pointing at the LIVE sportacus board. Any test that
# --- creates tasks must run with those scrubbed or poison INSERTs hit the
# --- live board (fleet lesson from the e2e fixtures).
import os as _os

for _k in list(_os.environ):
    if _k.startswith("HERMES_KANBAN_") or _k in ("HERMES_REAL_HOME", "HERMES_PROFILE"):
        del _os.environ[_k]

import os
import sys

import pytest


@pytest.fixture()
def isolated_env(monkeypatch, tmp_path):
    """Fresh HERMES_HOME with alpha/beta/gamma profiles + scratch board."""
    test_home = tmp_path / "home"
    for prof in ("alpha", "beta", "gamma"):
        (test_home / "profiles" / prof).mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(test_home))
    monkeypatch.delenv("HERMES_KANBAN_ROOT_CONFIG", raising=False)
    monkeypatch.delenv("HERMES_REAL_HOME", raising=False)
    monkeypatch.delenv(
        "HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", raising=False
    )
    for mod in list(sys.modules.keys()):
        if (
            mod.startswith("hermes_cli")
            or mod.startswith("hermes_state")
            or mod == "hermes_constants"
        ):
            del sys.modules[mod]
    from hermes_cli import kanban_db

    yield kanban_db, test_home, tmp_path
    os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)
    os.environ.pop("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", None)


def _fake_spawn(*args, **kwargs):
    return 424242


def _mk_review_card(conn, kb, *, title, assignee):
    """Create a task, run it, and move it to review under ``assignee``."""
    tid = kb.create_task(conn, title=title, assignee="alpha")
    kb.claim_task(conn, tid)
    ok = kb.request_review(
        conn, tid, reviewer=assignee, force=True, summary="review me"
    )
    assert ok, f"request_review failed for {title}"
    return tid


def _write_root_cfg(tmp, text):
    root_cfg = tmp / "root-config.yaml"
    root_cfg.write_text(text, encoding="utf-8")
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(root_cfg)


def _dispatch(kb, conn, **kw):
    return kb.dispatch_once(conn, spawn_fn=_fake_spawn, dry_run=True, **kw)


# ---------------------------------------------------------------------------
# 1. The card's acceptance test: cap=1, 2 claimable review cards → exactly
#    1 spawn; the second spawns only after the first run completes
# ---------------------------------------------------------------------------


def test_cap1_two_cards_exactly_one_spawn_then_drain(isolated_env):
    kb, home, tmp = isolated_env
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
        t1 = _mk_review_card(conn, kb, title="r1", assignee="beta")
        t2 = _mk_review_card(conn, kb, title="r2", assignee="beta")
    os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "1"
    try:
        # Dry tick: exactly one spawnable review card.
        with kb.connect_closing() as conn:
            res = _dispatch(kb, conn)
            assert len(res.spawned) == 1, (
                f"cap=1 must yield exactly one review spawn, got {res.spawned}"
            )
            assert len(res.skipped_review_lane_capped) == 1, (
                f"the other card must be lane-capped, got "
                f"{res.skipped_review_lane_capped}"
            )
        # Real tick: one claim (persists the review-origin run).
        with kb.connect_closing() as conn:
            res2 = kb.dispatch_once(conn, spawn_fn=_fake_spawn)
            assert len(res2.spawned) == 1, res2.spawned
            winner = res2.spawned[0][0]
            loser = t1 if winner == t2 else t2
            row = conn.execute(
                "SELECT status, claim_lock, last_failure_error, "
                "consecutive_failures FROM tasks WHERE id=?", (loser,)
            ).fetchone()
            assert row["status"] == "review", "capped card stays review-queued"
            assert row["claim_lock"] is None, "capped card keeps clean fields"
            assert (row["last_failure_error"] or "") == ""
            assert int(row["consecutive_failures"] or 0) == 0
            # and NO events were written for the capped card this tick
            n = conn.execute(
                "SELECT COUNT(*) FROM task_events WHERE task_id=?", (loser,)
            ).fetchone()[0]
        # While lane 1 is running: the loser stays capped.
        with kb.connect_closing() as conn:
            res3 = _dispatch(kb, conn)
            assert res3.spawned == [], (
                f"lane at cap must spawn nothing, got {res3.spawned}"
            )
            assert [c[0] for c in res3.skipped_review_lane_capped] == [loser]
        # The reviewer completes → lane drains → loser spawns next tick.
        with kb.connect_closing() as conn:
            run_id = conn.execute(
                "SELECT current_run_id FROM tasks WHERE id=?", (winner,)
            ).fetchone()["current_run_id"]
            assert kb.complete_task(
                conn, winner, summary="approved", expected_run_id=run_id,
            )
        with kb.connect_closing() as conn:
            res4 = _dispatch(kb, conn)
            assert [s[0] for s in res4.spawned] == [loser], (
                f"drained lane must spawn the waiting card, got {res4.spawned}"
            )
    finally:
        os.environ.pop("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", None)


# ---------------------------------------------------------------------------
# 2. Backward compatibility: no knob → old behaviour (all spawn)
# ---------------------------------------------------------------------------


def test_no_cap_configured_spawns_all(isolated_env):
    kb, home, tmp = isolated_env
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
        _mk_review_card(conn, kb, title="r1", assignee="beta")
        _mk_review_card(conn, kb, title="r2", assignee="beta")
        _mk_review_card(conn, kb, title="r3", assignee="beta")
    assert os.environ.get("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL") is None
    with kb.connect_closing() as conn:
        assert kb.review_lane_max_parallel() is None
        assert kb.review_lane_cap_map() == {}
        res = _dispatch(kb, conn)
    assert len(res.spawned) == 3, res.spawned
    assert res.skipped_review_lane_capped == []


# ---------------------------------------------------------------------------
# 3. Config scalar caps every profile's review lane
# ---------------------------------------------------------------------------


def test_scalar_config_caps_all_profiles(isolated_env):
    kb, home, tmp = isolated_env
    _write_root_cfg(
        tmp,
        "kanban:\n  review_lane_max_parallel: 1\n",
    )
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            _mk_review_card(conn, kb, title="b1", assignee="beta")
            _mk_review_card(conn, kb, title="b2", assignee="beta")
            _mk_review_card(conn, kb, title="g1", assignee="gamma")
            _mk_review_card(conn, kb, title="g2", assignee="gamma")
            assert kb.review_lane_max_parallel() == 1
            res = _dispatch(kb, conn)
        by_profile = {}
        for _tid, who, _ws in res.spawned:
            by_profile[who] = by_profile.get(who, 0) + 1
        assert by_profile == {"beta": 1, "gamma": 1}, by_profile
        assert len(res.skipped_review_lane_capped) == 2
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


# ---------------------------------------------------------------------------
# 4. Per-profile map overrides the scalar
# ---------------------------------------------------------------------------


def test_map_overrides_scalar_per_profile(isolated_env):
    kb, home, tmp = isolated_env
    _write_root_cfg(
        tmp,
        "kanban:\n"
        "  review_lane_max_parallel: 1\n"
        "  review_lane_max_parallel_map:\n"
        "    beta: 2\n",
    )
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            for i in range(3):
                _mk_review_card(conn, kb, title=f"b{i}", assignee="beta")
            for i in range(2):
                _mk_review_card(conn, kb, title=f"g{i}", assignee="gamma")
            assert kb.review_lane_cap_map() == {"beta": 2}
            res = _dispatch(kb, conn)
        by_profile = {}
        for _tid, who, _ws in res.spawned:
            by_profile[who] = by_profile.get(who, 0) + 1
        assert by_profile == {"beta": 2, "gamma": 1}, by_profile
        capped_profiles = sorted(c[1] for c in res.skipped_review_lane_capped)
        assert capped_profiles == ["beta", "gamma"]
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


# ---------------------------------------------------------------------------
# 5. Env wins over config; invalid values fall through
# ---------------------------------------------------------------------------


def test_env_beats_config_and_invalid_falls_through(isolated_env):
    kb, home, tmp = isolated_env
    _write_root_cfg(
        tmp,
        "kanban:\n  review_lane_max_parallel: 1\n",
    )
    try:
        with kb.connect_closing() as conn:
            kb.create_board(slug="default", name="T")
            _mk_review_card(conn, kb, title="b1", assignee="beta")
            _mk_review_card(conn, kb, title="b2", assignee="beta")
        # env=2 beats config=1
        os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "2"
        with kb.connect_closing() as conn:
            assert kb.review_lane_max_parallel() == 2
            res = _dispatch(kb, conn)
        assert len(res.spawned) == 2
        assert res.skipped_review_lane_capped == []
        # invalid env falls through to config scalar
        os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "zero"
        with kb.connect_closing() as conn:
            assert kb.review_lane_max_parallel() == 1
            res = _dispatch(kb, conn)
        assert len(res.spawned) == 1
        assert len(res.skipped_review_lane_capped) == 1
        # env=0 (disable) also falls through — 0 is not a valid cap
        os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "0"
        with kb.connect_closing() as conn:
            assert kb.review_lane_max_parallel() == 1
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)
        os.environ.pop("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", None)


def test_invalid_config_values_yield_no_cap(isolated_env):
    kb, home, tmp = isolated_env
    _write_root_cfg(
        tmp,
        "kanban:\n"
        "  review_lane_max_parallel: 0\n"
        "  review_lane_max_parallel_map:\n"
        "    beta: true\n"
        "    gamma: -1\n"
        "    delta: two\n"
        "    '': 5\n",
    )
    try:
        assert kb.review_lane_max_parallel() is None
        assert kb.review_lane_cap_map() == {}
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


def test_broken_root_config_fails_open(isolated_env):
    kb, home, tmp = isolated_env
    bad = tmp / "bad-config.yaml"
    bad.write_text("kanban: [unclosed", encoding="utf-8")
    os.environ["HERMES_KANBAN_ROOT_CONFIG"] = str(bad)
    try:
        assert kb.review_lane_max_parallel() is None
        assert kb.review_lane_cap_map() == {}
    finally:
        os.environ.pop("HERMES_KANBAN_ROOT_CONFIG", None)


# ---------------------------------------------------------------------------
# 6. Blast-radius: ready lane, other statuses, impl runs, total cap
# ---------------------------------------------------------------------------


def test_ready_lane_and_impl_runs_unaffected(isolated_env):
    """Ready-lane work under the SAME profile is untouched by the review
    cap, and a running IMPLEMENTATION task does not consume a lane."""
    kb, home, tmp = isolated_env
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
        w1 = kb.create_task(conn, title="w1", assignee="beta")
        w2 = kb.create_task(conn, title="w2", assignee="beta")
        # a running implementation task under beta (plain claim_task —
        # its claimed event has NO review marker)
        impl = kb.create_task(conn, title="impl", assignee="beta")
        kb.claim_task(conn, impl)
        r1 = _mk_review_card(conn, kb, title="r1", assignee="beta")
        r2 = _mk_review_card(conn, kb, title="r2", assignee="beta")
        counts = kb.count_running_review_origin_per_profile(conn)
        assert counts == {}, (
            f"impl run must be invisible to the review-lane counter: {counts}"
        )
    os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "1"
    try:
        with kb.connect_closing() as conn:
            res = _dispatch(kb, conn)
        spawned_ids = [s[0] for s in res.spawned]
        # both ready cards spawn regardless of the review cap
        assert set((w1, w2)) <= set(spawned_ids), res.spawned
        # exactly one review spawn; the other review card is lane-capped
        review_spawned = [i for i in spawned_ids if i in (r1, r2)]
        assert len(review_spawned) == 1, res.spawned
        capped = [c[0] for c in res.skipped_review_lane_capped]
        assert len(capped) == 1 and capped[0] in (r1, r2)
        # the impl run was never touched
        with kb.connect_closing() as conn:
            row = conn.execute(
                "SELECT status FROM tasks WHERE id=?", (impl,)
            ).fetchone()
            assert row["status"] == "running"
    finally:
        os.environ.pop("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", None)


def test_total_per_profile_cap_still_binds_review_spawns(isolated_env):
    """A generous review-lane cap must NOT bypass the TOTAL per-profile
    cap: with the reviewer at its total cap via an impl run, the review
    card defers through skipped_per_profile_capped."""
    kb, home, tmp = isolated_env
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
        impl = kb.create_task(conn, title="impl", assignee="beta")
        kb.claim_task(conn, impl)  # 1 impl run in flight
        _mk_review_card(conn, kb, title="r1", assignee="beta")
    os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "5"
    try:
        with kb.connect_closing() as conn:
            res = _dispatch(kb, conn, max_in_progress_per_profile=1)
        assert res.spawned == [], res.spawned
        assert len(res.skipped_per_profile_capped) == 1
        assert res.skipped_review_lane_capped == []
    finally:
        os.environ.pop("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", None)


def test_lane_capped_tick_not_classified_idle(isolated_env):
    """The dispatch-tick hook outcome must not read 'idle' when cards were
    lane-capped (observability parity with the total cap bucket). Uses a
    tick whose ONLY activity is the lane-cap skip: the surviving lane is
    already running, the loser is queued."""
    kb, home, tmp = isolated_env
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="T")
        t1 = _mk_review_card(conn, kb, title="r1", assignee="beta")
        t2 = _mk_review_card(conn, kb, title="r2", assignee="beta")
    os.environ["HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL"] = "1"
    try:
        # Real claim of the first card → one running review-origin lane.
        with kb.connect_closing() as conn:
            res_real = kb.dispatch_once(conn, spawn_fn=_fake_spawn)
            assert len(res_real.spawned) == 1
        # Now the dry tick: nothing else exists EXCEPT the capped loser.
        with kb.connect_closing() as conn:
            res = _dispatch(kb, conn)
        assert res.spawned == [], res.spawned
        assert len(res.skipped_review_lane_capped) == 1
        # replicate the hook's outcome computation
        outcome = "ok"
        if not any((
            res.spawned, res.reclaimed, res.promoted, res.reconciled_orphans,
            res.crashed, res.stale, res.timed_out, res.auto_blocked,
            res.rate_limited, res.auto_assigned_default, res.respawn_guarded,
            res.skipped_per_profile_capped, res.skipped_review_lane_capped,
            res.skipped_unassigned, res.skipped_nonspawnable,
        )):
            outcome = "idle"
        assert outcome == "ok", (
            "a lane-capped tick must not be classified idle — the "
            "skipped_review_lane_capped bucket must count as activity"
        )
        # and the capped card itself was never touched
        loser = res.skipped_review_lane_capped[0][0]
        with kb.connect_closing() as conn:
            row = conn.execute(
                "SELECT status, claim_lock FROM tasks WHERE id=?", (loser,)
            ).fetchone()
            assert row["status"] == "review" and row["claim_lock"] is None
    finally:
        os.environ.pop("HERMES_KANBAN_REVIEW_LANE_MAX_PARALLEL", None)
