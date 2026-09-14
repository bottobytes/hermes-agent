# kernel-hardening

This branch carries the Sportacus kernel hardening for Hermes Agent. It is the
build source for the `hermes-webui` container image (Coolify stack
`hermes-agent-with-webui-*`, `dockerfile_inline` clones this branch into
`/opt/hermes` at image build).

**Base tracking:** the `KERNEL_BASE` file at repo root records the upstream
tag (`NousResearch/hermes-agent`) this branch is currently based on.

## What lives here (post v2026.9.11 merge — upstream decomposed the kernel in Sep 2026)

Upstream's v2026.9.11 split the old monoliths (`kanban_db.py`,
`kanban.py`, `kanban_watchers.py`, `conversation_loop.py`, `cli.py`) into
decomposed modules. Our hardening follows the code: it now lives in the
files below (the only files we carry patches in — everything else is
upstream).

| File | Hardening |
|---|---|
| `hermes_cli/kanban_db.py` | The kernel core we kept origin-resident: blob-500 TEXT-coercion (`#997eb31b`), rate-limit sentinel sidecar + `KANBAN_RATE_LIMIT_EXIT_CODE` neutral lane (t_d4c215a7), restart windows (`kanban_restart_windows` table + open/close/holds), ghost-board tombstone guard (t_bbdec489), crash classifier w/ provider-cause death classification + blind-exit reclass + verdict-suspect (t_e586ea59/t_09c47c8d), unreachable-assignee sweep (t_09c47c8d), review/ready starvation detectors (t_f0393d9f/t_67d80b05), stuck-escalation idempotent triage cards, write-time assignee validation state (t_fbd0fb38), vacuous-dependency-block refusal (t_17cda1e8), default-reviewer routing + per-profile cap map (t_3c8f043d) |
| `hermes_cli/kanban_db_dispatch.py` | DispatchResult neutral lanes (`rate_limited`/`restart_killed`/`restart_orphans`/`flagged_unreachable`/`review_spawn_starved`/`respawn_starved`), restart-window spawn hold, the three hardening sweeps wired into `_run_reclaim_phase`, crash classifier delegation to the enriched `kanban_db` version |
| `tools/kanban_tools.py` | t_fbd0fb38 `_validate_write_assignee` gate on `kanban_request_review` (3-strike fallback + normalized writes) and stateless phantom rejection on `kanban_create`; t_17cda1e8 refusal-reason surfacing on `kanban_block` |
| `hermes_cli/kanban.py` | t_17cda1e8 `block` prints the kernel refusal reason; `restart-window` open/close/list command |
| `hermes_cli/kanban_parser.py` | `restart-window` subcommand spec |
| `hermes_cli/kanban_ops.py` | dispatch `--json` neutral-lane fields |
| `hermes_cli/config_defaults.py` | `kanban.max_in_progress_per_profile_map` + `kanban.default_reviewer` (t_3c8f043d) |
| `gateway/kanban_watchers.py` | cap-map/default-reviewer boot logging (t_3c8f043d), REVIEW-LANE/READY-LANE STALL warnings, nonspawnable early signal, stuck-escalation probe (t_67d80b05) |
| `gateway/kanban_watchers_notifier.py` | TERMINAL_KINDS/_WAKE_KINDS extended with `assignee_unreachable`/`review_spawn_starved`/`rate_limited`/`restart_orphan`/`respawn_starved`; formatters for each; quota-episode (rl-episode) suppression (t_e586ea59); crashed-with-provider-cause rendering |
| `agent/retry_utils.py` | transient-throttle long backoff (15/30/60/60) + `is_transient_throttle_reason` + `transient_throttle_retry_ceiling` + GLM-family model gate (`glm-` prefix) |
| `agent/turn_recovery.py` | throttle-family retry-ceiling extension + adaptive-backoff family + Retry-After policy notes (ported from the old conversation_loop) |
| `agent/turn_api_error.py` | `failure_reason` plumbing into `compute_error_backoff` |
| `cli.py` | `_kanban_exit_code_for_result` EX_TEMPFAIL(75) mapping + rate-limit sentinel sidecar writer, wired on both the quiet (-Q) and non-quiet (-q) single-query paths |
| `hermes_cli/cli_chat_turn_mixin.py` | `_last_turn_failure_reason`/`_last_turn_failed` stash in `_chat_settle_turn` |
| `locales/en.yaml` / `locales/zh.yaml` | kanban wake keys for the five hardening kinds |
| `cron/scheduler_delivery.py` | webui cron delivery lane (t_993b18df): `webui` pseudo-platform in `_KNOWN_DELIVERY_PLATFORMS`, explicit `webui:<sid>` target resolution, `_deliver_to_webui` (HTTP POST into the WebUI's own `/api/chat/start` → server-side `[CRON DELIVERY]` turn), `_deliver_result` webui branch; persistent busy-redelivery spool (t_ee4b2f97): `_WebuiBusyError` sentinel, spool dir + backoff/horizon, `_deliver_pending_webui_reports`; t_70dd9cc7 honest busy-exhaust logging (recurring jobs self-heal, no scheduling claim) |
| `cron/scheduler_preflight.py` | bot-chat-style preflight carve-out for `webui:` targets (t_993b18df) |
| `cron/scheduler.py` | tick-time webui redelivery pass (every tick incl. idle; t_ee4b2f97) + webui-lane name re-export surface for the `cron.scheduler` monkeypatch contract |
| `tools/cronjob_job_args.py` | webui-lane mode guidance notes (t_993b18df) |
| `tests/cron/test_cron_webui_delivery.py` | t_993b18df/t_ee4b2f97/t_70dd9cc7: 24-check webui delivery suite (target resolution, preflight carve-out, HTTP lane, spool, redelivery, tick hook) |
| `tests/hermes_cli/test_kanban_respawn_guard_staleness_t_3371481a.py` | t_3371481a: respawn-guard staleness + live-credential gates + rate-limit backoff suite |
| `tests/hermes_cli/test_kanban_review_lane_cap_t_a0d28a97.py` | t_a0d28a97: serial review-lane per-profile cap suite |
| `tests/hermes_cli/test_kanban_worker_real_home_t_d3f69e96.py` | t_d3f69e96: kanban worker real-home resolution suite |
| `tests/plugins/test_coolify_deploy_watch.py` | t_9b07e08e: 35-check deploy-wake contract suite (payload shapes, session resolution, dedup, adopt-recent, scanner) |
| `tests/hermes_cli/test_kanban_*.py` | the suites listed in the runbook below |

Note: upstream's own `kanban_db_connect.py` executes `_kb.SCHEMA_SQL`
(origin-resident in `kanban_db.py`), so our schema additions
(`kanban_restart_windows`) apply regardless of which connect path runs.

## Updating (runbook)

A weekly watcher (root-spool cron `hermes-agent upstream drift watch`,
Mondays 09:00, monitor `upstream-watch-kernel.sh`) compares the latest
`NousResearch/hermes-agent` tag against `KERNEL_BASE` and opens a kanban card
on drift — you will be pinged, don't need to poll.

When a new upstream tag lands:

1. `git fetch upstream --tags` (`upstream` = https://github.com/NousResearch/hermes-agent.git)
2. `git merge <new-tag>` — conflicts are expected in the files above
   (everything else should merge clean; if it doesn't, stop and look).
   Resolve keeping BOTH the upstream change and our hardening. When
   upstream moved one of our change-sites into a new decomposed module,
   PORT the delta to its new home (see the v2026.9.11 merge for the
   worked example) instead of keeping the old monolith copy.
3. Update `KERNEL_BASE` to the new tag.
4. Run the regression suites: `pytest tests/hermes_cli/test_kanban_transient_requeue.py`
   (must be all PASS) plus the review-wake suites
   (`test_kanban_review_wake_gaps.py`, `test_kanban_restart_orphan.py`,
   `test_kanban_blind_exit_reclass.py`, `test_kanban_qpath_quota_trap.py`,
   `test_kanban_assignee_validation.py`,
   `test_kanban_dependency_block_t_17cda1e8.py`,
   `test_kanban_stuck_escalation_t_67d80b05.py`,
   `test_kanban_default_reviewer_and_cap_map.py`,
   `test_kanban_respawn_guard_staleness_t_3371481a.py`,
   `test_kanban_review_lane_cap_t_a0d28a97.py`,
   `test_kanban_worker_real_home_t_d3f69e96.py`,
   `test_cron_webui_delivery.py` (tests/cron),
   `test_coolify_deploy_watch.py` (tests/plugins)).
5. Push: `git push origin kernel-hardening`.
6. Deploy the Coolify stack (one stack deploy). Post a heads-up comment on
   the sportacus board first if workers are running (restart-window
   coordination). Verify after: container healthy, `/opt/hermes` HEAD is the
   kernel-hardening tip, boot hook stamped `changed=0`, suites green,
   WebUI serves.

Typical cost: ~15 min when the conflict surface behaves. The v2026.9.11
merge (upstream decomposed ~5 monoliths) took considerably longer — budget
accordingly when upstream ships another big refactor.
