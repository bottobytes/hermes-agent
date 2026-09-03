# kernel-hardening

This branch carries the Sportacus kernel hardening for Hermes Agent. It is the
build source for the `hermes-webui` container image (Coolify stack
`hermes-agent-with-webui-*`, `dockerfile_inline` clones this branch into
`/opt/hermes` at image build).

**Base tracking:** the `KERNEL_BASE` file at repo root records the upstream
tag (`NousResearch/hermes-agent`) this branch is currently based on.

## What lives here (9 files + 5 tests — the only files we carry)

| File | Hardening |
|---|---|
| `hermes_cli/kanban_db.py` | blob-500 TEXT-coercion fix (bundle `997eb31b`) + rate-limit sentinel sidecar state + review-lane reliability stack (t_4ce2d942 union): unreachable-assignee sweep w/ review-lane 15-min grace, `review_spawn_starved` detector, restart-orphan neutral lane + epoch adopt, provider-cause death classification, blind-exit/qpath reclass |
| `hermes_cli/kanban.py` | kanban restart-window guard (`_cmd_restart_window`) |
| `cli.py` | `_kanban_transient_failure_reason` classification |
| `agent/retry_utils.py` | transient-throttle retry budget 3→8, adaptive 15/30/60/60s backoff |
| `agent/conversation_loop.py` | throttle-long classification wiring |
| `gateway/kanban_watchers.py` | wake terminal kinds (unreachable/starved/rate-limited/orphan/changes) + REVIEW-LANE STALL & nonspawnable WARNING log lines |
| `locales/en.yaml` / `locales/zh.yaml` | kanban wake keys for the above kinds |
| `tests/hermes_cli/test_kanban_review_wake_gaps.py` | 13-check review-wake suite |
| `tests/hermes_cli/test_kanban_restart_orphan.py` | restart-orphan suite |
| `tests/hermes_cli/test_kanban_blind_exit_reclass.py` + `test_kanban_qpath_quota_trap.py` | blind-exit / -q rc=0 quota suites |

The runtime boot hook `/workspace/kernel-restore/boot-entry.sh` (compose
`command:`) stays in place as an **idempotent tripwire**: on a pre-patched
tree it no-ops (stamp `changed=0`). It is not load-bearing.

## Updating (runbook)

A weekly watcher (root-spool cron `hermes-agent upstream drift watch`,
Mondays 09:00, monitor `upstream-watch-kernel.sh`) compares the latest
`NousResearch/hermes-agent` tag against `KERNEL_BASE` and opens a kanban card
on drift — you will be pinged, don't need to poll.

When a new upstream tag lands:

1. `git fetch upstream --tags`
2. `git merge <new-tag>` — expect conflicts **only** in the 5 files listed
   above (everything else should merge clean; if it doesn't, stop and look).
   Resolve keeping BOTH the upstream change and our hardening.
3. Update `KERNEL_BASE` to the new tag.
4. Run the regression suite: `pytest tests/hermes_cli/test_kanban_transient_requeue.py`
   (must be 30/30 PASS) plus the review-wake suites
   (`test_kanban_review_wake_gaps.py` 13/13, `test_kanban_restart_orphan.py`,
   `test_kanban_blind_exit_reclass.py`, `test_kanban_qpath_quota_trap.py`).
5. Push: `git push origin kernel-hardening`.
6. Deploy the Coolify stack (one stack deploy). Post a heads-up comment on
   the sportacus board first if workers are running (restart-window
   coordination). Verify after: container healthy, `/opt/hermes` HEAD is the
   kernel-hardening tip, boot hook stamped `changed=0`, verify_t9 30/30,
   WebUI serves.

Typical cost: ~15 min when the 5-file conflict surface behaves.
