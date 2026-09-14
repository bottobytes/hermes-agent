"""Cron delivery: target resolution (origin/home/explicit/bot-chat), transcript mirroring and
session seeding, live-adapter / relay / standalone send lanes, and ``_deliver_result``.

Split out of ``cron.scheduler``. Import names from this module directly (``cron.scheduler`` only
imports the few it calls itself). Origin-resident helpers and sibling split modules are reached
late-bound (``_sched`` / module refs at the bottom) so monkeypatching the defining module works.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import contextvars
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional

from hermes_cli._subprocess_compat import windows_hide_flags

# Log-record parity with the origin module.
logger = logging.getLogger("cron.scheduler")


# Validates user-supplied delivery platform names, preventing env-var enumeration via crafted names.
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "discord", "slack", "whatsapp", "signal",
    "matrix", "mattermost", "homeassistant", "dingtalk", "feishu",
    "wecom", "wecom_callback", "weixin", "sms", "email", "webhook", "bluebubbles",
    "qqbot", "yuanbao",
    # t_993b18df: WebUI pseudo-platform -- local HTTP delivery, no gateway
    # credentials (see _deliver_to_webui and the bot-chat-style preflight
    # carve-out in scheduler_preflight._preflight_check_delivery).
    "webui"})

# Platforms supporting a cron/notification home target -> env var used by gateway config.
_HOME_TARGET_ENV_VARS = {
    "matrix": "MATRIX_HOME_ROOM",
    "telegram": "TELEGRAM_HOME_CHANNEL",
    "discord": "DISCORD_HOME_CHANNEL",
    "slack": "SLACK_HOME_CHANNEL",
    "signal": "SIGNAL_HOME_CHANNEL",
    "mattermost": "MATTERMOST_HOME_CHANNEL",
    "sms": "SMS_HOME_CHANNEL",
    "email": "EMAIL_HOME_ADDRESS",
    "dingtalk": "DINGTALK_HOME_CHANNEL",
    "feishu": "FEISHU_HOME_CHANNEL",
    "wecom": "WECOM_HOME_CHANNEL",
    "weixin": "WEIXIN_HOME_CHANNEL",
    "bluebubbles": "BLUEBUBBLES_HOME_CHANNEL",
    "qqbot": "QQBOT_HOME_CHANNEL",
    "whatsapp": "WHATSAPP_HOME_CHANNEL",
    "whatsapp_cloud": "WHATSAPP_CLOUD_HOME_CHANNEL"}

# Back-compat: primary env var -> previous name, consulted when the primary is unset.
_LEGACY_HOME_TARGET_ENV_VARS = {"QQBOT_HOME_CHANNEL": "QQ_HOME_CHANNEL"}


def _resolve_cron_surface_mode(pconfig, logical_platform_name: str) -> str:
    """Return ``"in_channel"`` or ``"thread"`` (default) for a platform config.

    Native: flat ``platforms.<p>.extra.cron_continuable_surface``. Relay-fronted:
    ``platforms.relay.extra.<logical>.cron_continuable_surface`` (same sub-block as the relay's
    Slack knobs); the sub-block wins and is scoped to its logical platform. Unlike
    _relay_slack_extra, a sub-block that omits the knob falls back to the flat key (deliberate —
    the flat key must keep working), so a flat value applies to EVERY platform the relay fronts.
    """
    with contextlib.suppress(Exception):
        extra = getattr(pconfig, "extra", None) or {}
        sub = extra.get(str(logical_platform_name or "").lower())
        raw = sub.get("cron_continuable_surface") if isinstance(sub, dict) else None
        if raw is None:
            raw = extra.get("cron_continuable_surface")
        if raw is not None and str(raw).strip().lower() == "in_channel":
            return "in_channel"
    return "thread"


def _resolve_origin(job: dict) -> Optional[dict]:
    """Extract origin info from a job. Non-dict origins (provenance strings, hand-edited
    jobs.json) are treated as missing — otherwise every fire crashed on ``origin.get``.

    Without this guard, a job tagged with e.g. ``"combined-digest-replaces-x-and-y"`` crashed every fire
    attempt with ``'str' object has no attribute 'get'`` — ``mark_job_run`` recorded the failure, but the
    next tick re-loaded the same poisoned origin and crashed identically until the field was patched
    manually (#18722).
    """
    origin = job.get("origin")
    if isinstance(origin, dict) and origin.get("platform") and origin.get("chat_id"):
        return origin
    return None


def _cron_mirror_delivery_enabled(job: dict, cfg: Optional[dict] = None) -> bool:
    """Whether a cron delivery is also mirrored into the target chat's session transcript.

    Default OFF. Precedence: per-job ``attach_to_session`` (bool) → global
    ``cron.mirror_delivery`` → False. CARVE-OUT: the ``in_channel`` surface seeds its session
    independently of this knob (the seed IS that feature) — this governs only the thread-surface
    mirror. ``mirror_to_session`` runs at a turn boundary, so it is alternation- and cache-safe.
    """
    per_job = job.get("attach_to_session")
    if isinstance(per_job, bool):
        return per_job
    try:
        if cfg is None:
            cfg = _sched.load_config() or {}
        return bool((cfg.get("cron", {}) or {}).get("mirror_delivery", False))
    except Exception:
        return False


def _target_matches_origin(origin: dict, platform_name: str, chat_id: str,
                           thread_id: Optional[str]) -> bool:
    """True when a delivery target is the job's own origin conversation. A pinned origin
    thread_id must match — a target without it is a different lane. Mirror eligibility for
    non-origin targets is decided by ``_target_mirror_eligible``."""
    if (
        not origin
        or str(origin.get("platform", "")).lower() != str(platform_name).lower()
        or str(origin.get("chat_id", "")) != str(chat_id)
    ):
        return False
    origin_thread = origin.get("thread_id")
    return origin_thread is None or str(origin_thread) == str(thread_id or "")


# Provenance rank for the dedup OR-merge in _resolve_delivery_targets (higher = stronger mirror
# claim). Broadcasts rank 0 so "origin,all"/"all,origin" keep the origin tag regardless of order.
_MIRROR_PROVENANCE_RANK = {"origin": 3, "origin_fallback": 2, "home": 2, "explicit": 1}


def _target_mirror_eligible(
    job: dict, target: dict, *, global_mirror: bool, origin_match: Optional[bool] = None) -> bool:
    """Whether a resolved delivery target may receive the transcript mirror. Origin targets:
    always. ``origin_fallback`` (deliver=origin with no captured origin → home channel, standing
    in for the primary conversation) and ``home`` (user-written bare-platform token, e.g.
    ``deliver: slack`` — deliberately addresses that platform's home channel): same flags as a
    true origin. ``explicit`` ``platform:chat_id``: ONLY with per-job ``attach_to_session: true``
    — the global flag must never write transcripts into arbitrary explicitly-addressed chats.
    Untagged broadcast expansions (``all``) are never eligible. ``origin_match`` may be
    precomputed."""
    if origin_match is None:
        origin = _resolve_origin(job) or {}
        origin_match = _target_matches_origin(
            origin, target.get("platform", ""), target.get("chat_id", ""), target.get("thread_id"))
    if origin_match:
        return True
    resolved_from = target.get("_resolved_from")
    if resolved_from in ("origin_fallback", "home"):
        # Same precedence as _cron_mirror_delivery_enabled (keep in sync): a per-job False must
        # beat a global True even for callers that don't pre-merge `global_mirror`.
        per_job = job.get("attach_to_session")
        return per_job if isinstance(per_job, bool) else bool(global_mirror)
    if resolved_from == "explicit":
        return job.get("attach_to_session") is True
    return False


def _inchannel_seed_allowed(*, is_dm: bool, user_id: Optional[str]) -> bool:
    """Whether the flat in_channel seed may run. Group keys are user-isolated
    (``…:group:<chat_id>:<user_id>``): seeding without a real user_id creates an orphan session no
    reply resolves to — worse than no seed. DM keys omit user_id, so DMs are always seedable."""
    return bool(is_dm or user_id)


def _cron_mirror_message(job: dict, text: str) -> str:
    return f"[Cron delivery: {job.get('name') or job.get('id', 'cron')}]\n{text}"


def _maybe_mirror_cron_delivery(
    job: dict, platform_name: str, chat_id: str, mirror_text: str, thread_id: Optional[str] = None,
    user_id: Optional[str] = None, *, enabled: bool = False,
) -> None:
    """Best-effort mirror of a cron delivery into the origin chat's session. No-op unless
    ``enabled`` (caller resolves it, scoped to the origin target). Rides the same
    ``mirror_to_session`` path as ``send_message``, passing ``user_id`` so user-isolated group
    chats resolve to the scheduling member. All failures swallowed — a successful delivery must
    never be reported failed because the mirror broke."""
    if not enabled:
        return
    text = (mirror_text or "").strip()
    if not text:
        return
    try:
        from gateway.mirror import mirror_to_session
        # USER role + labelled prefix, NOT assistant: an assistant-role mirror lands
        # assistant→assistant and breaks strict alternation; consecutive user turns merge safely.
        # The brief is not the agent speaking; an assistant-role mirror lands as assistant→assistant after
        # the agent's last turn and breaks strict alternation (issue #2221, the exact failure #2313
        # removed). A user-role turn collapses safely via repair_message_sequence's consecutive-user merge
        # on every provider, and the prefix preserves the "this came from cron" context that the dropped
        # SQLite mirror metadata would otherwise lose on replay.
        ok = mirror_to_session(
            platform_name, str(chat_id), _cron_mirror_message(job, text),
            source_label="cron", thread_id=thread_id, user_id=user_id, role="user")
        if ok:
            logger.info(
                "Job '%s': mirrored delivery into %s:%s session transcript",
                job.get("id", "?"), platform_name, chat_id)
        else:
            logger.debug(
                "Job '%s': delivery mirror skipped for %s:%s "
                "(no matching gateway session — cold start)",
                job.get("id", "?"), platform_name, chat_id)
    except Exception as e:
        logger.debug(
            "Job '%s': delivery mirror failed for %s:%s: %s", job.get("id", "?"), platform_name,
            chat_id, e,
        )


def _open_continuable_cron_thread(job: dict, adapter, chat_id: str, loop) -> Optional[str]:
    """Open a thread for a continuable cron job via ``adapter.create_handoff_thread``. Returns the
    thread_id, or ``None`` (no thread primitive / failed) = caller falls back to the DM mirror."""
    create_thread = getattr(adapter, "create_handoff_thread", None)
    if not callable(create_thread) or loop is None:
        return None
    thread_name = f"Hermes — {job.get('name') or job.get('id', 'cron')}"
    try:
        from agent.async_utils import safe_schedule_threadsafe
        coro = create_thread(str(chat_id), thread_name)
        future = safe_schedule_threadsafe(coro, loop)  # type: ignore[arg-type]
        if future is None:
            return None
        new_thread_id = future.result(timeout=30)
        return str(new_thread_id) if new_thread_id else None
    except Exception as e:
        logger.debug(
            "Job '%s': create_handoff_thread failed on %s — falling back to "
            "DM-session mirror: %s",
            job.get("id", "?"), getattr(adapter, "name", "?"), e)
        return None


def _seed_cron_session(
    job: dict, adapter, platform_name: str, chat_id: str, text: str, *, thread_id: Optional[str],
    chat_type: str, user_id: Optional[str], user_name: Optional[str] = None,
    chat_name: Optional[str], scope_id: Optional[str], discord_keys_on_thread: bool = False,
) -> bool:
    """Create the session row (so the mirror has a target) and mirror the brief as a USER turn.
    The seeded key must equal the reply's ``build_session_key``: chat_type, user_id, thread_id and
    scope_id (Slack team id) are all part of it, so callers pass exactly what the reply carries."""
    from gateway.config import Platform
    from gateway.session import SessionSource
    from gateway.mirror import mirror_to_session
    seeded_session_id: Optional[str] = None
    session_store = getattr(adapter, "_session_store", None)
    if session_store is not None:
        try:
            platform_enum = Platform(platform_name.lower())
        except (ValueError, KeyError):
            platform_enum = None
        if platform_enum is not None:
            # Discord keys in-thread messages with chat_id == thread_id; Slack/Telegram use the
            # parent channel.
            seed_chat_id = (
                str(thread_id)
                if discord_keys_on_thread and platform_enum == Platform.DISCORD
                else str(chat_id)
            )
            dest_source = SessionSource(
                platform=platform_enum, chat_id=seed_chat_id, chat_name=chat_name,
                chat_type=chat_type,
                user_id=user_id, user_name=user_name, thread_id=thread_id,
                scope_id=str(scope_id) if scope_id else None)
            # Create the row and pass its exact id to the mirror — origin-heuristic rediscovery
            # bails on populated chats.
            _entry = session_store.get_or_create_session(dest_source)
            seeded_session_id = getattr(_entry, "session_id", None)
    return mirror_to_session(
        platform_name, str(chat_id), _cron_mirror_message(job, text),
        source_label="cron", thread_id=thread_id, user_id=user_id, role="user",
        session_id=seeded_session_id,
    )


def _seed_cron_thread_session(
    job: dict, adapter, platform_name: str, chat_id: str, thread_id: str, mirror_text: str,
    chat_name: Optional[str] = None, is_dm: bool = False, scope_id: Optional[str] = None,
) -> None:
    """Seed the freshly-opened cron thread's session with the brief (never raises), else the
    user's in-thread reply resolves to a transcript without it. Threads are participant-shared
    (no real user_id); a DM thread must seed ``chat_type="dm"`` — DM-thread replies route through
    the DM arm (``…:dm:<chat>:<thread>``), so a "thread"-typed seed is a row no DM reply hits."""
    text = (mirror_text or "").strip()
    if not text:
        return
    try:
        ok = _seed_cron_session(
            job, adapter, platform_name, chat_id, text,
            thread_id=str(thread_id), chat_type="dm" if is_dm else "thread",
            user_id="system:cron", user_name="Cron", chat_name=chat_name, scope_id=scope_id,
            discord_keys_on_thread=True)
        if ok:
            logger.info(
                "Job '%s': opened continuable thread %s on %s:%s and seeded the brief",
                job.get("id", "?"), thread_id, platform_name, chat_id)
        else:
            logger.warning(
                "Job '%s': thread seed did NOT land on %s:%s thread=%s — an "
                "in-thread reply will not see this brief",
                job.get("id", "?"), platform_name, chat_id, thread_id)
    except Exception as e:
        # WARNING, not debug: a silent seed failure IS the continuation-amnesia bug.
        logger.warning(
            "Job '%s': seeding cron thread session failed for %s:%s:%s: %s",
            job.get("id", "?"), platform_name, chat_id, thread_id, e)


def _seed_cron_channel_session(
    job: dict, adapter, platform_name: str, chat_id: str, mirror_text: str, *, is_dm: bool,
    user_id: Optional[str], chat_name: Optional[str] = None, scope_id: Optional[str] = None,
) -> bool:
    """Seed the FLAT (thread_id=None) session for an ``in_channel`` delivery; True on success.
    ``mirror_to_session`` only APPENDS to an existing session and the flat row is only created by
    an inbound human message, so create the row first or the brief is silently dropped. Group keys
    are user-isolated (``…:group:<chat_id>:<user_id>``): the seed MUST carry the origin's real
    user_id, not ``system:cron``; DM keys omit user_id. chat_type mirrors the inbound handler."""
    text = (mirror_text or "").strip()
    if not text:
        return False
    try:
        chat_type = "dm" if is_dm else "group"
        ok = _seed_cron_session(
            job, adapter, platform_name, chat_id, text,
            thread_id=None,  # flat — the whole-channel/DM session
            chat_type=chat_type, user_id=str(user_id) if user_id else None,
            chat_name=chat_name, scope_id=scope_id,
        )
        if ok:
            logger.info(
                "Job '%s': seeded flat in_channel session on %s:%s (chat_type=%s)",
                job.get("id", "?"), platform_name, chat_id, chat_type)
        return bool(ok)
    except Exception as e:
        # WARNING, not debug: a silent seed failure IS the continuation-amnesia bug.
        logger.warning(
            "Job '%s': seeding in_channel session failed for %s:%s: %s",
            job.get("id", "?"), platform_name, chat_id, e)
        return False


def _cron_job_origin_log_suffix(job: dict) -> str:
    """Secret-free provenance suffix (origin platform/chat/source-IP fields) for security warnings
    about a bad stored ``context_from`` reference, where no live request object exists."""
    origin = job.get("origin")
    if not isinstance(origin, dict):
        return ""
    fields = []
    for key in ("platform", "chat_id", "thread_id", "source_ip", "remote", "forwarded_for"):
        value = origin.get(key)
        if value is None:
            continue
        text = str(value).replace("\r", " ").replace("\n", " ").strip()
        if text:
            fields.append(f"origin_{key}={text[:200]!r}")
    return " " + " ".join(fields) if fields else ""


def _plugin_cron_env_var(platform_name: str) -> str:
    """Cron home-channel env var registered by a plugin ``PlatformEntry.cron_deliver_env_var``."""
    with contextlib.suppress(Exception):
        from hermes_cli.plugins import discover_plugins
        discover_plugins()  # idempotent
        from gateway.platform_registry import platform_registry
        entry = platform_registry.get(platform_name.lower())
        if entry and entry.cron_deliver_env_var:
            return entry.cron_deliver_env_var
    return ""


def _is_known_delivery_platform(platform_name: str) -> bool:
    """Valid cron delivery platform: built-in, or plugin with a ``cron_deliver_env_var``."""
    name = platform_name.lower()
    return name in _KNOWN_DELIVERY_PLATFORMS or bool(_plugin_cron_env_var(name))


def _resolve_home_env_var(platform_name: str) -> str:
    """Env var name for a platform's cron home channel (built-in table, then plugin registry)."""
    name = platform_name.lower()
    return _HOME_TARGET_ENV_VARS.get(name) or _plugin_cron_env_var(name)


def _get_config_home_channel(platform_name: str):
    """Persisted ``HomeChannel`` from gateway config — the canonical store ``/sethome`` writes.
    The ``<PLATFORM>_HOME_CHANNEL`` env var is only a best-effort mirror; relay-fronted platforms
    may exist solely in config.yaml, so reading only the env mirror would drop their delivery."""
    try:
        from gateway.config import load_gateway_config, Platform
        return load_gateway_config().get_home_channel(Platform(platform_name.lower()))
    except Exception:
        logger.debug(
            "config home_channel lookup failed for platform %r", platform_name, exc_info=True)
        return None


def _home_env_lookup(env_var: str, suffix: str = "", *, strip: bool = False) -> str:
    """Value of ``<env_var><suffix>``, falling back to the legacy name (same suffix) when unset.
    Reads via ``get_secret``, not ``os.getenv``: in a multiplex gateway the tick runs with the
    job-owning profile's secret scope (run_one_job sets it), so this resolves the OWNING profile's
    value, not the host environ. ``os.getenv`` only if the scope module is missing."""
    try:
        from agent.secret_scope import get_secret
    except Exception:
        get_secret = None  # type: ignore

    def read(name: str) -> str:
        value = (get_secret(name, "") or "") if get_secret is not None else os.getenv(name, "")
        return value.strip() if strip else value

    value = read(env_var + suffix)
    legacy = _LEGACY_HOME_TARGET_ENV_VARS.get(env_var)
    if not value and legacy:
        value = read(legacy + suffix)
    return value


def _env_home_target_chat_id(platform_name: str) -> str:
    """Home chat id from the env mirror only (no config).

    Reads through ``get_secret`` (not raw ``os.getenv``) so a profile-scoped secret scope wins in a
    multiplex gateway. ``DISCORD_HOME_CHANNEL`` lives in each profile's ``.env``; in a multiplex process the
    winning cron tick runs with the job-owning profile's scope installed (run_one_job sets it), so reading
    via ``get_secret`` resolves the OWNING profile's chat id rather than the host process's ``os.environ``
    (#83182, chat-id leg — the token leg was fixed earlier; chat id / thread id resolve through the same
    leak).
    """
    env_var = _resolve_home_env_var(platform_name)
    return _home_env_lookup(env_var) if env_var else ""


def _get_home_target_chat_id(platform_name: str) -> str:
    """Home target chat id: env var (first, so operator overrides win) → legacy env var →
    config.yaml ``home_channel``."""
    value = _env_home_target_chat_id(platform_name)
    if value:
        return value
    home = _get_config_home_channel(platform_name)
    return str(home.chat_id) if home is not None and home.chat_id else ""


def _get_home_target_thread_id(platform_name: str) -> Optional[str]:
    """Optional thread/topic id for a platform home target. Telegram: ``TELEGRAM_CRON_THREAD_ID``
    overrides ``TELEGRAM_HOME_CHANNEL_THREAD_ID`` — in topic mode a root-DM delivery lands in the
    system-only lobby where the user cannot reply.

    When topic mode is enabled, deliveries that land in the root DM (thread_id unset) end up in the
    system-only lobby where the user cannot reply — the gateway returns the lobby reminder and drops
    ``reply_to_message_id`` (#24409). Pointing cron at a dedicated topic via this env var lets replies work
    as expected without changing the lobby invariant.
    """
    if platform_name.lower() == "telegram":
        cron_thread = _home_env_lookup("TELEGRAM_CRON_THREAD_ID", strip=True)
        if cron_thread:
            return cron_thread
    env_var = _resolve_home_env_var(platform_name)
    value = _home_env_lookup(env_var, "_THREAD_ID", strip=True) if env_var else ""
    if value:
        return value
    # config.yaml fallback only when the chat id also came from config (an env-provided chat id
    # keeps its env-provided thread semantics).
    if not _env_home_target_chat_id(platform_name):
        home = _get_config_home_channel(platform_name)
        if home is not None and home.thread_id:
            return str(home.thread_id)
    return None


def _iter_home_target_platforms():
    """Iterate built-in + plugin platform names that expose a home channel."""
    yield from _HOME_TARGET_ENV_VARS
    with contextlib.suppress(Exception):
        from hermes_cli.plugins import discover_plugins
        discover_plugins()  # idempotent
        from gateway.platform_registry import platform_registry
        for entry in platform_registry.plugin_entries():
            if entry.cron_deliver_env_var and entry.name not in _HOME_TARGET_ENV_VARS:
                yield entry.name


def _relay_fronted_delivery_platforms(connected: set) -> set:
    """Logical platforms deliverable through a connected relay. ``get_connected_platforms()`` only
    sees native platforms; fronted ones come from the same ``GATEWAY_RELAY_PLATFORMS`` stamp
    fire-time routing uses (validation symmetric with routing). No relay -> empty set."""
    if "relay" not in connected:
        return set()
    try:
        from gateway.relay import relay_fronted_platforms
        return relay_fronted_platforms()
    except Exception:
        logger.debug("relay fronted-platform lookup failed", exc_info=True)
        return set()


def cron_delivery_targets() -> list[dict]:
    """Platforms a cron job can auto-deliver to (single source of truth for UIs): valid delivery
    platform AND gateway-configured; ``home_target_set`` flags whether the home channel exists.
    ``{"id", "name", "home_target_set", "home_env_var"}`` dicts in canonical order; callers
    prepend the implicit ``local`` option themselves."""
    targets: list[dict] = []
    try:
        from gateway.config import load_gateway_config
        connected = {p.value for p in load_gateway_config().get_connected_platforms()}
        connected |= _relay_fronted_delivery_platforms(connected)
    except Exception:
        logger.debug("cron_delivery_targets: gateway config unavailable", exc_info=True)
        connected = set()

    for name in _iter_home_target_platforms():
        if name not in connected or not _is_known_delivery_platform(name):
            continue
        targets.append({
            "id": name,
            "name": name.replace("_", " ").title(),
            "home_target_set": bool(_get_home_target_chat_id(name)),
            "home_env_var": _resolve_home_env_var(name) or None})

    # Bot Chat targets: one per local profile (machine-local; no gateway config or home channel).
    try:
        from hermes_cli.profiles import list_profile_names
        for profile_name in list_profile_names():
            targets.append({
                "id": f"{BOT_CHAT_PLATFORM}:{profile_name}",
                "name": f"Bot Chat ({profile_name})",
                "home_target_set": True,
                "home_env_var": None})
    except Exception:
        logger.debug("cron_delivery_targets: profile listing unavailable", exc_info=True)
    return targets


def _origin_thread_is_stale(origin: dict) -> bool:
    """True when a Slack origin's thread is a stale creation-turn artifact. Thread-per-message
    Slack stamps each top-level message id as the session thread (a KEY, not a location); old jobs
    carry it as ``origin.thread_id``. Heuristic: if the origin chat IS the Slack home chat, the
    pinned thread is that artifact and delivery goes top-level (or to the home target's thread)."""
    if str(origin.get("platform") or "").lower() != "slack" or not origin.get("thread_id"):
        return False
    home_chat = _get_home_target_chat_id("slack")
    return bool(home_chat) and str(origin.get("chat_id")) == str(home_chat)


def _origin_delivery_thread(origin: dict):
    """The thread a deliver=origin job should use, stale stamps dropped."""
    if _origin_thread_is_stale(origin):
        return _get_home_target_thread_id("slack") or None
    return origin.get("thread_id")


def _home_target(platform_name: str, chat_id: str, resolved_from: Optional[str] = None) -> dict:
    """Target dict for a platform's configured home channel (+ optional mirror provenance)."""
    target = {
        "platform": platform_name,
        "chat_id": chat_id,
        "thread_id": _get_home_target_thread_id(platform_name)}
    if resolved_from:
        target["_resolved_from"] = resolved_from
    return target


def _resolve_single_delivery_target(
    job: dict, deliver_value: str, *, from_broadcast: bool = False
) -> Optional[dict]:
    """Resolve one concrete auto-delivery target for a cron job.

    ``from_broadcast`` marks a bare-platform token that was produced by expanding a broadcast
    token (``all``) rather than written by the user; broadcast expansions carry no mirror
    provenance (fan-out is never continuable), while a user-written bare platform token is a
    deliberate home-channel address and gets the ``home`` tag."""
    origin = _resolve_origin(job)
    if deliver_value == "local":
        return None
    # Must precede the generic platform:chat_id split so the profile name isn't parsed as chat_id.
    bot_chat_profile = parse_bot_chat_deliver_token(deliver_value)
    if bot_chat_profile is not None:
        return _resolve_bot_chat_target(job, bot_chat_profile)

    # t_993b18df: explicit webui:<session_id> targets. WebUI session ids are
    # bare hex strings with no platform-native syntax -- the generic
    # platform:target resolver below would bounce them off the channel
    # directory (unknown platform "webui") and silently drop the target.
    # Resolve directly, mirroring the bot-chat carve-out above.
    if deliver_value.lower().startswith(WEBUI_DELIVERY_PLATFORM + ":"):
        webui_sid = deliver_value.split(":", 1)[1].strip().lower()
        if not _WEBUI_SESSION_ID_RE.fullmatch(webui_sid):
            logger.warning(
                "Invalid cron delivery target '%s': not a WebUI session id "
                "(expected bare hex)",
                deliver_value,
            )
            return None
        return {
            "platform": WEBUI_DELIVERY_PLATFORM,
            "chat_id": webui_sid,
            "thread_id": None,
            "_resolved_from": "explicit",
        }

    if deliver_value == "origin":
        if origin:
            return {
                "platform": origin["platform"],
                "chat_id": str(origin["chat_id"]),
                "thread_id": _origin_delivery_thread(origin),
                "_resolved_from": "origin",  # provenance for _target_mirror_eligible
            }
        # No origin (API/script job): fall back to a home channel instead of silently dropping.
        for platform_name in _iter_home_target_platforms():
            chat_id = _get_home_target_chat_id(platform_name)
            if chat_id:
                logger.info(
                    "Job '%s' has deliver=origin but no origin; falling back to %s home channel",
                    job.get("name", job.get("id", "?")), platform_name)
                # Stands in for the primary conversation (NOT a broadcast): mirror-eligible.
                return _home_target(platform_name, chat_id, "origin_fallback")
        return None

    if ":" in deliver_value:
        platform_name, rest = deliver_value.split(":", 1)
        platform_key = platform_name.lower()
        from tools.send_message_tool import prepare_send_message_platforms, resolve_send_target
        prepare_send_message_platforms()
        # pass_unresolved_references: no model in the loop to react; an unknown-to-directory target
        # must reach the adapter as written or the job's output is silently lost.
        chat_id, thread_id, resolution_error = resolve_send_target(
            platform_key, rest, pass_unresolved_references=True)
        if resolution_error:
            logger.warning("Invalid cron delivery target '%s': %s", deliver_value, resolution_error)
            return None
        if (
            thread_id is None
            and platform_key == "slack"
            and origin
            and str(origin.get("platform") or "").lower() == platform_key
            and str(origin.get("chat_id")) == str(chat_id)
            and origin.get("thread_id")
            and not _origin_thread_is_stale(origin)
        ):
            thread_id = origin.get("thread_id")
        return {
            "platform": platform_name,
            "chat_id": chat_id,
            "thread_id": thread_id,
            "_resolved_from": "explicit",  # mirror-eligible only under attach_to_session opt-in
        }
    platform_name = deliver_value
    home_provenance = None if from_broadcast else "home"
    if origin and origin.get("platform") == platform_name:
        chat_id = _get_home_target_chat_id(platform_name)
        if chat_id:
            return _home_target(platform_name, chat_id, home_provenance)
        # No home configured: falls back to the origin chat. No tag needed — the
        # origin-match check in _target_mirror_eligible already covers this target.
        return {
            "platform": platform_name,
            "chat_id": str(origin["chat_id"]),
            "thread_id": origin.get("thread_id"),
        }
    if not _is_known_delivery_platform(platform_name):
        return None
    chat_id = _get_home_target_chat_id(platform_name)
    return _home_target(platform_name, chat_id, home_provenance) if chat_id else None


def _get_bot_chat_delivery_timeout() -> int:
    """Timeout for one bot-chat delivery turn (a full agent turn — minutes, not seconds).
    ``cron.bot_chat_delivery_timeout_seconds``; default 600."""
    try:
        cfg = _sched.load_config()
        value = int(cfg.get("cron", {}).get("bot_chat_delivery_timeout_seconds", 600))
        return value if value > 0 else 600
    except Exception:
        return 600


def _deliver_to_bot_chat(job: dict, content: str, profile: str) -> Optional[str]:
    """Hand output to the live Bot Chat owner, or use the legacy unowned CLI lane.

    None means completed; a queued/claimed receipt returns an explicit unverified status
    string so existing Optional[str] callers cannot misreport admission as delivery.
    ``profile`` is ``""`` for the job's own profile.
    """
    import hashlib
    import json
    import tempfile
    import uuid
    from hermes_constants import get_hermes_home
    from hermes_cli.profiles import get_profile_dir
    from tools.bot_live_delivery import (
        deliver_to_live_owner, find_canonical_live_owner, read_delivery_result,
    )

    job_id = job.get("id", "?")
    profile_label = profile or "(own)"
    message = (
        f'[Cronjob "{job.get("name", job_id)}" output — scheduled job, not the user. '
        f"Review it, act on anything that needs action, and summarize "
        f"for the chat.]\n\n{content}"
    )
    try:
        source_home = get_hermes_home().resolve()
        home = (get_profile_dir(profile) if profile else source_home).resolve()
        # run_one_job/claim_fire attach the durable execution id before delivery. The
        # transient fallback supports direct helper callers, never deduping recurring
        # runs by their (potentially identical) output or previous last_run timestamp.
        run_id = job.get("execution_id")
        if not run_id:
            run_id = job.setdefault("_bot_chat_run_id", uuid.uuid4().hex)
        key = hashlib.sha256(json.dumps(
            [str(source_home), job_id, str(run_id), str(home)],
            ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        # Read BEFORE discovery: the previous owner may have exited after accepting.
        # No receipt state, including ambiguous/failed, authorizes a CLI replay.
        receipt = read_delivery_result(home, key)
        if receipt is None:
            owner = find_canonical_live_owner(home)
            if owner is not None:
                receipt = deliver_to_live_owner(home, owner, message, delivery_id=key)
        if receipt is not None:
            if receipt["message"] != message:
                raise ValueError("delivery id already belongs to a different payload")
            status = receipt["status"]
            target = f"bot-chat:{profile_label}"
            receipts = job.setdefault("_bot_chat_delivery_receipts", {})
            receipts[target] = {"status": status, "delivery_id": key}
            logger.info("Job '%s': Bot Chat %s receipt=%s status=%s",
                        job_id, profile_label, key, status)
            if status == "settled":
                return None
            detail = ("completion unverified; do not resend" if status in ("queued", "claimed")
                      else receipt.get("error") or receipt.get("reason") or "not completed")
            return f"{target} {status} (receipt {key}): {detail}"
    except Exception as exc:
        # Discovery/admission uncertainty must never open a second-writer fallback.
        return f"bot-chat delivery to profile '{profile_label}' unverified: {exc}"

    hermes_bin = shutil.which("hermes")
    if hermes_bin:
        argv = [hermes_bin]
    else:
        try:
            import importlib.util as _ilu
            found = _ilu.find_spec("hermes_cli") is not None
        except Exception:
            found = False
        if not found:
            return "bot-chat delivery failed: hermes CLI not resolvable"
        argv = [sys.executable, "-m", "hermes_cli.main"]

    def _fail(msg: str, **log_kwargs) -> str:
        logger.warning("Job '%s': %s", job_id, msg, **log_kwargs)
        return msg

    from agent.delegation_context import delegated_child_subprocess_env
    env = delegated_child_subprocess_env(os.environ)
    if profile:
        argv += ["-p", profile]
        # -p owns profile resolution; this scheduler's HERMES_HOME must not shadow it.
        env.pop("HERMES_HOME", None)
    else:
        # Multiplex workers carry the profile in a ContextVar, not os.environ.
        env["HERMES_HOME"] = str(source_home)

    query_file = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", suffix=".txt", prefix="hermes-cron-botchat-", delete=False,
        ) as fh:
            fh.write(message)
            query_file = fh.name

        argv += [
            "chat", "--in", "~", "-c", "Bot Chat", "--create-if-missing",
            "-Q", "--query-file", query_file,
        ]
        result = subprocess.run(
            argv, capture_output=True, text=True, timeout=_get_bot_chat_delivery_timeout(), env=env,
            creationflags=windows_hide_flags())
        if result.returncode != 0:
            tail = (result.stderr or result.stdout or "").strip()[-500:]
            return _fail(
                f"bot-chat delivery to profile '{profile_label}' failed (exit {result.returncode})"
                + (f": {tail}" if tail else ""))
        logger.info("Job '%s': delivered to Bot Chat of profile '%s'", job_id, profile_label)
        return None
    except subprocess.TimeoutExpired:
        return _fail(
            f"bot-chat delivery to profile '{profile_label}' timed out "
            f"after {_get_bot_chat_delivery_timeout()}s (the bot's turn may "
            "still complete; raise cron.bot_chat_delivery_timeout_seconds if "
            "this recurs)")
    except Exception as e:
        return _fail(f"bot-chat delivery failed: {str(e) or type(e).__name__}", exc_info=True)
    finally:
        if query_file:
            with contextlib.suppress(OSError):
                os.unlink(query_file)


# ---------------------------------------------------------------------------
# WebUI delivery lane (kernel t_993b18df / t_ee4b2f97 / t_70dd9cc7).
#
# Ported from the pre-decomposition cron/scheduler.py monolith into its new
# home during the v2026.9.11 rebase. The WebUI is a separate HTTP process
# from the gateway: "webui" targets deliver by POSTing the report to the
# WebUI's own /api/chat/start, which starts a server-side [CRON DELIVERY]
# agent turn in the exact session that created (or explicitly targeted)
# the job.
# ---------------------------------------------------------------------------

def _webui_delivery_base_url() -> str:
    """Base URL of this box's Hermes WebUI HTTP server (cron delivery lane).

    The WebUI HTTP server and the gateway share one container / network
    namespace in the standard deployment, so loopback is correct. Env
    overrides: HERMES_CRON_WEBUI_DELIVERY_URL wins outright; otherwise the
    WebUI's own HERMES_WEBUI_PORT (default 8787) on 127.0.0.1.
    """
    override = os.getenv("HERMES_CRON_WEBUI_DELIVERY_URL", "").strip()
    if override:
        return override.rstrip("/")
    port = os.getenv("HERMES_WEBUI_PORT", "").strip() or "8787"
    return f"http://127.0.0.1:{port}"


def _webui_login(base_url: str, timeout: float) -> tuple:
    """Authenticate to the WebUI HTTP API for a cron delivery (t_993b18df).

    Returns ``(headers, error)``. When no HERMES_WEBUI_PASSWORD is set the
    WebUI runs with auth disabled and the base header dict is returned (no
    cookie needed). With a password, POST /api/auth/login (public + and
    CSRF-exempt) and lift the session cookie off Set-Cookie.

    No CSRF token is required on the subsequent POST: the WebUI's CSRF gate
    only applies to browser-identified requests (those carrying
    Origin/Referer -- routes._check_csrf -> _is_browser_unsafe_request);
    this lane deliberately sends neither header, matching the WebUI's
    documented same-machine curl/API contract.
    """
    import urllib.error
    import urllib.request

    headers = {"Content-Type": "application/json"}
    password = os.getenv("HERMES_WEBUI_PASSWORD", "").strip()
    if not password:
        return headers, None
    req = urllib.request.Request(
        base_url + "/api/auth/login",
        data=json.dumps({"password": password}).encode("utf-8"),
        headers=dict(headers),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            set_cookie = resp.headers.get("Set-Cookie", "") or ""
    except urllib.error.HTTPError as e:
        return None, f"webui delivery login failed (HTTP {e.code})"
    except Exception as e:
        return None, f"webui delivery login failed: {e or type(e).__name__}"
    # Set-Cookie: name=value; Path=/; ...  ->  "name=value"
    first = set_cookie.split(";", 1)[0].strip()
    if "=" not in first:
        return None, "webui delivery login returned no session cookie"
    out = dict(headers)
    out["Cookie"] = first
    return out, None


class _WebuiBusyError(str):
    """Busy-sentinel string (t_ee4b2f97).

    A plain ``str`` for every existing caller — equality, logging, slicing,
    ``last_delivery_error`` all behave identically — but isinstance-
    detectable so ``_deliver_result``'s webui branch can spool the report
    for persistent redelivery, and the tick-time redelivery loop can tell
    "session busy again" (retry later) from terminal HTTP failures (404/401/
    5xx — redelivery cannot fix those).
    """

    __slots__ = ()

    @property
    def busy(self) -> bool:
        return True


def _webui_busy_retries_env() -> int:
    try:
        return max(0, int(os.getenv("HERMES_CRON_WEBUI_BUSY_RETRIES", "6") or 6))
    except ValueError:
        return 6


def _webui_busy_delay_env() -> float:
    try:
        return float(os.getenv("HERMES_CRON_WEBUI_BUSY_RETRY_DELAY", "10") or 10)
    except ValueError:
        return 10.0


def _deliver_to_webui(
    job: dict, chat_id: str, content: str, *, busy_retries: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Optional[str]:
    """Deliver a cron report into a WebUI chat session (t_993b18df).

    ``webui`` targets are Hermes WebUI browser sessions: the WebUI stamps
    HERMES_SESSION_PLATFORM=webui / HERMES_SESSION_CHAT_ID=<webui session
    id> when it runs an agent turn, so a deliver=origin job created there
    carries origin {"platform": "webui"}. The WebUI is a separate HTTP
    process from the gateway (no Platform member, no adapter), so delivery
    POSTs the report to the WebUI's own ``POST /api/chat/start`` -- the
    same server-side-turn entrypoint delegate_task completions and kanban
    wakes use internally. The report lands as an automatic
    ``[CRON DELIVERY]`` agent turn in the exact session that created (or
    explicitly targeted) the job.

    Success = the turn started (HTTP 2xx from chat/start, which returns as
    soon as the agent worker is spawned). A 409 (the session already has an
    active stream -- e.g. the user is mid-turn) is retried with a delay;
    if the session stays busy the failure is returned honestly so
    last_delivery_error says exactly what happened. Media attachments are
    not forwarded on this lane (text reports only; the session can read any
    absolute paths the report references).

    Env knobs: HERMES_CRON_WEBUI_DELIVERY_URL (base URL override),
    HERMES_CRON_WEBUI_DELIVERY_TIMEOUT (per-request seconds, default 30),
    HERMES_CRON_WEBUI_BUSY_RETRIES (409 retries after the first attempt,
    default 6), HERMES_CRON_WEBUI_BUSY_RETRY_DELAY (seconds between
    retries, default 10).
    """
    import urllib.error
    import urllib.request

    job_id = job.get("id", "?")
    sid = str(chat_id)
    base_url = _webui_delivery_base_url()
    if timeout is None:
        try:
            timeout = float(os.getenv("HERMES_CRON_WEBUI_DELIVERY_TIMEOUT", "30") or 30)
        except ValueError:
            timeout = 30.0
    if busy_retries is None:
        busy_retries = _webui_busy_retries_env()
    else:
        busy_retries = max(0, int(busy_retries))
    busy_delay = _webui_busy_delay_env()

    auth_headers, auth_err = _webui_login(base_url, timeout)
    if auth_err:
        logger.warning("Job '%s': %s", job_id, auth_err)
        return auth_err

    task_name = job.get("name", job_id)
    message = (
        f"[CRON DELIVERY] Scheduled job '{task_name}' ({job_id}) finished - report below.\n\n"
        f"{content}\n\n"
        "(Automatic delivery of this cron job's output into the WebUI session "
        "that created it. Manage the job with cronjob(action='list').)"
    )
    body = json.dumps({"session_id": sid, "message": message}).encode("utf-8")

    attempts = busy_retries + 1
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(
            base_url + "/api/chat/start",
            data=body,
            headers=dict(auth_headers),
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.status
        except urllib.error.HTTPError as e:
            status = e.code
            if status == 409 and attempt < attempts:
                logger.info(
                    "Job '%s': webui session %s busy (active turn); retry %d/%d in %.0fs",
                    job_id, sid, attempt, busy_retries, busy_delay,
                )
                time.sleep(busy_delay)
                continue
            detail = ""
            try:
                detail = (e.read() or b"")[:200].decode("utf-8", "replace").strip()
            except Exception:
                pass
            if status == 409:
                # Busy-exhaust is the ONE failure a LATER tick can fix (the
                # user finishes their turn): return the sentinel so the
                # caller can spool for redelivery (t_ee4b2f97), while the
                # string itself stays the honest last_delivery_error.
                # t_70dd9cc7: neither this WARNING nor the sentinel makes a
                # scheduling claim — whether anything is actually scheduled
                # is decided by the CALLER (_deliver_result spools ONE-SHOTs
                # only; recurring jobs self-heal on their next fire). The
                # spool path's own "spooled for redelivery" WARNING carries
                # that claim when it is true.
                logger.warning(
                    "Job '%s': webui session %s stayed busy through %d attempt(s)",
                    job_id, sid, attempt,
                )
                return _WebuiBusyError(
                    f"webui delivery to session {sid} failed (HTTP 409"
                    + (f": {detail}" if detail else "")
                    + ") — session busy (active turn)"
                )
            msg = (
                f"webui delivery to session {sid} failed (HTTP {status}"
                + (f": {detail}" if detail else "")
                + ")"
            )
            logger.warning("Job '%s': %s", job_id, msg)
            return msg
        except Exception as e:
            msg = f"webui delivery to session {sid} failed: {e or type(e).__name__}"
            logger.warning("Job '%s': %s", job_id, msg)
            return msg
        # 2xx -- the server-side [CRON DELIVERY] turn started in the session.
        logger.info(
            "Job '%s': delivered to webui session %s via %s/api/chat/start (attempt %d)",
            job_id, sid, base_url, attempt,
        )
        return None
    # Defensive: the loop returns on every path. A fall-through here means
    # every attempt hit the 409-retry branch — the busy sentinel.
    return _WebuiBusyError(
        f"webui delivery to session {sid} failed: session stayed busy "
        "(active turn) through all in-fire retries"
    )


# ---------------------------------------------------------------------------
# Persistent webui redelivery spool (t_ee4b2f97)
#
# A one-shot cron firing while the target WebUI session is mid-turn loses its
# report for good once the in-fire 409 retries exhaust: the job has already
# consumed its single run. Recurring jobs self-heal on the next fire; one-shots
# do not. Instead of losing the report, spool it to disk and let every tick
# re-attempt delivery until the session goes idle (bounded by
# HERMES_CRON_WEBUI_REDELIVER_HORIZON, default 3600s). The spool is plain JSON
# on the same filesystem as jobs.json, so redelivery survives gateway restarts.
# ---------------------------------------------------------------------------

_WEBUI_PENDING_DIR = "webui_pending"


def _set_pending_webui_marker(job_id: str, marker) -> None:
    """Best-effort set/clear of the pending_webui_delivery marker on a job.

    Deliberately NOT update_job(): at fire time _deliver_result runs BEFORE
    mark_job_run, so the record still carries enabled=True/state="scheduled"
    with no next_run_at — update_job's reschedule guard then rejects the
    write ("one-shot time ... in the past") for exactly the jobs this
    marker describes. The marker is cosmetic (cronjob list visibility); the
    spool file is the actual redelivery state. Never raises.
    """
    try:
        from cron.jobs import _jobs_lock, load_jobs, save_jobs

        with _jobs_lock():
            jobs = load_jobs()
            for i, job in enumerate(jobs):
                if job.get("id") != job_id:
                    continue
                if marker is None:
                    job.pop("pending_webui_delivery", None)
                else:
                    job["pending_webui_delivery"] = marker
                jobs[i] = job
                save_jobs(jobs)
                return
    except Exception:
        pass


def _webui_redeliver_horizon_seconds() -> float:
    try:
        return float(
            os.getenv("HERMES_CRON_WEBUI_REDELIVER_HORIZON", "3600") or 3600
        )
    except ValueError:
        return 3600.0


def _webui_pending_dir() -> Path:
    from cron.jobs import get_cron_output_dir

    return get_cron_output_dir().parent / _WEBUI_PENDING_DIR


_WEBUI_SPOOL_RE = re.compile(
    r"^(?P<job>[0-9a-zA-Z_-]+)__(?P<sid>[0-9a-zA-Z_-]+)__(?P<ts>\d{8}T\d{6})\.json$"
)


def _spool_pending_webui_delivery(job: dict, chat_id: str, content: str, error: str) -> Optional[str]:
    """Persist a busy-failed webui report for redelivery on later ticks.

    Returns a description of the spool location on success (the caller folds
    it into the delivery error surfaced via ``last_delivery_error`` so the
    honest-reporting contract keeps holding), or None if spooling itself
    failed (the original busy error is then kept by the caller).
    """
    job_id = str(job.get("id") or "?")
    sid = str(chat_id)
    from cron.jobs import _hermes_now as _jobs_now  # type: ignore[attr-defined]
    now = _jobs_now()
    try:
        base = _webui_pending_dir()
        base.mkdir(parents=True, exist_ok=True)
        ts = now.strftime("%Y%m%dT%H%M%S")
        record = {
            "job_id": job_id,
            "name": job.get("name"),
            "session_id": sid,
            "message": content,
            "first_try": now.isoformat(),
            "attempts": 1,
            "error": error,
        }
        # Deterministic base name: job__sid__timestamp.json. A same-second
        # collision (two fires of the same job into the same session) gets
        # a numeric suffix rather than a clobber.
        fname = f"{job_id}__{sid}__{ts}.json"
        fpath = base / fname
        n = 1
        while fpath.exists():
            n += 1
            fpath = base / f"{job_id}__{sid}__{ts}_{n}.json"
        fpath.write_text(json.dumps(record, ensure_ascii=False, indent=1), encoding="utf-8")
        # Best-effort marker on the job record — lets `cronjob list` show
        # "pending redelivery" even before the next tick. Never fatal.
        _set_pending_webui_marker(job_id, {
            "session_id": sid,
            "spool_file": fpath.name,
            "first_try": record["first_try"],
            "attempts": 1,
        })
        logger.warning(
            "Job '%s': webui session %s busy — report spooled for redelivery "
            "(%s; horizon %.0fs)",
            job_id, sid, fpath.name, _webui_redeliver_horizon_seconds(),
        )
        return (
            f"webui delivery to session {sid} deferred: session busy; report "
            f"spooled as {fpath.name} and will be redelivered when the session "
            "goes idle"
        )
    except Exception as e:
        logger.error("Job '%s': failed to spool webui redelivery: %s", job_id, e)
        return None


def _load_pending_webui_spools() -> list:
    """Return (path, record) pairs for every valid pending webui spool file.

    Robust to foreign/malformed files (skips them) and to the directory not
    existing (idle system).
    """
    out = []
    try:
        base = _webui_pending_dir()
        if not base.is_dir():
            return out
        for entry in sorted(base.iterdir()):
            if not entry.name.endswith(".json"):
                continue
            try:
                if not _WEBUI_SPOOL_RE.match(entry.name):
                    continue
                record = json.loads(entry.read_text(encoding="utf-8"))
                if not isinstance(record, dict):
                    continue
                if not record.get("session_id") or not record.get("message"):
                    continue
                out.append((entry, record))
            except Exception:
                continue
    except Exception:
        pass
    return out


def _parse_spool_time(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _webui_redelivery_backoff_seconds(attempts: int) -> float:
    """Exponential backoff between spool redelivery attempts: 30s doubling to
    a 5-minute ceiling. Cheap single HTTP attempts, so the curve stays flat
    compared to the in-fire block."""
    return min(300.0, 30.0 * (2 ** max(0, attempts - 1)))


def _deliver_pending_webui_reports() -> int:
    """One redelivery pass over the pending-webui spool; called from tick().

    For each spool whose backoff window has elapsed: re-attempt delivery
    (single in-fire attempt — the point is to probe idleness cheaply); on
    success drop the spool + clear the job marker; on renewed 409 bump the
    attempt count and update next-try; past the horizon, give up honestly
    (rename to .gaveup, keep content, surface in last_delivery_error).

    Never raises: a broken spool directory must not take the ticker down.
    Returns the number of reports successfully redelivered.
    """
    delivered = 0
    try:
        horizon = _webui_redeliver_horizon_seconds()
        now = datetime.now(timezone.utc)
        for path, record in _load_pending_webui_spools():
            try:
                job_id = str(record.get("job_id") or "?")
                sid = str(record.get("session_id"))
                attempts = int(record.get("attempts") or 0) + 1
                first_try = _parse_spool_time(record.get("first_try"))
                last_try = _parse_spool_time(record.get("last_try"))
                # Due for another poke? The anchor is the LAST redelivery
                # poke (last_try). A fresh spool has none — the in-fire
                # retry block JUST ended, so the next tick pokes once
                # immediately, and backoff chains from each poke after that.
                anchor = last_try
                if anchor is not None:
                    due_at = anchor + timedelta(
                        seconds=_webui_redelivery_backoff_seconds(attempts)
                    )
                    if now < due_at:
                        continue
                # Horizon check: give up honestly, keep the content on disk.
                if first_try is not None and (now - first_try).total_seconds() > horizon:
                    gaveup = path.with_suffix(".gaveup")
                    try:
                        n = 1
                        while gaveup.exists():
                            n += 1
                            gaveup = path.with_name(
                                path.stem + f".gaveup.{n}" + path.suffix
                            )
                        path.rename(gaveup)
                    except Exception:
                        gaveup = path
                    _set_pending_webui_marker(job_id, None)
                    try:
                        from cron.jobs import update_job
                        update_job(job_id, {
                            "last_delivery_error": (
                                f"webui delivery to session {sid} abandoned: "
                                f"session stayed busy for over {int(horizon)}s "
                                f"(report preserved at {gaveup.name})"
                            ),
                        })
                    except Exception:
                        pass
                    logger.warning(
                        "Job '%s': webui redelivery to session %s gave up after "
                        "%.0fs busy; report preserved at %s",
                        job_id, sid, horizon, gaveup.name,
                    )
                    continue
                # Single cheap probe: one POST, no in-fire retry loop. Bounded
                # timeout (10s) so a wedged WebUI can't stall the tick thread.
                # Late-bound through ``_sched`` (monkeypatch contract parity
                # with the fire-time lane).
                job_stub = {"id": job_id, "name": record.get("name") or job_id}
                err = _sched._deliver_to_webui(
                    job_stub, sid, record.get("message") or "",
                    busy_retries=0, timeout=10.0,
                )
                if err is None:
                    delivered += 1
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
                    _set_pending_webui_marker(job_id, None)
                    try:
                        from cron.jobs import update_job
                        update_job(job_id, {"last_delivery_error": None})
                    except Exception:
                        pass
                    logger.info(
                        "Job '%s': deferred webui report redelivered to session "
                        "%s (attempt %d)",
                        job_id, sid, attempts,
                    )
                elif isinstance(err, _WebuiBusyError):
                    record["attempts"] = attempts
                    record["last_try"] = now.isoformat()
                    record["error"] = str(err)
                    try:
                        path.write_text(
                            json.dumps(record, ensure_ascii=False, indent=1),
                            encoding="utf-8",
                        )
                    except Exception:
                        pass
                else:
                    # Non-busy terminal errors (404 session gone, 401, 5xx):
                    # redelivery cannot fix them; keep the content on disk
                    # and surface honestly.
                    gaveup = path.with_suffix(".gaveup")
                    try:
                        n = 1
                        while gaveup.exists():
                            n += 1
                            gaveup = path.with_name(
                                path.stem + f".gaveup.{n}" + path.suffix
                            )
                        path.rename(gaveup)
                    except Exception:
                        gaveup = path
                    _set_pending_webui_marker(job_id, None)
                    try:
                        from cron.jobs import update_job
                        update_job(job_id, {
                            "last_delivery_error": f"{err} (report preserved at {gaveup.name})",
                        })
                    except Exception:
                        pass
                    logger.warning(
                        "Job '%s': webui redelivery to session %s failed "
                        "terminally: %s (report preserved at %s)",
                        job_id, sid, err, gaveup.name,
                    )
            except Exception:
                logger.debug("Pending webui spool %s processing failed", path, exc_info=True)
    except Exception:
        logger.debug("Pending webui redelivery pass failed", exc_info=True)
    return delivered


def _normalize_deliver_value(deliver) -> str:
    """Normalize ``deliver`` to its canonical comma-separated string; ``"local"`` when falsy.
    Lists/tuples (MCP clients, hand-edited jobs.json) are flattened — ``str(["telegram"])`` would
    yield ``"['telegram']"`` and fail resolution silently."""
    if deliver is None or deliver == "":
        return "local"
    if isinstance(deliver, (list, tuple)):
        parts = [str(p).strip() for p in deliver if str(p).strip()]
        return ",".join(parts) if parts else "local"
    return str(deliver)


# Routing tokens resolve at fire time (a job outlives platform wiring). ``all`` = platforms with a
# configured home chat_id (_expand_routing_tokens); ``bot-chat`` is NOT in ``all`` (costs a turn).
_ROUTING_TOKENS = frozenset({"all"})

# Pseudo-platform: deliver output as a real inbound turn into a profile's "Bot Chat" (not a mirror).
# ``bot-chat`` = own profile; ``bot-chat:<name>`` = named profile on THIS machine.
BOT_CHAT_PLATFORM = "bot-chat"

# t_993b18df: the WebUI pseudo-platform. The Hermes WebUI binds
# HERMES_SESSION_PLATFORM=webui / HERMES_SESSION_CHAT_ID=<webui session
# id> for browser chat sessions (api/streaming._build_agent_thread_env),
# so a deliver=origin job created there carries origin
# {"platform": "webui", "chat_id": <sid>}. "webui" is not a gateway
# Platform (no adapter, no credential): like BOT_CHAT_PLATFORM it delivers
# out-of-band -- see _deliver_to_webui (HTTP POST into the WebUI's own
# /api/chat/start, which starts a server-side [CRON DELIVERY] agent turn
# in the exact session that created the job).
WEBUI_DELIVERY_PLATFORM = "webui"

# WebUI session ids are bare hex strings (12 hex chars today; accept a
# slightly wider shape so the explicit webui:<session_id> target form
# keeps working if the id format ever changes).
_WEBUI_SESSION_ID_RE = re.compile(r"^[0-9a-f]{4,64}$")


def parse_bot_chat_deliver_token(part: str) -> Optional[str]:
    """``bot-chat[:<name>]`` → ``""`` (own profile), the name, or ``None`` if not a bot-chat
    token. Token is case-insensitive; the name is normalized later by the profile layer."""
    raw = (part or "").strip()
    lowered = raw.lower()
    if lowered == BOT_CHAT_PLATFORM:
        return ""
    prefix = BOT_CHAT_PLATFORM + ":"
    if lowered.startswith(prefix):
        return raw[len(prefix):].strip()
    return None


def _resolve_bot_chat_target(job: dict, profile_arg: str) -> Optional[dict]:
    """Resolve a bot-chat token to a delivery target. ``""`` = own profile (no ``-p`` needed);
    otherwise the profile must exist locally — cross-machine delivery is intentionally unsupported
    so same-named profiles on other gateways can never be targeted by accident."""
    if not profile_arg:
        return {"platform": BOT_CHAT_PLATFORM, "chat_id": "", "thread_id": None}
    try:
        from hermes_cli.profiles import normalize_profile_name, profile_exists
        canon = normalize_profile_name(profile_arg)
        if not profile_exists(canon):
            logger.warning(
                "Job '%s': bot-chat delivery profile '%s' not found on this "
                "machine — skipping target",
                job.get("id", "?"), profile_arg)
            return None
        return {"platform": BOT_CHAT_PLATFORM, "chat_id": canon, "thread_id": None}
    except Exception:
        logger.warning(
            "Job '%s': failed to resolve bot-chat profile '%s'", job.get("id", "?"), profile_arg,
            exc_info=True,
        )
        return None


def _expand_routing_tokens(part: str) -> List[str]:
    """Expand ``all`` to every home-target platform with a configured chat_id; non-tokens pass
    through as a single-element list."""
    if part.lower() not in _ROUTING_TOKENS:
        return [part]
    return [p for p in _iter_home_target_platforms() if _get_home_target_chat_id(p)]


def _delivery_lane_value(job: dict, *, for_failure: bool = False):
    """Raw deliver-lane value for a run outcome: the failure lane when ``for_failure`` and the job
    overrides it, else ``deliver``. Bookkeeping (outcome classification, unresolved-origin, incident
    'alerted' marking) must read the SAME lane the notice was routed through (NS-788)."""
    if for_failure:
        failure_deliver = job.get("failure_deliver")
        if failure_deliver is not None and str(failure_deliver).strip():
            return failure_deliver
    return job.get("deliver", "local")


def _resolve_delivery_targets(job: dict, *, for_failure: bool = False) -> List[dict]:
    """Resolve auto-delivery targets from comma-separated ``deliver``; ``all`` expands to every
    platform with a home channel and combines with explicit targets. Dedup by (platform, chat_id,
    thread_id). ``for_failure=True`` (failure summaries, interrupted-run notices, drift/preflight
    alerts) resolves from ``failure_deliver`` INSTEAD when the job carries one —
    ``failure_deliver: local`` is the structural opt-out; absent, failures follow ``deliver``."""
    deliver = _normalize_deliver_value(_delivery_lane_value(job, for_failure=for_failure))
    if deliver == "local":
        return []

    seen = {}
    targets = []
    for raw in deliver.split(","):
        raw = raw.strip()
        if not raw:
            continue
        from_broadcast = raw.lower() in _ROUTING_TOKENS
        for part in _expand_routing_tokens(raw):
            target = _resolve_single_delivery_target(job, part, from_broadcast=from_broadcast)
            if not target:
                continue
            key = (target["platform"].lower(), str(target["chat_id"]), target.get("thread_id"))
            kept = seen.get(key)
            if kept is None:
                seen[key] = target
                targets.append(target)
            elif (
                # Keep origin/origin_fallback/home provenance regardless of broadcast token order.
                _MIRROR_PROVENANCE_RANK.get(str(target.get("_resolved_from") or ""), 0)
                > _MIRROR_PROVENANCE_RANK.get(str(kept.get("_resolved_from") or ""), 0)
            ):
                kept["_resolved_from"] = target.get("_resolved_from")
    return targets


def _resolve_delivery_target(job: dict) -> Optional[dict]:
    """Resolve the concrete auto-delivery target for a cron job, if any."""
    targets = _resolve_delivery_targets(job)
    return targets[0] if targets else None


# Audio routing is centralized in gateway.platforms.base.should_send_media_as_audio().
_VIDEO_EXTS = frozenset({'.mp4', '.mov', '.avi', '.mkv', '.webm', '.3gp'})
_IMAGE_EXTS = frozenset({'.jpg', '.jpeg', '.png', '.webp', '.gif'})


def _send_media_via_adapter(
    adapter, chat_id: str, media_files: list, metadata: dict | None, loop, job: dict, platform=None,
) -> list:
    """Send MEDIA files as native attachments (routed by extension, as in
    _process_message_background). Returns per-file error strings so a dropped attachment surfaces
    in run status, not just the gateway log."""
    from gateway.platforms.base import (
        BasePlatformAdapter, should_send_media_as_audio, validate_media_delivery_path)
    from agent.async_utils import safe_schedule_threadsafe
    job_ref = {"id": job.get("id", "?")}
    errors: list = []
    requested = [(str(p), v) for p, v in (media_files or [])]
    media_files = BasePlatformAdapter.filter_media_delivery_paths(media_files)
    # Report paths the safety filter dropped (missing file, denied prefix, strict-mode miss).
    kept = {p for p, _ in media_files}
    for raw_path, _v in requested:
        try:
            dropped = validate_media_delivery_path(raw_path) not in kept
        except Exception:
            dropped = True
        if dropped:
            errors.append(f"attachment dropped by media path policy: {raw_path}")

    route_platform = platform if platform is not None else getattr(adapter, "platform", None)
    for media_path, _is_voice in media_files:
        try:
            ext = _sched.Path(media_path).suffix.lower()
            if should_send_media_as_audio(route_platform, ext, is_voice=_is_voice):
                method, path_kw = "send_voice", "audio_path"
            elif ext in _VIDEO_EXTS:
                method, path_kw = "send_video", "video_path"
            elif ext in _IMAGE_EXTS:
                method, path_kw = "send_image_file", "image_path"
            else:
                method, path_kw = "send_document", "file_path"
            coro = getattr(adapter, method)(
                chat_id=chat_id, metadata=metadata, **{path_kw: media_path})
            future = safe_schedule_threadsafe(coro, loop)
            if future is None:
                _note_target_error(
                    job_ref, f"cannot send media {media_path}: gateway loop unavailable", errors)
                return errors
            try:
                # Large attachments can exceed 30s; configurable via _get_media_send_timeout().
                result = future.result(timeout=_script._get_media_send_timeout())
            except TimeoutError:
                future.cancel()
                raise
            if result and not getattr(result, "success", True):
                _note_target_error(
                    job_ref,
                    f"media send failed for {media_path}: {getattr(result, 'error', 'unknown')}",
                    errors,
                )
        except Exception as e:
            # TimeoutError etc. have an empty str(); fall back to the class name.
            _note_target_error(
                job_ref, f"failed to send media {media_path}: {str(e) or type(e).__name__}", errors)
    return errors


def _result_field(send_result, key: str, default=None):
    """Read ``key`` from a SendResult-like object or the plain dict the silence filter returns."""
    if isinstance(send_result, dict):
        return send_result.get(key, default)
    return getattr(send_result, key, default)


def _confirm_adapter_delivery(
    send_result, job_id: str = "?", unverified: Optional[list] = None) -> bool:
    """Return True only if ``send_result`` unambiguously confirms delivery. ``None`` or no
    ``success`` attr/key is NOT success (would log "delivered" while nothing was sent).
    ``delivered is False`` REJECTS even with truthy ``success`` (the silence-narration filter
    returns ``{"success": True, "delivered": False}``). No ``message_id``/``raw_response`` is still
    accepted (some adapters return a bare success) but logged at WARNING as UNVERIFIED.

    A live adapter that returns ``None`` (e.g. a swallowed exception, a busy platform, or a code path that
    returns early without producing a ``SendResult``) must NOT be treated as success — doing so causes the
    scheduler to log ``"delivered to <chat> via live adapter"`` while the gateway never actually sees the
    message (#47056).
    * No ``message_id`` and no ``raw_response`` means we have no positive evidence of a send. Telegram
    ``SendResult`` objects carry ``message_id``; the dict-filter shape does not. See #77763.
    """
    if send_result is None:
        return False
    if isinstance(send_result, dict):
        has_success = "success" in send_result
    else:
        has_success = hasattr(send_result, "success")
    if not has_success or not bool(_result_field(send_result, "success")):
        return False
    if _result_field(send_result, "delivered") is False:
        return False
    if (
        _result_field(send_result, "message_id") is None
        and not _result_field(send_result, "raw_response")
    ):
        logger.warning(
            "Job '%s': live adapter reported success with no delivery evidence "
            "(no message_id, no raw_response) — treating as delivered but "
            "UNVERIFIED",
            job_id)
        if unverified is not None:
            unverified.append(True)
    return True


def _is_channel_dm_topic(runtime_adapter: Any, chat_id: Any, loop: Any, job_id: str) -> bool:
    """Is an ambiguous ``telegram:<positive_chat_id>:<numeric_thread_id>`` target a channel
    Direct-Messages topic (``direct_messages_topic_id``) rather than a private-chat forum topic
    (``message_thread_id``)? Shape cannot decide; signal is ``get_chat_info`` type == ``channel``.
    Fails SAFE to False (thread routing) without a probe or on any probe error/timeout.

    Callers gate this on the ambiguous shape first (``telegram:<positive_chat_id>:<numeric_thread_id>``) —
    that shape is identical for both cases, so shape alone cannot decide (this was the #52060 regression).
    Probe the live adapter's ``get_chat_info`` once and only return True when the chat is a channel.
    See #22773.
    """
    # Resolve on the CLASS, not the instance: a MagicMock instance auto-creates a truthy
    # ``get_chat_info``, so an instance-level probe would misclassify test doubles.
    get_chat_info = getattr(type(runtime_adapter), "get_chat_info", None)
    if not callable(get_chat_info):
        return False
    try:
        from agent.async_utils import safe_schedule_threadsafe
        coro = get_chat_info(runtime_adapter, str(chat_id))
        future = safe_schedule_threadsafe(coro, loop)  # type: ignore[arg-type]
        if future is None:
            return False
        # Metadata-only call, so a shorter bound than the send waits is intentional.
        info = future.result(timeout=10)
    except Exception:
        logger.debug(
            "Job '%s': get_chat_info probe failed for chat=%s — "
            "defaulting to message_thread_id routing",
            job_id, chat_id, exc_info=True)
        return False
    is_channel = isinstance(info, dict) and str(info.get("type") or "").lower() == "channel"
    if is_channel:
        logger.info(
            "Job '%s': chat=%s is a channel — routing via direct_messages_topic_id",
            job_id, chat_id)
    return is_channel


def _cron_delivery_notify_enabled(cfg: Optional[dict]) -> bool:
    """Resolve ``cron.delivery.notify`` (default True). Only an explicit ``False`` disables; a
    missing/malformed section keeps the default so a typo cannot silently mute briefs."""
    try:
        cron_cfg = (cfg or {}).get("cron")
        delivery_cfg = cron_cfg.get("delivery") if isinstance(cron_cfg, dict) else None
        return not isinstance(delivery_cfg, dict) or delivery_cfg.get("notify", True) is not False
    except Exception:
        return True


def _record_delivery_verification(job: dict, unverified_targets: list) -> None:
    """Persist ``last_delivery_unverified``: list of ``platform:chat_id`` targets acked with no
    evidence, or None, alongside queued Bot Chat receipts. Never raises (bookkeeping must not fail a
    delivery)."""
    new_value = list(unverified_targets) or None
    queued = {target: receipt for target, receipt in
              job.get("_bot_chat_delivery_receipts", {}).items()
              if receipt["status"] in ("queued", "claimed")} or None
    values = {key: value for key, value in {
        "last_delivery_unverified": new_value, "last_delivery_queued": queued,
    }.items() if (job.get(key) or None) != value}
    if not values:
        return
    job.update(values)
    try:
        from cron.jobs import update_job
        update_job(job["id"], values)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Job '%s': could not record delivery verification: %s", job.get("id"), exc)


@dataclass
class _TargetDelivery:
    """Per-target delivery state shared by the live-adapter and standalone lanes."""

    job: dict
    platform: Any
    platform_name: str
    chat_id: str
    thread_id: Optional[str]
    transport: Any
    pconfig: Any
    runtime_adapter: Any
    target_adapters: Any
    config: Any
    loop: Any
    notify_delivery: bool
    origin: dict
    origin_target: bool
    origin_user_id: Optional[str]
    is_dm_target: bool
    mirror_text: str
    mirror_this_target: bool
    in_channel_surface: bool
    inchannel_continuable: bool
    opened_thread_id: Optional[str]
    live_adapter_ready: bool = False

    @property
    def is_relay(self) -> bool:
        return self.transport is not None and self.transport.is_relay

    @property
    def where(self) -> str:
        return f"{self.platform_name}:{self.chat_id}"


def _note_target_error(job: dict, msg: str, errors: list) -> None:
    """Log a per-target delivery failure as a WARNING and record it in ``errors``."""
    logger.warning("Job '%s': %s", job["id"], msg)
    errors.append(msg)


def _warn_live_lane_failure(job: dict, msg: str, is_relay: bool) -> None:
    """Relay targets have no standalone fallback, so the log line must not promise one."""
    if is_relay:
        logger.warning("Job '%s': %s", job["id"], msg)
    else:
        logger.warning("Job '%s': %s, falling back to standalone", job["id"], msg)


def _resolve_target_transport(
    job: dict, platform, platform_name: str, target: dict, adapters, config):
    """Resolve ``(transport, pconfig, runtime_adapter, target_adapters)`` for one target, or
    ``(None, error)`` when it cannot be served (relay-fronted with no live transport, or not
    configured/enabled)."""
    from gateway.delivery import resolve_delivery_transport
    target_adapters = adapters
    if isinstance(adapters, _preflight.SharedRouteAdapters):
        # Credentialless satellite: the primary adapter serves THIS target only when an exact
        # primary route maps it to this profile; a miss fails closed below.
        # See #101113.
        shared = adapters.get(platform, target)
        target_adapters = {platform: shared} if shared is not None else {}
    transport = resolve_delivery_transport(platform, config, target_adapters)
    if transport is not None:
        pconfig = transport.config
        runtime_adapter = transport.adapter
    else:
        # Relay-fronted platforms have NO standalone fallback (the connector owns the credential),
        # so surface that instead of the native configured/enabled gate, which misdiagnoses them.
        from gateway.relay import relay_fronted_platforms
        if platform_name in relay_fronted_platforms():
            return None, (
                f"platform '{platform_name}' is relay-fronted and has no "
                "live gateway transport; start the gateway (its ticker "
                "owns relay-fronted delivery and will fire the job on "
                "schedule)"
            )
        pconfig = config.platforms.get(platform)
        runtime_adapter = None

    if transport is not None and transport.is_relay:
        # Relay transport carries the RELAY adapter's config (enablement already checked). The
        # logical platform is deliberately NOT natively enabled, so the native gate must not apply.
        if pconfig is None:
            from gateway.config import PlatformConfig
            pconfig = PlatformConfig(enabled=True)
    elif not pconfig or not pconfig.enabled:
        return None, f"platform '{platform_name}' not configured/enabled"
    return (transport, pconfig, runtime_adapter, target_adapters), None


def _inchannel_surface_supported(runtime_adapter, platform_name: str) -> bool:
    """D6 probe: can this adapter deliver a continuable in_channel brief on ``platform_name``?
    Per-platform check first (one RelayAdapter fronts N platforms; the scalar attr only carries
    the PRIMARY identity's bit); native adapters use the class attribute."""
    per_platform_check = getattr(
        runtime_adapter, "supports_inchannel_continuable_for_platform", None)
    if callable(per_platform_check):
        try:
            return bool(per_platform_check(platform_name))
        except Exception:
            return False
    return bool(getattr(runtime_adapter, "supports_inchannel_continuable", False))


def _live_route_metadata(t: _TargetDelivery) -> tuple[Optional[str], dict, dict]:
    """Compute ``(route_thread_id, route_metadata, media_metadata)`` for a live send, ONCE so text
    and media agree. ``telegram:<positive_chat_id>:<numeric_thread_id>`` is ambiguous (private
    forum topic vs channel DM topic need OPPOSITE routing) — see ``_is_channel_dm_topic``.
    ``thread_id`` rides in ``route_metadata`` to bypass the router's private-chat anchor rule."""
    from gateway.config import Platform
    from gateway.delivery import _looks_like_int, looks_like_telegram_private_chat_id
    job = t.job
    thread_id = t.thread_id
    is_ambiguous_telegram_topic = (
        t.platform == Platform.TELEGRAM
        and thread_id is not None
        and looks_like_telegram_private_chat_id(str(t.chat_id))
        and _looks_like_int(str(thread_id))
    )
    if is_ambiguous_telegram_topic and _is_channel_dm_topic(
        t.runtime_adapter, t.chat_id, t.loop, job["id"]):
        # Channel DM topic: direct_messages_topic_id, no bare thread_id; media mirrors text.
        # See #22773.
        route_thread_id = None
        route_metadata = {
            "direct_messages_topic_id": str(thread_id), "job_id": job["id"],
            "notify": t.notify_delivery,
        }
        media_metadata = {"direct_messages_topic_id": str(thread_id), "notify": t.notify_delivery}
    else:
        # Forum-style topic or non-topic target: message_thread_id.
        # Put thread_id in *route_metadata* (not just the DeliveryTarget) deliberately — the
        # DeliveryRouter's private-chat topic detection (gateway/delivery.py) demands a reply anchor when
        # thread_id is absent from metadata; cron deliveries have no inbound reply anchor, so the metadata
        # key bypasses that check and lets the adapter route via a plain message_thread_id. See #52060.
        route_thread_id = str(thread_id) if thread_id is not None else None
        route_metadata = {"job_id": job["id"], "notify": t.notify_delivery}
        if route_thread_id:
            route_metadata["thread_id"] = route_thread_id
        media_metadata = {"notify": t.notify_delivery}
        if thread_id:
            media_metadata["thread_id"] = thread_id

    # Relay egress needs metadata.scope_id (fail-closed tenant guard; scope cache is COLD after a
    # restart; router stamps HOME only). Origin targets only: a wrong fan-out scope is worse than
    # none.
    if t.origin_target and t.origin.get("scope_id"):
        route_metadata.setdefault("scope_id", str(t.origin["scope_id"]))
        media_metadata.setdefault("scope_id", str(t.origin["scope_id"]))
    return route_thread_id, route_metadata, media_metadata


def _live_send_text(
    t: _TargetDelivery, text_to_send: str, route_thread_id: Optional[str], route_metadata: dict, *,
    target_errors: list, delivery_errors: list, unverified_targets: list,
) -> tuple[bool, bool, Any]:
    """Schedule the text send on the gateway loop; returns ``(adapter_ok, timed_out, message_id)``.
    Re-raises a real send error so the caller falls through to standalone."""
    from agent.async_utils import safe_schedule_threadsafe
    from gateway.delivery import DeliveryRouter, DeliveryTarget
    job = t.job
    router = DeliveryRouter(t.config, t.target_adapters)
    route_target = DeliveryTarget(
        platform=t.platform, chat_id=str(t.chat_id), thread_id=route_thread_id, is_explicit=True)
    # Thread routing goes via the target, not a bare metadata "thread_id": the router only applies
    # its Telegram DM-topic detection when thread_id/message_thread_id are absent from metadata.
    future = safe_schedule_threadsafe(
        router._deliver_to_platform(route_target, text_to_send, route_metadata), t.loop)
    if future is None:
        target_errors.append("live adapter event loop scheduling failed")
        return False, False, None
    try:
        send_result = future.result(timeout=60)
    except TimeoutError:
        # Slow confirmation != failure; future.cancel() disambiguates. False -> already in flight,
        # cannot be un-sent, standalone resend would DUPLICATE: assume delivered. True -> never
        # started (loop wedged): MUST fall through to standalone or it is silently dropped.
        if future.cancel():
            msg = f"live adapter send to {t.where} timed out before the coroutine was dispatched"
            logger.warning("Job '%s': %s, falling back to standalone", job["id"], msg)
            target_errors.append(msg)
            return False, False, None
        logger.warning(
            "Job '%s': live adapter send to %s:%s timed out "
            "after 60s; already dispatched (in flight), "
            "assuming delivered (skipping standalone fallback "
            "to avoid duplicate)",
            job["id"], t.platform_name, t.chat_id)
        return True, True, None
    except Exception as ex:
        # Real send error (not a slow confirmation): fall through to standalone.
        target_errors.append(f"live adapter send failed: {ex}")
        raise

    # _deliver_to_platform returns a SendResult, or a plain dict {"success": True, "delivered":
    # False, ...} when the silence-narration filter drops the message.
    send_raw_response = _result_field(send_result, "raw_response")
    delivered_message_id = _result_field(send_result, "message_id")
    _evidence_gap: list = []
    send_success = _confirm_adapter_delivery(send_result, job["id"], _evidence_gap)
    if send_success and _evidence_gap:
        unverified_targets.append(t.where)

    if not send_success:
        if send_result is None:
            err, shape = "no response from adapter", "None"
        elif isinstance(send_result, dict):
            # A filtered drop carries no "error" — name the filter instead of reporting "unknown".
            err = send_result.get("error") or send_result.get("filtered") or "unknown"
            shape = "dict"
        else:
            err, shape = getattr(send_result, "error", None), type(send_result).__name__
        msg = f"live adapter send to {t.where} returned unconfirmed result ({shape}, error={err})"
        _warn_live_lane_failure(job, msg, t.is_relay)
        target_errors.append(msg)
        return False, False, None
    if send_raw_response and t.thread_id and send_raw_response.get("thread_fallback"):
        requested_thread_id = send_raw_response.get("requested_thread_id") or t.thread_id
        _note_target_error(
            job,
            f"configured thread_id {requested_thread_id} for "
            f"{t.where} was not found; delivered without thread_id",
            delivery_errors)
    return True, False, delivered_message_id


def _live_send_media(
    t: _TargetDelivery, media_metadata: dict, media_files: list, delivery_errors: list) -> None:
    """Send extracted media as native attachments with the same routing as the text send."""
    routed_media_metadata = dict(media_metadata or {})
    if t.is_relay:
        routed_media_metadata["_relay_logical_platform"] = t.platform.value
        logical_home = t.config.get_home_channel(t.platform)
        if logical_home is not None and logical_home.chat_id == t.chat_id:
            if logical_home.user_id:
                routed_media_metadata["user_id"] = logical_home.user_id
            if logical_home.scope_id:
                routed_media_metadata["scope_id"] = logical_home.scope_id
    _media_errors = _send_media_via_adapter(
        t.runtime_adapter, t.chat_id, media_files, routed_media_metadata or None, t.loop, t.job,
        platform=t.platform,
    )
    # Surface per-file failures into run status: text delivered but attachment lost is not ok.
    for _me in _media_errors:
        delivery_errors.append(f"{_me} (target {t.where})")


def _seed_live_delivery_sessions(t: _TargetDelivery, delivered_message_id) -> None:
    """After a confirmed live send, seed continuation session(s) and run the generic mirror.
    Thread seeding is deferred here so open-succeeds/deliver-fails never seeds an unseen brief."""
    job = t.job
    origin = t.origin
    seed_kwargs = dict(
        chat_name=origin.get("chat_name"), is_dm=t.is_dm_target, scope_id=origin.get("scope_id"))
    thread_seeded = False
    inchannel_seeded = False
    if t.opened_thread_id:
        _seed_cron_thread_session(
            job, t.runtime_adapter, t.platform_name, t.chat_id, t.opened_thread_id, t.mirror_text,
            **seed_kwargs,
        )
        thread_seeded = True
    # in_channel: CREATE + seed the flat session (the mirror only APPENDS to an existing one). Same
    # `inchannel_continuable` gate as the flatten in _deliver_result (must not drift). Origin
    # seed without mirror opt-in; others only via _inchannel_seed_allowed (user-less seed = orphan).
    if t.in_channel_surface and t.inchannel_continuable and not thread_seeded:
        inchannel_seeded = _seed_cron_channel_session(
            job, t.runtime_adapter, t.platform_name, t.chat_id, t.mirror_text,
            user_id=t.origin_user_id, **seed_kwargs)
        if not inchannel_seeded:
            logger.warning(
                "Job '%s': in_channel seed did NOT land on %s:%s "
                "— a plain reply will not see this brief",
                job["id"], t.platform_name, t.chat_id)
        # Companion THREAD seed: a reply in the brief's own thread keys to (chat, thread=<ts>),
        # which the flat seed never touches. Seed it too so BOTH reply surfaces continue the job.
        if delivered_message_id:
            _seed_cron_thread_session(
                job, t.runtime_adapter, t.platform_name, t.chat_id, str(delivered_message_id),
                t.mirror_text,
                **seed_kwargs)
    elif t.in_channel_surface and not t.inchannel_continuable:
        logger.warning(
            "Job '%s': in_channel delivery to %s:%s is not a "
            "continuable target (origin=%s:%s thread=%s; not the "
            "origin conversation, and not a mirror-eligible "
            "fallback/opted-in target the seed can key) — seed "
            "skipped; the plain mirror below may still apply",
            job["id"], t.platform_name, t.chat_id,
            origin.get("platform"), origin.get("chat_id"), origin.get("thread_id"))
    _maybe_mirror_cron_delivery(
        job, t.platform_name, t.chat_id, t.mirror_text, thread_id=t.thread_id,
        user_id=t.origin_user_id,
        enabled=t.mirror_this_target and not thread_seeded and not inchannel_seeded)


def _deliver_via_live_adapter(
    t: _TargetDelivery, cleaned_text: str, media_files: list, *, target_errors: list,
    delivery_errors: list, unverified_targets: list,
) -> bool:
    """Deliver one target via the live gateway adapter; True once delivered. ``target_errors`` =
    this lane's soft failures (surfaced only if standalone also fails); ``delivery_errors`` =
    partial failures (media, thread fallback) that surface even on success."""
    job = t.job
    route_thread_id, route_metadata, media_metadata = _live_route_metadata(t)
    delivered = False
    try:
        # Send cleaned text (MEDIA tags stripped) through the gateway's DeliveryRouter so it gets
        # the same platform routing as live messages (Telegram's three-mode topic routing).
        text_to_send = cleaned_text.strip()
        adapter_ok, timed_out, delivered_message_id = True, False, None
        if not text_to_send and not media_files:
            # Fail closed so the run reports the empty payload.
            _note_target_error(
                job, f"live adapter send skipped (empty text and no media) for {t.where}",
                target_errors)
            adapter_ok = False
        elif text_to_send:
            adapter_ok, timed_out, delivered_message_id = _live_send_text(
                t, text_to_send, route_thread_id, route_metadata,
                target_errors=target_errors, delivery_errors=delivery_errors,
                unverified_targets=unverified_targets,
            )

        # Media rides the same DM-topic-aware routing as text. Skipped after a confirmation
        # timeout (loop contended, text already assumed delivered) — record the drop instead.
        # Send extracted media files as native attachments via the live adapter, using the same
        # DM-topic-aware routing as the text send (#22773 — media previously used a bare thread_id and
        # landed in the General lane for private DM topics). Skip on an in-flight confirmation timeout: the
        # gateway loop is contended, so each media send would also block its 30s budget, and the text
        # payload is already assumed delivered (#38922). Record the skipped attachments so the drop is
        # visible rather than silently lost.
        if adapter_ok and not timed_out and media_files:
            _live_send_media(t, media_metadata, media_files, delivery_errors)
        elif timed_out and media_files:
            _note_target_error(
                job,
                f"{len(media_files)} media attachment(s) not delivered to "
                f"{t.where} (live adapter confirmation timed out)",
                delivery_errors)

        if adapter_ok:
            # Log WHERE it went: a ghost delivery in the wrong lane is otherwise indistinguishable.
            logger.info(
                # Log WHERE it went, not just that it went: a ghost delivery that landed in the wrong lane
                # (General topic instead of the routed thread) is indistinguishable from a real one without
                # the routing identity (#77763).
                "Job '%s': delivered to %s:%s via live adapter thread=%s message_id=%s",
                job["id"], t.platform_name, t.chat_id,
                route_thread_id if route_thread_id is not None else "-",
                delivered_message_id if delivered_message_id is not None else "-")
            delivered = True
            _seed_live_delivery_sessions(t, delivered_message_id)
    except Exception as e:
        err_msg = f"live adapter delivery to {t.where} failed: {e}"
        if not any(err_msg in err for err in target_errors):
            target_errors.append(err_msg)
        _warn_live_lane_failure(job, err_msg, t.is_relay)
    return delivered


def _standalone_send(
    t: _TargetDelivery, content: str, media_files: list) -> tuple[Any, Optional[str]]:
    """Run the standalone sender for one target: ``(result, None)`` or ``(None, error)`` (already
    logged — WARNING for a shutdown race, ERROR with traceback otherwise)."""
    from tools.send_message_tool import _send_to_platform
    job = t.job
    shutdown_msg = f"delivery to {t.where} skipped — interpreter is shutting down"

    def _send():
        return _send_to_platform(
            t.platform, t.pconfig, t.chat_id, content, thread_id=t.thread_id,
            media_files=media_files)

    def _warned(msg: str) -> tuple[None, str]:
        logger.warning("Job '%s': %s", job["id"], msg)
        return None, msg

    def _failed(e) -> tuple[None, str]:
        msg = f"delivery to {t.where} failed: {e}"
        logger.error("Job '%s': %s", job["id"], msg, exc_info=True)
        return None, msg

    # Interpreter finalizing (SIGTERM/restart/OOM): asyncio.run and a fresh ThreadPoolExecutor both
    # raise "cannot schedule new futures after interpreter shutdown" — warn, not ERROR traceback.
    if _sched._interpreter_shutting_down():
        return _warned(shutdown_msg)
    # The live lane failed closed on an empty payload; standalone senders don't (Telegram returns
    # success=True for empty content WITHOUT an API call) — a phantom delivery would result.
    if not content.strip() and not media_files:
        return _warned(f"standalone send skipped (empty text and no media) for {t.where}")
    coro = _send()
    try:
        return asyncio.run(coro), None
    except RuntimeError as run_err:
        # asyncio.run() refuses inside a running loop; close the unstarted coro, retry in a thread.
        coro.close()
        if _sched._interpreter_shutting_down(run_err):
            return _warned(shutdown_msg)
        # The fallback can itself raise (SMTP, result timeout); catch it or remaining targets skip.
        try:
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                # A fresh thread does NOT inherit the profile ContextVars (home override + secret
                # scope); run in the active context or the sender reads the default bot token.
                return pool.submit(contextvars.copy_context().run, asyncio.run, _send()).result(
                    timeout=30), None
            finally:
                pool.shutdown(wait=False)
        except Exception as e:
            if _sched._interpreter_shutting_down(e):
                return _warned(shutdown_msg)
            return _failed(e)
    except Exception as e:
        return _failed(e)


def _deliver_standalone(
    t: _TargetDelivery, content: str, media_files: list, target_errors: list, delivery_errors: list,
) -> None:
    """Standalone fallback for a target the live lane did not deliver."""
    job = t.job
    if t.is_relay:
        # Relay owns the destination and credential; a native retry could duplicate — fail closed.
        if not target_errors:
            target_errors.append(f"relay delivery to {t.where} failed")
        delivery_errors.extend(target_errors)
        return
    result, err = _standalone_send(t, content, media_files)
    if err is None and result and result.get("error"):
        # Not inside an except block — the error comes from the result dict, no traceback.
        err = f"delivery error: {result['error']} (target {t.where})"
        logger.error("Job '%s': %s", job["id"], err)
    if err is not None:
        target_errors.append(err)
        delivery_errors.extend(target_errors)
        return
    # Standalone senders report per-file attachment failures in ``warnings`` while returning
    # success; surface them so a vanished attachment doesn't mark the run ok.
    for _w in (result.get("warnings") if isinstance(result, dict) else None) or []:
        msg = f"delivery warning: {_w} (target {t.where})"
        logger.error("Job '%s': %s", job["id"], msg)
        delivery_errors.append(msg)
    logger.info("Job '%s': delivered to %s:%s", job["id"], t.platform_name, t.chat_id)
    # Thread seeding only happens on the live lane, so no thread_seeded gate applies here.
    _maybe_mirror_cron_delivery(
        job, t.platform_name, t.chat_id, t.mirror_text, thread_id=t.thread_id,
        user_id=t.origin_user_id,
        enabled=t.mirror_this_target)


def _prepare_target_delivery(
    job: dict, target: dict, *, adapters, loop, config, notify_delivery: bool, mirror_enabled: bool,
    mirror_text: str, delivery_errors: list,
) -> Optional[_TargetDelivery]:
    """Per-target prologue of ``_deliver_result``: origin/mirror/in_channel gates, transport
    resolution, continuable-thread open. None (error noted in ``delivery_errors``) if unservable."""
    from gateway.config import Platform
    platform_name = target["platform"]
    chat_id = target["chat_id"]
    thread_id = target.get("thread_id")

    origin = _resolve_origin(job) or {}
    origin_thread = origin.get("thread_id")
    if origin_thread and not thread_id:
        logger.warning(
            "Job '%s': origin has thread_id=%s but delivery target lost it (deliver=%s, target=%s)",
            job["id"], origin_thread, job.get("deliver", "local"), target)
    elif thread_id:
        logger.debug(
            "Job '%s': delivering to %s:%s thread_id=%s",
            job["id"], platform_name, chat_id, thread_id)

    # Mirror: origin, origin-less home fallback, user-written home, or explicit-target opt-in.
    origin_target = _target_matches_origin(origin, platform_name, chat_id, thread_id)
    mirror_this_target = mirror_enabled and _target_mirror_eligible(
        job, target, global_mirror=mirror_enabled, origin_match=origin_target)
    # Resolved for ANY origin match (not just mirror-enabled): the in_channel seed needs it too.
    origin_user_id = origin.get("user_id") if origin_target else None

    # DM shape for BOTH the flatten gate and seed chat_type (Slack DM ids start with "D").
    origin_chat_type = str(origin.get("chat_type") or "").lower()
    is_dm_target = origin_chat_type == "dm" or (
        not origin_chat_type and str(chat_id).startswith("D"))

    # in_channel gate shared by thread-flatten and flat seed — they MUST match or brief and
    # session land in different places. Origin qualifies unconditionally; others only when the
    # seed can create a resolvable session (_inchannel_seed_allowed).
    inchannel_continuable = origin_target or (
        mirror_this_target and _inchannel_seed_allowed(is_dm=is_dm_target, user_id=origin_user_id))

    # Plugin platform names create dynamic members via Platform._missing_().
    try:
        platform = Platform(platform_name.lower())
    except (ValueError, KeyError):
        _note_target_error(job, f"unknown platform '{platform_name}'", delivery_errors)
        return None

    resolved, resolve_err = _resolve_target_transport(
        job, platform, platform_name, target, adapters, config)
    if resolved is None:
        _note_target_error(job, resolve_err, delivery_errors)
        return None
    transport, pconfig, runtime_adapter, target_adapters = resolved

    # Live send needs a RUNNING loop, not just an adapter. Computed ONCE so the in_channel
    # thread_id clear below stays in lockstep with the seed (standalone cannot seed flat).
    live_adapter_ready = (
        runtime_adapter is not None
        and loop is not None
        and getattr(loop, "is_running", lambda: False)()
    )

    # Continuable surface (D1/D2/D6) from platform config ``extra``; default "thread".
    # ``in_channel`` delivers FLAT so a plain channel reply continues via the shared session
    # ``(platform, chat_id, None)``. Unsupported adapters fail SAFE to thread.
    in_channel_surface = _resolve_cron_surface_mode(pconfig, platform_name) == "in_channel"
    if (
        in_channel_surface
        and runtime_adapter is not None
        and not _inchannel_surface_supported(runtime_adapter, platform_name)
    ):
        logger.debug(
            "Job '%s': cron_continuable_surface=in_channel not supported on %s, using thread",
            job.get("id", "?"), platform_name)
        in_channel_surface = False
    if in_channel_surface and inchannel_continuable and live_adapter_ready:
        # Force flat (D2): an inherited thread_id would never match the flat seed (None). Gated
        # on `inchannel_continuable` (SAME gate as the seed) AND `live_adapter_ready` (fallback
        # never seeds). Stay AFTER mirror_this_target/origin_user_id (need ORIGINAL thread_id).
        thread_id = None

    # Thread-preferred continuable cron: open a DEDICATED thread; its session is seeded after a
    # successful send. DM-only platforms return None → mirror the origin DM. in_channel SKIPS
    # this: it posts flat and _seed_cron_channel_session CREATES the session.
    opened_thread_id: Optional[str] = None
    if (
        mirror_this_target
        and not in_channel_surface
        and runtime_adapter is not None
        and loop is not None
        and not thread_id  # never override an explicit origin thread/topic
    ):
        opened_thread_id = _open_continuable_cron_thread(
            job, runtime_adapter, chat_id, loop) or None
        if opened_thread_id:
            thread_id = opened_thread_id
    return _TargetDelivery(
        job=job, platform=platform, platform_name=platform_name, chat_id=chat_id,
        thread_id=thread_id, transport=transport, pconfig=pconfig, runtime_adapter=runtime_adapter,
        target_adapters=target_adapters, config=config, loop=loop, notify_delivery=notify_delivery,
        origin=origin, origin_target=origin_target, origin_user_id=origin_user_id,
        is_dm_target=is_dm_target, mirror_text=mirror_text, mirror_this_target=mirror_this_target,
        in_channel_surface=in_channel_surface, inchannel_continuable=inchannel_continuable,
        opened_thread_id=opened_thread_id, live_adapter_ready=live_adapter_ready)


def _unresolved_delivery_outcome(job: dict, for_failure: bool) -> Optional[str]:
    """``_deliver_result`` outcome when no target resolved: None (not a failure) for ``local`` and
    origin-less ``origin`` (CLI jobs never capture an origin — a spurious error every run), else
    an error string."""
    deliver_value = _normalize_deliver_value(_delivery_lane_value(job, for_failure=for_failure))
    if deliver_value == "local":
        return None
    if deliver_value == "origin":
        logger.info(
            # deliver=origin with no resolvable origin and no configured home channels: treat as local
            # rather than reporting an error. CLI-created jobs never capture a {platform, chat_id} origin,
            # so failing here would make every CLI `deliver=origin` (or auto-detect) job emit a spurious "no
            # delivery target resolved" error on every run (#43014). The output is still persisted in
            # last_output for `cron list`/resume.
            "Job '%s': deliver=origin but no origin or home channels — "
            "skipping delivery (output saved in last_output)",
            job.get("name", job.get("id", "?")))
        return None
    msg = f"no delivery target resolved for deliver={deliver_value}"
    logger.warning("Job '%s': %s", job["id"], msg)
    return msg


def _deliver_result(
    job: dict, content: str, adapters=None, loop=None, *, for_failure: bool = False
) -> Optional[str]:
    """Deliver job output to the configured target(s). With ``adapters``/``loop`` (gateway
    running) the live adapter is tried first (E2EE rooms can't use the standalone HTTP path), then
    standalone fallback. ``for_failure=True`` routes failure-category notices through the job's
    ``failure_deliver`` override when present (NS-788). Returns None on success, else an error."""
    job.pop("_bot_chat_delivery_receipts", None)
    targets = _resolve_delivery_targets(job, for_failure=for_failure)
    if not targets:
        _record_delivery_verification(job, [])
        return _unresolved_delivery_outcome(job, for_failure)

    # Restart-safe workers have no live gateway adapters: hand the send back through a durable
    # queue so the current or replacement gateway performs it with relay/E2EE parity. The execution
    # id is the idempotency key (the queue never retries an uncertain claimed send). Match on THIS
    # job's own attempt: a worker's script may dispatch another job in-process (`hermes cron run`),
    # and that nested delivery must not be keyed under the outer execution id.
    external_execution = os.environ.get("_HERMES_CRON_EXTERNAL_WORKER", "")
    if (external_execution and adapters is None
            and external_execution == str(job.get("execution_id") or "")
            and any(target["platform"] != BOT_CHAT_PLATFORM for target in targets)):
        from cron.delivery_queue import enqueue_and_wait

        _record_delivery_verification(job, [])
        error = enqueue_and_wait(external_execution, job, content, for_failure=for_failure)
        from cron.jobs import get_job
        refreshed = get_job(job["id"]) or {}
        job["last_delivery_queued"] = refreshed.get("last_delivery_queued")
        return error

    from gateway.config import load_gateway_config

    # Wrap with header/footer unless cron.wrap_response: false.
    wrap_response = True
    user_cfg = None
    with contextlib.suppress(Exception):
        user_cfg = _sched.load_config()
        wrap_response = user_cfg.get("cron", {}).get("wrap_response", True)
    # Mark live sends FINAL so the platform pushes them (Telegram "important" mode mutes otherwise).
    notify_delivery = _cron_delivery_notify_enabled(user_cfg)
    # Targets acked with NO evidence (bare SendResult(success=True) — Slack/Matrix/Mattermost);
    # persisted as ``last_delivery_unverified`` so `hermes cron list` shows it.
    unverified_targets: list = []
    if wrap_response:
        task_name = job.get("name", job["id"])
        delivery_content = (
            f"Cronjob Response: {task_name}\n"
            f"(job_id: {job.get('id', '')})\n"
            f"-------------\n\n"
            f"{content}\n\n"
            "To stop or manage this job, send me a new message "
            f"(e.g. \"stop reminder {task_name}\")."
        )
    else:
        delivery_content = content

    from gateway.platforms.base import BasePlatformAdapter
    # Bridge media-policy config into the env vars the path validator reads. The gateway does this
    # at boot; standalone runs (`hermes cron run`) did not, silently dropping files. Idempotent.
    from gateway.media_policy import apply_media_policy_env
    apply_media_policy_env(user_cfg)
    media_files, cleaned_delivery_content = BasePlatformAdapter.extract_media(delivery_content)
    requested_media = len(media_files)
    media_files = BasePlatformAdapter.filter_media_delivery_paths(media_files)
    # Policy-dropped attachments will never be sent on ANY lane — record them in run status.
    _policy_dropped = requested_media - len(media_files)
    policy_drop_errors = [
        f"{_policy_dropped} media attachment(s) dropped by media path "
        "policy (missing file, denied prefix, or strict-mode miss); "
        "see gateway.strict / media_delivery_allow_dirs in config.yaml"
    ] if _policy_dropped > 0 else []

    # Resolve the mirror gate ONCE (default off): successful deliveries are appended to the target
    # chat's session transcript. Mirror the CLEAN, unwrapped output (not the header/footer).
    try:
        mirror_enabled = _cron_mirror_delivery_enabled(job, user_cfg)
    except Exception:
        mirror_enabled = False
    # Independent of the mirror knob: continuable surfaces (in_channel) must seed even when
    # attach_to_session=false and cron.mirror_delivery=false, else the seed gets "" and fails.
    _, mirror_text = BasePlatformAdapter.extract_media(content)
    mirror_text = (mirror_text or "").strip()

    try:
        config = load_gateway_config()
    except Exception as e:
        msg = f"failed to load gateway config: {e}"
        logger.error("Job '%s': %s", job["id"], msg)
        return msg

    delivery_errors = []
    for target in targets:
        # Bot Chat owns admission; never concurrently resume a live owner's transcript.
        if target["platform"] == BOT_CHAT_PLATFORM:
            bot_chat_error = _deliver_to_bot_chat(job, content, target["chat_id"])
            if bot_chat_error:
                receipt_target = f"bot-chat:{target['chat_id'] or '(own)'}"
                receipt = job.get("_bot_chat_delivery_receipts", {}).get(receipt_target)
                if not receipt or receipt["status"] not in ("queued", "claimed"):
                    delivery_errors.append(bot_chat_error)
                if receipt and receipt["status"] == "ambiguous":
                    unverified_targets.append(bot_chat_error)
            continue

        # t_993b18df: WebUI-origin targets (origin.platform == "webui" from
        # a browser session, or an explicit webui:<session_id>) don't ride a
        # gateway adapter -- the WebUI is a separate HTTP process. Deliver by
        # starting a server-side agent turn in the target session. Handled
        # BEFORE the Platform enum below, which knows nothing about this
        # pseudo-platform (it used to fail every such target with
        # "unknown platform 'webui'" and the job's report was lost).
        # Late-bound through ``_sched`` so monkeypatching the
        # cron.scheduler namespace (tests, hotfixes) takes effect here too.
        if target["platform"] == WEBUI_DELIVERY_PLATFORM:
            webui_error = _sched._deliver_to_webui(job, str(target["chat_id"]), cleaned_delivery_content)
            if isinstance(webui_error, _WebuiBusyError):
                # t_ee4b2f97: the target session stayed busy through every
                # in-fire retry. For a one-shot (schedule.kind == "once" or
                # repeat.times == 1) the report would be LOST once this fire
                # ends — the job is terminal and will never fire again. Spool
                # the report and let every subsequent tick redeliver it when
                # the session goes idle. Recurring jobs still self-heal on
                # their next fire, so they keep the honest plain error.
                _is_oneshot = False
                _sched_shape = job.get("schedule") or {}
                _rep = job.get("repeat") or {}
                if _sched_shape.get("kind") == "once" or _rep.get("times") == 1:
                    _is_oneshot = True
                if _is_oneshot:
                    spooled = _spool_pending_webui_delivery(
                        job, str(target["chat_id"]), cleaned_delivery_content, webui_error
                    )
                    if spooled:
                        webui_error = spooled
                else:
                    # t_70dd9cc7: nothing is scheduled for a recurring job —
                    # its next fire retries delivery on its own — so say that
                    # instead of letting the operator infer a queued
                    # redelivery from the busy error above.
                    logger.info(
                        "Job '%s': recurring job busy-exhausted into webui "
                        "session %s; keeping the plain error — next fire "
                        "will retry delivery",
                        job["id"], target["chat_id"],
                    )
            if webui_error:
                delivery_errors.append(webui_error)
            continue

        t = _prepare_target_delivery(
            job, target, adapters=adapters, loop=loop, config=config,
            notify_delivery=notify_delivery,
            mirror_enabled=mirror_enabled, mirror_text=mirror_text, delivery_errors=delivery_errors)
        if t is None:
            continue
        target_errors: list = []
        delivered = t.live_adapter_ready and _deliver_via_live_adapter(
            t, cleaned_delivery_content, media_files,
            target_errors=target_errors, delivery_errors=delivery_errors,
            unverified_targets=unverified_targets,
        )
        if not delivered:
            _deliver_standalone(
                t, cleaned_delivery_content, media_files, target_errors, delivery_errors)

    # Filter-time drops apply to every target; report them once.
    delivery_errors.extend(policy_drop_errors)
    _record_delivery_verification(job, unverified_targets)
    return "; ".join(delivery_errors) if delivery_errors else None


# Late-bound origin namespace (see module docstring). Imported LAST so this module is fully
# populated before ``scheduler`` re-exports from it.
from cron import scheduler as _sched  # noqa: E402
from cron import scheduler_preflight as _preflight  # noqa: E402
from cron import scheduler_script as _script  # noqa: E402
