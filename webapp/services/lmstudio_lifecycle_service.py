"""Controlled LM Studio lifecycle policy for production queues."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from webapp.config import (
    LMSTUDIO_AUTO_RELOAD_ENABLED,
    LMSTUDIO_UNLOAD_AFTER_QUEUE,
    LMSTUDIO_UNLOAD_GRACE_SECONDS,
    LMSTUDIO_UNLOAD_MODEL_ALLOWLIST,
    LMSTUDIO_UNLOAD_MODEL_DENYLIST,
)
from webapp.services import model_control_service


logger = logging.getLogger(__name__)

IdleProbe = Callable[[], bool]

_IDLE_PROBES: dict[str, IdleProbe] = {}
_cleanup_task: asyncio.Task | None = None
_activity_generation = 0


def register_idle_probe(name: str, probe: IdleProbe) -> None:
    """Register a subsystem-level idle probe.

    The probe must return True only when its subsystem has no active or queued
    work that may still need the local Qwen model.
    """
    _IDLE_PROBES[name] = probe


def _format_model_control_error(result: dict[str, Any]) -> str:
    error = result.get("error")
    if isinstance(error, dict):
        return str(error.get("message") or error.get("type") or error)
    if error:
        return str(error)
    response = result.get("response")
    if isinstance(response, dict):
        nested = response.get("error")
        if isinstance(nested, dict):
            return str(nested.get("message") or nested.get("type") or nested)
        if nested:
            return str(nested)
    return "unknown model-control error"


def _all_idle() -> bool:
    for name, probe in _IDLE_PROBES.items():
        try:
            if not bool(probe()):
                return False
        except Exception as exc:
            logger.warning("LM Studio unload skipped: idle probe %s failed: %s", name, exc)
            return False
    return True


def _eligible_instances(status: dict[str, Any]) -> list[dict[str, Any]]:
    loaded = list(status.get("loaded_instances") or [])
    out: list[dict[str, Any]] = []
    for item in loaded:
        if not isinstance(item, dict):
            continue
        model_key = str(item.get("model_key") or "").strip()
        instance_id = str(item.get("instance_id") or "").strip()
        if not model_key or not instance_id:
            continue
        if model_key in LMSTUDIO_UNLOAD_MODEL_DENYLIST:
            continue
        if model_key not in LMSTUDIO_UNLOAD_MODEL_ALLOWLIST:
            continue
        out.append(item)
    return out


def note_activity(reason: str = "") -> None:
    """Cancel any pending post-queue cleanup because new work appeared."""
    global _activity_generation
    global _cleanup_task

    _activity_generation += 1
    task = _cleanup_task
    if task is not None and not task.done():
        task.cancel()
        logger.info(
            "LM Studio post-queue cleanup cancelled: %s",
            reason or "new activity detected",
        )


async def _run_post_queue_cleanup(reason: str, scheduled_generation: int) -> None:
    global _cleanup_task
    current_task = asyncio.current_task()

    try:
        await asyncio.sleep(max(0, int(LMSTUDIO_UNLOAD_GRACE_SECONDS)))
    except asyncio.CancelledError:
        return

    try:
        if scheduled_generation != _activity_generation:
            logger.info("LM Studio unload skipped: new jobs started during grace period")
            return
        if not _all_idle():
            logger.info("LM Studio unload skipped: active jobs still running")
            return

        status = await asyncio.to_thread(model_control_service.get_status)
        if not isinstance(status, dict):
            logger.warning("LM Studio unload failed: invalid status payload")
            return

        candidates = _eligible_instances(status)
        if not candidates:
            logger.info("LM Studio unload complete: no allowlisted models loaded")
            return

        unloaded_count = 0
        for item in candidates:
            if scheduled_generation != _activity_generation or not _all_idle():
                logger.info("LM Studio unload skipped: active jobs still running")
                return

            model_key = str(item.get("model_key") or "")
            instance_id = str(item.get("instance_id") or "")
            logger.info("LM Studio unloading model: %s", model_key)
            try:
                result = await asyncio.to_thread(
                    model_control_service.unload_instance,
                    instance_id=instance_id,
                )
            except Exception as exc:
                logger.warning("LM Studio unload failed: %s", exc)
                continue

            if result.get("ok"):
                unloaded_count += 1
                continue

            logger.warning(
                "LM Studio unload failed: %s",
                _format_model_control_error(result),
            )

        logger.info("LM Studio unload complete: %d instance(s) processed", unloaded_count)
    finally:
        if _cleanup_task is current_task:
            _cleanup_task = None


def schedule_post_queue_cleanup(reason: str = "queue drained") -> bool:
    """Best-effort cleanup after all queues become idle."""
    global _cleanup_task

    if not LMSTUDIO_UNLOAD_AFTER_QUEUE:
        return False

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False

    if not _all_idle():
        logger.info("LM Studio unload skipped: active jobs still running")
        return False

    if _cleanup_task is not None and not _cleanup_task.done():
        _cleanup_task.cancel()

    logger.info("LM Studio post-queue cleanup scheduled: %s", reason)
    _cleanup_task = loop.create_task(_run_post_queue_cleanup(reason, _activity_generation))
    return True


def auto_reload_enabled() -> bool:
    """Public accessor used by runtime code and tests."""
    return bool(LMSTUDIO_AUTO_RELOAD_ENABLED)


def _reset_state_for_tests() -> None:
    global _cleanup_task
    global _activity_generation

    if _cleanup_task is not None and not _cleanup_task.done():
        _cleanup_task.cancel()
    _cleanup_task = None
    _activity_generation = 0
    _IDLE_PROBES.clear()
