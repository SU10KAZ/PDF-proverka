"""Read-only projections for the distributed-computing operator UI.

The browser-facing screen must not reconstruct worker/job semantics from a
collection of low-level endpoints.  This module turns the existing durable
facts into a small, credential-free UI contract.  It deliberately has no
write path: workers, jobs, provider state and gateway sessions are only read,
and scheduler/management capabilities are reported as disabled.
"""
from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.services.distributed_workers import (
    central_handoff,
    database,
    gateway_repository,
    progress_service,
    provider_accounts,
    redaction,
    repositories,
    result_import,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


REAL_JOB_TYPE = "audit_pipeline_v1"
PROVIDERS = ("claude", "codex", "openrouter")
TERMINAL_EXECUTION_STATES = frozenset(
    {"completed", "failed", "cancelled", "superseded_result_received"}
)
ERROR_EXECUTION_STATES = frozenset(
    {"failed", "cancelled", "superseded_result_received"}
)
MANAGEMENT_UNAVAILABLE = (
    "Управление распределёнными заданиями появится на следующем этапе. "
    "Сейчас экран работает только на чтение."
)

_STAGE_LABELS = {
    "queued": "В очереди",
    "transfer": "Передача исходных данных",
    "preparing": "Подготовка",
    "auditing": "Проверка",
    "collecting": "Сбор результата",
    "returning": "Возврат результата",
    "importing": "Импорт на центральном узле",
    "done": "Готово",
    "error": "Ошибка",
}


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw or not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _percent(value: Any) -> Optional[float]:
    number = _finite_number(value)
    if number is None or number < 0 or number > 100:
        return None
    return round(number, 1)


def _iso(value: Any) -> Optional[str]:
    number = _finite_number(value)
    if number is None or number <= 0:
        return None
    try:
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _age_label(value: Any, *, now: float) -> str:
    number = _finite_number(value)
    if number is None or number <= 0:
        return "нет данных"
    seconds = max(0, int(now - number))
    if seconds < 10:
        return "сейчас"
    if seconds < 60:
        return f"{seconds} сек назад"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    return f"{hours // 24} д назад"


def _duration_label(seconds: Any) -> str:
    number = _finite_number(seconds)
    if number is None:
        return "—"
    total = max(0, int(number))
    if total < 60:
        return f"{total} сек"
    minutes = total // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours, remainder = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} ч {remainder} мин" if remainder else f"{hours} ч"
    days, hours = divmod(hours, 24)
    return f"{days} д {hours} ч" if hours else f"{days} д"


def _safe_text(value: Any, *, limit: int = 500) -> Optional[str]:
    if value is None:
        return None
    text = redaction.redact(str(value))[:limit].strip()
    return text or None


def _params(job: dict[str, Any]) -> dict[str, Any]:
    payload = _json_object(job.get("payload"))
    params = payload.get("params")
    return params if isinstance(params, dict) else {}


def is_business_audit(job: dict[str, Any]) -> bool:
    """True only for real audit history, excluding explicit test/canary work."""
    if str(job.get("job_type") or "") != REAL_JOB_TYPE:
        return False
    payload = _json_object(job.get("payload"))
    params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
    if str(params.get("action") or "").lower() == "provider_selfcheck":
        return False
    for source in (payload, params):
        if any(source.get(key) is True for key in (
            "synthetic", "is_synthetic", "canary", "is_canary", "test", "test_mode"
        )):
            return False
        if str(source.get("purpose") or "").strip().lower() in {
            "synthetic", "canary", "test", "selfcheck"
        }:
            return False
        tags = source.get("tags")
        if isinstance(tags, list) and {
            str(tag).strip().lower() for tag in tags
        }.intersection({"synthetic", "canary", "test", "selfcheck"}):
            return False
    return True


def human_stage(job: dict[str, Any]) -> str:
    """Map the two durable state axes to the nine UI stages."""
    execution = str(job.get("state") or "")
    overall = str(job.get("overall_state") or "")
    if overall == "needs_operator" or execution in ERROR_EXECUTION_STATES:
        return "error"

    handoff = central_handoff.current(job)
    if handoff is central_handoff.HandoffState.FAILED:
        return "error"

    import_applied = str(job.get("result_import_state") or "") == "applied"
    if overall == "completed":
        return "done"
    if import_applied and execution == "completed":
        return "done"
    if handoff is central_handoff.HandoffState.COMPLETED:
        return "done"
    if handoff is central_handoff.HandoffState.RESULT_IMPORTED:
        return "done"

    if handoff in {
        central_handoff.HandoffState.RESULT_IMPORTING,
        central_handoff.HandoffState.CENTRAL_RESUME_PENDING,
        central_handoff.HandoffState.CENTRAL_RESUME_RUNNING,
    }:
        return "importing"
    if handoff in {
        central_handoff.HandoffState.RESULT_RECEIVED,
        central_handoff.HandoffState.RESULT_VALIDATING,
        central_handoff.HandoffState.RESULT_VALIDATED,
    }:
        return "importing"

    return {
        "created": "queued",
        "assigned": "queued",
        "source_uploading": "transfer",
        "source_ready": "preparing",
        "accepted_by_worker": "preparing",
        "running": "auditing",
        "cancel_requested": "auditing",
        "completed_locally": "collecting",
        "result_uploading": "returning",
        "result_received": "importing",
        "validating": "importing",
        "completed": "importing",
    }.get(execution, "error")


def _finding_count_from_file(path: Path) -> Optional[int]:
    """Read only the canonical finding count; never guess it from other files."""
    try:
        if not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return None
    if isinstance(data, list):
        return len(data)
    if not isinstance(data, dict):
        return None
    for key in ("findings", "items"):
        rows = data.get(key)
        if isinstance(rows, list):
            return len(rows)
    meta = data.get("meta")
    total = meta.get("total_findings") if isinstance(meta, dict) else None
    if isinstance(total, int) and not isinstance(total, bool) and total >= 0:
        return total
    return None


def finding_count(job: dict[str, Any]) -> Optional[int]:
    """Resolve the same persisted project layout as the 12H result importer."""
    try:
        version_dir = result_import._resolve_version_dir(job)  # noqa: SLF001
    except Exception:  # noqa: BLE001 - absence/malformed project stays nullable
        return None
    candidates = (
        Path(version_dir) / "03_analysis" / "latest" / "03_findings.json",
        Path(version_dir) / "_output" / "03_findings.json",
        Path(version_dir) / "03_findings.json",
    )
    for candidate in candidates:
        count = _finding_count_from_file(candidate)
        if count is not None:
            return count
    return None


def _progress(job: dict[str, Any], *, stage: str, now: float) -> dict[str, Any]:
    if stage == "done":
        return {"progressPercent": 100.0, "progressKind": "exact"}
    if stage == "queued":
        return {"progressPercent": 0.0, "progressKind": "exact"}
    snapshot = _json_object(job.get("progress_snapshot"))
    view = progress_service.build_view(job, snapshot or None, now=now)
    if view.get("percent_reliable") and _percent(view.get("percent")) is not None:
        return {
            "progressPercent": _percent(view.get("percent")),
            "progressKind": "exact",
        }
    return {"progressPercent": None, "progressKind": "unavailable"}


def _last_activity_at(job: dict[str, Any]) -> Optional[float]:
    values = [
        _finite_number(job.get(key))
        for key in (
            "central_completed_at", "central_handoff_at", "validated_at", "returned_at",
            "completed_locally_at", "started_at", "accepted_at", "assigned_at", "created_at"
        )
    ]
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _error_fields(job: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    data = _json_object(job.get("error"))
    code = _safe_text(data.get("code") or data.get("error_code"), limit=96)
    message = _safe_text(
        data.get("message") or data.get("detail") or data.get("error"), limit=700
    )
    if not message and str(job.get("overall_state") or "") == "needs_operator":
        message = "Историческая попытка требует решения оператора."
    return code, message


def _task_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows[-8:]:
        state = str(row.get("to_state") or "")
        reason = _safe_text(row.get("reason"), limit=240)
        text = state
        if reason:
            text = f"{state}: {reason}"
        stamp = _finite_number(row.get("at"))
        result.append({
            "at": _iso(stamp),
            "text": text,
        })
    return result


def _task(
    job: dict[str, Any], *, transitions: list[dict[str, Any]], now: float
) -> dict[str, Any]:
    params = _params(job)
    stage = human_stage(job)
    progress = _progress(job, stage=stage, now=now)
    last_at = _last_activity_at(job)
    started = _finite_number(
        job.get("started_at") or job.get("assigned_at") or job.get("created_at")
    )
    finished = last_at if stage in {"done", "error"} else now
    duration_seconds = (
        max(0.0, finished - started)
        if started is not None and finished is not None
        else None
    )
    discipline = _safe_text(params.get("discipline_id"), limit=48) or "дисциплина не указана"
    version = _safe_text(job.get("version_id"), limit=80) or "версия не указана"
    overall = str(job.get("overall_state") or "")
    disposition = str(job.get("attempt_disposition") or "active")
    is_active = (
        overall == "active"
        and disposition == "active"
        and str(job.get("state") or "") not in TERMINAL_EXECUTION_STATES
        and stage != "done"
    )
    code, message = _error_fields(job)
    count = finding_count(job) if stage == "done" else None
    completed_at = (
        job.get("central_completed_at") or job.get("central_handoff_at")
        or job.get("validated_at") or job.get("returned_at")
    ) if stage == "done" else None
    priority = params.get("priority")
    if priority not in {"critical", "high", "normal", "low"}:
        priority = None
    result = {
        "id": str(job.get("job_id") or ""),
        "attemptId": str(job.get("attempt_id") or ""),
        "project": _safe_text(
            job.get("project_external_id") or job.get("project_id"), limit=240
        ) or "—",
        "projectDisplayName": _safe_text(job.get("project_display_name"), limit=240),
        "packageName": f"{discipline} / {version}",
        "discipline": discipline,
        "version": version,
        "workerId": _safe_text(job.get("assigned_worker_id"), limit=96),
        "mode": "distributed_audit",
        "progress": progress["progressPercent"],
        **progress,
        "duration": _duration_label(duration_seconds),
        "durationSeconds": duration_seconds,
        "lastActivity": _age_label(last_at, now=now),
        "lastActivityAt": _iso(last_at),
        "stage": stage,
        "stageLabel": _STAGE_LABELS[stage],
        "status": _STAGE_LABELS[stage],
        "executionState": str(job.get("state") or "unknown"),
        "centralHandoffState": central_handoff.current(job).value,
        "overallState": overall or "unknown",
        "isActive": is_active,
        "needsOperator": overall == "needs_operator",
        "historical": not is_active,
        "priority": priority,
        "pageCount": None,
        "blockCount": None,
        "packageSizeBytes": None,
        "findingCount": count,
        "findingCountSource": "03_findings.json" if count is not None else None,
        "result": f"{count} замечаний" if count is not None else "Количество замечаний недоступно",
        "completedAt": _age_label(completed_at, now=now) if completed_at else None,
        "completedAtIso": _iso(completed_at),
        "errorMessage": message,
        "technicalCode": code,
        "events": _task_events(transitions),
        "modelUsage": {
            "claude": "Нет достоверных данных",
            "codex": "Нет достоверных данных",
        },
    }
    return result


def _ratio_used(total: Any, available: Any) -> Optional[float]:
    total_number = _finite_number(total)
    available_number = _finite_number(available)
    if total_number is None or available_number is None or total_number <= 0:
        return None
    return _percent((total_number - available_number) / total_number * 100.0)


def _resource_view(snapshot: Any) -> dict[str, Any]:
    data = _json_object(snapshot)
    ram = data.get("ram") if isinstance(data.get("ram"), dict) else {}
    cpu = data.get("cpu") if isinstance(data.get("cpu"), dict) else {}
    disk = data.get("disk") if isinstance(data.get("disk"), dict) else {}
    disk_report = data.get("disk_report") if isinstance(data.get("disk_report"), dict) else {}
    disk_percent = _ratio_used(disk.get("total_gb"), disk.get("free_gb"))
    if disk_percent is None:
        total_bytes = _finite_number(disk_report.get("total_bytes"))
        used_bytes = _finite_number(disk_report.get("used_bytes"))
        if total_bytes and used_bytes is not None:
            disk_percent = _percent(used_bytes / total_bytes * 100.0)
    return {
        # Heartbeat exposes load averages, not CPU utilisation.
        "cpu": None,
        "cpuLoad1": _finite_number(cpu.get("la1")),
        "cpuLoad5": _finite_number(cpu.get("la5")),
        "cpuCores": _finite_number(cpu.get("cores")),
        "ram": _ratio_used(ram.get("total_gb"), ram.get("available_gb")),
        "ramTotalGb": _finite_number(ram.get("total_gb")),
        "ramAvailableGb": _finite_number(ram.get("available_gb")),
        "gpu": None,
        "vramUsedGb": None,
        "vramTotalGb": None,
        "disk": disk_percent,
        "diskTotalGb": _finite_number(disk.get("total_gb")),
        "diskFreeGb": _finite_number(disk.get("free_gb")),
        "telemetryAt": _iso(data.get("at")),
    }


def _provider_quota(
    state: Optional[dict[str, Any]], *, settings: DistributedWorkersSettings, now: float
) -> dict[str, Any]:
    if state is None:
        return {
            "availability": "unknown", "percentageRemaining": None,
            "resetAt": None, "resetIn": None, "status": "unknown",
            "source": None, "confidence": None, "isEstimated": False,
            "usedToday": None, "stale": True,
        }
    quota = state.get("quota") if isinstance(state.get("quota"), dict) else {}
    observed = _finite_number(quota.get("observed_at") or state.get("observed_at"))
    stale = observed is None or (now - observed) > max(60, int(settings.quota_stale_sec))
    quota_state = str(quota.get("quota_state") or state.get("quota_state") or "unknown")
    install = str(state.get("installation_status") or "unknown")
    auth = str(state.get("auth_state") or "unknown")
    policy = str(state.get("policy_state") or "unknown")
    inference_allowed = bool(state.get("inference_allowed"))
    blocked = (
        install in {"missing", "broken", "error"}
        or auth in {"logged_out", "expired", "auth_required", "error", "missing"}
        or policy not in {"allowed"}
        or not inference_allowed
        or quota_state in {"limited", "cooldown", "auth_required", "policy_blocked", "error"}
    )
    availability = "unavailable" if blocked else (
        "available" if install == "installed" and auth in {"logged_in", "ready"} else "unknown"
    )
    remaining = _percent(quota.get("estimated_remaining_pct"))
    reset = _finite_number(quota.get("next_reset_at"))
    status = "unknown"
    if blocked:
        status = "unavailable"
    elif stale:
        status = "stale"
    elif remaining is not None:
        status = "critical" if remaining < 15 else "warning" if remaining < 25 else "ok"
    elif quota_state == "ready":
        status = "ok"
    return {
        "availability": availability,
        "percentageRemaining": remaining,
        "resetAt": _iso(reset),
        "resetIn": _duration_label(reset - now) if reset is not None and reset >= now else None,
        "status": status,
        "quotaState": quota_state,
        "source": _safe_text(quota.get("source"), limit=80),
        "confidence": _safe_text(quota.get("confidence"), limit=32),
        "isEstimated": remaining is not None,
        # No existing heartbeat field means "usage today"; never derive it
        # from a rolling-window remainder.
        "usedToday": None,
        "stale": stale,
        "observedAt": _iso(observed),
    }


def _diagnostic(
    row: dict[str, Any], view: dict[str, Any], *, settings: DistributedWorkersSettings,
    now: float, jobs: list[dict[str, Any]],
) -> dict[str, Any]:
    worker_id = str(row.get("worker_id") or "")
    session = gateway_repository.get_transport_session(worker_id, settings=settings)
    active_connection = (session or {}).get("active_connection_id")
    transport = _safe_text((session or {}).get("transport_mode"), limit=40)
    connection_status = str(view.get("connection_status") or "offline")
    executor = view.get("executor") if isinstance(view.get("executor"), dict) else {}
    assigned = [job for job in jobs if job.get("assigned_worker_id") == worker_id]
    acknowledged = any(job.get("result_acknowledged_at") for job in assigned)
    return {
        "workerId": worker_id,
        "instanceId": _safe_text(row.get("instance_id"), limit=96),
        "transport": transport or "unavailable",
        "grpcStream": (
            "connected" if active_connection and connection_status == "online"
            else "disconnected" if session else "unavailable"
        ),
        "connectionId": _safe_text(active_connection, limit=128),
        # The registry has no persisted certificate-verification fact.
        "mtls": "unavailable",
        "heartbeat": _age_label(row.get("last_seen_at"), now=now),
        "gatewayTarget": None,
        "sourceHost": None,
        "resultHost": None,
        "nginx": "unavailable",
        "agentStatus": connection_status,
        "executorStatus": _safe_text(executor.get("status"), limit=32) or "unknown",
        "eventOutbox": {"lastWrittenSeq": None, "lastAckedSeq": None, "pending": None},
        "resultAck": "confirmed" if acknowledged else "unavailable",
        "workerVersion": _safe_text(row.get("worker_version"), limit=40),
        "runtimeVersion": _safe_text(executor.get("version"), limit=40),
        "uptime": None,
        "certExpiry": None,
    }


def _capacity(slot_view: dict[str, Any], row: dict[str, Any]) -> int:
    components = slot_view.get("limit_components") or {}
    facts = [
        components.get(key)
        for key in ("center_configured", "worker_configured", "max_verified")
        if isinstance(components.get(key), int)
    ]
    if facts:
        return max(0, min(facts))
    return max(0, int(row.get("configured_max_slots") or 0))


def _worker(
    row: dict[str, Any], *, settings: DistributedWorkersSettings, now: float,
    tasks: list[dict[str, Any]], provider_states: dict[tuple[str, str], dict[str, Any]],
    jobs: list[dict[str, Any]],
) -> dict[str, Any]:
    worker_id = str(row.get("worker_id") or "")
    copied = dict(row)
    copied["connection_status"] = worker_registry.compute_connectivity(
        row.get("last_seen_at"), settings=settings, now=now
    ).value
    usage = repositories.worker_slot_snapshot(worker_id, settings=settings)
    view = worker_registry.to_view(copied, now=now, usage=usage)
    slot_view = view.get("slots") or {}
    current_tasks = [
        task for task in tasks
        if task.get("workerId") == worker_id and task.get("isActive")
        and task.get("stage") != "queued"
    ]
    connectivity = str(view.get("connection_status") or "offline")
    status = "offline" if connectivity != "online" else ("busy" if current_tasks else "online")
    quotas = {
        provider: _provider_quota(
            provider_states.get((worker_id, provider)), settings=settings, now=now
        )
        for provider in PROVIDERS
    }
    resource = _resource_view(view.get("resource_snapshot"))
    diagnostic = _diagnostic(
        copied, view, settings=settings, now=now, jobs=jobs
    )
    total = _capacity(slot_view, copied)
    used = max(0, int(slot_view.get("reserved") or 0))
    free = max(0, int(slot_view.get("effective_free_slots") or 0))
    return {
        "id": worker_id,
        "name": _safe_text(view.get("display_name"), limit=160) or worker_id,
        "location": None,
        "status": status,
        "connectionStatus": connectivity,
        "workerState": _safe_text(view.get("worker_state"), limit=40),
        "lastHeartbeat": _age_label(view.get("last_seen_at"), now=now),
        "lastHeartbeatAt": _iso(view.get("last_seen_at")),
        "uptime": None,
        "resources": resource,
        "resourceTelemetryAvailable": any(
            resource.get(key) is not None for key in ("cpuLoad1", "ram", "disk")
        ),
        "slots": {
            "used": used,
            "total": total,
            "free": free,
            "effectiveTotal": int(slot_view.get("effective_limit") or 0),
            "binding": slot_view.get("limit_binding"),
            "mismatch": bool(slot_view.get("slot_count_mismatch")),
        },
        "quotas": {"claude": quotas["claude"], "codex": quotas["codex"]},
        "openRouter": {
            "status": quotas["openrouter"]["availability"],
            "usedToday": None,
        },
        "currentTasks": current_tasks,
        "acceptsNewTasks": bool(view.get("intake_enabled")),
        "quotaDataStale": all(quotas[name]["stale"] for name in ("claude", "codex")),
        "diagnostic": diagnostic,
        "readOnly": True,
    }


def _queue_item(task: dict[str, Any], position: int) -> dict[str, Any]:
    return {
        "id": task["id"],
        "position": position,
        "project": task["project"],
        "packageName": task["packageName"],
        "mode": task["mode"],
        "priority": task.get("priority"),
        "pageCount": task.get("pageCount"),
        "blockCount": task.get("blockCount"),
        "suggestedWorkerId": task.get("workerId"),
        "expectedStart": None,
        "status": task["stageLabel"],
        "readOnly": True,
    }


def _project(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": task["id"],
        "project": task["project"],
        "packageName": task["packageName"],
        "mode": task["mode"],
        "pageCount": None,
        "blockCount": None,
        "packageSizeBytes": None,
        "priority": task.get("priority"),
        "status": task["stageLabel"],
        "assignment": task.get("workerId"),
        "readOnly": True,
    }


def _metadata(*, now: float, excluded: int, history_may_be_truncated: bool) -> dict[str, Any]:
    return {
        "mode": "real",
        "readOnly": True,
        "schedulerEnabled": False,
        "autoDispatchEnabled": False,
        "mutationsEnabled": False,
        "managementMessage": MANAGEMENT_UNAVAILABLE,
        "generatedAt": _iso(now),
        "source": "audit_manager_distributed_workers_db",
        "historyLimit": 500,
        "historyMayBeTruncated": history_may_be_truncated,
        "filtering": {
            "includedJobType": REAL_JOB_TYPE,
            "excludedTestSyntheticCanary": excluded,
        },
    }


def _recommendation() -> dict[str, Any]:
    return {
        "available": False,
        "source": "unavailable",
        "schedulerEnabled": False,
        "projectId": None,
        "workerId": None,
        "reasons": [],
        "reason": (
            "Read-only chooser для реальных audit_pipeline_v1 отсутствует; "
            "scheduler и auto-dispatch выключены."
        ),
    }


def _transitions_by_job(
    job_ids: list[str], *, settings: DistributedWorkersSettings
) -> dict[str, list[dict[str, Any]]]:
    """Fetch the last eight transitions per job in one read transaction."""
    if not job_ids:
        return {}
    placeholders = ",".join("?" for _ in job_ids)
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM ("
            " SELECT job_id, attempt_id, from_state, to_state, reason, at, event_seq,"
            " ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY id DESC) AS recent_no"
            " FROM job_state_transitions WHERE job_id IN (" + placeholders + ")"
            ") WHERE recent_no <= 8 ORDER BY job_id, at ASC",
            job_ids,
        ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        item.pop("recent_no", None)
        grouped.setdefault(str(item.get("job_id") or ""), []).append(item)
    return grouped


def build_snapshot(
    *, settings: DistributedWorkersSettings, now: Optional[float] = None
) -> dict[str, Any]:
    """Build one consistent snapshot using read-only repository operations."""
    moment = float(now if now is not None else time.time())
    all_jobs = repositories.list_jobs(limit=500, settings=settings)
    business_jobs = [job for job in all_jobs if is_business_audit(job)]
    excluded = len(all_jobs) - len(business_jobs)
    transitions = _transitions_by_job(
        [str(job.get("job_id") or "") for job in business_jobs], settings=settings
    )

    projected_tasks = [
        _task(
            job,
            transitions=transitions.get(str(job.get("job_id") or ""), []),
            now=moment,
        )
        for job in business_jobs
    ]
    by_id = {str(job.get("job_id")): job for job in business_jobs}
    queued_tasks = sorted(
        (task for task in projected_tasks if task["stage"] == "queued" and task["isActive"]),
        key=lambda task: float((by_id.get(task["id"]) or {}).get("created_at") or 0.0),
    )
    queue = [_queue_item(task, index) for index, task in enumerate(queued_tasks, start=1)]
    active = [
        task for task in projected_tasks
        if task["isActive"] and task["stage"] not in {"queued", "done", "error"}
    ]
    completed = [task for task in projected_tasks if task["stage"] == "done"]
    errors = [task for task in projected_tasks if task["stage"] == "error"]

    raw_provider_states = provider_accounts.list_worker_provider_states(settings=settings)
    state_by_worker = {
        (str(state.get("worker_id") or ""), str(state.get("provider") or "")): state
        for state in raw_provider_states
    }
    worker_rows = repositories.list_workers(settings=settings)
    workers = [
        _worker(
            row, settings=settings, now=moment, tasks=projected_tasks,
            provider_states=state_by_worker, jobs=business_jobs,
        )
        for row in worker_rows
    ]
    workers_by_id = {worker["id"]: worker for worker in workers}
    limits = [
        {
            "workerId": worker["id"],
            "workerName": worker["name"],
            "online": worker["connectionStatus"] == "online",
            "stale": worker["quotaDataStale"],
            "claude": worker["quotas"]["claude"],
            "codex": worker["quotas"]["codex"],
        }
        for worker in workers
    ]
    diagnostics = [
        {
            "workerName": worker["name"],
            "online": worker["connectionStatus"] == "online",
            "diagnostic": worker["diagnostic"],
        }
        for worker in workers
    ]
    attention = []
    for task in errors:
        worker_name = (workers_by_id.get(task.get("workerId")) or {}).get("name") or "не назначен"
        attention.append({
            "id": f"attention-{task['id']}",
            "workerId": task.get("workerId"),
            "taskId": task["id"],
            "title": "Исторически требует решения" if not task["isActive"] else "Активная ошибка",
            "description": (
                f"{task['project']} → {task['packageName']}; VPS {worker_name}. "
                f"{task.get('errorMessage') or 'Подробности недоступны.'}"
            ),
            "severity": "warning" if not task["isActive"] else "error",
            "active": bool(task["isActive"]),
            "readOnly": True,
        })

    today = datetime.fromtimestamp(moment).date()
    completed_today = 0
    for task in completed:
        stamp = task.get("completedAtIso")
        if stamp:
            try:
                if datetime.fromisoformat(stamp).astimezone().date() == today:
                    completed_today += 1
            except (TypeError, ValueError):
                pass
    total_slots = sum(worker["slots"]["total"] for worker in workers)
    free_slots = sum(
        worker["slots"]["free"] for worker in workers
        if worker["connectionStatus"] == "online"
    )
    kpis = {
        "online": sum(worker["connectionStatus"] == "online" for worker in workers),
        "totalWorkers": len(workers),
        "active": len(active),
        "queued": len(queue),
        # A terminal needs_operator record stays in history/attention and is
        # deliberately not promoted to an active-error KPI.
        "errors": sum(bool(task["isActive"]) for task in errors),
        "historicalNeedsOperator": sum(
            bool(task["needsOperator"] and not task["isActive"]) for task in errors
        ),
        "freeSlots": free_slots,
        "totalSlots": total_slots,
        "completedToday": completed_today,
    }
    recommendation = _recommendation()
    metadata = _metadata(
        now=moment,
        excluded=excluded,
        history_may_be_truncated=len(all_jobs) >= 500,
    )
    overview = {
        "kpis": kpis,
        "workers": workers,
        "recommendation": recommendation,
        "projects": [_project(task) for task in queued_tasks],
        "queuePreview": queue[:5],
        "attention": attention,
        "metadata": metadata,
    }
    return {
        "metadata": metadata,
        "overview": overview,
        "workers": workers,
        "tasks": {"active": active, "completed": completed, "errors": errors},
        "queue": queue,
        "limits": limits,
        "diagnostics": diagnostics,
        "recommendation": recommendation,
    }
