"""Сохраняемый процесс тиражирования принятого решения на уровне раздела.

Запуск не меняет PDF, спецификации и expert_review проектов. Он фиксирует
версию снимка, исходные принятые решения и целевые строки, после чего готовит
досье для отдельного экспертного решения.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import fcntl
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.services.common import object_service
from backend.app.services.section_optimization_candidate_types import (
    DISCOVERY_KIND, REPLICATION_KIND, SUPPORTED_KINDS,
)
from backend.app.services.section_optimization_pipeline_service import (
    get_latest_snapshot,
    section_data_dir,
)
from backend.app.services.section_optimization_agent_service import (
    analyze_replication_dossier,
    configured_agent_model,
)
from backend.app.services.section_optimization_graphics_agent_service import (
    analyze_graphics_requests,
)
from backend.app.services.section_optimization_critic_service import (
    review_replication_dossier,
)
from backend.app.services.section_optimization_passport_service import (
    build_engineering_passport,
)
from backend.app.services.section_optimization_alternative_service import (
    apply_alternative_inputs,
    build_alternative_evaluation,
)
from backend.app.services.section_optimization_dependency_service import (
    build_dependency_assessment,
    update_dependency_interface,
)


logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_ACTIVE_TASKS: dict[str, "asyncio.Task[Any]"] = {}
_ACTIVE_LEASES: dict[str, Any] = {}
_ACTIVE_STATUSES = {"queued", "running"}
# Графика доведена до конца — повторять её незачем.
_GRAPHICS_DONE_STATUSES = {"complete", "not_required"}
# Графика не доведена, но досье с оплаченным agent_review цело: задачу нужно
# ПРИЗНАТЬ (иначе start_all переоплатит текстового агента), но не доводить
# автоматически — графику догоняет отдельная кнопка повтора.
_GRAPHICS_RETRYABLE_STATUSES = {"pending", "partial", "failed"}
_STAGES = (
    ("validate", "Проверка кандидата"),
    ("package", "Подготовка досье"),
    ("agent", "Умный агент"),
    ("graphics", "Графическая проверка"),
    ("critic", "Critic"),
    ("expert", "Решение эксперта"),
)


class SectionReplicationNotFound(RuntimeError):
    """Кандидат или процесс тиражирования не найден."""


class SectionReplicationConflict(RuntimeError):
    """Тиражирование уже запущено либо находится не в том статусе."""


_EXPERT_DECISIONS = {"accepted", "accepted_with_conditions", "rejected", "returned"}
_IMPLEMENTATION_STATUSES = {
    "change_requested", "implementation_pending", "implemented",
    "partially_implemented", "not_implemented", "needs_data", "effect_verified",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_section(section: str) -> str:
    code = (section or "").strip().upper()
    if not code or len(code) > 32 or not all(char.isalnum() or char in "_-" for char in code):
        raise ValueError("Недопустимый код раздела")
    return code


def _resolve_object_id(object_id: Optional[str]) -> str:
    resolved = (object_id or object_service.get_current_id() or "").strip()
    if not resolved:
        raise ValueError("Не выбран объект для тиражирования решения")
    if object_id and object_service.get_object_by_id(resolved) is None:
        raise ValueError("Объект для тиражирования не найден")
    return resolved


def _replications_dir(section: str, object_id: str) -> Path:
    path = section_data_dir(section, object_id=object_id) / "replications"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _job_path(section: str, object_id: str, replication_id: str) -> Path:
    if not replication_id or not all(char.isalnum() or char in "_-" for char in replication_id):
        raise ValueError("Недопустимый идентификатор тиражирования")
    return _replications_dir(section, object_id) / f"{replication_id}.json"


def _lease_path(section: str, object_id: str, lease_key: str) -> Path:
    safe_key = hashlib.sha256(lease_key.encode("utf-8")).hexdigest()
    path = _replications_dir(section, object_id) / ".leases"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{safe_key}.lock"


def _acquire_lease(section: str, object_id: str, lease_key: str):
    """Acquire an OS-released exclusive lease shared by all web processes."""
    path = _lease_path(section, object_id, lease_key)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps({"pid": os.getpid(), "acquired_at": _utc_now()}, ensure_ascii=False))
    handle.flush()
    os.fsync(handle.fileno())
    return handle


def _release_lease(lease_key: str) -> None:
    handle = _ACTIVE_LEASES.pop(lease_key, None)
    if handle is None:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _lease_is_held(job: dict) -> bool:
    lease_key = str(job.get("lease_key") or "")
    if not lease_key:
        return bool(_ACTIVE_TASKS.get(str(job.get("replication_id") or "")))
    handle = _acquire_lease(job["section"], job["object_id"], lease_key)
    if handle is None:
        return True
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    handle.close()
    return False


def _read_json(path: Path) -> Optional[dict]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _normalize_legacy_job(job: dict) -> dict:
    """Привести задачу схем 1-2 к контракту схемы 3 — в памяти, без записи.

    Схема 2 не знала поля `graphics_status`, поэтому без нормализации ни одна
    старая задача не проходит гейт `_active_job_for_signal` и start_all заводит
    по ней дубль, заново оплачивая текстового агента.

    Отображение опирается на инварианты старого кода, а не на догадки:
    * `awaiting_expert` в схеме 2 достигался ТОЛЬКО веткой «графика не нужна»,
      поэтому отсутствие `graphics_status` там равнозначно `not_required`;
    * `awaiting_graphics` был терминальным состоянием «agent_review готов, ждём
      ручного запуска графики». Производителя у него больше нет, но досье цело,
      поэтому задача становится `awaiting_expert` + `graphics_status="pending"`:
      её видно эксперту, start_all её не переоплачивает, а графику догоняет
      кнопка повтора.
    """
    if "graphics_status" not in job:
        if job.get("status") == "awaiting_graphics":
            job["status"] = "awaiting_expert"
            job["graphics_status"] = "pending"
        elif job.get("status") == "awaiting_expert":
            job["graphics_status"] = "not_required"
        else:
            job["graphics_status"] = "pending"
    job.setdefault("graphics_reviews", [])
    return job


def _load_job(path: Path) -> Optional[dict]:
    """Прочитать задачу с диска и нормализовать её к текущей схеме."""
    job = _read_json(path)
    if job is None:
        return None
    return _ensure_stage_schema(_normalize_legacy_job(job))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)


def _stage(key: str, title: str) -> dict:
    return {
        "key": key,
        "title": title,
        "status": "pending",
        "message": "Ожидает запуска",
        "started_at": None,
        "finished_at": None,
        "metrics": {},
    }


def _ensure_stage_schema(job: dict) -> dict:
    existing = {
        str(stage.get("key") or ""): stage
        for stage in (job.get("stages") or [])
        if isinstance(stage, dict) and stage.get("key")
    }
    job["stages"] = [existing.get(key) or _stage(key, title) for key, title in _STAGES]
    return job


def _stage_ref(job: dict, key: str) -> dict:
    for stage in job.get("stages") or []:
        if stage.get("key") == key:
            return stage
    raise KeyError(key)


def _write_job(job: dict) -> None:
    job["updated_at"] = _utc_now()
    for attempt in reversed(job.get("attempts") or []):
        if attempt.get("status") == "running":
            attempt["heartbeat_at"] = job["updated_at"]
            break
    _write_json(_job_path(job["section"], job["object_id"], job["replication_id"]), job)


def _start_attempt(job: dict, kind: str) -> None:
    now = _utc_now()
    job.setdefault("attempts", []).append({
        "attempt_id": "attempt-" + uuid.uuid4().hex[:12],
        "kind": kind,
        "status": "running",
        "started_at": now,
        "heartbeat_at": now,
        "finished_at": None,
        "error": "",
    })


def _finish_attempt(job: dict, status: str, error: str = "") -> None:
    for attempt in reversed(job.get("attempts") or []):
        if attempt.get("status") == "running":
            attempt.update({"status": status, "finished_at": _utc_now(), "error": error[:3000]})
            return


def _public_job(job: dict, *, include_dossier: bool = False) -> dict:
    result = copy.deepcopy(job)
    result.pop("object_id", None)
    if not include_dossier:
        result.pop("dossier", None)
    return result


def _begin_stage(job: dict, key: str, message: str) -> None:
    stage = _stage_ref(job, key)
    stage.update({
        "status": "running",
        "message": message,
        "started_at": _utc_now(),
        "finished_at": None,
        "metrics": {},
    })
    _write_job(job)


def _finish_stage(job: dict, key: str, message: str, metrics: Optional[dict] = None) -> None:
    stage = _stage_ref(job, key)
    stage.update({
        "status": "done",
        "message": message,
        "finished_at": _utc_now(),
        "metrics": metrics or {},
    })
    _write_job(job)


def _signal_from_snapshot(snapshot: dict, signal_id: str) -> dict:
    signal = next(
        (item for item in (snapshot.get("signals") or []) if str(item.get("signal_id") or "") == signal_id),
        None,
    )
    if not signal or signal.get("kind") not in SUPPORTED_KINDS:
        raise SectionReplicationNotFound("Поддерживаемый кандидат не найден в сохранённом снимке")
    return signal


def _replication_input_fingerprint(
    snapshot: dict,
    signal: dict,
    target_project_ids: list[str],
) -> str:
    """Идентичность конкретного досье без времени формирования снимка."""
    accepted_by_ref = {
        str(item.get("source_ref") or ""): item
        for item in (snapshot.get("accepted_optimizations") or [])
    }
    rows_by_id = {
        str(item.get("row_id") or ""): item
        for item in (snapshot.get("specification_rows") or [])
    }
    target_set = set(target_project_ids)
    payload = {
        "fingerprint_version": 1,
        "candidate": {
            key: signal.get(key)
            for key in (
                "signal_id", "kind", "representative_proposal", "graphics_recommended",
            )
        },
        "source_decisions": [
            accepted_by_ref[ref]
            for ref in sorted(str(value) for value in (signal.get("evidence_refs") or []))
            if ref in accepted_by_ref
        ],
        "target_rows": [
            rows_by_id[row_id]
            for row_id in sorted(str(value) for value in (signal.get("target_row_ids") or []))
            if row_id in rows_by_id
            and str(rows_by_id[row_id].get("project_id") or "") in target_set
        ],
        "target_project_ids": sorted(target_set),
    }
    if signal.get("kind") == DISCOVERY_KIND:
        payload["discovery_basis"] = {
            key: signal.get(key) for key in ("variants", "reason", "match_basis")
        }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _active_job_for_signal(
    section: str,
    object_id: str,
    signal_id: str,
    *,
    input_fingerprint: Optional[str] = None,
    snapshot_generated_at: Optional[str] = None,
) -> Optional[dict]:
    for path in _replications_dir(section, object_id).glob("*.json"):
        job = _load_job(path)
        if not job or job.get("signal_id") != signal_id:
            continue
        job = _mark_interrupted_if_needed(job)
        if input_fingerprint is not None:
            job_fingerprint = str(job.get("input_fingerprint") or "")
            if job_fingerprint:
                if job_fingerprint != input_fingerprint:
                    continue
            elif not (
                snapshot_generated_at
                and job.get("snapshot_generated_at") == snapshot_generated_at
            ):
                # Старое досье без доказуемой идентичности остаётся в истории,
                # но не блокирует проверку актуальных данных.
                continue
        if job.get("status") in _ACTIVE_STATUSES:
            return job
        # Задачу нужно признать, если текстовый агент уже отработал: его сессия
        # оплачена, а досье лежит на диске. Недоведённую графику догоняет
        # отдельный повтор, а не повторная оплата всего процесса.
        if (
            job.get("status") == "awaiting_expert"
            and job.get("agent_status") == "complete"
            and job.get("graphics_status")
            in (_GRAPHICS_DONE_STATUSES | _GRAPHICS_RETRYABLE_STATUSES)
        ):
            return job
    return None


def _job_input_stale(job: dict, snapshot: Optional[dict]) -> bool:
    """Проверить, относится ли сохранённая задача к текущим входам кандидата."""
    if not snapshot:
        return True
    try:
        signal = _signal_from_snapshot(snapshot, str(job.get("signal_id") or ""))
    except SectionReplicationNotFound:
        return True
    targets = [str(value) for value in (job.get("target_project_ids") or []) if value]
    if not targets or not set(targets).issubset(set(signal.get("target_project_ids") or [])):
        return True
    fingerprint = str(job.get("input_fingerprint") or "")
    if fingerprint:
        return fingerprint != _replication_input_fingerprint(snapshot, signal, targets)
    snapshot_generated_at = (snapshot.get("meta") or {}).get("generated_at")
    return not snapshot_generated_at or job.get("snapshot_generated_at") != snapshot_generated_at


def _apply_graphics_reviews(job: dict, graphics_reviews: list[dict], graphics_meta: dict) -> None:
    """Разложить обзоры графики по оценкам агента и обновить статус задачи.

    Вызывается и из первичной подготовки, и из повтора графики — логика обязана
    быть одна, иначе повтор со временем разойдётся с основным путём.
    """
    reviews_by_project = {
        str(review.get("project_id") or ""): review
        for review in graphics_reviews
        if review.get("project_id")
    }
    enriched_assessments: list[dict] = []
    for assessment in job.get("agent_assessments") or []:
        enriched = dict(assessment)
        review = reviews_by_project.get(str(assessment.get("project_id") or ""))
        if review:
            enriched["graphics_review"] = review
            # У мягко упавшего обзора resolved_verdict пуст: вердикт текстового
            # агента сохраняется, а не подменяется на needs_data.
            enriched["resolved_verdict"] = review.get("resolved_verdict") or assessment.get("verdict")
        else:
            enriched["resolved_verdict"] = assessment.get("verdict")
        enriched_assessments.append(enriched)
    job["agent_assessments"] = enriched_assessments
    job["dossier"]["agent_review"]["target_assessments"] = copy.deepcopy(enriched_assessments)
    job["graphics_reviews"] = graphics_reviews
    job["graphics_agent"] = graphics_meta
    # Статус берётся из метрик стадии, а не проставляется оптимистично: часть
    # проектов могла деградировать мягко (partial/failed), и эксперт обязан это
    # видеть, а повтор — знать, что доводить.
    job["graphics_status"] = graphics_meta.get("status") or "complete"


def _graphics_assessments_to_retry(job: dict) -> list[dict]:
    """Оценки, по которым графику нужно догнать: запрошена, но не выполнена."""
    pending: list[dict] = []
    for assessment in job.get("agent_assessments") or []:
        if not assessment.get("graphics_required"):
            continue
        review = assessment.get("graphics_review") or {}
        if review and review.get("status") != "failed":
            continue
        pending.append(assessment)
    return pending


async def _prepare_replication(job: dict, snapshot: dict, signal: dict) -> None:
    task_key = job["replication_id"]
    try:
        job["status"] = "running"
        _write_job(job)
        await asyncio.sleep(0)

        _begin_stage(job, "validate", "Проверяем сохранённый кандидат и выбранные проекты")
        accepted_by_ref = {
            str(item.get("source_ref") or ""): item
            for item in (snapshot.get("accepted_optimizations") or [])
        }
        rows_by_id = {
            str(item.get("row_id") or ""): item
            for item in (snapshot.get("specification_rows") or [])
        }
        source_decisions = [
            accepted_by_ref[source_ref]
            for source_ref in (signal.get("evidence_refs") or [])
            if source_ref in accepted_by_ref
        ]
        selected_projects = set(job.get("target_project_ids") or [])
        target_rows = [
            rows_by_id[row_id]
            for row_id in (signal.get("target_row_ids") or [])
            if row_id in rows_by_id and rows_by_id[row_id].get("project_id") in selected_projects
        ]
        discovery = signal.get("kind") == DISCOVERY_KIND
        if discovery:
            # Новая гипотеза не является ранее одобренным решением.
            source_decisions = []
        if not discovery and not source_decisions:
            raise SectionReplicationNotFound("В снимке отсутствуют исходные принятые решения")
        if not target_rows:
            raise SectionReplicationNotFound("В снимке отсутствуют выбранные целевые позиции")
        _finish_stage(
            job,
            "validate",
            "Кандидат подтверждён сохранённым снимком",
            {
                "source_decisions": len(source_decisions),
                "target_projects": len(selected_projects),
                "target_rows": len(target_rows),
            },
        )
        await asyncio.sleep(0)

        _begin_stage(job, "package", "Фиксируем основания и пакет по каждому целевому проекту")
        targets: list[dict] = []
        for project_id in job.get("target_project_ids") or []:
            rows = [row for row in target_rows if row.get("project_id") == project_id]
            if not rows:
                continue
            targets.append({
                "project_id": project_id,
                "project_name": rows[0].get("project_name") or project_id,
                "version_id": rows[0].get("version_id") or "",
                "rows": [
                    {
                        key: row.get(key)
                        for key in (
                            "row_id", "page", "sheet", "sheet_name", "category", "position",
                            "name", "designation", "type_mark", "code", "manufacturer", "unit",
                            "quantity", "mass", "total_mass", "note", "source",
                        )
                    }
                    for row in rows
                ],
            })
        job["dossier"] = {
            "snapshot_generated_at": job.get("snapshot_generated_at"),
            "candidate": {
                key: signal.get(key)
                for key in (
                    "signal_id", "kind", "variants", "title", "reason", "match_basis", "match_score",
                    "representative_proposal", "graphics_recommended",
                )
            },
            "source_decisions": [
                {
                    key: item.get(key)
                    for key in (
                        "source_ref", "project_id", "project_name", "version_id", "id", "current",
                        "proposed", "risks", "norm", "savings_pct", "spec_items",
                        "page", "sheet",
                    )
                }
                for item in source_decisions
            ],
            "targets": targets,
            "guardrails": [
                "Проверка не изменяет исходные PDF и спецификации автоматически.",
                "Решение принимается отдельно для каждого целевого проекта.",
                "Пожарное исполнение и степень IP должны сохраняться.",
                "При графической зависимости сначала требуется проверка связанного листа или блока.",
            ],
        }
        if discovery:
            job["dossier"]["guardrails"].append(
                "Это новая гипотеза без исходного принятого решения. Сходство названий не доказывает "
                "взаимозаменяемость. Нужны сравнение исполнений и конкретное изменение по каждому проекту."
            )
        job["dossier"]["engineering_passport"] = build_engineering_passport(
            job["dossier"]["candidate"],
            job["dossier"]["source_decisions"],
            job["dossier"]["targets"],
        )
        job["engineering_passport"] = copy.deepcopy(job["dossier"]["engineering_passport"])
        job["dossier"]["dependency_assessment"] = build_dependency_assessment(
            job["dossier"]["engineering_passport"]
        )
        job["dependency_assessment"] = copy.deepcopy(
            job["dossier"]["dependency_assessment"]
        )
        job["dossier"]["alternative_evaluation"] = build_alternative_evaluation(
            job["dossier"]["candidate"],
            job["dossier"]["source_decisions"],
            job["dossier"]["targets"],
        )
        job["alternative_evaluation"] = copy.deepcopy(
            job["dossier"]["alternative_evaluation"]
        )
        _finish_stage(
            job,
            "package",
            "Досье кандидата сохранено",
            {"target_projects": len(targets), "target_rows": len(target_rows)},
        )

        agent_stage = _stage_ref(job, "agent")
        agent_stage.update({
            "status": "waiting",
            "message": "Ожидает свободный слот умного агента",
        })
        job["agent_status"] = "queued"
        _write_job(job)

        def on_agent_slot_acquired() -> None:
            _begin_stage(job, "agent", "Умный агент проверяет применимость по каждому проекту")
            job["agent_status"] = "running"
            _write_job(job)

        agent_review, agent_meta = await analyze_replication_dossier(
            job["dossier"],
            object_id=job["object_id"],
            section=job["section"],
            replication_id=job["replication_id"],
            on_slot_acquired=on_agent_slot_acquired,
        )
        job["dossier"]["agent_review"] = agent_review
        job["agent_status"] = "complete"
        job["agent"] = agent_meta
        job["agent_model"] = agent_meta.get("model") or configured_agent_model()
        job["agent_recommendation"] = agent_review.get("overall_recommendation")
        job["agent_summary"] = agent_review.get("summary") or agent_review.get("expert_summary") or ""
        job["agent_assessments"] = list(agent_review.get("target_assessments") or [])
        job["agent_target_counts"] = {
            verdict: sum(
                1 for item in (agent_review.get("target_assessments") or [])
                if item.get("verdict") == verdict
            )
            for verdict in (
                "applicable", "applicable_with_conditions", "needs_graphics",
                "needs_data", "reject",
            )
        }
        _finish_stage(
            job,
            "agent",
            "Агент подготовил заключение по целевым проектам",
            {
                "model": job["agent_model"],
                "recommendation": job["agent_recommendation"],
                "targets": len(agent_review.get("target_assessments") or []),
                "input_tokens": agent_meta.get("input_tokens", 0),
                "output_tokens": agent_meta.get("output_tokens", 0),
            },
        )

        graphics = _stage_ref(job, "graphics")
        expert = _stage_ref(job, "expert")
        graphics_assessments = [
            item for item in (agent_review.get("target_assessments") or [])
            if item.get("graphics_required") or item.get("verdict") == "needs_graphics"
        ]
        job["graphics_required"] = bool(graphics_assessments)
        job["graphics_requests"] = [
            {
                "project_id": item.get("project_id"),
                "reason": item.get("graphics_reason") or item.get("reason") or "",
                "pages": list(item.get("suggested_pages") or []),
                "target_row_ids": list(item.get("target_row_ids") or []),
            }
            for item in graphics_assessments
        ]
        if graphics_assessments:
            graphics.update({
                "status": "waiting",
                "message": f"Ожидает vision-проверку: {len(graphics_assessments)} проект(а)",
            })
            job["graphics_status"] = "queued"
            _write_job(job)

            job["graphics_status"] = "running"
            _begin_stage(
                job,
                "graphics",
                f"Графический агент проверяет блоки: {len(graphics_assessments)} проект(а)",
            )
            graphics_reviews, graphics_meta = await analyze_graphics_requests(
                job["dossier"],
                graphics_assessments,
                object_id=job["object_id"],
                section=job["section"],
                replication_id=job["replication_id"],
            )
            _apply_graphics_reviews(job, graphics_reviews, graphics_meta)
            _finish_stage(
                job,
                "graphics",
                f"Графический агент проверил {len(graphics_reviews)} проект(а)",
                graphics_meta,
            )
            job["status"] = "awaiting_expert"
        else:
            graphics.update({
                "status": "skipped",
                "message": "Для этого кандидата графическая проверка не требуется",
                "finished_at": _utc_now(),
            })
            job["graphics_status"] = "not_required"
            job["agent_assessments"] = [
                {**assessment, "resolved_verdict": assessment.get("verdict")}
                for assessment in (job.get("agent_assessments") or [])
            ]
            job["dossier"]["agent_review"]["target_assessments"] = copy.deepcopy(
                job["agent_assessments"]
            )
            job["status"] = "awaiting_expert"
        _begin_stage(job, "critic", "Проверяем полноту и непротиворечивость досье")
        critic = review_replication_dossier(job["dossier"], job.get("agent_assessments") or [])
        job["critic"] = critic
        data_requests: list[dict] = []
        for review in critic.get("target_reviews") or []:
            for finding in review.get("findings") or []:
                if finding.get("severity") != "blocking":
                    continue
                data_requests.append({
                    "request_id": (
                        f"REQ-{job['replication_id']}-{review.get('project_id')}-"
                        f"{finding.get('code')}"
                    ),
                    "project_id": review.get("project_id"),
                    "kind": finding.get("code"),
                    "question": finding.get("message"),
                    "status": "open",
                    "created_at": _utc_now(),
                })
        job["data_requests"] = data_requests
        _finish_stage(
            job,
            "critic",
            (
                "Досье прошло контроль полноты"
                if critic.get("status") == "pass"
                else f"Найдены блокирующие вопросы: {len(data_requests)}"
            ),
            critic.get("counts") or {},
        )
        expert.update({
            "status": "waiting",
            "message": "Ожидает решения по каждому целевому проекту",
        })
        job["prepared_at"] = _utc_now()
        _write_job(job)
    except Exception as exc:  # pragma: no cover - аварийная защита фоновой задачи
        job["status"] = "failed"
        job["error"] = str(exc)
        if job.get("agent_status") in {"queued", "running"}:
            job["agent_status"] = "failed"
        if job.get("graphics_status") in {"queued", "running"}:
            job["graphics_status"] = "failed"
        for stage in job.get("stages") or []:
            if stage.get("status") == "running":
                stage.update({"status": "failed", "message": str(exc), "finished_at": _utc_now()})
                break
        _write_job(job)
    finally:
        _finish_attempt(job, "failed" if job.get("status") == "failed" else "completed", str(job.get("error") or ""))
        _write_job(job)
        _ACTIVE_TASKS.pop(task_key, None)
        _release_lease(str(job.get("lease_key") or ""))


def start_replication(
    section: str,
    signal_id: str,
    *,
    object_id: Optional[str] = None,
    target_project_ids: Optional[list[str]] = None,
) -> dict:
    """Запустить подготовку сохраняемого досье тиражирования."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)
    if not snapshot:
        raise SectionReplicationNotFound("Сначала сформируйте и сохраните оптимизацию раздела")
    signal = _signal_from_snapshot(snapshot, signal_id)
    available_targets = [str(value) for value in (signal.get("target_project_ids") or []) if value]
    requested_targets = list(dict.fromkeys(str(value) for value in (target_project_ids or available_targets) if value))
    if not requested_targets or not set(requested_targets).issubset(set(available_targets)):
        raise ValueError("Выбраны проекты, которых нет среди целей кандидата")
    if signal.get("kind") == DISCOVERY_KIND and set(requested_targets) != set(available_targets):
        raise ValueError("Для сравнения типоразмеров нужны все проекты кандидата")
    snapshot_generated_at = (snapshot.get("meta") or {}).get("generated_at")
    input_fingerprint = _replication_input_fingerprint(snapshot, signal, requested_targets)

    with _LOCK:
        lease_key = f"full:{code}:{resolved_object_id}:{signal_id}:{input_fingerprint}"
        lease_handle = _acquire_lease(code, resolved_object_id, lease_key)
        if lease_handle is None:
            raise SectionReplicationConflict("Процесс тиражирования этого кандидата уже выполняется другим worker")
        existing = _active_job_for_signal(
            code,
            resolved_object_id,
            signal_id,
            input_fingerprint=input_fingerprint,
            snapshot_generated_at=snapshot_generated_at,
        )
        if existing:
            fcntl.flock(lease_handle.fileno(), fcntl.LOCK_UN)
            lease_handle.close()
            raise SectionReplicationConflict("Процесс тиражирования этого кандидата уже запущен")
        now = _utc_now()
        job = {
            "schema_version": 4,
            "replication_id": "repl-" + uuid.uuid4().hex[:12],
            "section": code,
            "object_id": resolved_object_id,
            "signal_id": signal_id,
            "candidate_kind": signal.get("kind"),
            "title": signal.get("title") or "Тиражирование принятого решения",
            "status": "queued",
            "error": "",
            "created_at": now,
            "updated_at": now,
            "prepared_at": None,
            "snapshot_generated_at": snapshot_generated_at,
            "input_fingerprint": input_fingerprint,
            "source_project_ids": list(signal.get("source_project_ids") or []),
            "target_project_ids": requested_targets,
            "source_decision_refs": list(signal.get("evidence_refs") or []),
            "target_row_ids": list(signal.get("target_row_ids") or []),
            "graphics_required": bool(signal.get("graphics_recommended")),
            "graphics_hint": bool(signal.get("graphics_recommended")),
            "graphics_requests": [],
            "graphics_status": "pending",
            "graphics_reviews": [],
            "graphics_agent": None,
            "agent_status": "pending",
            "agent_model": configured_agent_model(),
            "agent_recommendation": None,
            "agent_summary": "",
            "agent_assessments": [],
            "agent_target_counts": {},
            "agent": None,
            "stages": [_stage(key, title) for key, title in _STAGES],
            "dossier": None,
            "lease_key": lease_key,
            "attempts": [],
        }
        _start_attempt(job, "full")
        _write_job(job)
        _ACTIVE_LEASES[lease_key] = lease_handle
        try:
            task = asyncio.create_task(
                _prepare_replication(job, snapshot, signal),
                name=job["replication_id"],
            )
            _ACTIVE_TASKS[job["replication_id"]] = task
            return _public_job(job)
        except Exception:
            _release_lease(lease_key)
            raise


async def _run_graphics_retry(job: dict, assessments: list[dict]) -> None:
    """Догнать графику по сохранённому досье. Текстовый агент не перезапускается."""
    task_key = job["replication_id"]
    try:
        _begin_stage(
            job,
            "graphics",
            f"Повтор графической проверки: {len(assessments)} проект(а)",
        )
        reviews, meta = await analyze_graphics_requests(
            job["dossier"],
            assessments,
            object_id=job["object_id"],
            section=job["section"],
            replication_id=job["replication_id"],
        )
        with _LOCK:
            # Слить с уже имеющимися обзорами: повтор гонит только недоведённые
            # проекты, а успешные из прошлого прогона обязаны сохраниться.
            merged = {
                str(r.get("project_id") or ""): r
                for r in (job.get("graphics_reviews") or [])
                if r.get("project_id") and r.get("status") != "failed"
            }
            for review in reviews:
                merged[str(review.get("project_id") or "")] = review
            _apply_graphics_reviews(job, list(merged.values()), meta)
            _finish_stage(
                job,
                "graphics",
                f"Графический агент проверил {len(reviews)} проект(а)",
                meta,
            )
            job["status"] = "awaiting_expert"
            _write_job(job)
    except Exception as exc:  # noqa: BLE001 — повтор не должен ронять задачу
        logger.exception("Повтор графики упал: %s", task_key)
        with _LOCK:
            job["graphics_status"] = "failed"
            job["error"] = str(exc)
            job["status"] = "awaiting_expert"
            _write_job(job)
    finally:
        _finish_attempt(job, "failed" if job.get("graphics_status") == "failed" else "completed", str(job.get("error") or ""))
        _write_job(job)
        _ACTIVE_TASKS.pop(task_key, None)
        _release_lease(str(job.get("lease_key") or ""))


def retry_graphics(
    section: str,
    replication_id: str,
    *,
    object_id: Optional[str] = None,
) -> dict:
    """Повторить ТОЛЬКО графическую проверку по уже готовому досье.

    Существует потому, что упавшая или недоведённая графика не должна стоить
    повторной оплаты текстового агента: его сессия уже оплачена, а результат
    лежит в `dossier.agent_review`.
    """
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        path = _job_path(code, resolved_object_id, replication_id)
        job = _load_job(path)
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        if job["replication_id"] in _ACTIVE_TASKS or job.get("status") in _ACTIVE_STATUSES:
            raise SectionReplicationConflict("Процесс тиражирования уже выполняется")
        if job.get("agent_status") != "complete" or not (job.get("dossier") or {}).get("agent_review"):
            raise SectionReplicationConflict(
                "Нет готового досье умного агента — запустите подготовку целиком"
            )
        assessments = _graphics_assessments_to_retry(job)
        if not assessments:
            raise SectionReplicationConflict("Графическая проверка не требуется или уже выполнена")

        lease_key = f"graphics:{code}:{resolved_object_id}:{replication_id}"
        lease_handle = _acquire_lease(code, resolved_object_id, lease_key)
        if lease_handle is None:
            raise SectionReplicationConflict("Графическая проверка уже выполняется другим worker")
        job["lease_key"] = lease_key
        _start_attempt(job, "graphics_retry")
        job["graphics_status"] = "running"
        job["error"] = ""
        _write_job(job)
        _ACTIVE_LEASES[lease_key] = lease_handle
        try:
            task = asyncio.create_task(
                _run_graphics_retry(job, assessments),
                name=f"{job['replication_id']}-graphics-retry",
            )
            _ACTIVE_TASKS[job["replication_id"]] = task
            return _public_job(job)
        except Exception:
            _release_lease(lease_key)
            raise


def save_expert_decision(
    section: str,
    replication_id: str,
    project_id: str,
    decision: str,
    *,
    object_id: Optional[str] = None,
    reviewer: str = "",
    note: str = "",
    conditions: Optional[list[str]] = None,
    expected_input_fingerprint: str,
    expected_updated_at: str,
) -> dict:
    """Сохранить решение эксперта по одной цели с optimistic locking."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    normalized_decision = str(decision or "").strip()
    if normalized_decision not in _EXPERT_DECISIONS:
        raise ValueError("Недопустимое решение эксперта")
    target_project_id = str(project_id or "").strip()
    if not target_project_id:
        raise ValueError("Не указан целевой проект")
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)

    with _LOCK:
        path = _job_path(code, resolved_object_id, replication_id)
        job = _load_job(path)
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        if job.get("status") in _ACTIVE_STATUSES:
            raise SectionReplicationConflict("Подготовка заключения ещё выполняется")
        if _job_input_stale(job, snapshot):
            raise SectionReplicationConflict(
                "Досье устарело: пересчитайте кандидата по текущим версиям проектов"
            )
        if not expected_input_fingerprint or expected_input_fingerprint != job.get("input_fingerprint"):
            raise SectionReplicationConflict("Решение относится к другой ревизии досье")
        if not expected_updated_at or expected_updated_at != job.get("updated_at"):
            raise SectionReplicationConflict(
                "Досье уже изменено другим пользователем; обновите страницу"
            )
        target_ids = {str(value) for value in (job.get("target_project_ids") or [])}
        if target_project_id not in target_ids:
            raise ValueError("Проект отсутствует среди целей этого досье")
        critic_review = next(
            (
                item for item in ((job.get("critic") or {}).get("target_reviews") or [])
                if str(item.get("project_id") or "") == target_project_id
            ),
            None,
        )
        allowed_decisions = set((critic_review or {}).get("allowed_expert_decisions") or [])
        if allowed_decisions and normalized_decision not in allowed_decisions:
            raise SectionReplicationConflict(
                "Critic обнаружил незакрытые данные; цель можно отклонить или вернуть на доработку"
            )

        cleaned_conditions = [
            " ".join(str(value).split())[:1500]
            for value in (conditions or [])
            if str(value).strip()
        ]
        cleaned_note = " ".join(str(note or "").split())[:3000]
        if normalized_decision == "accepted_with_conditions" and not cleaned_conditions:
            raise ValueError("Для принятия с условиями укажите хотя бы одно условие")
        if normalized_decision in {"rejected", "returned"} and not cleaned_note:
            raise ValueError("Для отклонения или возврата укажите причину")
        now = _utc_now()
        event = {
            "decision_id": "section-decision-" + uuid.uuid4().hex[:12],
            "project_id": target_project_id,
            "decision": normalized_decision,
            "conditions": cleaned_conditions,
            "note": cleaned_note,
            "reviewer": " ".join(str(reviewer or "").split())[:300],
            "decided_at": now,
            "input_fingerprint": job.get("input_fingerprint"),
        }
        if job.get("candidate_kind") == DISCOVERY_KIND:
            assessment = next((item for item in job.get("agent_assessments") or []
                               if item.get("project_id") == target_project_id), {})
            event["proposed_action"] = assessment.get("proposed_action") or ""
            event["comparison_row_ids"] = list(assessment.get("comparison_row_ids") or [])
        history = list(job.get("expert_decision_history") or [])
        history.append(event)
        latest = {
            str(item.get("project_id") or ""): item
            for item in (job.get("expert_decisions") or [])
            if item.get("project_id")
        }
        latest[target_project_id] = event
        job["expert_decision_history"] = history
        job["expert_decisions"] = [latest[key] for key in sorted(latest)]

        implementation_by_project = {
            str(item.get("project_id") or ""): item
            for item in (job.get("implementation_checks") or [])
            if item.get("project_id")
        }
        if normalized_decision in {"accepted", "accepted_with_conditions"}:
            target = next(
                (item for item in ((job.get("dossier") or {}).get("targets") or [])
                 if str(item.get("project_id") or "") == target_project_id),
                {},
            )
            existing_check = implementation_by_project.get(target_project_id) or {}
            implementation_by_project[target_project_id] = {
                **existing_check,
                "check_id": existing_check.get("check_id") or "implementation-" + uuid.uuid4().hex[:12],
                "project_id": target_project_id,
                "accepted_decision_id": event["decision_id"],
                "accepted_version_id": target.get("version_id") or "",
                "expected_change": event.get("proposed_action") or (job.get("engineering_passport") or {}).get("action") or "",
                "conditions": cleaned_conditions,
                "status": "change_requested",
                "created_at": existing_check.get("created_at") or now,
                "updated_at": now,
                "evidence_refs": [],
            }
        elif target_project_id in implementation_by_project:
            implementation_by_project[target_project_id].update({
                "status": "cancelled",
                "updated_at": now,
                "cancelled_by_decision_id": event["decision_id"],
            })
        job["implementation_checks"] = [
            implementation_by_project[key] for key in sorted(implementation_by_project)
        ]

        decided_targets = set(latest) & target_ids
        expert_stage = _stage_ref(job, "expert")
        if decided_targets == target_ids:
            decisions = {latest[key]["decision"] for key in target_ids}
            if decisions.issubset({"accepted", "accepted_with_conditions"}):
                job["status"] = "approved"
            elif decisions == {"rejected"}:
                job["status"] = "rejected"
            else:
                job["status"] = "reviewed"
            expert_stage.update({
                "status": "done",
                "message": f"Решения сохранены по {len(target_ids)} проектам",
                "finished_at": now,
                "metrics": {"decided_projects": len(target_ids)},
            })
        else:
            job["status"] = "awaiting_expert"
            expert_stage.update({
                "status": "waiting",
                "message": f"Сохранено решений: {len(decided_targets)} из {len(target_ids)}",
            })
        _write_job(job)
        result = _public_job(job)
        result["input_stale"] = False
        return result


def update_implementation_check(
    section: str,
    replication_id: str,
    project_id: str,
    status: str,
    *,
    object_id: Optional[str] = None,
    reviewer: str = "",
    note: str = "",
    evidence_refs: Optional[list[str]] = None,
    expected_updated_at: str,
) -> dict:
    """Update implementation separately from the engineering decision."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    normalized_status = str(status or "").strip()
    if normalized_status not in _IMPLEMENTATION_STATUSES:
        raise ValueError("Недопустимый статус внедрения")
    target_project_id = str(project_id or "").strip()
    cleaned_refs = [
        " ".join(str(value).split())[:1000]
        for value in (evidence_refs or []) if str(value).strip()
    ]
    if normalized_status in {"implemented", "partially_implemented", "effect_verified"} and not cleaned_refs:
        raise ValueError("Для подтверждения внедрения укажите новую версию или доказательство")

    with _LOCK:
        path = _job_path(code, resolved_object_id, replication_id)
        job = _load_job(path)
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        if expected_updated_at != job.get("updated_at"):
            raise SectionReplicationConflict("Досье уже изменено другим пользователем; обновите страницу")
        checks = list(job.get("implementation_checks") or [])
        check = next((item for item in checks if item.get("project_id") == target_project_id), None)
        if not check or check.get("status") == "cancelled":
            raise SectionReplicationConflict("Для проекта нет действующего принятого решения")
        if normalized_status == "effect_verified" and check.get("status") not in {
            "implemented", "partially_implemented", "effect_verified",
        }:
            raise SectionReplicationConflict("Сначала подтвердите фактическое внедрение")
        now = _utc_now()
        event = {
            "project_id": target_project_id,
            "status": normalized_status,
            "note": " ".join(str(note or "").split())[:3000],
            "evidence_refs": cleaned_refs,
            "reviewer": " ".join(str(reviewer or "").split())[:300],
            "recorded_at": now,
        }
        check.update({
            "status": normalized_status,
            "note": event["note"],
            "evidence_refs": cleaned_refs,
            "updated_at": now,
            "updated_by": event["reviewer"],
        })
        history = list(job.get("implementation_history") or [])
        history.append(event)
        job["implementation_history"] = history
        job["implementation_checks"] = checks
        _write_job(job)
        result = _public_job(job)
        result["input_stale"] = _job_input_stale(job, get_latest_snapshot(code, object_id=resolved_object_id))
        return result


def update_alternative_evaluation(
    section: str,
    replication_id: str,
    inputs: dict,
    *,
    object_id: Optional[str] = None,
    reviewer: str = "",
    expected_updated_at: str,
) -> dict:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        job = _load_job(_job_path(code, resolved_object_id, replication_id))
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        if expected_updated_at != job.get("updated_at"):
            raise SectionReplicationConflict("Досье уже изменено другим пользователем; обновите страницу")
        current = job.get("alternative_evaluation") or (job.get("dossier") or {}).get("alternative_evaluation")
        if not current:
            raise SectionReplicationConflict("В досье отсутствует сравнение вариантов")
        updated = apply_alternative_inputs(current, inputs)
        updated["updated_by"] = " ".join(str(reviewer or "").split())[:300]
        updated["updated_at"] = _utc_now()
        job["alternative_evaluation"] = updated
        if job.get("dossier") is not None:
            job["dossier"]["alternative_evaluation"] = copy.deepcopy(updated)
        job.setdefault("alternative_evaluation_history", []).append(copy.deepcopy(updated))
        _write_job(job)
        result = _public_job(job)
        result["input_stale"] = _job_input_stale(job, get_latest_snapshot(code, object_id=resolved_object_id))
        return result


def save_dependency_review(
    section: str, replication_id: str, interface_id: str, status: str, *,
    object_id: Optional[str] = None, reviewer: str = "", note: str = "",
    evidence_refs: Optional[list[str]] = None, expected_updated_at: str,
) -> dict:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        job = _load_job(_job_path(code, resolved_object_id, replication_id))
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        if expected_updated_at != job.get("updated_at"):
            raise SectionReplicationConflict("Досье уже изменено другим пользователем; обновите страницу")
        current = job.get("dependency_assessment") or (job.get("dossier") or {}).get("dependency_assessment")
        if not current:
            raise SectionReplicationConflict("Карта инженерных интерфейсов отсутствует")
        updated = update_dependency_interface(current, interface_id, status, evidence_refs or [], note)
        job["dependency_assessment"] = updated
        if job.get("dossier") is not None:
            job["dossier"]["dependency_assessment"] = copy.deepcopy(updated)
        job.setdefault("dependency_review_history", []).append({
            "interface_id": interface_id, "status": status, "note": note,
            "evidence_refs": list(evidence_refs or []), "reviewer": reviewer, "recorded_at": _utc_now(),
        })
        _write_job(job)
        return _public_job(job)


def answer_data_request(
    section: str,
    replication_id: str,
    request_id: str,
    answer: str,
    *,
    object_id: Optional[str] = None,
    answered_by: str = "",
    evidence_refs: Optional[list[str]] = None,
    expected_input_fingerprint: str,
    expected_updated_at: str,
) -> dict:
    """Сохранить ответ на блокирующий вопрос без ложного закрытия проверки."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    cleaned_answer = " ".join(str(answer or "").split())[:5000]
    if not cleaned_answer:
        raise ValueError("Ответ не может быть пустым")
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)
    with _LOCK:
        path = _job_path(code, resolved_object_id, replication_id)
        job = _load_job(path)
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        if _job_input_stale(job, snapshot):
            raise SectionReplicationConflict("Досье устарело; ответ нужно привязать к новой ревизии")
        if expected_input_fingerprint != job.get("input_fingerprint"):
            raise SectionReplicationConflict("Ответ относится к другой ревизии досье")
        if expected_updated_at != job.get("updated_at"):
            raise SectionReplicationConflict("Досье уже изменено другим пользователем; обновите страницу")
        requests = list(job.get("data_requests") or [])
        target = next((item for item in requests if item.get("request_id") == request_id), None)
        if not target:
            raise SectionReplicationNotFound("Запрос недостающих данных не найден")
        target.update({
            "status": "answered_pending_reanalysis",
            "answer": cleaned_answer,
            "answered_by": " ".join(str(answered_by or "").split())[:300],
            "answered_at": _utc_now(),
            "evidence_refs": [
                " ".join(str(value).split())[:1000]
                for value in (evidence_refs or [])
                if str(value).strip()
            ],
        })
        job["data_requests"] = requests
        critic = review_replication_dossier(
            job.get("dossier") or {},
            job.get("agent_assessments") or [],
            requests,
        )
        job["critic"] = critic
        findings_by_project = {
            str(review.get("project_id") or ""): {
                str(finding.get("code") or "")
                for finding in (review.get("findings") or [])
            }
            for review in (critic.get("target_reviews") or [])
        }
        for request in requests:
            if request.get("status") != "answered_pending_reanalysis":
                continue
            remaining = findings_by_project.get(str(request.get("project_id") or ""), set())
            request["status"] = (
                "resolved" if str(request.get("kind") or "") not in remaining
                else "answer_insufficient"
            )
            request["rechecked_at"] = _utc_now()
        critic_stage = _stage_ref(job, "critic")
        critic_stage.update({
            "status": "done",
            "message": (
                "Ответы перепроверены, блокирующих вопросов нет"
                if critic.get("status") == "pass"
                else "Ответы перепроверены, часть вопросов остаётся блокирующей"
            ),
            "finished_at": _utc_now(),
            "metrics": critic.get("counts") or {},
        })
        _write_job(job)
        result = _public_job(job)
        result["input_stale"] = False
        return result


def start_all_replications(
    section: str,
    *,
    object_id: Optional[str] = None,
    candidate_kind: str = REPLICATION_KIND,
) -> dict:
    """Запустить подготовку всех ещё не подготовленных кандидатов раздела.

    Уже запущенные и ожидающие эксперта/графику процессы не дублируются.
    Кандидаты с ошибкой или прерванным процессом запускаются повторно.
    """
    if candidate_kind not in SUPPORTED_KINDS:
        raise ValueError("Неизвестная группа кандидатов")
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)
    if not snapshot:
        raise SectionReplicationNotFound("Сначала сформируйте и сохраните оптимизацию раздела")

    signals = [
        signal
        for signal in (snapshot.get("signals") or [])
        if signal.get("kind") == candidate_kind and signal.get("signal_id")
    ]
    if not signals:
        raise SectionReplicationNotFound("В сохранённом снимке нет кандидатов выбранной группы")

    started: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []
    for signal in signals:
        signal_id = str(signal["signal_id"])
        try:
            started.append(
                start_replication(
                    code,
                    signal_id,
                    object_id=resolved_object_id,
                    target_project_ids=list(signal.get("target_project_ids") or []),
                )
            )
        except SectionReplicationConflict:
            existing = _active_job_for_signal(code, resolved_object_id, signal_id)
            skipped.append({
                "signal_id": signal_id,
                "replication_id": (existing or {}).get("replication_id"),
                "status": (existing or {}).get("status") or "already_started",
            })
        except Exception as exc:  # один кандидат не должен останавливать весь раздел
            failed.append({"signal_id": signal_id, "error": str(exc)})

    return {
        "total_candidates": len(signals),
        "candidate_kind": candidate_kind,
        "started_count": len(started),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "replications": started,
        "skipped": skipped,
        "failed": failed,
    }


def _mark_interrupted_if_needed(job: dict) -> dict:
    if job.get("status") not in _ACTIVE_STATUSES:
        return job
    if _lease_is_held(job):
        return job
    job["status"] = "interrupted"
    job["error"] = "Сервер был перезапущен во время подготовки. Запустите процесс повторно."
    for stage in job.get("stages") or []:
        if stage.get("status") in {"pending", "running"}:
            stage.update({"status": "interrupted", "message": job["error"], "finished_at": _utc_now()})
    _finish_attempt(job, "interrupted", job["error"])
    _write_job(job)
    return job


def get_replication(
    section: str,
    replication_id: str,
    *,
    object_id: Optional[str] = None,
    include_dossier: bool = True,
) -> dict:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)
    with _LOCK:
        job = _load_job(_job_path(code, resolved_object_id, replication_id))
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        job = _mark_interrupted_if_needed(job)
        result = _public_job(job, include_dossier=include_dossier)
        result["input_stale"] = _job_input_stale(job, snapshot)
        return result


def list_replications(section: str, *, object_id: Optional[str] = None) -> list[dict]:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)
    with _LOCK:
        jobs: list[dict] = []
        for path in _replications_dir(code, resolved_object_id).glob("*.json"):
            job = _load_job(path)
            if not job:
                continue
            job = _mark_interrupted_if_needed(job)
            public = _public_job(job)
            public["input_stale"] = _job_input_stale(job, snapshot)
            jobs.append(public)
        return sorted(jobs, key=lambda item: str(item.get("created_at") or ""), reverse=True)


__all__ = [
    "SectionReplicationConflict",
    "SectionReplicationNotFound",
    "get_replication",
    "list_replications",
    "retry_graphics",
    "save_expert_decision",
    "answer_data_request",
    "start_all_replications",
    "start_replication",
    "update_implementation_check",
    "update_alternative_evaluation",
    "save_dependency_review",
]
