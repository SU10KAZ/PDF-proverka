"""Сохраняемый процесс тиражирования принятого решения на уровне раздела.

Запуск не меняет PDF, спецификации и expert_review проектов. Он фиксирует
версию снимка, исходные принятые решения и целевые строки, после чего готовит
досье для отдельного экспертного решения.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.services.common import object_service
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


logger = logging.getLogger(__name__)

_LOCK = threading.RLock()
_ACTIVE_TASKS: dict[str, "asyncio.Task[Any]"] = {}
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
    ("expert", "Решение эксперта"),
)


class SectionReplicationNotFound(RuntimeError):
    """Кандидат или процесс тиражирования не найден."""


class SectionReplicationConflict(RuntimeError):
    """Тиражирование уже запущено либо находится не в том статусе."""


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
    return _normalize_legacy_job(job) if job is not None else None


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


def _stage_ref(job: dict, key: str) -> dict:
    for stage in job.get("stages") or []:
        if stage.get("key") == key:
            return stage
    raise KeyError(key)


def _write_job(job: dict) -> None:
    job["updated_at"] = _utc_now()
    _write_json(_job_path(job["section"], job["object_id"], job["replication_id"]), job)


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
    if not signal or signal.get("kind") != "replicate_accepted_optimization":
        raise SectionReplicationNotFound("Кандидат на тиражирование не найден в сохранённом снимке")
    return signal


def _active_job_for_signal(section: str, object_id: str, signal_id: str) -> Optional[dict]:
    for path in _replications_dir(section, object_id).glob("*.json"):
        job = _load_job(path)
        if not job or job.get("signal_id") != signal_id:
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
        if not source_decisions:
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
                            "quantity", "note", "source",
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
                    "signal_id", "title", "reason", "match_basis", "match_score",
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
                "Тиражирование не изменяет исходные PDF и спецификации автоматически.",
                "Решение принимается отдельно для каждого целевого проекта.",
                "Пожарное исполнение и степень IP должны сохраняться.",
                "При графической зависимости сначала требуется проверка связанного листа или блока.",
            ],
        }
        _finish_stage(
            job,
            "package",
            "Досье тиражирования сохранено",
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
        _ACTIVE_TASKS.pop(task_key, None)


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

    with _LOCK:
        existing = _active_job_for_signal(code, resolved_object_id, signal_id)
        if existing:
            raise SectionReplicationConflict("Процесс тиражирования этого кандидата уже запущен")
        now = _utc_now()
        job = {
            "schema_version": 3,
            "replication_id": "repl-" + uuid.uuid4().hex[:12],
            "section": code,
            "object_id": resolved_object_id,
            "signal_id": signal_id,
            "title": signal.get("title") or "Тиражирование принятого решения",
            "status": "queued",
            "error": "",
            "created_at": now,
            "updated_at": now,
            "prepared_at": None,
            "snapshot_generated_at": (snapshot.get("meta") or {}).get("generated_at"),
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
        }
        _write_job(job)
        task = asyncio.create_task(
            _prepare_replication(job, snapshot, signal),
            name=job["replication_id"],
        )
        _ACTIVE_TASKS[job["replication_id"]] = task
        return _public_job(job)


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
        _ACTIVE_TASKS.pop(task_key, None)


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

        job["graphics_status"] = "running"
        job["error"] = ""
        _write_job(job)
        task = asyncio.create_task(
            _run_graphics_retry(job, assessments),
            name=f"{job['replication_id']}-graphics-retry",
        )
        _ACTIVE_TASKS[job["replication_id"]] = task
        return _public_job(job)


def start_all_replications(
    section: str,
    *,
    object_id: Optional[str] = None,
) -> dict:
    """Запустить подготовку всех ещё не подготовленных кандидатов раздела.

    Уже запущенные и ожидающие эксперта/графику процессы не дублируются.
    Кандидаты с ошибкой или прерванным процессом запускаются повторно.
    """
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    snapshot = get_latest_snapshot(code, object_id=resolved_object_id)
    if not snapshot:
        raise SectionReplicationNotFound("Сначала сформируйте и сохраните оптимизацию раздела")

    signals = [
        signal
        for signal in (snapshot.get("signals") or [])
        if signal.get("kind") == "replicate_accepted_optimization" and signal.get("signal_id")
    ]
    if not signals:
        raise SectionReplicationNotFound("В сохранённом снимке нет кандидатов на тиражирование")

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
    task = _ACTIVE_TASKS.get(str(job.get("replication_id") or ""))
    if task and not task.done():
        return job
    job["status"] = "interrupted"
    job["error"] = "Сервер был перезапущен во время подготовки. Запустите процесс повторно."
    for stage in job.get("stages") or []:
        if stage.get("status") in {"pending", "running"}:
            stage.update({"status": "interrupted", "message": job["error"], "finished_at": _utc_now()})
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
    with _LOCK:
        job = _load_job(_job_path(code, resolved_object_id, replication_id))
        if not job:
            raise SectionReplicationNotFound("Процесс тиражирования не найден")
        job = _mark_interrupted_if_needed(job)
        return _public_job(job, include_dossier=include_dossier)


def list_replications(section: str, *, object_id: Optional[str] = None) -> list[dict]:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        jobs: list[dict] = []
        for path in _replications_dir(code, resolved_object_id).glob("*.json"):
            job = _load_job(path)
            if not job:
                continue
            jobs.append(_public_job(_mark_interrupted_if_needed(job)))
        return sorted(jobs, key=lambda item: str(item.get("created_at") or ""), reverse=True)


__all__ = [
    "SectionReplicationConflict",
    "SectionReplicationNotFound",
    "get_replication",
    "list_replications",
    "retry_graphics",
    "start_all_replications",
    "start_replication",
]
