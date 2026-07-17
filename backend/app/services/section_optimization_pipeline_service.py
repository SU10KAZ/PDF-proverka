"""Выполняемый pipeline сводной оптимизации раздела.

Срез ``section_optimization_service`` остаётся read-only и может быть
построен для предпросмотра. Этот модуль добавляет явный operator-triggered
конвейер: состояние job сохраняется на диске, а первые три этапа выполняются
последовательно в фоне. Текстовый и графический агенты запускаются общей
кнопкой кандидатов через сохраняемые replication jobs. Последний шаг всегда
ожидает эксперта.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.core.config import APP_DATA_DIR
from backend.app.services.common import object_service
from backend.app.services.section_optimization_service import (
    collect_section_optimization_data,
    normalize_section_optimization_data,
    synthesize_section_optimization_data,
)


_LOCK = threading.RLock()
_ACTIVE_TASKS: dict[str, "asyncio.Task[Any]"] = {}
_STAGES = (
    ("collect", "Сбор"),
    ("normalize", "Нормализация"),
    ("synthesize", "Синтез"),
    ("agent", "Умный агент"),
    ("graphics", "Графика по запросу"),
    ("review", "Эксперт"),
)


class SectionPipelineConflict(RuntimeError):
    """Для раздела уже выполняется pipeline."""


class SectionPipelineNotFound(RuntimeError):
    """Pipeline пока не запускался."""


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
        raise ValueError("Не выбран объект для pipeline раздела")
    if object_id and object_service.get_object_by_id(resolved) is None:
        raise ValueError("Объект для pipeline раздела не найден")
    return resolved


def _key(section: str, object_id: str) -> str:
    return f"{object_id}__{section}"


def _root() -> Path:
    return APP_DATA_DIR / "section_optimization"


def _legacy_root() -> Path:
    return APP_DATA_DIR / "section_optimization_pipeline"


def _safe_segment(value: str, label: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned or not all(char.isalnum() or char in "_-" for char in cleaned):
        raise ValueError(f"Недопустимый {label} для хранилища оптимизации раздела")
    return cleaned


def _section_dir(section: str, object_id: str) -> Path:
    return _root() / _safe_segment(object_id, "object_id") / _safe_segment(section, "код раздела")


def _state_path(section: str, object_id: str) -> Path:
    return _section_dir(section, object_id) / "pipeline.json"


def _snapshot_path(section: str, object_id: str) -> Path:
    return _section_dir(section, object_id) / "snapshot.json"


def _history_snapshot_path(section: str, object_id: str, run_id: str) -> Path:
    return _section_dir(section, object_id) / "history" / f"{_safe_segment(run_id, 'run_id')}.snapshot.json"


def _ensure_section_storage(section: str, object_id: str) -> Path:
    """Создать постоянную папку и мягко перенести старые flat-файлы."""
    target = _section_dir(section, object_id)
    target.mkdir(parents=True, exist_ok=True)
    legacy_state = _legacy_root() / f"{_key(section, object_id)}.json"
    legacy_snapshot = _legacy_root() / f"{_key(section, object_id)}.snapshot.json"
    for source, destination in (
        (legacy_state, target / "pipeline.json"),
        (legacy_snapshot, target / "snapshot.json"),
    ):
        if source.is_file() and not destination.exists():
            shutil.copy2(source, destination)
    return target


def section_data_dir(section: str, *, object_id: Optional[str] = None) -> Path:
    """Постоянная папка данных конкретного объекта и раздела."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        return _ensure_section_storage(code, resolved_object_id)


def _read_json(path: Path) -> Optional[dict]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)


def _stage(key: str) -> dict:
    for stage_key, title in _STAGES:
        if stage_key == key:
            return {
                "key": stage_key,
                "title": title,
                "status": "pending",
                "message": "Ожидает запуска",
                "started_at": None,
                "finished_at": None,
                "metrics": {},
            }
    raise KeyError(key)


def _new_state(section: str, object_id: str) -> dict:
    now = _utc_now()
    return {
        "schema_version": 2,
        "job_id": "section-opt-" + uuid.uuid4().hex[:12],
        "section": section,
        "object_id": object_id,
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        "error": "",
        "stages": [_stage(key) for key, _ in _STAGES],
        "snapshot_generated_at": None,
        "snapshot_persisted": False,
        "serving_previous_snapshot": False,
        "graphics_plan": None,
    }


def _stage_ref(state: dict, key: str) -> dict:
    for stage in state.get("stages") or []:
        if stage.get("key") == key:
            return stage
    raise KeyError(key)


def _ensure_stage_schema(state: dict) -> dict:
    """Добавить новые этапы в старые сохранённые pipeline без пересчёта."""
    existing = {
        str(stage.get("key") or ""): stage
        for stage in (state.get("stages") or [])
        if isinstance(stage, dict) and stage.get("key")
    }
    stages = [existing.get(key) or _stage(key) for key, _ in _STAGES]
    if state.get("status") == "ready_for_review" and "agent" not in existing:
        agent = next(stage for stage in stages if stage.get("key") == "agent")
        agent.update({
            "status": "waiting",
            "message": "Запускается общей кнопкой на вкладке «Кандидаты»",
        })
    if state.get("status") == "ready_for_review":
        graphics = next(stage for stage in stages if stage.get("key") == "graphics")
        if graphics.get("status") == "waiting":
            graphics["message"] = "Запускается автоматически, если умный агент запросил проверку блоков"
    state["stages"] = stages
    return state


def _write_state(state: dict) -> None:
    state["updated_at"] = _utc_now()
    _write_json(_state_path(state["section"], state["object_id"]), state)


def _task_alive(section: str, object_id: str) -> bool:
    task = _ACTIVE_TASKS.get(_key(section, object_id))
    return bool(task and not task.done())


def _mark_interrupted_if_needed(state: dict) -> dict:
    if state.get("status") not in {"queued", "running"}:
        return state
    if _task_alive(state["section"], state["object_id"]):
        return state
    state["status"] = "interrupted"
    state["error"] = "Сервер был перезапущен до завершения pipeline. Запустите его повторно."
    for stage in state.get("stages") or []:
        if stage.get("status") in {"pending", "running"}:
            stage["status"] = "interrupted"
            stage["message"] = "Прервано перезапуском сервера"
    _write_state(state)
    return state


def public_state(state: Optional[dict]) -> dict:
    """Не отдавать в polling тяжёлый snapshot спецификаций."""
    if not state:
        return {
            "status": "not_started",
            "job_id": None,
            "stages": [_stage(key) for key, _ in _STAGES],
            "snapshot_generated_at": None,
            "snapshot_persisted": False,
            "serving_previous_snapshot": False,
            "graphics_plan": None,
        }
    result = copy.deepcopy(_ensure_stage_schema(state))
    result.pop("object_id", None)
    return result


def get_pipeline_state(section: str, *, object_id: Optional[str] = None) -> dict:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        _ensure_section_storage(code, resolved_object_id)
        state = _read_json(_state_path(code, resolved_object_id))
        if state:
            state = _ensure_stage_schema(state)
            state = _mark_interrupted_if_needed(state)
            state["snapshot_persisted"] = _snapshot_path(code, resolved_object_id).is_file()
            return public_state(state)
        snapshot = _read_json(_snapshot_path(code, resolved_object_id))
        result = public_state(None)
        if snapshot:
            result["snapshot_generated_at"] = (snapshot.get("meta") or {}).get("generated_at")
            result["snapshot_persisted"] = True
        return result


def get_latest_snapshot(section: str, *, object_id: Optional[str] = None) -> Optional[dict]:
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        _ensure_section_storage(code, resolved_object_id)
        return _read_json(_snapshot_path(code, resolved_object_id))


def store_latest_snapshot(
    section: str,
    snapshot: dict,
    *,
    object_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> None:
    """Атомарно сохранить текущий снимок и его историческую копию."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    archive_id = run_id or ("initial-" + uuid.uuid4().hex[:12])
    with _LOCK:
        _ensure_section_storage(code, resolved_object_id)
        _write_json(_history_snapshot_path(code, resolved_object_id, archive_id), snapshot)
        _write_json(_snapshot_path(code, resolved_object_id), snapshot)


def _begin_stage(state: dict, key: str, message: str) -> None:
    stage = _stage_ref(state, key)
    stage.update({"status": "running", "message": message, "started_at": _utc_now(), "finished_at": None, "metrics": {}})
    _write_state(state)


def _finish_stage(state: dict, key: str, message: str, metrics: Optional[dict] = None) -> None:
    stage = _stage_ref(state, key)
    stage.update({"status": "done", "message": message, "finished_at": _utc_now(), "metrics": metrics or {}})
    _write_state(state)


async def _run_pipeline(state: dict) -> None:
    key = _key(state["section"], state["object_id"])
    try:
        state["status"] = "running"
        _write_state(state)

        _begin_stage(state, "collect", "Собираем актуальные спецификации и решения проектов")
        collected = await asyncio.to_thread(
            collect_section_optimization_data,
            state["section"],
            object_id=state["object_id"],
        )
        _finish_stage(
            state,
            "collect",
            "Исходные данные собраны",
            {
                "projects": collected.get("section_project_count", 0),
                "specification_rows": len(collected.get("specification_rows") or []),
                "accepted_optimizations": len(collected.get("accepted_optimizations") or []),
            },
        )

        _begin_stage(state, "normalize", "Нормализуем номенклатуру и ключи сопоставления")
        normalized = await asyncio.to_thread(normalize_section_optimization_data, collected)
        _finish_stage(
            state,
            "normalize",
            "Номенклатура нормализована с сохранением источников",
            {"normalized_rows": len(normalized.get("specification_rows") or [])},
        )

        _begin_stage(state, "synthesize", "Ищем проекты для тиражирования принятых решений")
        snapshot = await asyncio.to_thread(synthesize_section_optimization_data, normalized)
        store_latest_snapshot(
            state["section"],
            snapshot,
            object_id=state["object_id"],
            run_id=state["job_id"],
        )
        state["snapshot_generated_at"] = snapshot.get("meta", {}).get("generated_at") or _utc_now()
        state["snapshot_persisted"] = True
        state["serving_previous_snapshot"] = False
        _finish_stage(
            state,
            "synthesize",
            "Кандидаты сформированы",
            {
                "replication_candidates": snapshot.get("meta", {}).get("replication_candidates", 0),
                "signals": snapshot.get("meta", {}).get("signals", 0),
            },
        )

        agent = _stage_ref(state, "agent")
        agent.update({
            "status": "waiting",
            "message": "Запускается общей кнопкой на вкладке «Кандидаты»",
        })

        graphics = _stage_ref(state, "graphics")
        graphics.update({
            "status": "waiting",
            "message": "Запускается автоматически, если умный агент запросил проверку блоков",
        })
        review = _stage_ref(state, "review")
        review.update({
            "status": "waiting",
            "message": "Ожидает решения эксперта; автоматически ничего не применяется",
        })
        state["status"] = "ready_for_review"
        state["completed_at"] = _utc_now()
        _write_state(state)
    except Exception as exc:  # pragma: no cover - защита фоновой задачи
        state["status"] = "failed"
        state["error"] = str(exc)
        for stage in state.get("stages") or []:
            if stage.get("status") == "running":
                stage["status"] = "failed"
                stage["message"] = str(exc)
                stage["finished_at"] = _utc_now()
                break
        _write_state(state)
    finally:
        _ACTIVE_TASKS.pop(key, None)


def start_pipeline(section: str, *, object_id: Optional[str] = None) -> dict:
    """Создать и запустить section-level pipeline в текущем event loop."""
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    key = _key(code, resolved_object_id)
    with _LOCK:
        _ensure_section_storage(code, resolved_object_id)
        if _task_alive(code, resolved_object_id):
            raise SectionPipelineConflict("Pipeline этого раздела уже выполняется")
        state = _new_state(code, resolved_object_id)
        current_snapshot = _read_json(_snapshot_path(code, resolved_object_id))
        if current_snapshot:
            state["snapshot_generated_at"] = (current_snapshot.get("meta") or {}).get("generated_at")
            state["snapshot_persisted"] = True
            state["serving_previous_snapshot"] = True
        _write_state(state)
        _ACTIVE_TASKS[key] = asyncio.create_task(_run_pipeline(state), name=state["job_id"])
        return public_state(state)


def request_graphics_plan(section: str, *, object_id: Optional[str] = None) -> dict:
    """Legacy-preview доказательного плана графической проверки.

    Оставлен для совместимости API. Реальный targeted vision теперь запускается
    автоматически внутри процесса тиражирования после решения текстового
    агента и использует его точечный вопрос.
    """
    code = _clean_section(section)
    resolved_object_id = _resolve_object_id(object_id)
    with _LOCK:
        state = _read_json(_state_path(code, resolved_object_id))
        if not state or not state.get("snapshot_generated_at"):
            raise SectionPipelineNotFound("Сначала выполните этапы «Сбор — Синтез»")
        if _task_alive(code, resolved_object_id):
            raise SectionPipelineConflict("Нельзя отобрать графику, пока основной pipeline выполняется")
        snapshot = _read_json(_snapshot_path(code, resolved_object_id)) or {}
        graphics_stage = _stage_ref(state, "graphics")
        graphics_stage.update({"status": "running", "message": "Отбираем доступные графические доказательства", "started_at": _utc_now()})
        _write_state(state)

        project_by_id = {str(project.get("project_id") or ""): project for project in snapshot.get("projects") or []}
        signals = [signal for signal in (snapshot.get("signals") or []) if signal.get("graphics_recommended")]
        project_ids = sorted({str(project_id) for signal in signals for project_id in (signal.get("project_ids") or [])})
        projects = [
            {
                "project_id": project_id,
                "project_name": (project_by_id.get(project_id) or {}).get("project_name") or project_id,
                "version_id": (project_by_id.get(project_id) or {}).get("version_id") or "",
                "graphic_blocks": int((project_by_id.get(project_id) or {}).get("graphic_blocks") or 0),
            }
            for project_id in project_ids
        ]
        plan = {
            "created_at": _utc_now(),
            "signals_count": len(signals),
            "signal_ids": [signal.get("signal_id") for signal in signals],
            "projects": projects,
            "graphic_blocks_available": sum(project["graphic_blocks"] for project in projects),
            "note": "План подготовлен. Vision/LLM не запускался: подтвердите конкретный кандидат и объём графической проверки отдельно.",
        }
        state["graphics_plan"] = plan
        _finish_stage(
            state,
            "graphics",
            "План графической проверки подготовлен без запуска модели",
            {
                "signals": plan["signals_count"],
                "projects": len(projects),
                "graphic_blocks_available": plan["graphic_blocks_available"],
            },
        )
        return public_state(state)


__all__ = [
    "SectionPipelineConflict",
    "SectionPipelineNotFound",
    "get_latest_snapshot",
    "get_pipeline_state",
    "public_state",
    "request_graphics_plan",
    "section_data_dir",
    "start_pipeline",
    "store_latest_snapshot",
]
