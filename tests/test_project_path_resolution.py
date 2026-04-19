"""Regression: resolve_project_dir должен быть object-scope-aware.

История:
    project_id вида "OV/133-23-ГК-ОВ2.2" существует в двух объектах
    (213. Мосфильмовская / 214. Alia). resolve_project_dir молча возвращал
    путь внутри текущего активного объекта, из-за чего job одного объекта
    мог писать артефакты в _output другого.

Сейчас:
    - `resolve_project_dir(pid, object_id=X)` — явно резолвит в объект X;
    - `resolve_project_dir(pid, strict=True)` — падает AmbiguousProjectError
      если scope (object_id / binding) не задан и project_id неоднозначен;
    - `pinned_object(X) / bind_object(X)` — per-task ContextVar, который
      pipeline ставит на старте job; вложенные resolve_project_dir видят X.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ─── Фикстура: два объекта с одинаковым short project_id ──────────────────
@pytest.fixture
def two_objects(tmp_path):
    """projects_dir для объектов A и B с идентичной подпапкой project_id."""
    obj_a = tmp_path / "A_root"
    obj_b = tmp_path / "B_root"
    short_pid = "OV/demo-project"
    (obj_a / short_pid / "_output").mkdir(parents=True)
    (obj_b / short_pid / "_output").mkdir(parents=True)

    objects = {
        "objects": [
            {"id": "OBJ_A", "name": "A", "projects_dir": str(obj_a),
             "created_at": "2026-04-18T00:00:00"},
            {"id": "OBJ_B", "name": "B", "projects_dir": str(obj_b),
             "created_at": "2026-04-18T00:00:00"},
        ],
        "current_id": "OBJ_B",  # current = B
    }
    objects_file = tmp_path / "objects.json"
    objects_file.write_text(json.dumps(objects, ensure_ascii=False), encoding="utf-8")

    # Патчим путь к objects.json и сбрасываем кеши project_service
    from webapp.services import object_service, project_service
    old_path = object_service.OBJECTS_FILE
    object_service.OBJECTS_FILE = objects_file
    project_service._PROJECT_DIRS_CACHE = []
    project_service._PROJECT_DIRS_CACHE_TIME = 0.0
    try:
        yield {
            "short_pid": short_pid,
            "obj_a_root": obj_a, "obj_b_root": obj_b,
            "A_id": "OBJ_A", "B_id": "OBJ_B",
            "objects_file": objects_file,
        }
    finally:
        object_service.OBJECTS_FILE = old_path
        project_service._PROJECT_DIRS_CACHE = []
        project_service._PROJECT_DIRS_CACHE_TIME = 0.0


# ─── 1) Ambiguity error без object context при strict=True ────────────────
def test_ambiguous_project_id_without_context_raises(two_objects):
    from webapp.services.project_service import (
        resolve_project_dir, AmbiguousProjectError, find_object_dirs_for,
    )
    hits = find_object_dirs_for(two_objects["short_pid"])
    assert len(hits) == 2, "фикстура должна дать один path в A и один в B"

    with pytest.raises(AmbiguousProjectError):
        resolve_project_dir(two_objects["short_pid"], strict=True)


# ─── 2) Резолв в нужный объект при правильном context ────────────────────
def test_resolve_with_explicit_object_id(two_objects):
    from webapp.services.project_service import resolve_project_dir

    p_a = resolve_project_dir(two_objects["short_pid"], object_id="OBJ_A")
    p_b = resolve_project_dir(two_objects["short_pid"], object_id="OBJ_B")
    assert str(p_a).startswith(str(two_objects["obj_a_root"]))
    assert str(p_b).startswith(str(two_objects["obj_b_root"]))


def test_resolve_with_pinned_object_binding(two_objects):
    from webapp.services.project_service import resolve_project_dir, pinned_object

    with pinned_object("OBJ_A"):
        p = resolve_project_dir(two_objects["short_pid"])
        assert str(p).startswith(str(two_objects["obj_a_root"]))

    # после выхода — снова current (B)
    p_after = resolve_project_dir(two_objects["short_pid"])
    assert str(p_after).startswith(str(two_objects["obj_b_root"]))


# ─── 3) Write-пути используют canonical path (binding) ────────────────────
def test_write_paths_use_canonical_bound_path(two_objects):
    """Во время binding все resolve_project_dir → bound object, даже если
    callers вызывают через разные helpers (audit_logger, findings_service и
    др. используют ту же функцию). Здесь проверяем саму инвариант."""
    from webapp.services.project_service import resolve_project_dir, pinned_object

    pid = two_objects["short_pid"]
    with pinned_object("OBJ_A"):
        p1 = resolve_project_dir(pid)
        p2 = resolve_project_dir(pid)  # повторный вызов, симулирует N мест
    # Оба вызова резолвятся в A, независимо от current_id=B
    assert str(p1).startswith(str(two_objects["obj_a_root"]))
    assert str(p2).startswith(str(two_objects["obj_a_root"]))


# ─── 4) Переключение current_id не сбивает уже стартованный job ───────────
def test_current_id_switch_does_not_leak_to_bound_job(two_objects):
    """Имитация: job стартовал при current_id=A, зафиксировал binding. Затем
    current_id переключили на B. Pipeline должен продолжить писать в A.
    """
    from webapp.services.project_service import resolve_project_dir, pinned_object

    pid = two_objects["short_pid"]

    # Старт job'а: фиксируем binding на A, current_id тоже A (перезаписываем).
    data = json.loads(two_objects["objects_file"].read_text(encoding="utf-8"))
    data["current_id"] = "OBJ_A"
    two_objects["objects_file"].write_text(json.dumps(data), encoding="utf-8")

    with pinned_object("OBJ_A"):
        p_before = resolve_project_dir(pid)
        assert str(p_before).startswith(str(two_objects["obj_a_root"]))

        # Оператор переключил current_id на B прямо посреди работы job'а.
        data = json.loads(two_objects["objects_file"].read_text(encoding="utf-8"))
        data["current_id"] = "OBJ_B"
        two_objects["objects_file"].write_text(json.dumps(data), encoding="utf-8")

        p_after_switch = resolve_project_dir(pid)
        assert str(p_after_switch).startswith(str(two_objects["obj_a_root"])), (
            "binding должен победить current_id"
        )


# ─── 5) ContextVar наследуется через asyncio.create_task ─────────────────
def test_binding_inherited_by_asyncio_tasks(two_objects):
    """Проверяем: sub-task, созданный в рамках _create_bound_task, видит
    binding родителя (ContextVar copied по умолчанию в asyncio)."""
    from webapp.services.project_service import resolve_project_dir, bind_object, unbind_object

    pid = two_objects["short_pid"]
    results: list[Path] = []

    async def child():
        results.append(resolve_project_dir(pid))

    async def parent():
        token = bind_object("OBJ_A")
        try:
            t = asyncio.create_task(child())
            await t
        finally:
            unbind_object(token)

    asyncio.run(parent())
    assert len(results) == 1
    assert str(results[0]).startswith(str(two_objects["obj_a_root"]))


# ─── 6) _create_bound_task из PipelineManager сам ставит binding ─────────
def test_pipeline_create_bound_task_sets_binding(two_objects, monkeypatch):
    """Проверяем именно helper из PipelineManager."""
    from webapp.services.pipeline_service import PipelineManager
    from webapp.services.project_service import resolve_project_dir
    from webapp.models.audit import AuditJob

    pid = two_objects["short_pid"]
    observed = {}

    async def child_coro():
        observed["resolved"] = resolve_project_dir(pid)

    async def runner():
        job = AuditJob(job_id="j-1", project_id=pid, object_id="OBJ_A")
        mgr = PipelineManager.__new__(PipelineManager)  # без __init__
        t = PipelineManager._create_bound_task(child_coro(), job)
        await t

    asyncio.run(runner())
    assert str(observed["resolved"]).startswith(str(two_objects["obj_a_root"]))
