"""PROTOTYPE (Шаг 5/10) — v2-primary write path в изолированном режиме.

НЕ подключён к production chokepoints (это Шаг 6/10). Модуль лишь КОМПОЗИРУЕТ
уже реализованные методы `StorageWriteFacade` под режимом
`WRITE_MODE_V2_PRIMARY` и читает результат обратно через `ProjectsV2Adapter`,
чтобы доказать в tempdir: механизм v2-primary работает без зависимости от
legacy `projects/`.

Поведение по режимам наследуется от `facade._execute`:
  * `legacy`               — пишет только legacy (v2 не трогается);
  * `dual_write_shadow`    — legacy авторитетна, v2 — тень (fail-soft);
  * `projects_v2_primary`  — v2 первичен (исключение пробрасывается), legacy как
    архив (fail-soft, опционален).

Импорт/использование этого модуля НЕ меняет поведение backend/UI: production
endpoints его не вызывают.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional, Union

from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
from backend.app.services.storage.storage_write_facade import (
    StorageWriteFacade,
    V2Target,
    WriteResult,
)

# Поздние артефакты завершённого аудита — те же, что зеркалит manager после
# job.status == COMPLETED (03_findings + reviews + нормы + оптимизация + лог).
LATE_AUDIT_ARTIFACTS = (
    "03_findings.json",
    "03_findings_review.json",
    "norm_checks.json",
    "optimization.json",
    "optimization_review.json",
    "pipeline_log.json",
)


def write_project_metadata_v2(
    facade: StorageWriteFacade,
    target: V2Target,
    project_info: dict,
    *,
    legacy_write: Optional[Callable[[], Any]] = None,
) -> WriteResult:
    """Записать метаданные проекта (project_info) как version.json в v2-primary.

    В режиме v2_primary v2-запись первична; legacy_write (если передан) —
    fail-soft архив. В legacy-режиме v2 не трогается.
    """
    version_json = {
        "version_id": target.vid_disk(),
        "project_info": dict(project_info),
    }
    return facade.save_version_metadata(target, version_json, legacy_write=legacy_write)


def write_input_bundle_v2(
    facade: StorageWriteFacade,
    target: V2Target,
    files: list[tuple[str, bytes]],
    *,
    legacy_write: Optional[Callable[[], Any]] = None,
) -> WriteResult:
    """Записать исходные файлы (PDF/MD) версии в v2 `01_input/`."""
    return facade.save_input_bundle(target, files, legacy_write=legacy_write)


def write_completed_audit_artifacts_v2(
    facade: StorageWriteFacade,
    target: V2Target,
    source_output_dir: Union[str, Path],
    *,
    run_id: Optional[str] = None,
    legacy_write: Optional[Callable[[], Any]] = None,
) -> dict[str, WriteResult]:
    """Записать поздние артефакты завершённого аудита из `source_output_dir` в v2.

    `source_output_dir` — legacy `_output` (или fake-фикстура в тестах).
    Копируются ТОЛЬКО реально существующие артефакты из `LATE_AUDIT_ARTIFACTS`
    (отсутствующие молча пропускаются — без фабрикации пустых файлов).
    """
    src = Path(source_output_dir)
    results: dict[str, WriteResult] = {}
    for name in LATE_AUDIT_ARTIFACTS:
        f = src / name
        if not f.is_file():
            continue
        results[name] = facade.save_analysis_artifact(
            target, name, f.read_bytes(), run_id=run_id, legacy_write=legacy_write,
        )
    return results


def read_project_v2(v2_root: Union[str, Path], target: V2Target) -> dict:
    """READ-ONLY снимок версии из v2 (без legacy fallback).

    Возвращает то, что нужно read-path'у: найден ли документ, число замечаний,
    наличие pipeline_log, список входных файлов, состав latest-анализа.
    Если v2-снимок неполон — возвращает честные нули/пустые списки (НЕ
    фабрикует данные).
    """
    v2_root = Path(v2_root)
    adapter = ProjectsV2Adapter(v2_root)
    doc_dir = target.doc_dir(v2_root)
    vid = target.vid_disk()
    return {
        "found": (doc_dir / "document.json").is_file(),
        "findings_count": adapter.findings_count(doc_dir, vid),
        "findings": adapter.read_findings(doc_dir, vid),
        "has_pipeline_log": adapter.has_pipeline_log(doc_dir, vid),
        "input_files": adapter.input_files(doc_dir, vid),
        "analysis_files": adapter.latest_analysis_files(doc_dir, vid),
        "legacy_used": False,
    }
