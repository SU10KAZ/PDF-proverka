"""Шаг 6A/10 — подключение non-destructive write-чокпоинтов к V2_PRIMARY.

Активно ТОЛЬКО при `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary`. В проде
этот флаг НЕ выставлен (`legacy`), поэтому весь код модуля — мёртвая ветка в
production: чокпоинты вызывают его лишь после явной проверки `v2_is_primary()`.

Семантика v2-primary (наследуется от `StorageWriteFacade._execute`):
  * v2-запись ПЕРВИЧНА — исключение пробрасывается (primary failure НЕ
    маскируется legacy-плечом);
  * legacy — fail-soft архив (выполняется ТОЛЬКО после успешной v2-записи).

Резолв `V2Target` из legacy `project_id` переиспользует проверенные helpers из
`scripts/projects_v2/v2lib.py` (object_id_for / resolve_object_folder), как и
production-миграция, поэтому адрес документа в v2 совпадает с миграционным.
"""
from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.storage import v2_primary_prototype as _proto
from backend.app.services.storage.storage_write_facade import (
    StorageWriteFacade,
    V2Target,
    v2_is_primary,
)


_V2LIB_CACHE = None


def v2_primary_enabled() -> bool:
    """Service-layer gate for v2-primary branches used by API routers."""
    return v2_is_primary()


def _load_v2lib():
    """Лениво загрузить scripts/projects_v2/v2lib.py (переиспользует loader фасада).

    Кэшируется на модуль, чтобы не re-exec'ить v2lib на каждый вызов (loader
    фасада кэширует на инстанс, а инстанс здесь одноразовый)."""
    global _V2LIB_CACHE
    if _V2LIB_CACHE is None:
        _V2LIB_CACHE = StorageWriteFacade()._load_v2lib()
    return _V2LIB_CACHE


def _root_entry(legacy_project_dir: Path) -> Path:
    """Верхнеуровневая запись проекта: контейнер `(main)` или сам проект —
    ровно то, что `migrate_project` ожидает как `project_path`."""
    p = Path(legacy_project_dir).resolve()
    try:
        from backend.app.services.common.version_service import container_dir_for
        c = container_dir_for(p)
        if c is not None:
            return Path(c).resolve()
    except Exception:
        pass
    parent = p.parent
    if parent.name.endswith("(main)") and (parent / "version_group.json").exists():
        return parent
    return p


def resolve_v2_target(
    legacy_project_dir: Path,
    version_id: str,
    *,
    v2_root: Path,
    objects_map: Optional[dict] = None,
) -> Optional[V2Target]:
    """Построить `V2Target` ТЕМ ЖЕ способом, что и `v2lib.migrate_project`,
    чтобы адрес документа в v2 совпадал с миграционным (никакого
    path-divergence): discipline = имя папки-раздела, document_code =
    `document_code_for(root_entry)` (снимает `.pdf`, читает контейнерный
    `logical_project_id`), object_folder = `allocate_object_folder(...)`.

    Возвращает None, если раздел/код документа не извлекаются — в v2-primary
    это surfaced failure (а НЕ молчаливый откат в legacy).
    """
    v2lib = _load_v2lib()
    root_entry = _root_entry(legacy_project_dir)

    parent = root_entry.parent
    object_dir = parent.parent
    # слишком мелкий путь (нет уровня объект/раздел) → не резолвится
    if object_dir == object_dir.parent:
        return None

    discipline = parent.name
    if not discipline:
        return None

    if objects_map is None:
        # objects.json лежит в <DATA>/backend/app/data; <DATA> = parent от projects_v2
        objects_map = v2lib.load_objects_map(root=Path(v2_root).parent)
    object_id = v2lib.object_id_for(object_dir, objects_map)
    object_folder = v2lib.allocate_object_folder(
        Path(v2_root), object_id, object_dir.name,
    ).name
    document_code = (v2lib.document_code_for(root_entry) or "").strip()
    if not document_code:
        return None

    return V2Target(
        object_folder=object_folder,
        discipline=v2lib.safe_component(discipline),
        document_code=v2lib.safe_component(document_code),
        version_id=version_id or "v001",
    )


def resolve_v2_target_by_id(
    project_id: str,
    version_id: str,
    *,
    v2_root: Path,
    object_id: Optional[str] = None,
    legacy_project_dir: Optional[Path] = None,
) -> Optional[V2Target]:
    """v2-native резолв V2Target по project_id — БЕЗ зависимости от legacy.

    project_id = путь относительно projects/, его basename = document_code
    (инвариант project_versions.md). Документ ищется в projects_v2 через adapter
    (опц. в рамках `object_id` — как на read-стороне), поэтому работает даже
    когда legacy `projects/` недоступен. Фолбэк на legacy-структурный
    `resolve_v2_target` — только если документа в v2 ещё нет И передан
    существующий legacy-путь (новый, ещё не мигрированный проект).

    Если документ найден, но запрошенная версия ОТСУТСТВУЕТ в нём (legacy/v2
    drift) — возвращает None (surfaced failure), а НЕ фабрикует `versions/vNNN`.
    """
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.storage_write_facade import normalize_vid_for_disk

    doc_code = os.path.basename(str(project_id or "").strip().rstrip("/"))
    candidates = [doc_code] if doc_code else []
    if doc_code.lower().endswith(".pdf"):
        candidates.append(doc_code[:-4].strip())  # gotcha `.pdf`-суффикс
    want_vid = normalize_vid_for_disk(version_id or "v001")
    if candidates:
        adapter = ProjectsV2Adapter(Path(v2_root))
        for code in candidates:
            if not code:
                continue
            doc = adapter.find_document(code, object_id=object_id)
            if doc is None:
                continue
            # версия обязана существовать в v2-документе (без фабрикации пути)
            doc_vids = {normalize_vid_for_disk(v) for v in (doc.get("version_ids") or []) if v}
            if doc_vids and want_vid not in doc_vids:
                return None  # legacy/v2 drift — явный отказ
            return V2Target(
                object_folder=doc["object_folder"],
                discipline=doc["discipline"],
                document_code=doc["document_code"],
                version_id=version_id or "v001",
            )
    # фолбэк: legacy-структурный резолв (нужен существующий legacy-путь)
    if legacy_project_dir is not None and Path(legacy_project_dir).exists():
        return resolve_v2_target(legacy_project_dir, version_id, v2_root=v2_root)
    return None


def resolve_v2_job_paths(
    project_id: str,
    version_id: str,
    *,
    run_id: Optional[str],
    v2_root: Optional[Path] = None,
    object_id: Optional[str] = None,
    legacy_project_dir: Optional[Path] = None,
) -> Optional[tuple]:
    """v2-primary аналог `manager._resolve_job_paths`.

    Возвращает `(doc_dir, version_dir, output_dir)` в раскладке projects_v2:
      * doc_dir     = `.../documents/<code>`;
      * version_dir = `.../versions/<vid>` (источник: 01_input/ + 02_work/);
      * output_dir  = `version_dir/03_analysis/runs/<run_id>` — это эквивалент
        legacy `_output` (миграция кладёт verbatim-_output именно сюда).
    None — если v2-target/run_id не резолвятся (surfaced failure, без legacy).
    """
    facade = StorageWriteFacade(v2_root=v2_root) if v2_root is not None else StorageWriteFacade()
    resolved = facade.v2_root()
    if resolved is None:
        return None
    target = resolve_v2_target_by_id(
        project_id, version_id, v2_root=resolved,
        object_id=object_id, legacy_project_dir=legacy_project_dir,
    )
    if target is None:
        return None
    safe_run = os.path.basename((run_id or "").strip())
    if not safe_run or safe_run in (".", ".."):
        return None
    doc_dir = target.doc_dir(resolved)
    version_dir = target.version_dir(resolved)
    output_dir = version_dir / "03_analysis" / "runs" / safe_run
    return doc_dir, version_dir, output_dir


def _current_v2_version_id(project_id: str, *, v2_root: Path, object_id: Optional[str] = None) -> Optional[str]:
    """Resolve current projects_v2 version id without legacy fallback."""
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    doc_code = os.path.basename(str(project_id or "").strip().rstrip("/"))
    candidates = [doc_code] if doc_code else []
    if doc_code.lower().endswith(".pdf"):
        candidates.append(doc_code[:-4].strip())
    adapter = ProjectsV2Adapter(Path(v2_root))
    for code in candidates:
        if not code:
            continue
        doc = adapter.find_document(code, object_id=object_id)
        if doc is None:
            continue
        current = doc.get("current_version")
        if current:
            return current
        versions = [v for v in (doc.get("version_ids") or []) if v]
        if versions:
            return versions[-1]
    return None


def resolve_v2_prepare_paths(
    project_id: str,
    version_id: Optional[str],
    *,
    v2_root: Optional[Path] = None,
    object_id: Optional[str] = None,
    legacy_project_dir: Optional[Path] = None,
) -> Optional[tuple[Path, Path]]:
    """v2-primary prepare paths: ``(version_dir, latest_analysis_dir)``.

    Prepare is version-level work, not run-level work, so it writes Gemma/crop
    artifacts to ``03_analysis/latest`` and does not require a run_id.
    """
    facade = StorageWriteFacade(v2_root=v2_root) if v2_root is not None else StorageWriteFacade()
    resolved = facade.v2_root()
    if resolved is None:
        return None
    effective_vid = version_id or _current_v2_version_id(project_id, v2_root=resolved, object_id=object_id)
    if not effective_vid:
        return None
    target = resolve_v2_target_by_id(
        project_id,
        effective_vid,
        v2_root=resolved,
        object_id=object_id,
        legacy_project_dir=legacy_project_dir,
    )
    if target is None:
        return None
    version_dir = target.version_dir(resolved)
    return version_dir, version_dir / "03_analysis" / "latest"


_BACKUP_COMPONENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_backup_component(value: str, fallback: str = "item") -> str:
    safe = _BACKUP_COMPONENT_RE.sub("_", str(value or "").strip()).strip("._-")
    return safe[:80] or fallback


def _destructive_backups_dir(v2_root: Path) -> Path:
    return Path(v2_root) / "_system" / "destructive_backups"


def _confirmation_log_path(v2_root: Path) -> Path:
    return Path(v2_root) / "_system" / "destructive_confirmations.jsonl"


def _resolve_backup_dir(v2_root: Path, backup_id: str) -> Path:
    raw = str(backup_id or "").strip()
    clean = os.path.basename(raw)
    if not clean or clean != raw or clean in (".", ".."):
        raise ValueError("Некорректный backup_id")
    backups = _destructive_backups_dir(Path(v2_root)).resolve()
    path = (backups / clean).resolve()
    if not path.is_relative_to(backups):
        raise ValueError("Некорректный backup_id")
    if not path.is_dir():
        raise FileNotFoundError(f"Backup '{backup_id}' не найден")
    return path


def backup_version_before_destructive(target: V2Target, v2_root: Path, op: str) -> str:
    """Скопировать всю версию перед destructive-op и вернуть stable backup_id.

    Backup лежит вне документа: ``projects_v2/_system/destructive_backups``.
    Никакого auto-cleanup: восстановление и retention остаются явными действиями.
    """
    root = Path(v2_root)
    version_dir = target.version_dir(root)
    if not version_dir.is_dir():
        raise FileNotFoundError(f"projects_v2 version dir not found: {version_dir}")

    backups = _destructive_backups_dir(root)
    backups.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    base_id = "{ts}_{code}_{vid}__{op}".format(
        ts=ts,
        code=_safe_backup_component(target.document_code, "document"),
        vid=_safe_backup_component(target.vid_disk(), "version"),
        op=_safe_backup_component(op, "op"),
    )
    backup_dir = backups / base_id
    suffix = 1
    while backup_dir.exists():
        backup_dir = backups / f"{base_id}_{suffix}"
        suffix += 1
    shutil.copytree(version_dir, backup_dir, symlinks=True)
    return backup_dir.name


def record_destructive_confirmation(
    target: V2Target,
    v2_root: Path,
    *,
    op: str,
    backup_id: str,
    project_id: Optional[str] = None,
) -> Path:
    """Append-only JSONL log: кто/что было подтверждено перед destructive-op."""
    log_path = _confirmation_log_path(Path(v2_root))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "op": op,
        "project_id": project_id,
        "object_folder": target.object_folder,
        "discipline": target.discipline,
        "document_code": target.document_code,
        "version_id": target.vid_disk(),
        "backup_id": backup_id,
    }
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return log_path


def restore_from_backup_id(target: V2Target, v2_root: Path, backup_id: str) -> dict:
    """Восстановить ``version_dir`` из destructive backup byte-for-byte.

    Текущая версия перед заменой тоже уходит в backup, если существует. Это
    сохраняет возможность отката даже для restore-операции.
    """
    root = Path(v2_root)
    source = _resolve_backup_dir(root, backup_id)
    version_dir = target.version_dir(root)
    pre_restore_backup_id = None
    if version_dir.exists():
        pre_restore_backup_id = backup_version_before_destructive(
            target, root, "restore_clean_preimage",
        )
        shutil.rmtree(version_dir)
    version_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, version_dir, symlinks=True)
    return {
        "backup_id": backup_id,
        "pre_restore_backup_id": pre_restore_backup_id,
        "version_id": target.vid_disk(),
        "restored_path": str(version_dir),
    }


def guard_destructive_v2_primary(
    op: str, *, confirmed: bool = False, backup_id: Optional[str] = None,
) -> None:
    """Gate destructive ops in V2_PRIMARY unless backup+confirmation exists.

    Legacy/dual_write_shadow remain no-op. In v2-primary callers must create a
    backup and append the confirmation log first, then call this guard with both
    ``confirmed=True`` and ``backup_id``.
    """
    from backend.app.services.storage import storage_write_facade as _swf
    if _swf.v2_is_primary() and not (confirmed and backup_id):
        _swf.get_write_facade().block_destructive(op)


def v2_source_pdf(project_id: str, version_id: str, *, v2_root: Optional[Path] = None,
                  object_id: Optional[str] = None) -> Optional[Path]:
    """Найти исходный PDF версии в projects_v2 `01_input/` (для export/read).

    Работает без legacy. Возвращает путь к первому .pdf в 01_input версии или
    None, если документ/версия/PDF не найдены.
    """
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    facade = StorageWriteFacade(v2_root=v2_root) if v2_root is not None else StorageWriteFacade()
    resolved = facade.v2_root()
    if resolved is None:
        return None
    target = resolve_v2_target_by_id(project_id, version_id, v2_root=resolved, object_id=object_id)
    if target is None:
        return None
    adapter = ProjectsV2Adapter(resolved)
    doc_dir = target.doc_dir(resolved)
    for name in adapter.input_files(doc_dir, target.vid_disk()):
        if name.lower().endswith(".pdf"):
            return target.version_dir(resolved) / "01_input" / name
    return None


def save_project_info_v2_primary(
    project_id: str,
    data: dict,
    *,
    version_id: str,
    legacy_root: Path,
    legacy_path: Path,
    v2_root: Optional[Path] = None,
) -> bool:
    """v2-primary запись project_info (вызывается ТОЛЬКО при v2_is_primary()).

    Пишет project_info как version.json-метаданные в projects_v2 (первично);
    legacy project_info.json — fail-soft архив. Возвращает True только если
    v2-запись удалась. Любой сбой резолва/записи v2 → False (не маскируется).
    """
    facade = StorageWriteFacade(v2_root=v2_root) if v2_root is not None else StorageWriteFacade()
    resolved_root = facade.v2_root()
    if resolved_root is None:
        return False

    target = resolve_v2_target(
        Path(legacy_root), version_id, v2_root=resolved_root,
    )
    if target is None:
        # surfaced failure: в v2-primary мы НЕ откатываемся молча в legacy
        return False

    def _legacy_archive() -> Any:
        # fail-soft архив: выполняется только после успешной v2-записи (_execute)
        with open(legacy_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True

    try:
        res = _proto.write_project_metadata_v2(
            facade, target, data, legacy_write=_legacy_archive,
        )
    except Exception:
        # v2 primary write упал — НЕ маскируем legacy-плечом
        return False
    return bool(res.v2_ok)
