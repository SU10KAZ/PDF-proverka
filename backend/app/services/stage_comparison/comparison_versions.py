"""Версии сравнения стадий: несколько независимых сравнений одного объекта.

Версия сравнения — отдельная рабочая папка с тем же устройством, что и
единственное сравнение объекта до 18.11.2026::

    projects_v2/objects/<object>/comparison/            ← Версия 1 (cv001)
        stage_1/ stage_2/ document_groups.json assemblies/
        comparison_versions.json                        ← реестр версий
        versions/cv002/                                 ← Версия 2
            stage_1/ stage_2/ document_groups.json assemblies/

Версия 1 — прежняя папка ``comparison/``. Её ничего не переносит:
``session.json`` хранит абсолютные пути к PDF. Без реестра «Версия 1»
синтезируется на чтении, файл появляется при первом создании или
переименовании версии.

Сессии, раскладка пар, прогоны, решения инженера и отчёты различаются по путям
папок стадий, поэтому у каждой версии они свои. Модуль даёт:

* реестр версий: список, создание, переименование;
* разбор пути стадии в (объект, версия) — для проверок принадлежности сессий;
* строку области ``<object_id>@cvNNN`` (:func:`scope_id`). Группы документов и
  сборки передают ею версию дальше, в том числе в отдельный процесс сборки;
  ``stage_upload.resolve_object_dir`` превращает её в рабочую папку версии.
  Для Версии 1 область совпадает с ``object_id`` — прежнее поведение не
  меняется. Место, не знающее об областях, получит «объект не найден», а не
  запишет молча в Версию 1.

Уже загруженный комплект копируется в новую версию. PDF — жёсткой ссылкой:
их после импорта никто не переписывает, и это ≈95 % объёма. Остальное —
обычной копией: ``stage_storage`` пишет JSON на месте.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.common.atomic_json import load_modify_save

from . import stage_storage

logger = logging.getLogger(__name__)

LEGACY = "cv001"
SCHEMA = "stage_comparison_versions/1"
LIST_SCHEMA = "stage-comparison-versions/1"
REGISTRY_NAME = "comparison_versions.json"
VERSIONS_DIR = "versions"
STAGES = ("stage_1", "stage_2")
SCOPE_SEPARATOR = "@"
LABEL_MAX = 100
MAX_NUMBER = 999
_VERSION_RE = re.compile(r"^cv(\d{3})$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
# Жёсткая ссылка только для файлов, которые после импорта не переписываются.
_LINK_SUFFIXES = {".pdf"}


class ComparisonVersionError(ValueError):
    """Ошибка версии сравнения; ``status`` — HTTP-код для роутера."""

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ─── номера и области ───────────────────────────────────────────────────────

def is_version_id(value: Any) -> bool:
    match = _VERSION_RE.fullmatch(value) if isinstance(value, str) else None
    return bool(match) and int(match.group(1)) >= 1


def normalize(comparison_version: str | None) -> str:
    """Номер версии; пустой — Версия 1. Неверный формат — ошибка 400."""
    text = str(comparison_version or "").strip()
    if not text:
        return LEGACY
    if not is_version_id(text):
        raise ComparisonVersionError("BAD_COMPARISON_VERSION", "Некорректный номер версии сравнения")
    return text


def number(comparison_version: str) -> int:
    return int(normalize(comparison_version)[2:])


def default_label(comparison_version: str) -> str:
    return f"Версия {number(comparison_version)}"


def scope_id(object_id: str, comparison_version: str | None = None) -> str:
    """Область версии: ``object_id`` для Версии 1, иначе ``<object_id>@cvNNN``."""
    version = normalize(comparison_version)
    base = str(object_id or "")
    if not base or SCOPE_SEPARATOR in base:
        raise ComparisonVersionError("BAD_SCOPE", "Некорректный объект версии сравнения")
    return base if version == LEGACY else f"{base}{SCOPE_SEPARATOR}{version}"


def split_scope(scope: str) -> tuple[str, str]:
    """``(object_id, comparison_version)`` области; запись области одна на версию."""
    text = str(scope or "")
    if SCOPE_SEPARATOR not in text:
        return text, LEGACY
    base, version = text.rsplit(SCOPE_SEPARATOR, 1)
    if not base or SCOPE_SEPARATOR in base or not is_version_id(version) or version == LEGACY:
        raise ComparisonVersionError("BAD_SCOPE", "Некорректная область версии сравнения")
    return base, version


# ─── пути ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StagePath:
    object_dir: Path
    legacy_dir: Path
    workspace: Path
    comparison_version: str
    stage: str


def parse_stage_path(path: str | Path | None) -> StagePath | None:
    """Структурный разбор ``<obj>/comparison[/versions/cvNNN]/stage_N``; реестр не читает."""
    if not path:
        return None
    candidate = Path(str(path)).expanduser().resolve()
    if candidate.name not in STAGES:
        return None
    workspace = candidate.parent
    if workspace.name == "comparison":
        return StagePath(workspace.parent, workspace, workspace, LEGACY, candidate.name)
    versions = workspace.parent
    legacy = versions.parent
    if (is_version_id(workspace.name) and workspace.name != LEGACY
            and versions.name == VERSIONS_DIR and legacy.name == "comparison"):
        return StagePath(legacy.parent, legacy, workspace, workspace.name, candidate.name)
    return None


def version_from_stage_path(path: str | Path | None) -> str | None:
    info = parse_stage_path(path)
    return info.comparison_version if info else None


def legacy_dir_for_workspace(workspace: str | Path) -> Path:
    """Папка Версии 1 (``comparison``) по рабочей папке любой версии."""
    workspace = Path(workspace)
    if is_version_id(workspace.name) and workspace.name != LEGACY and workspace.parent.name == VERSIONS_DIR:
        return workspace.parent.parent
    return workspace


def object_dir_for_workspace(workspace: str | Path) -> Path:
    return legacy_dir_for_workspace(workspace).parent


def workspace_path(legacy_dir: str | Path, comparison_version: str) -> Path:
    version = normalize(comparison_version)
    legacy_dir = Path(legacy_dir)
    return legacy_dir if version == LEGACY else legacy_dir / VERSIONS_DIR / version


# ─── реестр ─────────────────────────────────────────────────────────────────

def registry_path(legacy_dir: str | Path) -> Path:
    return Path(legacy_dir) / REGISTRY_NAME


def clean_label(value: Any) -> str:
    """Название версии: 1–100 символов без управляющих, края обрезаются."""
    text = str(value if value is not None else "").strip()
    if not text:
        raise ComparisonVersionError("LABEL_REQUIRED", "Укажите название версии")
    if _CONTROL_RE.search(text):
        raise ComparisonVersionError("LABEL_INVALID", "Название версии содержит недопустимые символы")
    if len(text) > LABEL_MAX:
        raise ComparisonVersionError("LABEL_TOO_LONG", f"Название версии длиннее {LABEL_MAX} символов")
    return text


def _legacy_entry(legacy_dir: Path) -> dict[str, Any]:
    try:
        created = json.loads((legacy_dir / "object.json").read_text(encoding="utf-8")).get("created_at")
    except (OSError, ValueError, AttributeError):
        created = None
    return {
        "comparison_version": LEGACY,
        "number": 1,
        "label": default_label(LEGACY),
        "created_at": created,
        "created_by": None,
        "sources": {stage: {"kind": "upload"} for stage in STAGES},
    }


def _normalize_entries(raw: Any, legacy_dir: Path) -> list[dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    items = raw.get("versions") if isinstance(raw, dict) else None
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        version = item.get("comparison_version")
        if not is_version_id(version) or version in entries:
            continue
        entry = dict(item)
        entry["number"] = int(version[2:])
        try:
            entry["label"] = clean_label(entry.get("label"))
        except ComparisonVersionError:
            entry["label"] = default_label(version)
        if not isinstance(entry.get("sources"), dict):
            entry["sources"] = {}
        entries[version] = entry
    if LEGACY not in entries:
        entries[LEGACY] = _legacy_entry(legacy_dir)
    return sorted(entries.values(), key=lambda entry: entry["number"])


def _read_raw(legacy_dir: Path) -> dict[str, Any]:
    try:
        data = json.loads(registry_path(legacy_dir).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        raise ComparisonVersionError("REGISTRY_CORRUPT", "Повреждён реестр версий сравнения", 500) from exc
    return data if isinstance(data, dict) else {}


def entries_for_legacy_dir(legacy_dir: str | Path) -> list[dict[str, Any]]:
    """Записи реестра по номеру; Версия 1 есть всегда, версия без папки не видна."""
    legacy_dir = Path(legacy_dir)
    return [
        entry for entry in _normalize_entries(_read_raw(legacy_dir), legacy_dir)
        if entry["comparison_version"] == LEGACY
        or workspace_path(legacy_dir, entry["comparison_version"]).is_dir()
    ]


def stage_paths_for_legacy_dir(legacy_dir: str | Path) -> dict[str, tuple[Path, Path]]:
    """``{версия: (stage_1, stage_2)}`` всех версий; не бросает (для проверок владения)."""
    legacy_dir = Path(legacy_dir)
    try:
        entries = entries_for_legacy_dir(legacy_dir)
    except ComparisonVersionError as exc:
        logger.warning("Реестр версий сравнения %s не прочитан: %s", legacy_dir, exc)
        entries = [_legacy_entry(legacy_dir)]
    out: dict[str, tuple[Path, Path]] = {}
    for entry in entries:
        workspace = workspace_path(legacy_dir, entry["comparison_version"])
        out[entry["comparison_version"]] = (workspace / "stage_1", workspace / "stage_2")
    return out


def workspace_from_legacy(legacy_dir: str | Path, comparison_version: str | None) -> Path:
    """Рабочая папка зарегистрированной версии; неизвестная — ошибка 404."""
    version = normalize(comparison_version)
    legacy_dir = Path(legacy_dir)
    if version == LEGACY:
        return legacy_dir
    if version not in {entry["comparison_version"] for entry in entries_for_legacy_dir(legacy_dir)}:
        raise ComparisonVersionError("UNKNOWN_COMPARISON_VERSION", "Версия сравнения не найдена", 404)
    return workspace_path(legacy_dir, version)


# ─── объект ─────────────────────────────────────────────────────────────────

def _object_dirs(object_id: str, *, create: bool = False) -> tuple[dict[str, Any], Path]:
    """(объект реестра, папка Версии 1) по ``object_id`` без области."""
    from . import stage_upload

    if SCOPE_SEPARATOR in str(object_id or ""):
        raise ComparisonVersionError("BAD_SCOPE", "Ожидается объект без версии сравнения")
    try:
        return stage_upload.resolve_object_dir(object_id, create=create)
    except stage_upload.StageUploadError as exc:
        raise ComparisonVersionError("UNKNOWN_OBJECT", str(exc), 404) from exc


def workspace_dir(object_id: str, comparison_version: str | None = None) -> Path:
    _obj, legacy = _object_dirs(object_id)
    return workspace_from_legacy(legacy, comparison_version)


def require_version(object_id: str, comparison_version: str | None) -> str:
    """Проверенный номер версии объекта: формат — 400, нет такой версии — 404."""
    version = normalize(comparison_version)
    if version != LEGACY:
        workspace_dir(object_id, version)
    return version


def stage_summary(stage_dir: str | Path) -> dict[str, Any]:
    """Число текущих документов стороны и ключ комплекта для склейки одинаковых.

    Ключ — sha256 по ``(код, версия документа, sha256 input_manifest.json)``:
    у копии комплекта в другой версии он тот же.
    """
    stage_dir = Path(stage_dir)
    if not stage_storage.is_versioned_stage(stage_dir):
        try:
            count = sum(1 for item in stage_dir.rglob("*.pdf") if item.is_file()) if stage_dir.is_dir() else 0
        except OSError:
            count = 0
        return {"documents": count, "set_key": None, "reusable": False}
    items: list[list[str]] = []
    for row in stage_storage.iter_current_documents(stage_dir):
        manifest = Path(row["version_dir"]) / "01_input" / "input_manifest.json"
        try:
            digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
        except OSError:
            try:
                stat = Path(row["pdf_path"]).stat()
                digest = f"{stat.st_size}:{stat.st_mtime_ns}"
            except OSError:
                digest = ""
        code = str((row.get("document") or {}).get("document_code") or Path(row["document_dir"]).name)
        items.append([code, str(row["version_id"]), digest])
    items.sort()
    key = hashlib.sha256(json.dumps(items, ensure_ascii=False).encode("utf-8")).hexdigest() if items else None
    return {"documents": len(items), "set_key": key, "reusable": bool(items)}


def _public(entry: dict[str, Any], legacy_dir: Path) -> dict[str, Any]:
    version = entry["comparison_version"]
    workspace = workspace_path(legacy_dir, version)
    sources = entry.get("sources") or {}
    stages = {}
    for stage in STAGES:
        stages[stage] = {
            "path": str(workspace / stage),
            **stage_summary(workspace / stage),
            "source": sources.get(stage) if isinstance(sources.get(stage), dict) else {"kind": "upload"},
        }
    return {
        "comparison_version": version,
        "number": entry["number"],
        "label": entry["label"],
        "legacy": version == LEGACY,
        "created_at": entry.get("created_at"),
        "created_by": entry.get("created_by"),
        "renamed_at": entry.get("renamed_at"),
        "renamed_by": entry.get("renamed_by"),
        "stages": stages,
    }


def list_versions(object_id: str) -> dict[str, Any]:
    """Версии объекта по номеру (ничего не пишет)."""
    obj, legacy = _object_dirs(object_id)
    versions = [_public(entry, legacy) for entry in entries_for_legacy_dir(legacy)]
    return {
        "schema": LIST_SCHEMA,
        "object_id": str(obj.get("id") or object_id),
        "versions": versions,
        "latest": versions[-1]["comparison_version"],
    }


def get_version(object_id: str, comparison_version: str) -> dict[str, Any]:
    version = normalize(comparison_version)
    for item in list_versions(object_id)["versions"]:
        if item["comparison_version"] == version:
            return item
    raise ComparisonVersionError("UNKNOWN_COMPARISON_VERSION", "Версия сравнения не найдена", 404)


@contextmanager
def stage_lock(workspace: str | Path, stage: str, *, exclusive: bool) -> Iterator[None]:
    """Копия комплекта (общая) не идёт одновременно с подменой папки стадии (эксклюзивная).

    Загрузка (``stage_upload._commit_extracted_stage``) подменяет папку стадии
    целиком через ``os.replace``; копия, начатая до подмены, дочитывала бы уже
    другую папку.
    """
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}")
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    with open(workspace / f".{stage}.lock", "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _link_or_copy(src: str, dst: str, *, follow_symlinks: bool = True) -> str:
    if Path(src).suffix.lower() in _LINK_SUFFIXES:
        try:
            os.link(src, dst)
            return dst
        except OSError:
            pass
    return shutil.copy2(src, dst, follow_symlinks=follow_symlinks)


def _source_spec(spec: Any) -> dict[str, str]:
    if not isinstance(spec, dict):
        raise ComparisonVersionError("BAD_SOURCE", "Не выбран комплект стороны")
    kind = spec.get("source")
    if kind == "new":
        return {"kind": "new"}
    if kind == "existing":
        stage = spec.get("stage")
        if stage not in STAGES:
            raise ComparisonVersionError("BAD_SOURCE", "Не указана сторона комплекта")
        if not str(spec.get("comparison_version") or "").strip():
            # Без номера источник неоднозначен — молча брать Версию 1 нельзя.
            raise ComparisonVersionError("BAD_SOURCE", "Не указана версия, из которой взять комплект")
        return {"kind": "existing", "comparison_version": normalize(spec.get("comparison_version")), "stage": stage}
    raise ComparisonVersionError("BAD_SOURCE", "Неизвестный способ выбора комплекта")


def _load_modify_registry(legacy_dir: Path, mutate) -> None:
    try:
        load_modify_save(registry_path(legacy_dir), mutate, default={})
    except json.JSONDecodeError as exc:
        raise ComparisonVersionError("REGISTRY_CORRUPT", "Повреждён реестр версий сравнения", 500) from exc


def create_version(
    object_id: str,
    *,
    stage_1: dict[str, Any],
    stage_2: dict[str, Any],
    label: str | None = None,
    author: str | None = None,
) -> dict[str, Any]:
    """Создать версию: сторона — уже загруженный комплект любой версии или пустая для загрузки.

    Комплект копируется в рабочую папку новой версии (PDF — жёсткой ссылкой),
    поэтому загрузки в новую версию не трогают исходную.
    """
    obj, legacy = _object_dirs(object_id, create=True)
    oid = str(obj.get("id") or object_id)
    object_name = str(obj.get("name") or legacy.parent.name)
    specs = {"stage_1": _source_spec(stage_1), "stage_2": _source_spec(stage_2)}
    wanted_label = clean_label(label) if label not in (None, "") else None
    created: dict[str, Any] = {}

    def mutate(raw: Any) -> dict[str, Any]:
        entries = _normalize_entries(raw, legacy)
        known = {entry["comparison_version"] for entry in entries
                 if entry["comparison_version"] == LEGACY
                 or workspace_path(legacy, entry["comparison_version"]).is_dir()}
        resolved: dict[str, tuple[Path, dict[str, Any]]] = {}
        for stage, spec in specs.items():
            if spec["kind"] != "existing":
                continue
            if spec["comparison_version"] not in known:
                raise ComparisonVersionError("UNKNOWN_COMPARISON_VERSION", "Версия-источник комплекта не найдена", 404)
            source = workspace_path(legacy, spec["comparison_version"]) / spec["stage"]
            if not stage_storage.is_versioned_stage(source):
                raise ComparisonVersionError(
                    "SOURCE_NOT_VERSIONED", "Комплект хранится в старом формате — загрузите его заново", 409)
            summary = stage_summary(source)
            if not summary["documents"]:
                raise ComparisonVersionError("EMPTY_SET", "В выбранном комплекте нет документов", 409)
            resolved[stage] = (source, summary)
        if len(resolved) == 2:
            (left, left_summary), (right, right_summary) = resolved["stage_1"], resolved["stage_2"]
            if left.resolve() == right.resolve() or left_summary["set_key"] == right_summary["set_key"]:
                raise ComparisonVersionError("SAME_SET", "Слева и справа выбран один и тот же комплект", 409)

        used = {entry["number"] for entry in entries}
        versions_root = legacy / VERSIONS_DIR
        if versions_root.is_dir():
            # Папка без записи (сбой посреди создания) номер не отдаёт повторно.
            for child in versions_root.iterdir():
                match = _VERSION_RE.fullmatch(child.name)
                if match:
                    used.add(int(match.group(1)))
        next_number = max(used) + 1
        if next_number > MAX_NUMBER:
            raise ComparisonVersionError("VERSION_LIMIT", "Достигнут предел числа версий сравнения", 409)
        version = f"cv{next_number:03d}"
        final_label = wanted_label or default_label(version)
        if any(entry["label"].casefold() == final_label.casefold() for entry in entries):
            raise ComparisonVersionError("LABEL_TAKEN", "Версия с таким названием уже есть", 409)

        versions_root.mkdir(parents=True, exist_ok=True)
        target = versions_root / version
        temporary = versions_root / f".{version}.tmp-{uuid.uuid4().hex[:12]}"
        try:
            temporary.mkdir()
            for stage, (source, _summary) in resolved.items():
                with stage_lock(source.parent, source.name, exclusive=False):
                    shutil.copytree(source, temporary / stage, copy_function=_link_or_copy)
            os.replace(temporary, target)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
        # Каркас пишет object.json рабочей папки и stage.json сторон: у копии
        # стороны другой версии в нём исправляется имя стороны.
        stage_storage.ensure_comparison_object_scaffold(target, object_id=oid, object_name=object_name)

        sources: dict[str, Any] = {}
        for stage, spec in specs.items():
            if stage in resolved:
                summary = resolved[stage][1]
                sources[stage] = {**spec, "documents": summary["documents"], "set_key": summary["set_key"]}
            else:
                sources[stage] = {"kind": "new"}
        entry = {
            "comparison_version": version,
            "number": next_number,
            "label": final_label,
            "workspace": f"{VERSIONS_DIR}/{version}",
            "created_at": _now(),
            "created_by": author or None,
            "sources": sources,
        }
        created.update(entry)
        return {"schema": SCHEMA, "object_id": oid, "updated_at": _now(), "versions": [*entries, entry]}

    _load_modify_registry(legacy, mutate)
    return get_version(oid, created["comparison_version"])


def rename_version(object_id: str, comparison_version: str | None, label: Any,
                   *, author: str | None = None) -> dict[str, Any]:
    version = normalize(comparison_version)
    new_label = clean_label(label)
    obj, legacy = _object_dirs(object_id, create=True)
    oid = str(obj.get("id") or object_id)

    def mutate(raw: Any) -> dict[str, Any]:
        entries = _normalize_entries(raw, legacy)
        entry = next((item for item in entries if item["comparison_version"] == version), None)
        if entry is None or (version != LEGACY and not workspace_path(legacy, version).is_dir()):
            raise ComparisonVersionError("UNKNOWN_COMPARISON_VERSION", "Версия сравнения не найдена", 404)
        if any(item is not entry and item["label"].casefold() == new_label.casefold() for item in entries):
            raise ComparisonVersionError("LABEL_TAKEN", "Версия с таким названием уже есть", 409)
        entry["label"] = new_label
        entry["renamed_at"] = _now()
        entry["renamed_by"] = author or None
        return {"schema": SCHEMA, "object_id": oid, "updated_at": _now(), "versions": entries}

    _load_modify_registry(legacy, mutate)
    return get_version(oid, version)


__all__ = [
    "LEGACY",
    "ComparisonVersionError",
    "StagePath",
    "create_version",
    "entries_for_legacy_dir",
    "get_version",
    "legacy_dir_for_workspace",
    "list_versions",
    "normalize",
    "object_dir_for_workspace",
    "parse_stage_path",
    "rename_version",
    "require_version",
    "scope_id",
    "split_scope",
    "stage_lock",
    "stage_paths_for_legacy_dir",
    "stage_summary",
    "version_from_stage_path",
    "workspace_dir",
    "workspace_from_legacy",
]
