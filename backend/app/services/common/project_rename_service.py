"""
project_rename_service — безопасное переименование загруженной папки проекта.

Переименовывает **саму папку** проекта (а значит и `project_id`, т.к.
`project_id = basename` папки) и синхронно переписывает все персистентные
сторы, которые ключуются по `project_id`:

  * `project_info.json` внутри папки (project_id + name);
  * `version_group.json` контейнера версий (logical_project_id / container /
    versions[].folder), если проект уже промоутнут в `<база>(main)/`;
  * `decisions_log.json`  → entries[].source_project (scope по object_id);
  * `usage_data.json`     → records[].project_id;
  * `project_groups.json` → groups[object_id][section][].project_ids;
  * legacy `missing_norms_vault.json` → norms{}.occurrences[].project_id;
    новый строковый список не содержит project_id и не требует ремапа.

Внутренние data-файлы проекта (PDF / `<имя>_document.md` / `*_result.json` и
прочие артефакты) НЕ переименовываются: они адресуются относительным именем
файла внутри папки, поэтому после переезда папки резолвятся корректно, а их
имена остаются косметическим эхом старого названия. Переименование этих файлов
потребовало бы переписать `md_file`/`pdf_file` в `project_info.json` и
внутренние ссылки артефактов — это отдельный, более рискованный объём работы.

Слой хранения (`AUDIT_PROJECTS_V2_WRITE_MODE`):
  * `legacy` / `dual_write_shadow` — переименовывается legacy-папка, а
    `projects_v2` догоняется shadow-синхронизацией (`sync_v2_shadow_rename`);
  * `projects_v2_primary` — источник истины projects_v2 (legacy-папки может уже
    не быть), работает `_rename_project_v2_primary`: переезжает папка документа
    `documents/<document_code>`, обновляются `document.json`, метаданные версий
    (`01_input/project_info.json`, `04_review/expert_review.json`) и
    `_system/old_to_new_map.json`. Как и clean/delete, операция проходит через
    safety-контракт (снимок метаданных → confirmation log → guard), но снимок
    лёгкий: rename ничего не удаляет, полный copytree версии не нужен.

Безопасность:
  * имя валидируется (без слэшей, обратных слэшей, '..', управляющих символов,
    не начинается с точки, разумная длина);
  * все целевые пути проверяются на вложенность в `projects_dir` (`resolve()`);
  * все проверки (имя, конфликт, busy) выполняются ДО первого перемещения;
  * перемещение через `shutil.move` (не shell `mv`);
  * пишется reverse-log для отката;
  * никакой аудит не запускается; running-проект переименовать нельзя.
"""
from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from backend.app.core import config
from backend.app.services.common import (
    object_service,
    project_service,
    version_service,
)

CONTAINER_SUFFIX = version_service.CONTAINER_SUFFIX
_MAX_NAME_LEN = 200


# ─── Ошибки (мапятся на HTTP-коды в роутере) ────────────────────────────────
class RenameError(Exception):
    """Базовая ошибка переименования."""


class InvalidProjectNameError(RenameError):
    """Невалидное новое имя → 400."""


class ProjectNotFoundError(RenameError):
    """Проект не найден → 404."""


class RenameConflictError(RenameError):
    """Папка/контейнер с таким именем уже существует → 409."""


class ProjectBusyError(RenameError):
    """По проекту идёт аудит, переименование запрещено → 409."""


# ─── Валидация имени ────────────────────────────────────────────────────────
def sanitize_new_name(raw: Optional[str]) -> str:
    """Очистить и провалидировать новое имя папки. Поднимает InvalidProjectNameError."""
    if raw is None:
        raise InvalidProjectNameError("Имя проекта не может быть пустым")
    name = str(raw).strip()
    if not name:
        raise InvalidProjectNameError("Имя проекта не может быть пустым")
    if len(name) > _MAX_NAME_LEN:
        raise InvalidProjectNameError(f"Слишком длинное имя (макс. {_MAX_NAME_LEN} символов)")
    if any(ord(c) < 32 for c in name):
        raise InvalidProjectNameError("Имя содержит управляющие символы")
    if "/" in name or "\\" in name:
        raise InvalidProjectNameError("Имя не может содержать '/' или '\\'")
    if name in (".", ".."):
        raise InvalidProjectNameError("Недопустимое имя")
    if name.startswith("."):
        raise InvalidProjectNameError("Имя не может начинаться с точки")
    if name.endswith(CONTAINER_SUFFIX):
        raise InvalidProjectNameError(f"Имя не может оканчиваться на '{CONTAINER_SUFFIX}'")
    return name


# ─── Вспомогательные ────────────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _within(path: Path, root: Path) -> bool:
    """Лежит ли `path` (после resolve) внутри `root` (или равен ему)."""
    try:
        rp = path.resolve()
    except Exception:
        return False
    return rp == root or root in rp.parents


def _replace_basename(project_id: str, new_base: str) -> str:
    """Заменить последний сегмент project_id (basename), сохранив префикс дисциплины."""
    parent = Path(project_id).parent
    return new_base if str(parent) in (".", "") else f"{parent.as_posix()}/{new_base}"


def _map_folder(folder: str, old_base: str, new_base: str) -> str:
    """Переименовать имя братской папки версии при смене базового имени.

    `<old_base>` → `<new_base>`;  `<old_base> V2` → `<new_base> V2`.
    Папки, не начинающиеся с old_base, не трогаем (defensive).
    """
    if folder == old_base:
        return new_base
    if folder.startswith(old_base):
        return new_base + folder[len(old_base):]
    return folder


def _atomic_write_json(path: Path, data: Any) -> None:
    tmp = path.with_name(path.name + ".tmp_rename")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ─── projects_v2 shadow sync ────────────────────────────────────────────────
# Production читает список/resolve через projects_v2 (read-cutover). Документ в
# v2 идентифицируется полем `document_code` в `document.json`, а папка документа
# ключуется по этому же коду (`documents/<document_code>`). rename папки legacy
# обязан синхронно обновить v2-shadow, иначе UI продолжает показывать старое имя.
def _v2_root(v2_root: Optional[Path] = None) -> Path:
    if v2_root is not None:
        return Path(v2_root)
    try:
        from backend.app.services.storage.projects_v2_adapter import _default_v2_root
        return _default_v2_root()
    except Exception:
        return Path(config.DATA_DIR) / "projects_v2"


def _norm_doc_name(s: Optional[str]) -> str:
    """Каноничное имя документа: без `.pdf` и без суффикса контейнера `(main)`."""
    if not s:
        return ""
    s = str(s).strip()
    if s.lower().endswith(".pdf"):
        s = s[:-4]
    if s.endswith(CONTAINER_SUFFIX):
        s = s[:-len(CONTAINER_SUFFIX)]
    return s


def _v2_old_identity_candidates(dj: dict, doc_dir: Path) -> set[str]:
    """Все значения, которыми v2-документ мог представлять СТАРОЕ имя."""
    cands = [dj.get("document_code"), dj.get("legacy_project_name"), doc_dir.name]
    lpp = dj.get("legacy_project_path")
    if lpp:
        cands.append(Path(lpp).name)
    for v in dj.get("versions", []) or []:
        if isinstance(v, dict):
            cands.append(v.get("legacy_folder_name"))
    return {_norm_doc_name(c) for c in cands if c}


def sync_v2_shadow_rename(
    old_base: str,
    new_base: str,
    *,
    object_id: Optional[str] = None,
    v2_root: Optional[Path] = None,
    backup: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Синхронизировать projects_v2-shadow под новое имя legacy-папки.

    Находит документ(ы) v2 по СТАРОЙ идентичности (в рамках object_id), обновляет
    `document_code` / `legacy_project_name` / `legacy_project_path` /
    `versions[].legacy_folder_name` и при необходимости переименовывает папку
    документа в новый код. Fail-soft: нет v2-root / нет документа → warning,
    исключение не бросается. Возвращает dict с `updated` / `renamed_dirs` /
    `fields` / `warnings`.
    """
    result: dict[str, Any] = {
        "updated": [], "renamed_dirs": [], "fields": [], "warnings": [],
        "dry_run": dry_run,
    }
    # defense-in-depth: new_base уже валидируется в rename_project, но repair-скрипт
    # может звать sync напрямую.
    new_norm = _norm_doc_name(sanitize_new_name(new_base))
    old_norm = _norm_doc_name(old_base)
    if not old_norm or not new_norm:
        result["warnings"].append("пустое old/new имя — пропуск v2-sync")
        return result

    root = _v2_root(v2_root).resolve()
    objects_root = root / "objects"
    if not objects_root.is_dir():
        result["warnings"].append(f"projects_v2 root отсутствует: {objects_root}")
        return result

    fields: set[str] = set()
    for doc_json in objects_root.glob("*/disciplines/*/documents/*/document.json"):
        dj = _load_json(doc_json)
        if not isinstance(dj, dict):
            continue
        if object_id and dj.get("object_id") and dj.get("object_id") != object_id:
            continue
        doc_dir = doc_json.parent
        if old_norm not in _v2_old_identity_candidates(dj, doc_dir):
            continue

        # ── обновить поля document.json ──
        changed: list[str] = []
        if dj.get("document_code") != new_norm:
            dj["document_code"] = new_norm
            changed.append("document_code")
        if "legacy_project_name" in dj and _norm_doc_name(dj.get("legacy_project_name")) == old_norm:
            dj["legacy_project_name"] = new_norm
            changed.append("legacy_project_name")
        lpp = dj.get("legacy_project_path")
        if lpp:
            new_lpp = str(Path(lpp).with_name(new_norm))
            if new_lpp != lpp:
                dj["legacy_project_path"] = new_lpp
                changed.append("legacy_project_path")
        for v in dj.get("versions", []) or []:
            if isinstance(v, dict) and _norm_doc_name(v.get("legacy_folder_name")) == old_norm:
                v["legacy_folder_name"] = new_norm
                changed.append("versions[].legacy_folder_name")

        if changed and not dry_run:
            if backup:
                bak = doc_json.with_name(doc_json.name + ".rename_bak")
                try:
                    bak.write_text(doc_json.read_text(encoding="utf-8"), encoding="utf-8")
                except Exception as ex:
                    result["warnings"].append(f"backup {doc_json}: {ex}")
            _atomic_write_json(doc_json, dj)

        if changed:
            result["updated"].append(str(doc_json))
            fields |= set(changed)

        # ── при необходимости переименовать папку документа в новый код ──
        # (`get_document` ключует папку по document_code → folder обязан совпасть).
        if doc_dir.name != new_norm:
            target = doc_dir.parent / new_norm
            if not _within(target, objects_root):
                result["warnings"].append(f"v2 folder target вне root: {target}")
            elif target.exists():
                result["warnings"].append(
                    f"v2 folder уже существует, переименование пропущено: {target.name}"
                )
            elif not dry_run:
                try:
                    shutil.move(str(doc_dir), str(target))
                    result["renamed_dirs"].append([str(doc_dir), str(target)])
                except Exception as ex:
                    result["warnings"].append(f"v2 folder rename {doc_dir.name}: {ex}")
            else:
                result["renamed_dirs"].append([str(doc_dir), str(target)])

    result["fields"] = sorted(fields)
    if not result["updated"] and not result["renamed_dirs"]:
        result["warnings"].append(
            f"projects_v2: документ для '{old_base}' (object_id={object_id}) не найден"
        )
    return result


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _update_project_info(info_path: Path, new_base: str) -> None:
    """project_info.json → project_id + name = new_base (version_id не трогаем)."""
    d: dict[str, Any] = {}
    if info_path.exists():
        loaded = _load_json(info_path)
        if isinstance(loaded, dict):
            d = loaded
    d["project_id"] = new_base
    d["name"] = new_base
    try:
        _atomic_write_json(info_path, d)
    except Exception:
        pass


def _object_id_for_dir(proj_dir: Path) -> Optional[str]:
    """object_id объекта, чей projects_dir является предком папки проекта."""
    try:
        objects = object_service.list_objects()
    except Exception:
        objects = []
    proj_dir = proj_dir.resolve()
    for obj in objects:
        try:
            od = Path(obj["projects_dir"]).resolve()
        except Exception:
            continue
        if proj_dir == od or od in proj_dir.parents:
            return obj.get("id")
    try:
        return object_service.get_current_id()
    except Exception:
        return None


def _is_running(project_id: str, basename: str) -> bool:
    try:
        from backend.app.pipeline.manager import pipeline_manager
    except Exception:
        return False
    for pid in {project_id, basename}:
        try:
            if pipeline_manager.is_running(pid):
                return True
        except Exception:
            continue
    return False


# ─── Ремап сторов ───────────────────────────────────────────────────────────
def _remap_decisions_log(path: Path, id_map: dict[str, str],
                         object_id: Optional[str]) -> int:
    data = _load_json(path)
    if data is None:
        return 0
    entries = data["entries"] if isinstance(data, dict) and "entries" in data else data
    if not isinstance(entries, list):
        return 0
    changed = 0
    for e in entries:
        if not isinstance(e, dict):
            continue
        if object_id is not None and e.get("object_id") and e.get("object_id") != object_id:
            continue
        sp = e.get("source_project")
        if sp in id_map:
            e["source_project"] = id_map[sp]
            changed += 1
    if changed:
        _atomic_write_json(path, data)
    return changed


def _remap_usage(path: Path, id_map: dict[str, str]) -> int:
    data = _load_json(path)
    if not isinstance(data, dict):
        return 0
    changed = 0
    for r in data.get("records", []):
        if isinstance(r, dict) and r.get("project_id") in id_map:
            r["project_id"] = id_map[r["project_id"]]
            changed += 1
    if changed:
        _atomic_write_json(path, data)
    return changed


def _remap_groups(path: Path, id_map: dict[str, str],
                  object_id: Optional[str]) -> int:
    data = _load_json(path)
    if not isinstance(data, dict):
        return 0
    changed = 0
    obj_keys = [object_id] if object_id is not None and object_id in data else list(data.keys())
    for ok in obj_keys:
        sections = data.get(ok)
        if not isinstance(sections, dict):
            continue
        for glist in sections.values():
            if not isinstance(glist, list):
                continue
            for g in glist:
                if not isinstance(g, dict):
                    continue
                ids = g.get("project_ids")
                if not isinstance(ids, list):
                    continue
                new_ids, seen = [], set()
                for pid in ids:
                    npid = id_map.get(pid, pid)
                    if npid in seen:
                        continue
                    seen.add(npid)
                    new_ids.append(npid)
                    if npid != pid:
                        changed += 1
                g["project_ids"] = new_ids
    if changed:
        _atomic_write_json(path, data)
    return changed


def _remap_vault(path: Path, id_map: dict[str, str]) -> int:
    data = _load_json(path)
    if not isinstance(data, dict):
        return 0
    norms = data.get("norms")
    if not isinstance(norms, dict):
        return 0
    changed = 0
    for entry in norms.values():
        if not isinstance(entry, dict):
            continue
        for occ in entry.get("occurrences", []) or []:
            if isinstance(occ, dict) and occ.get("project_id") in id_map:
                occ["project_id"] = id_map[occ["project_id"]]
                changed += 1
    if changed:
        _atomic_write_json(path, data)
    return changed


# ─── projects_v2-primary ветка ─────────────────────────────────────────────
def _remap_old_to_new_map(map_path: Path, old_code: str, new_code: str,
                          old_doc_dir: Path, new_doc_dir: Path) -> int:
    """`old_to_new_map.json`: document_code + пути v2 после переезда папки."""
    data = _load_json(map_path)
    if not isinstance(data, dict):
        return 0
    old_prefix = str(old_doc_dir)
    new_prefix = str(new_doc_dir)
    changed = 0

    def _repath(value: Any) -> Any:
        if isinstance(value, str) and (value == old_prefix or value.startswith(old_prefix + os.sep)):
            return new_prefix + value[len(old_prefix):]
        return value

    for entry in data.get("migrations", []) or []:
        if not isinstance(entry, dict):
            continue
        touched = False
        if entry.get("document_code") == old_code:
            entry["document_code"] = new_code
            touched = True
        for key in ("v2_document_dir", "v2_version_dir"):
            if key in entry:
                new_val = _repath(entry.get(key))
                if new_val != entry.get(key):
                    entry[key] = new_val
                    touched = True
        for f in entry.get("files", []) or []:
            if not isinstance(f, dict):
                continue
            new_val = _repath(f.get("new_path"))
            if new_val != f.get("new_path"):
                f["new_path"] = new_val
                touched = True
        if touched:
            changed += 1
    if changed:
        _atomic_write_json(map_path, data)
    return changed


def _update_v2_version_metadata(doc_dir: Path, new_base: str) -> list[str]:
    """`project_info.json` / `expert_review.json` версий → новый project_id."""
    updated: list[str] = []
    versions_root = doc_dir / "versions"
    if not versions_root.is_dir():
        return updated
    for vdir in sorted(versions_root.iterdir()):
        if not vdir.is_dir():
            continue
        info = vdir / "01_input" / "project_info.json"
        if info.exists():
            _update_project_info(info, new_base)
            updated.append(str(info))
        review = vdir / "04_review" / "expert_review.json"
        if review.exists():
            data = _load_json(review)
            if isinstance(data, dict) and data.get("project_id") not in (None, new_base):
                data["project_id"] = new_base
                try:
                    _atomic_write_json(review, data)
                    updated.append(str(review))
                except Exception:
                    pass
    return updated


def _rename_project_v2_primary(
    project_id: str,
    new_name: str,
    *,
    object_id: Optional[str] = None,
    decisions_log_file: Optional[Path] = None,
    usage_data_file: Optional[Path] = None,
    project_groups_file: Optional[Path] = None,
    missing_norms_vault_file: Optional[Path] = None,
    reverse_log_file: Optional[Path] = None,
    check_running: bool = True,
    v2_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Переименование документа в `projects_v2` (write mode = v2-primary).

    Legacy-папки в этом режиме может не быть вовсе, поэтому источник истины —
    `projects_v2/objects/<obj>/disciplines/<disc>/documents/<document_code>`.
    Операция обратима (папка переезжает, ничего не удаляется), но проходит через
    тот же safety-контракт, что и clean/delete: снимок мутируемых метаданных →
    append-only confirmation log → guard. Все проверки — ДО первой мутации.
    """
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.storage_write_facade import StorageWriteFacade, V2Target
    from backend.app.services.storage.v2_primary_wiring import (
        backup_paths_before_destructive,
        guard_destructive_v2_primary,
        record_destructive_confirmation,
    )

    if v2_root is not None:
        root = Path(v2_root).resolve()
    else:
        resolved = StorageWriteFacade().v2_root()
        if resolved is None:
            raise RenameError("projects_v2 root не настроен")
        root = Path(resolved).resolve()

    new_base = sanitize_new_name(new_name)
    new_code = _norm_doc_name(new_base)
    if not new_code:
        raise InvalidProjectNameError("Имя проекта не может быть пустым")

    adapter = ProjectsV2Adapter(root)
    doc = adapter.find_document_by_project_id(project_id, object_id=object_id)
    if doc is None:
        raise ProjectNotFoundError(f"Проект '{project_id}' не найден")

    old_code = doc["document_code"]
    doc_dir = Path(doc["doc_dir"]).resolve()
    objects_root = (root / "objects").resolve()
    if not _within(doc_dir, objects_root):
        raise ProjectNotFoundError(f"Проект '{project_id}' вне projects_v2")
    if object_id is None:
        object_id = doc.get("object_id")

    new_project_id = _replace_basename(project_id, new_base)
    warnings: list[str] = []

    if new_code == _norm_doc_name(old_code) and doc_dir.name == new_code:
        return {
            "status": "renamed", "project_id": project_id,
            "old_name": old_code, "new_name": new_code,
            "old_path": str(doc_dir), "new_path": str(doc_dir),
            "storage_layer": "projects_v2", "stores": {},
            "warnings": ["Имя не изменилось"],
        }

    if check_running and _is_running(project_id, old_code):
        raise ProjectBusyError(
            f"По проекту '{old_code}' идёт аудит. Сначала отмените его."
        )

    new_doc_dir = doc_dir.parent / new_code
    if not _within(doc_dir.parent, objects_root):
        raise RenameError("Целевой путь вне projects_v2")
    if new_doc_dir.exists():
        raise RenameConflictError(f"Документ '{new_code}' уже существует")
    if adapter.find_document(new_code, object_id=object_id) is not None:
        raise RenameConflictError(f"Проект '{new_code}' уже существует в этом объекте")

    # ── safety-контракт: снимок метаданных → confirmation → guard ──
    map_path = root / "_system" / "old_to_new_map.json"
    backup_id = backup_paths_before_destructive(
        [doc_dir / "document.json", map_path], root, "rename_project", label=old_code,
    )
    target = V2Target(
        object_folder=doc["object_folder"],
        discipline=doc["discipline"],
        document_code=old_code,
        version_id=doc.get("current_version") or "v001",
    )
    record_destructive_confirmation(
        target, root, op="rename_project", backup_id=backup_id, project_id=project_id,
    )
    guard_destructive_v2_primary("rename_project", confirmed=True, backup_id=backup_id)

    # ── APPLY: перенос папки документа + поля document.json ──
    v2_sync = sync_v2_shadow_rename(
        old_code, new_code, object_id=object_id, v2_root=root,
    )
    warnings.extend(f"projects_v2: {w}" for w in v2_sync.get("warnings", []))
    if not new_doc_dir.is_dir():
        raise RenameError(
            "projects_v2: папка документа не переименована "
            f"({old_code} → {new_code}); warnings={v2_sync.get('warnings')}"
        )

    id_map = {old_code: new_code}
    reverse: dict[str, Any] = {
        "at": _now_iso(), "old_base": old_code, "new_base": new_code,
        "storage_layer": "projects_v2", "backup_id": backup_id,
        "moves": [[str(new_doc_dir), str(doc_dir)]],
        "id_map": id_map, "object_id": object_id, "stores": {},
    }

    # ── метаданные версий + карта миграции ──
    try:
        reverse["version_metadata"] = _update_v2_version_metadata(new_doc_dir, new_code)
    except Exception as ex:
        warnings.append(f"метаданные версий: {ex}")
    try:
        reverse["old_to_new_map"] = _remap_old_to_new_map(
            map_path, old_code, new_code, doc_dir, new_doc_dir,
        )
    except Exception as ex:
        warnings.append(f"old_to_new_map: {ex}")

    # ── ремап сторов, ключуемых по project_id ──
    stores: dict[str, int] = {}
    remaps = (
        ("decisions_log", decisions_log_file or config.DECISIONS_LOG_FILE,
         lambda p: _remap_decisions_log(p, id_map, object_id)),
        ("usage_data", usage_data_file or config.USAGE_DATA_FILE,
         lambda p: _remap_usage(p, id_map)),
        ("project_groups", project_groups_file or config.PROJECT_GROUPS_FILE,
         lambda p: _remap_groups(p, id_map, object_id)),
        ("missing_norms_vault", missing_norms_vault_file or config.MISSING_NORMS_VAULT_FILE,
         lambda p: _remap_vault(p, id_map)),
    )
    for name, path, fn in remaps:
        try:
            stores[name] = fn(Path(path))
        except Exception as ex:
            warnings.append(f"{name}: {ex}")
    reverse["stores"] = stores

    rl = Path(reverse_log_file) if reverse_log_file else (config.APP_DATA_DIR / "project_rename.reverse.json")
    try:
        _atomic_write_json(rl, reverse)
    except Exception as ex:
        warnings.append(f"reverse-log: {ex}")

    project_service.invalidate_project_cache()

    print(
        f"[rename_project] projects_v2: {doc_dir} -> {new_doc_dir} | "
        f"backup={backup_id} | stores={stores} | object_id={object_id} | "
        f"warnings={warnings}"
    )

    return {
        "status": "renamed",
        "project_id": new_project_id,
        "old_name": old_code,
        "new_name": new_code,
        "old_path": str(doc_dir),
        "new_path": str(new_doc_dir),
        "storage_layer": "projects_v2",
        "stores": stores,
        "backup_id": backup_id,
        "v2_shadow": v2_sync,
        "warnings": warnings,
    }


# ─── Главная функция ──────────────────────────────────────────────────────
def rename_project(
    project_id: str,
    new_name: str,
    *,
    projects_dir: Optional[Path] = None,
    object_id: Optional[str] = None,
    decisions_log_file: Optional[Path] = None,
    usage_data_file: Optional[Path] = None,
    project_groups_file: Optional[Path] = None,
    missing_norms_vault_file: Optional[Path] = None,
    reverse_log_file: Optional[Path] = None,
    check_running: bool = True,
    sync_v2: bool = True,
    v2_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Переименовать папку проекта и синхронизировать ссылки.

    Возвращает dict с new project_id / old_name / new_name / old_path /
    new_path / storage_layer / store-counts / warnings.
    """
    # В V2_PRIMARY источник истины — projects_v2 (legacy-папки может не быть):
    # отдельная ветка с обязательным backup+confirmation. В legacy/
    # dual_write_shadow guard — no-op, работает прежний legacy-путь ниже.
    from backend.app.services.storage.storage_write_facade import v2_is_primary
    from backend.app.services.storage.v2_primary_wiring import guard_destructive_v2_primary
    if v2_is_primary():
        return _rename_project_v2_primary(
            project_id, new_name,
            object_id=object_id,
            decisions_log_file=decisions_log_file,
            usage_data_file=usage_data_file,
            project_groups_file=project_groups_file,
            missing_norms_vault_file=missing_norms_vault_file,
            reverse_log_file=reverse_log_file,
            check_running=check_running,
            v2_root=v2_root,
        )
    guard_destructive_v2_primary("rename_project")
    new_base = sanitize_new_name(new_name)

    if projects_dir is None:
        projects_dir = project_service._get_projects_dir()
    projects_dir = Path(projects_dir)
    if not projects_dir.exists():
        raise ProjectNotFoundError("Каталог проектов не найден")
    root = projects_dir.resolve()

    proj_dir = project_service.resolve_project_dir(project_id, object_id=object_id)
    if not proj_dir.exists():
        raise ProjectNotFoundError(f"Проект '{project_id}' не найден")
    proj_dir = proj_dir.resolve()
    if not _within(proj_dir, root):
        raise ProjectNotFoundError(f"Проект '{project_id}' вне каталога проектов")

    old_base = proj_dir.name
    warnings: list[str] = []

    if new_base == old_base:
        return {
            "status": "renamed", "project_id": project_id,
            "old_name": old_base, "new_name": new_base,
            "old_path": str(proj_dir), "new_path": str(proj_dir),
            "storage_layer": "legacy", "stores": {},
            "warnings": ["Имя не изменилось"],
        }

    if check_running and _is_running(project_id, old_base):
        raise ProjectBusyError(
            f"По проекту '{old_base}' идёт аудит. Сначала отмените его."
        )

    if object_id is None:
        object_id = _object_id_for_dir(proj_dir)

    container = version_service.container_dir_for(proj_dir)

    # id_map: basename старой версии → новой (для ремапа сторов). Для контейнера
    # это V1 + все братские версии (V2…), у которых source_project = имя папки.
    id_map: dict[str, str] = {}
    new_project_id = _replace_basename(project_id, new_base)
    reverse: dict[str, Any] = {
        "at": _now_iso(), "old_base": old_base, "new_base": new_base,
        "moves": [], "stores": {},
    }

    # ── Сборка плана + проверки конфликтов ДО любых перемещений ──
    if container is None:
        # Плоский (одноверсионный) проект.
        new_dir = proj_dir.parent / new_base
        if new_dir.exists():
            raise RenameConflictError(f"Папка '{new_base}' уже существует")
        if not _within(new_dir, root):
            raise RenameError("Целевой путь вне каталога проектов")
        storage_layer = "legacy"
        id_map[old_base] = new_base

        # APPLY
        shutil.move(str(proj_dir), str(new_dir))
        reverse["moves"].append([str(new_dir), str(proj_dir)])
        _update_project_info(new_dir / "project_info.json", new_base)
        final_path = new_dir
    else:
        # Контейнерный (версионный) проект `<old_base>(main)/…`.
        container = container.resolve()
        if container.name != f"{old_base}{CONTAINER_SUFFIX}":
            # proj_dir.name (V1) должен совпадать с базой контейнера.
            warnings.append(
                f"Контейнер '{container.name}' не совпадает с базой '{old_base}'"
            )
        container_base = (
            container.name[:-len(CONTAINER_SUFFIX)]
            if container.name.endswith(CONTAINER_SUFFIX) else old_base
        )
        new_container = container.parent / f"{new_base}{CONTAINER_SUFFIX}"
        if new_container.exists():
            raise RenameConflictError(f"Контейнер '{new_container.name}' уже существует")
        if not _within(new_container, root):
            raise RenameError("Целевой путь вне каталога проектов")
        storage_layer = "container"

        manifest = version_service._read_group_manifest_raw(container) or {}
        folder_map: dict[str, str] = {}
        for v in manifest.get("versions", []):
            folder = v.get("folder") or "."
            if folder in (".", ""):
                continue
            nf = _map_folder(folder, container_base, new_base)
            folder_map[folder] = nf
            if nf != folder:
                id_map[folder] = nf
        # Конфликты дочерних папок (после переезда контейнера).
        for folder, nf in folder_map.items():
            if nf != folder and (container / nf).exists() and (container / folder) != (container / nf):
                raise RenameConflictError(f"Папка версии '{nf}' уже существует")

        # APPLY: 1) контейнер целиком переезжает (дети уезжают вместе с ним).
        shutil.move(str(container), str(new_container))
        reverse["moves"].append([str(new_container), str(container)])
        # 2) переименовать братские папки версий внутри нового контейнера.
        for folder, nf in folder_map.items():
            if nf == folder:
                continue
            src = new_container / folder
            dst = new_container / nf
            if src.exists():
                shutil.move(str(src), str(dst))
                reverse["moves"].append([str(dst), str(src)])
        # 3) version_group.json: logical_project_id / container / versions[].folder.
        manifest["logical_project_id"] = new_base
        manifest["container"] = new_container.name
        for v in manifest.get("versions", []):
            folder = v.get("folder") or "."
            if folder in (".", ""):
                continue
            v["folder"] = folder_map.get(folder, folder)
        version_service._write_group_manifest(new_container, manifest)
        # 4) project_info.json каждой версии → project_id/name = new_base.
        for v in manifest.get("versions", []):
            folder = v.get("folder") or "."
            vdir = new_container if folder in (".", "") else new_container / folder
            _update_project_info(vdir / "project_info.json", new_base)
        # путь активной (резолвимой) версии = new_container/new_base (V1).
        final_path = new_container / new_base
        if not final_path.exists():
            final_path = new_container

    # ── Ремап сторов по id_map ──
    dec_file = decisions_log_file or config.DECISIONS_LOG_FILE
    usage_file = usage_data_file or config.USAGE_DATA_FILE
    groups_file = project_groups_file or config.PROJECT_GROUPS_FILE
    vault_file = missing_norms_vault_file or config.MISSING_NORMS_VAULT_FILE

    stores: dict[str, int] = {}
    try:
        stores["decisions_log"] = _remap_decisions_log(Path(dec_file), id_map, object_id)
    except Exception as ex:
        warnings.append(f"decisions_log: {ex}")
    try:
        stores["usage_data"] = _remap_usage(Path(usage_file), id_map)
    except Exception as ex:
        warnings.append(f"usage_data: {ex}")
    try:
        stores["project_groups"] = _remap_groups(Path(groups_file), id_map, object_id)
    except Exception as ex:
        warnings.append(f"project_groups: {ex}")
    try:
        stores["missing_norms_vault"] = _remap_vault(Path(vault_file), id_map)
    except Exception as ex:
        warnings.append(f"missing_norms_vault: {ex}")
    reverse["stores"] = stores
    reverse["id_map"] = id_map
    reverse["object_id"] = object_id

    # ── projects_v2 shadow (production читает list/resolve из v2) ──
    v2_sync: dict[str, Any] = {}
    if sync_v2:
        try:
            v2_sync = sync_v2_shadow_rename(
                old_base, new_base, object_id=object_id, v2_root=v2_root,
            )
            for w in v2_sync.get("warnings", []):
                warnings.append(f"projects_v2: {w}")
        except Exception as ex:
            warnings.append(f"projects_v2 sync: {ex}")
            v2_sync = {"error": str(ex)}
    reverse["v2_shadow"] = v2_sync

    # reverse-log
    rl = Path(reverse_log_file) if reverse_log_file else (config.APP_DATA_DIR / "project_rename.reverse.json")
    try:
        _atomic_write_json(rl, reverse)
    except Exception as ex:
        warnings.append(f"reverse-log: {ex}")

    project_service.invalidate_project_cache()

    print(
        f"[rename_project] {storage_layer}: {proj_dir} -> {final_path} | "
        f"id_map={id_map} | stores={stores} | object_id={object_id} | "
        f"v2_updated={v2_sync.get('updated')} v2_renamed={v2_sync.get('renamed_dirs')} | "
        f"warnings={warnings}"
    )

    return {
        "status": "renamed",
        "project_id": new_project_id,
        "old_name": old_base,
        "new_name": new_base,
        "old_path": str(proj_dir),
        "new_path": str(final_path),
        "storage_layer": storage_layer,
        "stores": stores,
        "v2_shadow": v2_sync,
        "warnings": warnings,
    }
