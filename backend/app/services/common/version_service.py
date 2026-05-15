"""
Сервис версионности проектов.

Логический проект может иметь несколько версий документации. Манифест версий
хранится в `project_versions.json` в корне папки проекта.

На этом этапе:
- legacy-проекты без `project_versions.json` автоматически считаются V1;
- V1 указывает на корневую папку проекта (`folder = "."`);
- старшие версии (V2+) физически располагаются в `_versions/<version_id>/`;
- ничего не переносится автоматически.

Формат файла:

    {
      "schema_version": 1,
      "logical_project_id": "<project_id>",
      "latest_version_id": "v1",
      "versions": [
        {
          "version_id": "v1",
          "version_no": 1,
          "label": "V1",
          "folder": ".",
          "created_at": "2026-05-13T10:00:00",
          "status": "legacy",
          "source": "legacy"
        }
      ]
    }
"""
from __future__ import annotations

import contextvars
import json
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


VERSIONS_MANIFEST_FILENAME = "project_versions.json"
VERSIONS_DIR_NAME = "_versions"
SCHEMA_VERSION = 1

_VERSION_ID_RE = re.compile(r"^v(\d+)$")


class VersionNotFoundError(KeyError):
    """version_id не найден в манифесте."""


class VersionFileError(ValueError):
    """Ошибка загрузки/именования файла версии (path traversal, расширение, и т.п.)."""


class VersionFileConflictError(FileExistsError):
    """Файл с таким именем уже существует и replace_existing=False."""


class VersionUploadForbiddenError(PermissionError):
    """Загрузка в эту версию запрещена (например, V1 legacy)."""


# ─── Per-job version binding ──────────────────────────────────────────────
# По аналогии с `bind_object` из project_service: pipeline-runner может
# выставить ContextVar на старте job'а, и все service-методы под ним будут
# использовать именно эту версию, даже если ?version_id явно не передан.

_bound_version_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pdf_proverka.bound_version_id", default=None,
)


def bind_version(version_id: Optional[str]):
    """Назначить активный version_id для текущего async-контекста.

    Возвращает token. Внутри `asyncio.create_task(...)` контекст копируется,
    так что binding наследуется дочерними задачами.
    """
    return _bound_version_id.set(version_id)


def unbind_version(token) -> None:
    _bound_version_id.reset(token)


@contextmanager
def pinned_version(version_id: Optional[str]):
    """Sync context-manager для `bind_version` (удобно в тестах/smoke)."""
    token = _bound_version_id.set(version_id)
    try:
        yield
    finally:
        _bound_version_id.reset(token)


def get_bound_version_id() -> Optional[str]:
    """Прочитать current-job version_id, выставленный через bind_version."""
    return _bound_version_id.get()


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _manifest_path(project_dir: Path) -> Path:
    return project_dir / VERSIONS_MANIFEST_FILENAME


def _legacy_manifest(project_id: str) -> dict[str, Any]:
    """Сформировать манифест-по-умолчанию для legacy-проекта (V1)."""
    return {
        "schema_version": SCHEMA_VERSION,
        "logical_project_id": project_id,
        "latest_version_id": "v1",
        "versions": [
            {
                "version_id": "v1",
                "version_no": 1,
                "label": "V1",
                "folder": ".",
                "created_at": _now_iso(),
                "status": "legacy",
                "source": "legacy",
            }
        ],
    }


def _normalize_version_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Привести запись версии к каноническому виду, не теряя посторонних полей."""
    version_id = str(entry.get("version_id") or "").strip() or "v1"
    try:
        version_no = int(entry.get("version_no") or 0)
    except (TypeError, ValueError):
        version_no = 0
    if version_no <= 0:
        m = _VERSION_ID_RE.match(version_id)
        version_no = int(m.group(1)) if m else 1

    label = str(entry.get("label") or "").strip() or f"V{version_no}"
    folder = str(entry.get("folder") or ".").strip() or "."
    status = str(entry.get("status") or "active").strip() or "active"
    source = str(entry.get("source") or "manual").strip() or "manual"
    created_at = entry.get("created_at") or _now_iso()

    normalized = dict(entry)
    normalized.update({
        "version_id": version_id,
        "version_no": version_no,
        "label": label,
        "folder": folder,
        "status": status,
        "source": source,
        "created_at": created_at,
    })
    return normalized


def _normalize_manifest(raw: dict[str, Any], project_id: str) -> dict[str, Any]:
    """Привести произвольный (возможно повреждённый) манифест к каноническому виду."""
    versions_raw = raw.get("versions") or []
    if not isinstance(versions_raw, list) or not versions_raw:
        return _legacy_manifest(project_id)

    versions: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for entry in versions_raw:
        if not isinstance(entry, dict):
            continue
        norm = _normalize_version_entry(entry)
        if norm["version_id"] in seen_ids:
            continue
        seen_ids.add(norm["version_id"])
        versions.append(norm)

    if not versions:
        return _legacy_manifest(project_id)

    versions.sort(key=lambda v: v["version_no"])

    latest = str(raw.get("latest_version_id") or "").strip()
    valid_ids = {v["version_id"] for v in versions}
    if latest not in valid_ids:
        latest = versions[-1]["version_id"]

    return {
        "schema_version": int(raw.get("schema_version") or SCHEMA_VERSION),
        "logical_project_id": str(raw.get("logical_project_id") or project_id),
        "latest_version_id": latest,
        "versions": versions,
    }


def _read_manifest_raw(project_dir: Path) -> Optional[dict[str, Any]]:
    path = _manifest_path(project_dir)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _write_manifest(project_dir: Path, manifest: dict[str, Any]) -> bool:
    path = _manifest_path(project_dir)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        return True
    except OSError:
        return False


def read_project_versions(project_dir: Path, project_id: str) -> dict[str, Any]:
    """Прочитать манифест версий (или вернуть legacy-манифест в памяти)."""
    raw = _read_manifest_raw(project_dir)
    if raw is None:
        return _legacy_manifest(project_id)
    return _normalize_manifest(raw, project_id)


def ensure_project_versions_manifest(project_dir: Path, project_id: str) -> dict[str, Any]:
    """Создать `project_versions.json` для legacy-проекта, если его нет.

    Файл не создаётся, если папки проекта нет на ФС — это позволяет безопасно
    вызывать функцию из read-paths.
    """
    if not project_dir.exists() or not project_dir.is_dir():
        return _legacy_manifest(project_id)

    raw = _read_manifest_raw(project_dir)
    if raw is not None:
        return _normalize_manifest(raw, project_id)

    manifest = _legacy_manifest(project_id)
    _write_manifest(project_dir, manifest)
    return manifest


def get_latest_version_id(project_dir: Path, project_id: str) -> str:
    """Идентификатор последней версии (по умолчанию `v1` для legacy)."""
    manifest = read_project_versions(project_dir, project_id)
    return manifest.get("latest_version_id") or "v1"


def _find_version(manifest: dict[str, Any], version_id: str) -> Optional[dict[str, Any]]:
    for entry in manifest.get("versions", []):
        if entry.get("version_id") == version_id:
            return entry
    return None


def get_version_dir(
    project_dir: Path,
    project_id: str,
    version_id: Optional[str] = None,
) -> Path:
    """Папка, в которой лежат данные версии.

    - `version_id=None` → latest version;
    - V1 (folder=".") → корневая папка проекта;
    - старшие версии → `<project_dir>/<folder>` (обычно `_versions/<version_id>`).

    Бросает `VersionNotFoundError`, если указанный version_id отсутствует.
    """
    manifest = read_project_versions(project_dir, project_id)
    vid = version_id or manifest.get("latest_version_id") or "v1"
    entry = _find_version(manifest, vid)
    if entry is None:
        raise VersionNotFoundError(
            f"Версия '{vid}' не найдена в проекте '{project_id}'"
        )

    folder = entry.get("folder") or "."
    if folder in (".", ""):
        return project_dir
    return project_dir / folder


def get_versions_summary(project_dir: Path, project_id: str) -> dict[str, Any]:
    """Краткая сводка по версиям для API/UI.

    В каждой версии дополнительно отдаём аудит-готовность (PDF/MD счётчики
    и `can_run_audit`). Подсчёт идёт по содержимому version_dir на ФС,
    манифест используется только для путей.
    """
    manifest = read_project_versions(project_dir, project_id)
    latest_id = manifest.get("latest_version_id") or "v1"
    versions = manifest.get("versions", [])

    enriched: list[dict[str, Any]] = []
    for v in versions:
        vid = v["version_id"]
        folder = v.get("folder") or "."
        version_dir = project_dir if folder in (".", "") else project_dir / folder
        pdf_count = md_count = source_count = 0
        if version_dir.exists():
            for p in version_dir.iterdir():
                if not p.is_file():
                    continue
                if p.name in {"project_info.json", VERSIONS_MANIFEST_FILENAME}:
                    continue
                if p.name.startswith("."):
                    continue
                t = _classify_file(p.name)
                if t in ("pdf", "md", "txt", "json", "html"):
                    source_count += 1
                if t == "pdf":
                    pdf_count += 1
                elif t == "md":
                    md_count += 1
        can_run = pdf_count > 0
        enriched.append({
            "version_id": vid,
            "version_no": v["version_no"],
            "label": v["label"],
            "folder": folder,
            "status": v.get("status", "active"),
            "source": v.get("source", "manual"),
            "created_at": v.get("created_at"),
            "comment": v.get("comment"),
            "is_latest": vid == latest_id,
            "has_source_files": (pdf_count > 0 or md_count > 0),
            "pdf_count": pdf_count,
            "md_count": md_count,
            "source_files_count": source_count,
            "can_run_audit": can_run,
        })

    return {
        "project_id": project_id,
        "logical_project_id": manifest.get("logical_project_id", project_id),
        "latest_version_id": latest_id,
        "version_count": len(versions),
        "has_versions": len(versions) > 1,
        "versions": enriched,
    }


def get_latest_version_meta(project_dir: Path, project_id: str) -> dict[str, Any]:
    """Метаданные последней версии (для подмешивания в ProjectStatus)."""
    manifest = read_project_versions(project_dir, project_id)
    latest_id = manifest.get("latest_version_id") or "v1"
    entry = _find_version(manifest, latest_id) or manifest["versions"][0]
    return {
        "version_id": entry["version_id"],
        "version_no": entry["version_no"],
        "version_label": entry["label"],
        "latest_version_id": latest_id,
        "version_count": len(manifest.get("versions", [])),
        "has_versions": len(manifest.get("versions", [])) > 1,
        "is_latest_version": entry["version_id"] == latest_id,
    }


def get_version_entry(
    project_dir: Path,
    project_id: str,
    version_id: Optional[str] = None,
) -> dict[str, Any]:
    """Нормализованная запись конкретной версии (или latest при None)."""
    manifest = read_project_versions(project_dir, project_id)
    vid = version_id or manifest.get("latest_version_id") or "v1"
    entry = _find_version(manifest, vid)
    if entry is None:
        raise VersionNotFoundError(
            f"Версия '{vid}' не найдена в проекте '{project_id}'"
        )
    return dict(entry)


def create_next_version(
    project_dir: Path,
    project_id: str,
    *,
    label: Optional[str] = None,
    source: str = "manual",
    status: str = "new",
    comment: Optional[str] = None,
    create_folder: bool = True,
    seed_project_info: bool = True,
) -> dict[str, Any]:
    """Зарегистрировать новую версию (V{N+1}) в манифесте.

    Создаёт `<project_dir>/_versions/v{N+1}/_output/`, если `create_folder=True`,
    и (опционально) минимальный `project_info.json` внутри новой версии, чтобы
    пайплайн/сервисы могли находить версию как самостоятельную единицу.

    Существующие данные V1 не копируются и не переносятся.

    Args:
        label: человекочитаемая метка (по умолчанию "V{N+1}").
        source: откуда пришла версия ("manual", "upload" и т.п.).
        status: начальный статус ("new" / "draft").
        comment: необязательное описание новой редакции.
        create_folder: создавать ли папку версии физически.
        seed_project_info: создавать ли пустой `project_info.json` в папке версии.

    Возвращает запись добавленной версии.
    """
    if not project_dir.exists() or not project_dir.is_dir():
        raise FileNotFoundError(f"Папка проекта не найдена: {project_dir}")

    manifest = ensure_project_versions_manifest(project_dir, project_id)
    versions = list(manifest.get("versions", []))

    next_no = max((v["version_no"] for v in versions), default=0) + 1
    next_id = f"v{next_no}"
    folder = f"{VERSIONS_DIR_NAME}/{next_id}"

    new_entry: dict[str, Any] = {
        "version_id": next_id,
        "version_no": next_no,
        "label": (label or f"V{next_no}").strip() or f"V{next_no}",
        "folder": folder,
        "created_at": _now_iso(),
        "status": status,
        "source": source,
    }
    if comment:
        new_entry["comment"] = comment

    if create_folder:
        version_dir = project_dir / folder
        (version_dir / "_output").mkdir(parents=True, exist_ok=True)

        if seed_project_info:
            info_path = version_dir / "project_info.json"
            if not info_path.exists():
                root_info_path = project_dir / "project_info.json"
                base_info: dict[str, Any] = {}
                if root_info_path.exists():
                    try:
                        with open(root_info_path, "r", encoding="utf-8") as f:
                            base_info = json.load(f) or {}
                    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                        base_info = {}
                seed = {
                    "project_id": base_info.get("project_id", project_id),
                    "name": base_info.get("name", project_id),
                    "section": base_info.get("section", "EOM"),
                    "description": base_info.get("description", ""),
                    "pdf_file": "",
                    "pdf_files": [],
                    "version_id": next_id,
                    "version_label": new_entry["label"],
                    "version_source": source,
                }
                if comment:
                    seed["version_comment"] = comment
                try:
                    with open(info_path, "w", encoding="utf-8") as f:
                        json.dump(seed, f, ensure_ascii=False, indent=2)
                except OSError:
                    pass

    versions.append(new_entry)
    manifest["versions"] = versions
    manifest["latest_version_id"] = next_id
    _write_manifest(project_dir, manifest)

    return new_entry


# ─── Unified resolver ──────────────────────────────────────────────────────


def resolve_effective_version_id(
    project_dir: Path,
    project_id: str,
    version_id: Optional[str] = None,
) -> str:
    """Финальный version_id для запроса.

    Приоритет: явный аргумент > bind_version() > latest.
    """
    if version_id:
        return version_id
    bound = get_bound_version_id()
    if bound:
        return bound
    return get_latest_version_id(project_dir, project_id)


def resolve_project_version_context(
    project_id: str,
    version_id: Optional[str] = None,
    *,
    resolve_project_dir_fn=None,
) -> dict[str, Any]:
    """Единая точка резолва: вернуть всё, что нужно эндпоинту/сервису.

    Args:
        project_id: ID проекта.
        version_id: явный version_id (если None → bind_version / latest).
        resolve_project_dir_fn: внедряемый резолвер папки проекта; по умолчанию
            используется `project_service.resolve_project_dir`. Параметр нужен,
            чтобы не создавать circular import: version_service не должен
            импортировать project_service на уровне модуля.

    Returns:
        dict с полями `project_id`, `version_id`, `project_dir`, `version_dir`,
        `output_dir`, `version_entry`, `is_latest`.

    Бросает `VersionNotFoundError`, если запрошенная версия отсутствует.
    Бросает `FileNotFoundError`, если папки проекта нет на ФС.
    """
    if resolve_project_dir_fn is None:
        from backend.app.services.common.project_service import resolve_project_dir
        resolve_project_dir_fn = resolve_project_dir

    project_dir: Path = resolve_project_dir_fn(project_id)
    if not project_dir.exists():
        raise FileNotFoundError(f"Папка проекта '{project_id}' не найдена: {project_dir}")

    target = resolve_effective_version_id(project_dir, project_id, version_id)

    # get_version_entry бросит VersionNotFoundError, если version_id невалидный
    entry = get_version_entry(project_dir, project_id, target)
    version_dir = get_version_dir(project_dir, project_id, target)
    latest_id = get_latest_version_id(project_dir, project_id)

    return {
        "project_id": project_id,
        "version_id": entry["version_id"],
        "project_dir": project_dir,
        "version_dir": version_dir,
        "output_dir": version_dir / "_output",
        "version_entry": entry,
        "is_latest": entry["version_id"] == latest_id,
        "latest_version_id": latest_id,
    }


def resolve_version_output_dir(
    project_id: str,
    version_id: Optional[str] = None,
    *,
    resolve_project_dir_fn=None,
) -> Path:
    """Удобный shortcut: только `_output/` нужной версии.

    Бросает `VersionNotFoundError` / `FileNotFoundError` как
    `resolve_project_version_context`.
    """
    ctx = resolve_project_version_context(
        project_id, version_id, resolve_project_dir_fn=resolve_project_dir_fn,
    )
    return ctx["output_dir"]


# ─── Загрузка исходных файлов в версию ─────────────────────────────────────


# Разрешённые расширения исходных файлов проекта. Расширяем список аккуратно —
# Stage 01 (prepare) ожидает PDF + Markdown, иногда OCR sidecar.
ALLOWED_SOURCE_EXTENSIONS = {".pdf", ".md", ".txt", ".json", ".html"}

_SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9_\-.,()\[\] +а-яА-ЯёЁ]+$")


def validate_filename(filename: str, *, allowed_exts: Optional[set[str]] = None) -> str:
    """Валидировать и нормализовать имя файла перед сохранением.

    Защита от path traversal: не допускаются `..`, абсолютные пути, разделители
    каталогов. Возвращает «чистое» имя файла (без директорий). Регистр
    расширения нормализуется в нижний.

    Бросает `VersionFileError`, если имя небезопасно или расширение не из
    `allowed_exts` (по умолчанию `ALLOWED_SOURCE_EXTENSIONS`).
    """
    if not filename:
        raise VersionFileError("Имя файла пустое")
    raw = str(filename).strip()
    if not raw:
        raise VersionFileError("Имя файла пустое")

    # Любые разделители путей запрещены — берём только базовое имя
    base = Path(raw).name
    if base != raw or base in ("", ".", ".."):
        raise VersionFileError(f"Недопустимое имя файла: {raw!r}")
    if "/" in base or "\\" in base or base.startswith("."):
        raise VersionFileError(f"Недопустимое имя файла: {raw!r}")
    # Дополнительная проверка по символам — отбрасываем не-печатные/служебные
    if not _SAFE_FILENAME_RE.match(base):
        raise VersionFileError(
            f"Имя файла содержит недопустимые символы: {raw!r}"
        )

    suffix = Path(base).suffix.lower()
    allowed = allowed_exts if allowed_exts is not None else ALLOWED_SOURCE_EXTENSIONS
    if suffix not in allowed:
        raise VersionFileError(
            f"Расширение '{suffix}' не разрешено. Допустимы: {sorted(allowed)}"
        )

    # Возвращаем имя с приведённым к нижнему регистру расширением, чтобы избежать
    # коллизий 'document.PDF' vs 'document.pdf' на case-insensitive FS.
    stem = Path(base).stem
    return f"{stem}{suffix}"


def _classify_file(name: str) -> str:
    """pdf / md / txt / json / html / other — для индексации project_info."""
    s = Path(name).suffix.lower()
    return {
        ".pdf": "pdf",
        ".md": "md",
        ".txt": "txt",
        ".json": "json",
        ".html": "html",
    }.get(s, "other")


def _update_version_project_info(
    version_dir: Path,
    project_id: str,
    saved_files: list[str],
    *,
    comment: Optional[str] = None,
) -> dict[str, Any]:
    """Перечитать `project_info.json` версии и обновить списки PDF/MD/др.

    Сохраняет неизвестные поля. Возвращает обновлённый info.
    """
    info_path = version_dir / "project_info.json"
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8")) or {}
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            info = {}
    else:
        info = {}
    if not isinstance(info, dict):
        info = {}

    # Все файлы в папке версии (с учётом ранее загруженных)
    all_files = [
        p.name for p in version_dir.iterdir()
        if p.is_file()
        and p.name != "project_info.json"
        and not p.name.startswith(".")
    ]

    pdf_files = sorted({n for n in all_files if _classify_file(n) == "pdf"})
    md_files = sorted({n for n in all_files if _classify_file(n) == "md"})

    info["project_id"] = info.get("project_id", project_id)
    info["name"] = info.get("name", project_id)
    info["pdf_files"] = pdf_files
    if pdf_files:
        # Сохраняем pdf_file как «основной» PDF: первый по алфавиту, либо
        # сохраняем существующий выбор, если он всё ещё валиден.
        existing = info.get("pdf_file")
        info["pdf_file"] = existing if existing in pdf_files else pdf_files[0]
    else:
        info["pdf_file"] = ""
    info["md_files"] = md_files
    if md_files:
        existing_md = info.get("md_file")
        info["md_file"] = existing_md if existing_md in md_files else md_files[0]
    else:
        # Удаляем устаревший md_file, если файла нет
        info.pop("md_file", None)
    info["updated_at"] = _now_iso()
    info["last_uploaded_files"] = list(saved_files)
    if comment:
        info["last_upload_comment"] = comment

    try:
        info_path.parent.mkdir(parents=True, exist_ok=True)
        info_path.write_text(
            json.dumps(info, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as e:
        raise VersionFileError(f"Не удалось сохранить project_info.json: {e}")

    return info


def list_version_files(
    project_id: str,
    version_id: Optional[str] = None,
    *,
    resolve_project_dir_fn=None,
) -> dict[str, Any]:
    """Список исходных файлов версии + текущий `project_info.json`.

    Не считает файлы внутри `_output/` и `_versions/` — только исходники в
    корне папки версии.
    """
    ctx = resolve_project_version_context(
        project_id, version_id, resolve_project_dir_fn=resolve_project_dir_fn,
    )
    version_dir: Path = ctx["version_dir"]

    files: list[dict[str, Any]] = []
    if version_dir.exists():
        for p in sorted(version_dir.iterdir()):
            if not p.is_file():
                continue
            if p.name == "project_info.json":
                continue
            if p.name == VERSIONS_MANIFEST_FILENAME:
                continue
            if p.name.startswith("."):
                continue
            stat = p.stat()
            files.append({
                "name": p.name,
                "type": _classify_file(p.name),
                "size": stat.st_size,
                "updated_at": datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0).isoformat(),
            })

    info_path = version_dir / "project_info.json"
    project_info: dict[str, Any] = {}
    if info_path.exists():
        try:
            project_info = json.loads(info_path.read_text(encoding="utf-8")) or {}
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            project_info = {}

    return {
        "project_id": project_id,
        "version_id": ctx["version_id"],
        "version_dir": str(version_dir),
        "is_latest": ctx["is_latest"],
        "files": files,
        "project_info": project_info,
    }


def _version_allows_upload(version_id: str, *, allow_v1: bool) -> bool:
    if version_id in (None, "v1"):
        return allow_v1
    return True


def save_files_to_version(
    project_id: str,
    version_id: str,
    files: list[tuple[str, bytes]],
    *,
    replace_existing: bool = False,
    comment: Optional[str] = None,
    allow_v1_upload: bool = False,
    resolve_project_dir_fn=None,
) -> dict[str, Any]:
    """Сохранить исходные файлы в папку указанной версии проекта.

    Args:
        project_id: ID проекта.
        version_id: целевая версия (v1 запрещён по умолчанию).
        files: список `(filename, bytes_content)`.
        replace_existing: при `False` — конфликт по имени файла поднимает
            `VersionFileConflictError`; при `True` — файл перезаписывается.
        comment: опциональное описание загрузки, сохраняется в
            `last_upload_comment` project_info.
        allow_v1_upload: явно разрешить загрузку в legacy V1 (по умолчанию нет).

    Returns:
        dict с полями `saved`, `project_info`, `version_dir`.

    Raises:
        VersionNotFoundError, VersionUploadForbiddenError, VersionFileConflictError,
        VersionFileError.
    """
    if not version_id:
        raise VersionFileError("version_id обязателен")
    if not _version_allows_upload(version_id, allow_v1=allow_v1_upload):
        raise VersionUploadForbiddenError(
            f"Загрузка в '{version_id}' запрещена. Создайте новую версию (V2+) "
            f"или передайте allow_v1_upload=True."
        )

    ctx = resolve_project_version_context(
        project_id, version_id, resolve_project_dir_fn=resolve_project_dir_fn,
    )
    version_dir: Path = ctx["version_dir"]
    version_dir.mkdir(parents=True, exist_ok=True)

    # Сначала валидируем все имена и проверяем конфликты — атомарно: либо все
    # сохраняем, либо ничего.
    plan: list[tuple[Path, bytes]] = []
    saved_names: list[str] = []
    seen_in_batch: set[str] = set()
    for raw_name, content in files:
        if not content:
            raise VersionFileError(f"Файл '{raw_name}' пустой")
        safe = validate_filename(raw_name)
        if safe in seen_in_batch:
            raise VersionFileError(
                f"Дубликат в одной загрузке: {safe!r}"
            )
        seen_in_batch.add(safe)
        target = version_dir / safe
        if target.exists() and not replace_existing:
            raise VersionFileConflictError(
                f"Файл '{safe}' уже существует. Используйте replace_existing=true."
            )
        # Защита: убедимся, что resolved-путь остался внутри version_dir
        try:
            target.resolve().relative_to(version_dir.resolve())
        except ValueError:
            raise VersionFileError(f"Путь вне папки версии: {safe!r}")
        plan.append((target, content))
        saved_names.append(safe)

    # Атомарная запись (write_bytes сам по себе атомарен на POSIX внутри одной
    # папки; для нашей задачи этого достаточно).
    for target, content in plan:
        target.write_bytes(content)

    info = _update_version_project_info(
        version_dir, project_id, saved_names, comment=comment,
    )

    return {
        "project_id": project_id,
        "version_id": ctx["version_id"],
        "version_dir": str(version_dir),
        "saved": saved_names,
        "project_info": info,
    }


# ─── Аудит-готовность версии ───────────────────────────────────────────────


def has_source_files(project_id: str, version_id: Optional[str] = None) -> bool:
    """Есть ли в папке версии хотя бы один PDF или MD-файл."""
    try:
        ctx = resolve_project_version_context(project_id, version_id)
    except (VersionNotFoundError, FileNotFoundError):
        return False
    version_dir: Path = ctx["version_dir"]
    if not version_dir.exists():
        return False
    for p in version_dir.iterdir():
        if not p.is_file():
            continue
        if p.name in {"project_info.json", VERSIONS_MANIFEST_FILENAME}:
            continue
        if _classify_file(p.name) in ("pdf", "md"):
            return True
    return False


def version_audit_readiness(project_id: str, version_id: Optional[str] = None) -> dict[str, Any]:
    """Сводка готовности версии к запуску аудита (PDF/MD счётчики).

    Для V1 (legacy) `can_run_audit` опирается на старую логику project_status:
    наличие хотя бы одного PDF. Для V2+ — то же, плюс проверка, что версия
    создана через manifest.
    """
    try:
        ctx = resolve_project_version_context(project_id, version_id)
    except (VersionNotFoundError, FileNotFoundError):
        return {
            "version_id": version_id,
            "has_source_files": False,
            "pdf_count": 0,
            "md_count": 0,
            "source_files_count": 0,
            "can_run_audit": False,
            "reason": "Версия не найдена",
        }
    version_dir: Path = ctx["version_dir"]
    pdf_count = 0
    md_count = 0
    source_count = 0
    if version_dir.exists():
        for p in version_dir.iterdir():
            if not p.is_file():
                continue
            if p.name in {"project_info.json", VERSIONS_MANIFEST_FILENAME}:
                continue
            if p.name.startswith("."):
                continue
            t = _classify_file(p.name)
            if t in ("pdf", "md", "txt", "json", "html"):
                source_count += 1
            if t == "pdf":
                pdf_count += 1
            elif t == "md":
                md_count += 1

    can_run = pdf_count > 0
    reason = "" if can_run else "Нет PDF-файлов в версии"
    return {
        "version_id": ctx["version_id"],
        "has_source_files": (pdf_count > 0 or md_count > 0),
        "pdf_count": pdf_count,
        "md_count": md_count,
        "source_files_count": source_count,
        "can_run_audit": can_run,
        "reason": reason,
    }


# ─── Создание версии из найденных файлов ───────────────────────────────────


def _resolve_candidate_path(
    raw_path: str,
    *,
    allowed_roots: list[Path],
) -> Path:
    """Разрешить путь candidate-файла и убедиться, что он лежит в `allowed_roots`.

    Защита от path traversal: путь резолвится в абсолютный, и проверяется, что
    он находится внутри хотя бы одного из allowed_roots (PROJECTS_DIR или
    explicitly allowed external scan path).
    """
    if not raw_path:
        raise VersionFileError("Пустой путь к файлу")
    p = Path(raw_path).expanduser()
    try:
        resolved = p.resolve(strict=True)
    except (OSError, RuntimeError):
        raise VersionFileError(f"Файл не найден: {raw_path!r}")
    if not resolved.is_file():
        raise VersionFileError(f"Не является файлом: {raw_path!r}")
    for root in allowed_roots:
        try:
            resolved.relative_to(root.resolve())
            return resolved
        except ValueError:
            continue
    raise VersionFileError(
        f"Путь '{raw_path}' вне разрешённых директорий"
    )


def create_version_from_existing_files(
    target_project_id: str,
    candidate_files: dict[str, Optional[str]],
    *,
    expected_section: Optional[str] = None,
    comment: Optional[str] = None,
    source: str = "section_add_project_modal",
    allowed_roots: Optional[list[Path]] = None,
    resolve_project_dir_fn=None,
) -> dict[str, Any]:
    """Создать новую версию (V{N+1}) у target-проекта из уже лежащих на сервере
    PDF/MD/result.json. Файлы копируются server-side, без upload через браузер.

    Args:
        target_project_id: проект-основание, к которому добавляется версия.
        candidate_files: dict с ключами:
            - `pdf` — путь к PDF (обязателен);
            - `md`  — путь к MD (опционально);
            - `result_json` — путь к OCR-результату (опционально);
            - `extra` — list[str] дополнительных файлов (необязательно).
        expected_section: если задан — проверим, что у target проекта
            `section` совпадает. Защищает от случайного создания версии в
            проекте другого раздела.
        comment: комментарий к версии.
        source: метка источника (по умолчанию section_add_project_modal).
        allowed_roots: список разрешённых корней путей. По умолчанию —
            `PROJECTS_DIR`. Чтобы пускать пути из внешней папки, добавьте её.
        resolve_project_dir_fn: внедряемый резолвер папки проекта.

    Returns:
        dict: { "version": <new_entry>, "versions_summary": ..., "saved": [...],
                "warnings": [...] }

    Raises:
        FileNotFoundError — target проект не найден.
        ValueError — section проекта не совпадает с expected_section.
        VersionFileError — PDF отсутствует, путь невалиден, ext недопустим.
    """
    if resolve_project_dir_fn is None:
        from backend.app.services.common.project_service import resolve_project_dir
        resolve_project_dir_fn = resolve_project_dir

    proj_dir: Path = resolve_project_dir_fn(target_project_id)
    if not proj_dir.exists() or not proj_dir.is_dir():
        raise FileNotFoundError(
            f"Проект '{target_project_id}' не найден: {proj_dir}"
        )

    # Проверка section.
    if expected_section:
        root_info_path = proj_dir / "project_info.json"
        target_section: Optional[str] = None
        if root_info_path.exists():
            try:
                with open(root_info_path, "r", encoding="utf-8") as f:
                    target_section = (json.load(f) or {}).get("section")
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                target_section = None
        if target_section and target_section != expected_section:
            raise ValueError(
                f"Раздел target проекта '{target_section}' не совпадает с "
                f"ожидаемым '{expected_section}'"
            )

    if allowed_roots is None:
        # Используем project_service._get_projects_dir() — поддерживает
        # monkeypatch в тестах. Без circular import: project_service
        # импортируется лениво.
        from backend.app.services.common import project_service as _ps
        allowed_roots = [_ps._get_projects_dir()]
    else:
        # фильтр пустых и приведение к Path
        allowed_roots = [Path(r) for r in allowed_roots if r]

    pdf_raw = (candidate_files or {}).get("pdf")
    md_raw = (candidate_files or {}).get("md")
    result_raw = (candidate_files or {}).get("result_json")
    extra_raw = (candidate_files or {}).get("extra") or []

    if not pdf_raw:
        raise VersionFileError("Не передан PDF — версия требует хотя бы один PDF")

    warnings: list[str] = []

    # Резолвим и валидируем все пути ДО физических операций (атомарность).
    pdf_path = _resolve_candidate_path(pdf_raw, allowed_roots=allowed_roots)
    validate_filename(pdf_path.name)  # extension + safe name

    md_path: Optional[Path] = None
    if md_raw:
        md_path = _resolve_candidate_path(md_raw, allowed_roots=allowed_roots)
        validate_filename(md_path.name)
    else:
        warnings.append("MD не найден — аудит может быть недоступен до загрузки MD")

    result_path: Optional[Path] = None
    if result_raw:
        result_path = _resolve_candidate_path(result_raw, allowed_roots=allowed_roots)
        validate_filename(result_path.name)

    extra_paths: list[Path] = []
    for ex in extra_raw:
        ep = _resolve_candidate_path(ex, allowed_roots=allowed_roots)
        validate_filename(ep.name)
        extra_paths.append(ep)

    # Переиспользуем пустую latest-версию (V2+), если она есть. Иначе — V{N+1}.
    latest_summary = get_versions_summary(proj_dir, target_project_id)
    reused_empty_latest = False
    new_entry: dict[str, Any] = {}
    for v in latest_summary.get("versions", []):
        if v.get("is_latest") and (v.get("pdf_count", 0) == 0):
            if v.get("version_id") != "v1":
                new_entry = {
                    "version_id": v["version_id"],
                    "version_no": v["version_no"],
                    "label": v.get("label") or f"V{v['version_no']}",
                    "folder": v.get("folder") or f"{VERSIONS_DIR_NAME}/{v['version_id']}",
                    "status": v.get("status", "new"),
                    "source": source,
                }
                if comment:
                    new_entry["comment"] = comment
                reused_empty_latest = True
            break

    if not reused_empty_latest:
        new_entry = create_next_version(
            proj_dir,
            target_project_id,
            source=source,
            status="new",
            comment=comment,
            create_folder=True,
            seed_project_info=True,
        )
    new_version_id = new_entry["version_id"]

    # Копируем файлы. save_files_to_version делает атомарную пакетную запись.
    payload: list[tuple[str, bytes]] = []
    payload.append((pdf_path.name, pdf_path.read_bytes()))
    if md_path is not None:
        payload.append((md_path.name, md_path.read_bytes()))
    if result_path is not None:
        payload.append((result_path.name, result_path.read_bytes()))
    for ep in extra_paths:
        payload.append((ep.name, ep.read_bytes()))

    saved_result = save_files_to_version(
        target_project_id,
        new_version_id,
        payload,
        replace_existing=False,
        comment=comment,
        allow_v1_upload=False,
        resolve_project_dir_fn=resolve_project_dir_fn,
    )

    # Подмешаем section/version_source/version_comment в project_info новой
    # версии, если их seed-вариант не содержал.
    version_dir = proj_dir / new_entry["folder"]
    info_path = version_dir / "project_info.json"
    try:
        info = json.loads(info_path.read_text(encoding="utf-8")) if info_path.exists() else {}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        info = {}
    if expected_section and not info.get("section"):
        info["section"] = expected_section
    info["version_source"] = source
    if comment:
        info["version_comment"] = comment
    try:
        info_path.write_text(
            json.dumps(info, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass

    summary = get_versions_summary(proj_dir, target_project_id)

    return {
        "status": "ok",
        "project_id": target_project_id,
        "version": new_entry,
        "version_id": new_version_id,
        "reused_empty_latest": reused_empty_latest,
        "saved": saved_result["saved"],
        "warnings": warnings,
        "versions_summary": summary,
    }


def merge_project_as_version(
    source_project_id: str,
    target_project_id: str,
    *,
    comment: Optional[str] = None,
    source: str = "edit_projects_modal",
    delete_source: bool = True,
    resolve_project_dir_fn=None,
) -> dict[str, Any]:
    """Слить source-проект в target как новую версию (V{N+1}).

    - source ≠ target;
    - section должны совпадать (защита от cross-section merge);
    - все PDF/MD source-проекта переносятся в `_versions/v{N+1}/` target'a;
    - V1 (корень) target'a не трогается;
    - `_output/` source-проекта НЕ копируется (новая версия начинается с нуля);
    - после успешного копирования source-папка удаляется (delete_source=True).

    Raises:
        FileNotFoundError — source или target не найдены.
        ValueError — source == target, section не совпадает, нет PDF в source.
    """
    import shutil

    if resolve_project_dir_fn is None:
        from backend.app.services.common.project_service import resolve_project_dir
        resolve_project_dir_fn = resolve_project_dir

    if not source_project_id or not target_project_id:
        raise ValueError("source_project_id и target_project_id обязательны")
    if source_project_id == target_project_id:
        raise ValueError("source и target — один и тот же проект")

    source_dir: Path = resolve_project_dir_fn(source_project_id)
    target_dir: Path = resolve_project_dir_fn(target_project_id)
    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError(f"Source проект '{source_project_id}' не найден: {source_dir}")
    if not target_dir.exists() or not target_dir.is_dir():
        raise FileNotFoundError(f"Target проект '{target_project_id}' не найден: {target_dir}")

    # Section guard
    def _section(d: Path) -> Optional[str]:
        info_path = d / "project_info.json"
        if not info_path.exists():
            return None
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                return (json.load(f) or {}).get("section")
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None

    src_section = _section(source_dir)
    tgt_section = _section(target_dir)
    if src_section and tgt_section and src_section != tgt_section:
        raise ValueError(
            f"Раздел source ('{src_section}') не совпадает с target ('{tgt_section}')"
        )

    # Собираем PDF/MD из корня source. _versions/ и _output/ не трогаем.
    source_files: list[tuple[str, bytes]] = []
    pdf_found = False
    for p in sorted(source_dir.iterdir()):
        if not p.is_file():
            continue
        if p.name in {"project_info.json", VERSIONS_MANIFEST_FILENAME}:
            continue
        if p.name.startswith("."):
            continue
        suffix = p.suffix.lower()
        if suffix not in ALLOWED_SOURCE_EXTENSIONS:
            continue
        validate_filename(p.name)  # ловит unsafe имена заранее
        source_files.append((p.name, p.read_bytes()))
        if suffix == ".pdf":
            pdf_found = True

    if not pdf_found:
        raise ValueError(f"В source проекте '{source_project_id}' нет PDF — слияние невозможно")

    # Если у target latest-версия пустая (pdf_count == 0) — переиспользуем её,
    # вместо того чтобы плодить новую. Типичный кейс: пользователь нажал
    # «Создать новую версию», получил пустую V2, теперь привязывает source.
    latest_summary = get_versions_summary(target_dir, target_project_id)
    reused_empty_latest = False
    new_entry: dict[str, Any] = {}
    for v in latest_summary.get("versions", []):
        if v.get("is_latest") and (v.get("pdf_count", 0) == 0):
            # Не переиспользуем V1 (legacy) — только V2+.
            if v.get("version_id") != "v1":
                new_entry = {
                    "version_id": v["version_id"],
                    "version_no": v["version_no"],
                    "label": v.get("label") or f"V{v['version_no']}",
                    "folder": v.get("folder") or f"{VERSIONS_DIR_NAME}/{v['version_id']}",
                    "status": v.get("status", "new"),
                    "source": source,
                }
                if comment:
                    new_entry["comment"] = comment
                reused_empty_latest = True
            break

    if not reused_empty_latest:
        # Создаём новую версию у target.
        new_entry = create_next_version(
            target_dir,
            target_project_id,
            source=source,
            status="new",
            comment=comment,
            create_folder=True,
            seed_project_info=True,
        )
    new_version_id = new_entry["version_id"]

    # Атомарный batch-write
    saved_result = save_files_to_version(
        target_project_id,
        new_version_id,
        source_files,
        replace_existing=False,
        comment=comment,
        allow_v1_upload=False,
        resolve_project_dir_fn=resolve_project_dir_fn,
    )

    # Обогатим project_info новой версии
    version_dir = target_dir / new_entry["folder"]
    info_path = version_dir / "project_info.json"
    try:
        info = json.loads(info_path.read_text(encoding="utf-8")) if info_path.exists() else {}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        info = {}
    if tgt_section and not info.get("section"):
        info["section"] = tgt_section
    info["version_source"] = source
    info["merged_from_project_id"] = source_project_id
    if comment:
        info["version_comment"] = comment
    try:
        info_path.write_text(
            json.dumps(info, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass

    # Удаляем source-папку. Делается после успешного копирования —
    # если копирование упало, save_files_to_version поднял бы исключение
    # и сюда мы бы не дошли.
    if delete_source:
        try:
            shutil.rmtree(source_dir)
        except OSError as e:
            # Не катастрофа — сообщим в warnings, файлы уже скопированы.
            return {
                "status": "ok",
                "project_id": target_project_id,
                "source_project_id": source_project_id,
                "version": new_entry,
                "version_id": new_version_id,
                "saved": saved_result["saved"],
                "warnings": [f"Не удалось удалить source папку: {e}"],
                "versions_summary": get_versions_summary(target_dir, target_project_id),
            }

    return {
        "status": "ok",
        "project_id": target_project_id,
        "source_project_id": source_project_id,
        "version": new_entry,
        "version_id": new_version_id,
        "reused_empty_latest": reused_empty_latest,
        "saved": saved_result["saved"],
        "warnings": [],
        "versions_summary": get_versions_summary(target_dir, target_project_id),
    }
