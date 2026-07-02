"""
Сервис для работы с проектами.
Сканирование, чтение project_info.json, определение статуса конвейера.
"""
import contextvars
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
from datetime import datetime

from backend.app.pipeline.stages.crop_blocks.block_markdown import BLOCK_HEADER_RE
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import GEMMA_BLOCKS_DIRNAME, gemma_blocks_index_path
from backend.app.core.config import PROJECTS_DIR as _DEFAULT_PROJECTS_DIR, SEVERITY_CONFIG, HIDDEN_PROJECTS_FILE
from backend.app.models.project import (
    ProjectInfo, ProjectStatus, PipelineStatus, TextExtractionQuality,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import GEMMA_STAGE_LABEL, evaluate_gemma_enrichment
from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import detect_gemma_migration_state
from backend.app.services.common import version_service
from backend.app.services.storage.projects_v2_source_resolver import (
    is_projects_v2_version_dir,
    load_version_project_info,
    resolve_version_source_files,
)

logger = logging.getLogger(__name__)


# ─── Per-job object binding ────────────────────────────────────────────────
# ContextVar, который pipeline устанавливает на старте job'а. Если он задан,
# resolve_project_dir() использует projects_dir привязанного объекта и
# игнорирует глобальный current_id из objects.json. Это нужно, чтобы job,
# стартовавший для объекта A, не записал свои артефакты в объект B, если
# оператор тем временем переключил current_id в UI.

_bound_object_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pdf_proverka.bound_object_id", default=None,
)


class AmbiguousProjectError(RuntimeError):
    """project_id существует в нескольких объектах, и scope не задан."""


class ProjectNotResolvedError(RuntimeError):
    """project_id не резолвится в существующую папку проекта/контейнера.

    Бросается только при `resolve_project_dir(..., must_exist=True)` — для
    writer-ов (expert_review, audit output), чтобы они НЕ создавали `_output`
    по несуществующему `direct = projects_dir / project_id` на корне объекта
    (источник orphan-папок)."""


class ProjectByPdfError(RuntimeError):
    """Не удалось однозначно разрешить проект по имени PDF."""

    def __init__(self, message: str, *, matches: list[Path] | None = None,
                 suggestions: list[str] | None = None):
        super().__init__(message)
        self.matches = list(matches or [])
        self.suggestions = list(suggestions or [])


def resolve_project_by_pdf(
    pdf_name: str,
    *,
    projects_dir: Path | None = None,
    max_depth: int = 6,
    suggestion_limit: int = 5,
) -> tuple[str, Path]:
    """Найти папку проекта по точному имени файла PDF.

    Рекурсивно сканирует `projects_dir` (по умолчанию PROJECTS_DIR), ищет
    файлы с именем == `pdf_name` и пытается определить уникальный проект.

    Правила:
      - найден ровно один PDF → возвращаем (project_id, project_dir);
      - найдено несколько → ProjectByPdfError со списком всех проектов;
      - не найдено → ProjectByPdfError с ближайшими похожими именами;
      - «проект» — ближайший предок PDF, содержащий project_info.json,
        либо (fallback) папка, в которой лежит PDF.

    project_id — путь, относительный к projects_dir, используется в
    `resolve_project_dir`.
    """
    base = projects_dir or _DEFAULT_PROJECTS_DIR
    if not base.exists():
        raise ProjectByPdfError(f"Папка projects/ не существует: {base}")

    needle = pdf_name.strip()
    if not needle.lower().endswith(".pdf"):
        needle = needle + ".pdf"

    matches: list[Path] = []
    all_pdf_names: list[str] = []

    # BFS с ограничением глубины — избегаем случайно взорваться на symlink-циклах
    stack: list[tuple[Path, int]] = [(base, 0)]
    while stack:
        cur, depth = stack.pop()
        if depth > max_depth:
            continue
        try:
            entries = list(cur.iterdir())
        except (OSError, PermissionError):
            continue
        for entry in entries:
            if entry.is_symlink():
                continue
            if entry.is_dir():
                # Пропускаем явно служебные ветки
                if entry.name.startswith("_output") or entry.name == "_experiments":
                    continue
                stack.append((entry, depth + 1))
            elif entry.is_file() and entry.suffix.lower() == ".pdf":
                all_pdf_names.append(entry.name)
                if entry.name == needle:
                    matches.append(entry)

    if not matches:
        import difflib
        suggestions = difflib.get_close_matches(needle, all_pdf_names, n=suggestion_limit, cutoff=0.6)
        raise ProjectByPdfError(
            f"PDF '{needle}' не найден в {base}. "
            + (f"Похожие имена: {suggestions}" if suggestions else "Похожих имён не найдено."),
            suggestions=suggestions,
        )

    # Уникальные проектные папки (один PDF может лежать и в <proj>/file.pdf,
    # и в <proj>/file.pdf/file.pdf из-за Chandra OCR, где созданная папка
    # названа как PDF). Берём ближайшего предка с project_info.json.
    projects: dict[str, Path] = {}
    for pdf_path in matches:
        proj_dir = _nearest_project_dir(pdf_path, base)
        rel = proj_dir.relative_to(base)
        projects[str(rel)] = proj_dir

    if len(projects) > 1:
        names = [str(p) for p in projects]
        raise ProjectByPdfError(
            f"PDF '{needle}' найден в {len(projects)} проектах: {names}. "
            "Уточните путь или используйте уникальное имя.",
            matches=list(projects.values()),
        )

    project_id, project_dir = next(iter(projects.items()))
    return project_id, project_dir


def _nearest_project_dir(pdf_path: Path, base: Path) -> Path:
    """Ближайший предок PDF c project_info.json или (fallback) его родитель."""
    parent = pdf_path.parent
    # OCR-pipeline создаёт папку с таким же именем, как PDF — её нужно игнорировать
    # как «проект», если она пустая/технологическая. Берём project_info.json.
    cur: Path | None = parent
    while cur is not None and cur != base.parent:
        if (cur / "project_info.json").is_file():
            return cur
        if cur == base:
            break
        cur = cur.parent
    return parent


def bind_object(object_id: Optional[str]):
    """Назначить активный object_id для текущего async-контекста.

    Возвращает token. Чтобы снять — вызови `unbind_object(token)`. Внутри
    `asyncio.create_task(...)` контекст копируется, так что binding
    наследуется дочерними задачами.
    """
    return _bound_object_id.set(object_id)


def unbind_object(token) -> None:
    _bound_object_id.reset(token)


@contextmanager
def pinned_object(object_id: Optional[str]):
    """Sync context-manager для bind_object (удобно в тестах/smoke)."""
    token = _bound_object_id.set(object_id)
    try:
        yield
    finally:
        _bound_object_id.reset(token)


def _get_bound_object_id() -> Optional[str]:
    return _bound_object_id.get()


def _bound_projects_dir() -> Optional[Path]:
    """projects_dir связанного через ContextVar объекта (если он есть)."""
    bound = _get_bound_object_id()
    if not bound:
        return None
    try:
        from backend.app.services.common.object_service import get_projects_dir_for
    except Exception:
        return None
    return get_projects_dir_for(bound)


def _get_projects_dir() -> Path:
    """Получить папку проектов.

    Приоритет:
      1) ContextVar-binding (per-job), если установлен → projects_dir этого объекта.
      2) current_id из objects.json (legacy глобальный state).
      3) Default PROJECTS_DIR.
    """
    bound = _bound_projects_dir()
    if bound is not None:
        return bound
    try:
        from backend.app.services.common.object_service import get_current_projects_dir
        return get_current_projects_dir()
    except Exception:
        return _DEFAULT_PROJECTS_DIR


def find_object_dirs_for(project_id: str) -> list[Path]:
    """Все объекты, где такой project_id существует на ФС.

    Используется для ambiguity-детекции. Не кэшируется — вызов редкий.
    """
    if not project_id:
        return []
    try:
        from backend.app.services.common.object_service import list_projects_dirs
    except Exception:
        return []
    hits: list[Path] = []
    for root in list_projects_dirs():
        candidate = root / project_id
        if candidate.exists():
            hits.append(candidate)
    return hits


# TTL-кеш для iter_project_dirs (30 сек). #78: ключуется по resolved projects_dir —
# при смене PROJECTS_DIR (тесты, smoke-sandbox, override) кеш не отдаёт чужой список.
_PROJECT_DIRS_CACHE: list[tuple[str, Path]] = []
_PROJECT_DIRS_CACHE_TIME: float = 0.0
_PROJECT_DIRS_CACHE_KEY: str = ""
_PROJECT_DIRS_TTL: float = 30.0


def invalidate_project_cache() -> None:
    """Сбросить TTL-кеш `iter_project_dirs`.

    Вызывать после операций, которые меняют состав папок в `PROJECTS_DIR`:
    добавление/удаление/переименование проектов (например, после merge-as-version
    удаления source-папки). Без этого `/api/projects` будет ~30 сек показывать
    устаревший список.
    """
    global _PROJECT_DIRS_CACHE, _PROJECT_DIRS_CACHE_TIME, _PROJECT_DIRS_CACHE_KEY
    _PROJECT_DIRS_CACHE = []
    _PROJECT_DIRS_CACHE_TIME = 0.0
    _PROJECT_DIRS_CACHE_KEY = ""


def _container_primary(container: Path) -> Optional[tuple[str, Path]]:
    """Для папки-контейнера `(main)` вернуть (project_id, primary_version_dir).

    project_id = basename папки primary-версии (= исходное имя V1), поэтому при
    промоуте проекта в контейнер его `project_id` не меняется. Возвращает None,
    если папка не контейнер или манифест пуст.
    """
    if not version_service.is_version_container(container):
        return None
    raw = version_service._read_group_manifest_raw(container) or {}
    primary_id = raw.get("primary_version_id") or "v1"
    versions = raw.get("versions") or []
    entry = next((v for v in versions if v.get("version_id") == primary_id), None)
    if entry is None and versions:
        entry = versions[0]
    if entry is None:
        return None
    folder = entry.get("folder") or "."
    pdir = container if folder in (".", "") else container / folder
    return (pdir.name, pdir)


def iter_project_dirs(force: bool = False) -> list[tuple[str, Path]]:
    """Рекурсивно найти все папки проектов (включая подпапки-группы).

    Возвращает [(project_id, path), ...] где project_id = имя папки.
    Проект = папка с project_info.json или PDF-файлами.
    Подпапка-группа (OV/, EOM/ и т.д.) = папка без project_info.json и без PDF.

    Папка-контейнер версий `<база>(main)/` проектом НЕ считается: вместо неё в
    список попадает ровно одна запись — primary-версия (V1) с её basename как
    project_id. Остальные версии доступны через versions_summary.

    Кеш обновляется раз в 30 секунд (или force=True).
    """
    global _PROJECT_DIRS_CACHE, _PROJECT_DIRS_CACHE_TIME, _PROJECT_DIRS_CACHE_KEY

    now = time.time()
    projects_dir = _get_projects_dir()
    cache_key = str(projects_dir)
    # #78: кеш валиден только если построен под ТОТ ЖЕ projects_dir.
    if (not force and _PROJECT_DIRS_CACHE
            and _PROJECT_DIRS_CACHE_KEY == cache_key
            and (now - _PROJECT_DIRS_CACHE_TIME) < _PROJECT_DIRS_TTL):
        return _PROJECT_DIRS_CACHE

    results: list[tuple[str, Path]] = []
    if not projects_dir.exists():
        return results
    for entry in sorted(projects_dir.iterdir()):
        if not entry.is_dir() or entry.name.startswith("_"):
            continue
        # Контейнер версий на верхнем уровне → отдаём primary-версию, не контейнер.
        primary = _container_primary(entry)
        if primary is not None:
            results.append(primary)
            continue
        # glob("*.pdf") матчит и папки с таким именем (UI/OCR иногда создают
        # `<name>.pdf/`). Фильтруем по is_file(), иначе группа-папка ошибочно
        # классифицируется как проект и её подпапки пропадают из списка.
        has_pdf_file = any(p.is_file() for p in entry.glob("*.pdf"))
        # Если внутри лежат подпапки с project_info.json — это точно группа
        # (разделы AR/EOM/...), даже если на её корне есть PDF или info.
        # Защищает от phantom-родителя, который обобщает все подпапки.
        has_child_projects = any(
            sub.is_dir() and not sub.name.startswith("_")
            and (sub / "project_info.json").exists()
            for sub in entry.iterdir()
        )
        is_project = (entry / "project_info.json").exists() or has_pdf_file
        if is_project and not has_child_projects:
            results.append((entry.name, entry))
        else:
            # Подпапка-группа — заходим внутрь (1 уровень)
            for sub in sorted(entry.iterdir()):
                if not sub.is_dir() or sub.name.startswith("_"):
                    continue
                # Контейнер версий внутри дисциплины → primary-версия.
                primary = _container_primary(sub)
                if primary is not None:
                    results.append(primary)
                else:
                    results.append((sub.name, sub))

    _PROJECT_DIRS_CACHE = results
    _PROJECT_DIRS_CACHE_TIME = now
    _PROJECT_DIRS_CACHE_KEY = cache_key
    return results


def _resolve_pdf_suffixed_dir(project_id: str, projects_dir: Path) -> Optional[Path]:
    """Обратный `.pdf`-fallback для `resolve_project_dir`.

    Кейс: `project_id` пришёл БЕЗ `.pdf` (например, из projects_v2 read-cutover,
    где `document_code` = basename без расширения), а реальная legacy-папка на
    диске исторически названа `<project_id>.pdf` (папку назвали по имени
    PDF-файла вместе с расширением). Пробуем ровно один эквивалентный путь
    `<project_id>.pdf` через те же lookup-примитивы (direct / контейнер `(main)`
    / подпапка-дисциплина), что и основной резолв.

    Это зеркало уже существующего прямого `.pdf`-fallback'а (id `<база>.pdf` →
    реальный `<база>`). Гарантии:

      * НЕ рекурсивный (не зовёт `resolve_project_dir`) → нет mutual-recursion с
        прямым `.pdf`-fallback'ом;
      * кандидат принимается ТОЛЬКО если папка реально существует И лежит ВНУТРИ
        `projects_dir` (anti-traversal: суффикс добавляется в КОНЕЦ имени —
        уйти вверх по дереву им нельзя, плюс явная проверка вложенности);
      * прямой путь имеет приоритет (сюда попадаем только когда он не найден);
      * при НЕСКОЛЬКИХ разных существующих кандидатах НЕ угадываем → `None`
        (вызывающий вернёт прежнюю ошибку / несуществующий путь).

    Возвращает `Path` единственного кандидата или `None`.
    """
    if project_id.endswith(".pdf") or not projects_dir.exists():
        return None
    try:
        pdir_resolved = projects_dir.resolve()
    except Exception:
        return None

    pid = project_id + ".pdf"
    seen: set = set()
    matches: list = []

    def _consider(candidate: Path) -> None:
        try:
            if not candidate.exists():
                return
            rp = candidate.resolve()
        except Exception:
            return
        # anti-traversal: кандидат обязан лежать внутри projects_dir
        if rp != pdir_resolved and pdir_resolved not in rp.parents:
            return
        if rp in seen:
            return
        seen.add(rp)
        matches.append(candidate)

    base = Path(pid).name
    parent_rel = Path(pid).parent
    # 1) прямой путь
    _consider(projects_dir / pid)
    # 2) контейнер версий <parent>/<база>(main)/<база>
    _consider(
        projects_dir / parent_rel
        / f"{base}{version_service.CONTAINER_SUFFIX}" / base
    )
    # 3) подпапки-дисциплины и контейнеры версий внутри них
    for subdir in projects_dir.iterdir():
        if not subdir.is_dir() or subdir.name.startswith("_"):
            continue
        _consider(subdir / pid)
        if not subdir.name.endswith(version_service.CONTAINER_SUFFIX):
            for child in subdir.iterdir():
                if (
                    child.is_dir()
                    and child.name.endswith(version_service.CONTAINER_SUFFIX)
                ):
                    _consider(child / pid)

    if len(matches) == 1:
        return matches[0]
    return None


def resolve_project_dir(
    project_id: str,
    *,
    object_id: Optional[str] = None,
    strict: bool = False,
    must_exist: bool = False,
) -> Path:
    """Найти папку проекта по ID.

    Порядок:
      1) Если передан `object_id` — резолвим в рамках projects_dir ЭТОГО объекта.
      2) Иначе если установлен ContextVar-binding — в рамках привязанного объекта.
      3) Иначе — старое поведение (через current_id / default).

    strict=True: если project_id существует в НЕСКОЛЬКИХ объектах и scope
    (object_id / binding) не задан — поднимаем `AmbiguousProjectError`. По
    умолчанию strict=False, чтобы не ломать существующие read-эндпоинты.

    must_exist=True: для writer-ов. Если ни один реальный путь не найден —
    поднимаем `ProjectNotResolvedError` вместо возврата несуществующего
    `direct = projects_dir / project_id` (иначе writer молча создаёт orphan
    `_output` на корне объекта). Перед ошибкой пробуется fallback со снятием
    суффикса `.pdf` (id вида `<база>.pdf` → реальный `<база>`).
    """
    explicit_scope = False
    if object_id is not None:
        try:
            from backend.app.services.common.object_service import get_projects_dir_for
            pd = get_projects_dir_for(object_id)
        except Exception:
            pd = None
        if pd is not None:
            projects_dir = pd
            explicit_scope = True
        else:
            projects_dir = _get_projects_dir()
    else:
        bound = _bound_projects_dir()
        if bound is not None:
            projects_dir = bound
            explicit_scope = True
        else:
            projects_dir = _get_projects_dir()

    direct = projects_dir / project_id

    # strict-ambiguity check срабатывает только если scope явно не задан.
    if strict and not explicit_scope:
        hits = find_object_dirs_for(project_id)
        if len(hits) > 1:
            names = ", ".join(str(h) for h in hits)
            raise AmbiguousProjectError(
                f"project_id '{project_id}' существует в {len(hits)} объектах: {names}. "
                f"Укажите object_id или установите bind_object(...)."
            )

    if direct.exists():
        return direct
    # Если projects_dir не существует — не падаем, возвращаем direct path
    if not projects_dir.exists():
        return direct
    # Контейнерная раскладка: <parent>/<база>(main)/<база>. Покрывает и
    # project_id со слешем (например "KJ/TGT2" → KJ/TGT2(main)/TGT2).
    base = Path(project_id).name
    parent_rel = Path(project_id).parent
    container_loc = (
        projects_dir / parent_rel
        / f"{base}{version_service.CONTAINER_SUFFIX}" / base
    )
    if container_loc.exists():
        return container_loc
    # Поиск в подпапках (дисциплина) + внутри контейнеров версий `(main)`.
    for subdir in projects_dir.iterdir():
        if not subdir.is_dir() or subdir.name.startswith("_"):
            continue
        candidate = subdir / project_id
        if candidate.exists():
            return candidate
        if subdir.name.endswith(version_service.CONTAINER_SUFFIX):
            # Контейнер версий на верхнем уровне: projects_dir/<база>(main)/<id>
            inner = subdir / project_id
            if inner.exists():
                return inner
        else:
            # Дисциплина может содержать контейнеры:
            # projects_dir/<дисциплина>/<база>(main)/<id>
            for child in subdir.iterdir():
                if (
                    child.is_dir()
                    and child.name.endswith(version_service.CONTAINER_SUFFIX)
                ):
                    inner = child / project_id
                    if inner.exists():
                        return inner

    # Fallback: id вида `<база>.pdf` (приходит из version-имени V2 `… .pdf`).
    # Реальные папки проектов/контейнеров — без `.pdf`. Пробуем снять суффикс
    # и резолвить штатно; принимаем результат ТОЛЬКО если он существует и
    # отличается от исходного id (без рекурсии в бесконечность — у stripped
    # уже нет `.pdf`). Не трогаем легитимные `.pdf`-id: для них `direct`
    # существует и мы бы вернули его раньше (см. `direct.exists()`).
    if project_id.endswith(".pdf"):
        stripped = project_id[:-4]
        if stripped and stripped != project_id:
            alt = resolve_project_dir(
                stripped, object_id=object_id, strict=strict,
            )
            if alt.exists():
                return alt

    # Зеркало предыдущего fallback'а: project_id БЕЗ `.pdf`, а реальная
    # legacy-папка названа `<project_id>.pdf`. Так приходит id из projects_v2
    # read-cutover (document_code без расширения), когда на диске папка сохранила
    # `.pdf` в имени. Узкий, не-рекурсивный, anti-traversal, без угадывания при
    # неоднозначности (см. `_resolve_pdf_suffixed_dir`). Прямой путь имеет
    # приоритет — мы здесь только потому, что он не найден.
    alt_pdf = _resolve_pdf_suffixed_dir(project_id, projects_dir)
    if alt_pdf is not None:
        return alt_pdf

    if must_exist:
        raise ProjectNotResolvedError(
            f"Project directory not resolved for project_id={project_id!r}"
        )
    return direct  # fallback (legacy: допускает несуществующий путь для read/create-new)


def resolve_active_project_dir(
    project_id: str,
    *,
    object_id: Optional[str] = None,
    strict: bool = False,
) -> Path:
    """Папка АКТИВНОЙ версии проекта.

    В отличие от `resolve_project_dir`, который всегда возвращает корень,
    эта функция учитывает `project_versions.json`:
      - bound через `version_service.bind_version(...)` → этот version_id;
      - иначе — `latest_version_id` из манифеста;
      - legacy без манифеста → корень (V1 эквивалент).

    Pipeline-стейджи и subprocess'ы должны видеть source-файлы и `_output/`
    активной версии, а не v1, лежащую в корне.
    """
    root = resolve_project_dir(project_id, object_id=object_id, strict=strict)
    vid = version_service.get_bound_version_id()
    try:
        return version_service.get_version_dir(root, project_id, vid)
    except version_service.VersionNotFoundError:
        return root


def _load_hidden_projects() -> set[str]:
    """Прочитать множество скрытых project_id из hidden_projects.json."""
    if not HIDDEN_PROJECTS_FILE.exists():
        return set()
    try:
        with open(HIDDEN_PROJECTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("hidden", []))
    except Exception:
        return set()


def _save_hidden_projects(hidden: set[str]) -> None:
    HIDDEN_PROJECTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HIDDEN_PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"hidden": sorted(hidden)}, f, ensure_ascii=False, indent=2)


def hide_project(project_id: str) -> None:
    hidden = _load_hidden_projects()
    hidden.add(project_id)
    _save_hidden_projects(hidden)


def unhide_project(project_id: str) -> None:
    hidden = _load_hidden_projects()
    hidden.discard(project_id)
    _save_hidden_projects(hidden)


def delete_project(project_id: str) -> dict:
    """Жёстко удалить проект: legacy-папку (контейнер версий или plain) + его
    документ(ы) в projects_v2 + записи old_to_new_map + запись в hidden_projects.

    Семантика — безвозвратное удаление (выбор оператора). Сначала убираем v2
    (fail-soft, пока legacy ещё на месте для резолва по map), затем удаляем
    legacy-папку (авторитетно). Гард «не во время аудита» — на уровне endpoint.

    Raises:
        ValueError: проект не найден.
    """
    try:
        proj_dir = resolve_project_dir(project_id, must_exist=True)
    except (ProjectNotResolvedError, AmbiguousProjectError, FileNotFoundError) as e:
        raise ValueError(f"Проект '{project_id}' не найден") from e

    # верхнеуровневая запись: контейнер `(main)` (удалит все версии) или plain
    root_entry = Path(proj_dir)
    try:
        c = version_service.container_dir_for(proj_dir)
        if c is not None:
            root_entry = Path(c)
    except Exception:
        pass
    root_entry = root_entry.resolve()

    if not root_entry.exists():
        raise ValueError(f"Проект '{project_id}' не найден")

    # 1) убрать v2-документ(ы) (no-op в legacy-режиме, fail-soft). Делается до
    #    rmtree, но сопоставление с map идёт по строке пути — переживает удаление.
    v2_info = None
    try:
        from backend.app.services.storage import storage_write_facade as _swf
        wr = _swf.remove_project_from_v2_safe(root_entry)
        v2_info = wr.to_dict() if wr is not None else None
    except Exception:
        v2_info = None

    # 2) жёсткое удаление legacy (авторитетно)
    shutil.rmtree(root_entry)

    # 3) очистить запись из hidden_projects (если была)
    try:
        unhide_project(project_id)
    except Exception:
        pass

    # 4) инвалидировать кеш списка проектов (иначе удалённый висит ~30с по TTL)
    try:
        invalidate_project_cache()
    except Exception:
        pass

    return {
        "project_id": project_id,
        "deleted_legacy": str(root_entry),
        "v2": v2_info,
    }


def _v2_read_enabled() -> bool:
    try:
        from backend.app.services.storage.storage_read_facade import production_uses_v2
        return production_uses_v2()
    except Exception:
        return False


def _version_no_from_id(version_id: str, fallback: int = 1) -> int:
    m = re.match(r"v0*(\d+)$", str(version_id or ""))
    return int(m.group(1)) if m else fallback


def _v2_versions_summary(adapter, doc: dict, doc_dir: Path, latest_id: str) -> dict:
    versions = []
    for idx, entry in enumerate(adapter.list_versions(doc_dir), start=1):
        vid = entry.get("version_id") or f"v{idx:03d}"
        inputs = adapter.input_files(doc_dir, vid)
        pdf_count = sum(1 for name in inputs if str(name).lower().endswith(".pdf"))
        md_count = sum(1 for name in inputs if str(name).lower().endswith(".md"))
        versions.append({
            "version_id": vid,
            "version_no": entry.get("version_no") or _version_no_from_id(vid, idx),
            "label": entry.get("label") or f"V{entry.get('version_no') or _version_no_from_id(vid, idx)}",
            "folder": f"versions/{vid}",
            "status": entry.get("status", "active"),
            "source": entry.get("source", "projects_v2"),
            "created_at": entry.get("created_at"),
            "comment": entry.get("comment"),
            "is_latest": vid == latest_id,
            "has_source_files": bool(pdf_count or md_count),
            "pdf_count": pdf_count,
            "md_count": md_count,
            "source_files_count": len(inputs),
            "can_run_audit": pdf_count > 0,
        })
    return {
        "project_id": doc["document_code"],
        "logical_project_id": doc["document_code"],
        "latest_version_id": latest_id,
        "version_count": len(versions),
        "has_versions": len(versions) > 1,
        "versions": versions,
    }


def _v2_pipeline_status(adapter, doc_dir: Path, version_id: str) -> PipelineStatus:
    status = PipelineStatus()
    log = adapter.read_pipeline_log(doc_dir, version_id) or {}
    stages = log.get("stages", {}) if isinstance(log, dict) else {}
    mapping = {
        "crop_blocks": "crop_blocks",
        "gemma_enrichment": "gemma_enrichment",
        "text_analysis": "text_analysis",
        "block_analysis": "blocks_analysis",
        "block_retry": "block_retry",
        "findings_merge": "findings",
        "findings_critic": "findings_critic",
        "findings_corrector": "findings_corrector",
        "norm_verify": "norms_verified",
        "optimization": "optimization",
        "optimization_critic": "optimization_critic",
        "optimization_corrector": "optimization_corrector",
        "evidence_verify": "evidence_verify",
        "excel": "excel",
        "prepare": "crop_blocks",
        "main_audit": "findings",
    }
    valid = {"done", "error", "partial", "running", "skipped", "interrupted"}
    for log_key, field in mapping.items():
        raw = (stages.get(log_key) or {}).get("status")
        if raw in valid:
            setattr(status, field, "error" if raw == "interrupted" else raw)
    files = adapter.latest_analysis_files(doc_dir, version_id)
    if files.get("has_01_text_analysis") and status.text_analysis == "pending":
        status.text_analysis = "done"
    if files.get("has_02_blocks_analysis") and status.blocks_analysis == "pending":
        status.blocks_analysis = "done"
    if files.get("has_03_findings") and status.findings == "pending":
        status.findings = "done"
    if adapter.analysis_artifact_path(doc_dir, version_id, "03a_norms_verified.json") and status.norms_verified == "pending":
        status.norms_verified = "done"
    if adapter.analysis_artifact_path(doc_dir, version_id, "optimization.json") and status.optimization == "pending":
        status.optimization = "done"
    if adapter.analysis_artifact_path(doc_dir, version_id, "optimization_review.json") and status.optimization_critic == "pending":
        status.optimization_critic = "done"
    # Evidence Verifier — интегрирован в пайплайн, по умолчанию OFF.
    # При OFF всегда показываем 'disabled' (в UI — «временно отключена»), даже
    # если в pipeline_log осталась старая запись. При ON — обычная логика: статус
    # из лога, а если он ещё дефолтный — вычисляем по наличию артефакта.
    from backend.app.core import config as _cfg
    if not getattr(_cfg, "EVIDENCE_VERIFY_IN_PIPELINE_ENABLED", False):
        status.evidence_verify = "disabled"
    elif status.evidence_verify in ("disabled", "pending"):
        if adapter.analysis_artifact_path(doc_dir, version_id, "evidence_validation.json"):
            status.evidence_verify = "done"
        else:
            status.evidence_verify = "pending"
    return status


def _v2_project_status_from_doc(adapter, doc: dict, *, version_id: Optional[str] = None) -> Optional[ProjectStatus]:
    doc_dir = Path(doc["doc_dir"])
    vid = adapter.resolve_version_id(doc, version_id)
    if not vid:
        return None
    vdir = adapter.version_dir(doc_dir, vid)
    vj = adapter.read_version_json(doc_dir, vid) or {}
    dj = adapter.read_document_json(doc_dir) or {}
    latest_id = adapter.current_version_id(doc_dir, dj) or vid
    versions_summary = _v2_versions_summary(adapter, doc, doc_dir, latest_id)
    version_entry = next((v for v in versions_summary["versions"] if v["version_id"] == vid), None)
    if version_entry is None:
        return None

    project_info = vj.get("project_info") or dj.get("project_info") or {}
    object_info = next((o for o in adapter.list_objects() if o["folder_name"] == doc["object_folder"]), {})
    input_files = adapter.input_files(doc_dir, vid)
    pdf_files = [name for name in input_files if str(name).lower().endswith(".pdf")]
    pdf_size = 0.0
    for name in pdf_files:
        fp = vdir / "01_input" / name
        if fp.is_file():
            pdf_size += fp.stat().st_size / 1024 / 1024
    md_candidates = [name for name in input_files if str(name).lower().endswith(".md")]
    work_md = vdir / "02_work" / "document.md"
    has_md = bool(md_candidates) or (work_md.is_file() and work_md.stat().st_size > 0)
    md_name = "02_work/document.md" if work_md.is_file() else (md_candidates[0] if md_candidates else None)
    md_size_kb = 0.0
    if work_md.is_file():
        md_size_kb = round(work_md.stat().st_size / 1024, 1)
    elif md_candidates:
        mp = vdir / "01_input" / md_candidates[0]
        if mp.is_file():
            md_size_kb = round(mp.stat().st_size / 1024, 1)

    findings_data = adapter.read_findings(doc_dir, vid) or {}
    findings_items = findings_data if isinstance(findings_data, list) else findings_data.get("findings", findings_data.get("items", [])) or []
    findings_by_severity: dict[str, int] = {}
    for item in findings_items:
        if isinstance(item, dict):
            sev = item.get("severity", "НЕИЗВЕСТНО")
            findings_by_severity[sev] = findings_by_severity.get(sev, 0) + 1

    opt_data = adapter.read_analysis_artifact(doc_dir, vid, "optimization.json") or {}
    opt_items = opt_data.get("items", []) if isinstance(opt_data, dict) else []
    opt_meta = opt_data.get("meta", {}) if isinstance(opt_data, dict) else {}
    opt_by_type = opt_meta.get("by_type") or {}
    if not opt_by_type:
        for item in opt_items:
            if isinstance(item, dict):
                typ = item.get("type", "unknown")
                opt_by_type[typ] = opt_by_type.get(typ, 0) + 1

    latest_dir = adapter.latest_dir(doc_dir, vid)
    pipeline_version = project_info.get("pipeline_version", vj.get("pipeline_version", "legacy")) or "legacy"
    return ProjectStatus(
        project_id=doc["document_code"],
        name=project_info.get("name") or dj.get("display_name") or doc["document_code"],
        description=project_info.get("description", ""),
        section=project_info.get("section") or doc.get("discipline") or "EOM",
        object=project_info.get("object") or object_info.get("display_name"),
        has_pdf=bool(pdf_files),
        pdf_size_mb=round(pdf_size, 1),
        pdf_files=pdf_files,
        has_extracted_text=False,
        text_size_kb=0.0,
        has_md_file=has_md,
        md_file_name=md_name,
        md_file_size_kb=md_size_kb,
        text_source="md" if has_md else "none",
        pipeline=_v2_pipeline_status(adapter, doc_dir, vid),
        findings_count=len(findings_items),
        findings_by_severity=findings_by_severity,
        optimization_count=len(opt_items) or int(opt_meta.get("total_items", 0) or 0),
        optimization_by_type=opt_by_type,
        optimization_savings_pct=opt_meta.get("estimated_savings_pct", 0),
        last_audit_date=(findings_data or {}).get("audit_date", (findings_data or {}).get("generated_at")) if isinstance(findings_data, dict) else None,
        total_batches=0,
        completed_batches=0,
        has_ocr=any(str(name).lower().endswith(("result.json", "ocr.html")) for name in input_files),
        block_count=0,
        block_errors=0,
        block_expected=0,
        pipeline_summary=_build_pipeline_summary(latest_dir, pipeline_version),
        pipeline_issues=_build_pipeline_issues(latest_dir, pipeline_version),
        pipeline_version=pipeline_version,
        version_id=version_entry["version_id"],
        version_no=version_entry["version_no"],
        version_label=version_entry["label"],
        latest_version_id=latest_id,
        version_count=versions_summary["version_count"],
        has_versions=versions_summary["has_versions"],
        is_latest_version=(version_entry["version_id"] == latest_id),
        versions_summary=versions_summary["versions"],
    )


def _get_project_status_v2(project_id: str, *, version_id: Optional[str] = None) -> Optional[ProjectStatus]:
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    adapter = ProjectsV2Adapter()
    if not adapter.is_available():
        raise FileNotFoundError(f"projects_v2 root not available: {adapter.objects_root}")
    doc = adapter.find_document_by_project_id(project_id)
    if doc is None:
        return None
    return _v2_project_status_from_doc(adapter, doc, version_id=version_id)


def _list_projects_v2(hidden: set[str]) -> list[ProjectStatus]:
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    adapter = ProjectsV2Adapter()
    if not adapter.is_available():
        raise FileNotFoundError(f"projects_v2 root not available: {adapter.objects_root}")
    projects: list[ProjectStatus] = []
    for doc in adapter.list_documents():
        if doc["document_code"] in hidden:
            continue
        status = _v2_project_status_from_doc(adapter, doc)
        if status:
            projects.append(status)
    return projects

def list_projects() -> list[ProjectStatus]:
    """Получить список всех проектов с их статусом."""
    hidden = _load_hidden_projects()
    if _v2_read_enabled():
        try:
            return _list_projects_v2(hidden)
        except Exception as exc:
            print(f"[projects_v2 read] list_projects fallback to legacy: {exc}")
    projects = []
    for project_id, entry in iter_project_dirs():
        if project_id in hidden:
            continue
        info_path = entry / "project_info.json"
        if not info_path.exists():
            pdf_files = list(entry.glob("*.pdf"))
            if not pdf_files:
                continue
            projects.append(ProjectStatus(
                project_id=project_id,
                name=project_id,
                description="(не подготовлен — нет project_info.json)",
                has_pdf=True,
                pdf_size_mb=round(pdf_files[0].stat().st_size / 1024 / 1024, 1),
            ))
            continue

        status = get_project_status(project_id)
        if status:
            projects.append(status)

    return projects


def get_project_status(
    project_id: str,
    *,
    version_id: Optional[str] = None,
) -> Optional[ProjectStatus]:
    """Получить полный статус одного проекта.

    При `version_id=None` читается **latest** версия проекта. Для legacy-проектов
    без `project_versions.json` это эквивалентно чтению корневой папки (V1).
    Для V2+ показатели читаются из `_versions/<version_id>/`; данные V1 НЕ
    смешиваются.
    """
    if _v2_read_enabled():
        try:
            status = _get_project_status_v2(project_id, version_id=version_id)
            if status is not None:
                return status
        except Exception as exc:
            print(f"[projects_v2 read] get_project_status fallback to legacy: {exc}")
    proj_dir = resolve_project_dir(project_id)
    if not proj_dir.exists():
        return None

    # Метаданные версий (legacy-проекты без project_versions.json → V1)
    versions_summary = version_service.get_versions_summary(proj_dir, project_id)
    latest_id = versions_summary["latest_version_id"]
    target_version_id = version_id or latest_id

    try:
        version_dir = version_service.get_version_dir(
            proj_dir, project_id, target_version_id,
        )
        version_entry = version_service.get_version_entry(
            proj_dir, project_id, target_version_id,
        )
    except version_service.VersionNotFoundError:
        return None

    # project_info: предпочитаем info из самой версии (V2+ создаёт свой
    # project_info.json через create_next_version), иначе fallback на корень.
    if is_projects_v2_version_dir(version_dir):
        info = load_version_project_info(version_dir)
    else:
        version_info_path = version_dir / "project_info.json"
        root_info_path = proj_dir / "project_info.json"
        info_path = version_info_path if version_info_path.exists() else root_info_path
        if not info_path.exists():
            return None
        info = _load_json(info_path)
    if not info:
        return None

    output_dir = version_dir / ("03_analysis/latest" if is_projects_v2_version_dir(version_dir) else "_output")
    if is_projects_v2_version_dir(version_dir):
        sources = resolve_version_source_files(version_dir, project_id, project_info=info)
        pdf_files = [str(p.relative_to(version_dir)) for p in sources.pdf_paths]
        has_pdf = bool(sources.pdf_paths)
        pdf_size_mb = round(sum(p.stat().st_size for p in sources.pdf_paths if p.is_file()) / 1024 / 1024, 1)
        md_path = sources.md_path
        has_md = md_path is not None and md_path.exists() and md_path.stat().st_size > 0
        md_file_name = str(md_path.relative_to(version_dir)) if has_md and md_path is not None else ""
        md_size_kb = round(md_path.stat().st_size / 1024, 1) if has_md and md_path is not None else 0.0
        has_ocr = bool(sources.result_json_paths)
    else:
        pdf_file = info.get("pdf_file") or ""
        pdf_files = info.get("pdf_files") or ([pdf_file] if pdf_file else [])
        # Пустая строка `pdf_file=""` (новая V2 без загрузок) → не пытаемся
        # сверяться с `version_dir / ""`, потому что Path("dir") / "" == Path("dir"),
        # и `dir.exists()` ошибочно даёт True.
        has_pdf = bool(pdf_file) and (version_dir / pdf_file).exists()
        pdf_size_mb = 0.0
        for pf in pdf_files:
            if not pf:
                continue
            pp = version_dir / pf
            if pp.exists() and pp.is_file():
                has_pdf = True
                pdf_size_mb += pp.stat().st_size / 1024 / 1024
        pdf_size_mb = round(pdf_size_mb, 1)

        # MD-файл (структурированный текст из внешнего OCR)
        md_file_name = info.get("md_file")
        has_md = False
        md_size_kb = 0.0
        if md_file_name:
            md_path = version_dir / md_file_name
            if md_path.exists() and md_path.stat().st_size > 0:
                has_md = True
                md_size_kb = round(md_path.stat().st_size / 1024, 1)
        has_ocr = bool(list(version_dir.glob("*_result.json")))

    text_path = output_dir / "extracted_text.txt"
    has_text = text_path.exists() and text_path.stat().st_size > 0
    text_size_kb = round(text_path.stat().st_size / 1024, 1) if has_text else 0.0

    # Основной текстовый источник аудита: только Markdown PDF representation.
    # extracted_text.txt может отображаться как артефакт, но не используется
    # как fallback для Stage 01.
    text_source = "md" if has_md else "none"

    # OCR-блоки (кропнутые image-блоки) — в папке версии
    block_count = 0
    block_errors = 0
    block_expected = 0
    blocks_index = gemma_blocks_index_path(version_dir)
    if not blocks_index.exists():
        # Fallback на legacy-папку для немигрированных проектов
        legacy_index = output_dir / "blocks" / "index.json"
        if legacy_index.exists():
            blocks_index = legacy_index
    if blocks_index.exists():
        bi = _load_json(blocks_index)
        if bi:
            block_count = bi.get("total_blocks", 0)
            block_errors = bi.get("errors", 0)
            block_expected = bi.get("total_expected", 0)

    # Pipeline status
    pipeline = _get_pipeline_status(output_dir, project_id=project_id)

    # Замечания
    findings_count = 0
    findings_by_severity = {}
    audit_date = None
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        findings_path = output_dir / "03_findings_pre_merge.json"
    if findings_path.exists():
        fdata = _load_json(findings_path)
        if fdata:
            items = fdata.get("findings", fdata.get("items", []))
            findings_count = len(items)
            for item in items:
                sev = item.get("severity", "НЕИЗВЕСТНО")
                findings_by_severity[sev] = findings_by_severity.get(sev, 0) + 1
            audit_date = fdata.get("audit_date", fdata.get("generated_at"))

    # Оптимизации
    optimization_count = 0
    optimization_by_type = {}
    optimization_savings_pct = 0
    opt_path = output_dir / "optimization.json"
    if opt_path.exists():
        odata = _load_json(opt_path)
        if odata and "meta" in odata:
            optimization_count = odata["meta"].get("total_items", 0)
            optimization_by_type = odata["meta"].get("by_type", {})
            optimization_savings_pct = odata["meta"].get("estimated_savings_pct", 0)

    # Пакеты блоков (приоритет) или тайлов (legacy)
    total_batches = 0
    completed_batches = 0
    batches_path = output_dir / "block_batches.json"
    batch_prefix = "block_batch"
    if not batches_path.exists():
        batches_path = output_dir / "tile_batches.json"
        batch_prefix = "tile_batch"
    if batches_path.exists():
        bdata = _load_json(batches_path)
        if bdata:
            total_batches = bdata.get("total_batches", len(bdata.get("batches", [])))
            for i in range(1, total_batches + 1):
                batch_file = output_dir / f"{batch_prefix}_{i:03d}.json"
                if batch_file.exists() and batch_file.stat().st_size > 100:
                    completed_batches += 1

    # Детальное саммари конвейера (зависит от pipeline_version)
    pipeline_version = info.get("pipeline_version", "legacy") or "legacy"
    pipeline_summary = _build_pipeline_summary(output_dir, pipeline_version)
    pipeline_issues = _build_pipeline_issues(output_dir, pipeline_version)

    # Статус экспертной оценки
    expert_review_status = ""
    findings_review_status = ""
    optimization_review_status = ""
    total_items = findings_count + optimization_count
    if total_items > 0:
        review_path = output_dir / "expert_review.json"
        if review_path.exists():
            rdata = _load_json(review_path)
            if rdata and "decisions" in rdata:
                decisions = rdata["decisions"]
                reviewed_count = len([d for d in decisions if d.get("decision") in ("accepted", "rejected")])
                if reviewed_count >= total_items:
                    expert_review_status = "complete"
                elif reviewed_count > 0:
                    expert_review_status = "partial"
                # Раздельный статус: findings vs optimizations.
                # Пустая строка означает "нет данных, не рисовать индикатор".
                if findings_count > 0:
                    f_reviewed = len([
                        d for d in decisions
                        if d.get("item_type") == "finding"
                        and d.get("decision") in ("accepted", "rejected")
                    ])
                    if f_reviewed >= findings_count:
                        findings_review_status = "complete"
                    elif f_reviewed > 0:
                        findings_review_status = "partial"
                if optimization_count > 0:
                    o_reviewed = len([
                        d for d in decisions
                        if d.get("item_type") == "optimization"
                        and d.get("decision") in ("accepted", "rejected")
                    ])
                    if o_reviewed >= optimization_count:
                        optimization_review_status = "complete"
                    elif o_reviewed > 0:
                        optimization_review_status = "partial"

    return ProjectStatus(
        project_id=project_id,
        name=info.get("name", project_id),
        description=info.get("description", ""),
        section=info.get("section", "EOM"),
        object=info.get("object"),
        has_pdf=has_pdf,
        pdf_size_mb=pdf_size_mb,
        pdf_files=[pf for pf in pdf_files if (version_dir / pf).exists()],
        has_extracted_text=has_text,
        text_size_kb=text_size_kb,
        has_md_file=has_md,
        md_file_name=md_file_name if has_md else None,
        md_file_size_kb=md_size_kb,
        text_source=text_source,
        pipeline=pipeline,
        findings_count=findings_count,
        findings_by_severity=findings_by_severity,
        optimization_count=optimization_count,
        optimization_by_type=optimization_by_type,
        optimization_savings_pct=optimization_savings_pct,
        last_audit_date=audit_date,
        total_batches=total_batches,
        completed_batches=completed_batches,
        has_ocr=has_ocr,
        block_count=block_count,
        block_errors=block_errors,
        block_expected=block_expected,
        pipeline_summary=pipeline_summary,
        pipeline_issues=pipeline_issues,
        pipeline_version=pipeline_version,
        expert_review_status=expert_review_status,
        findings_review_status=findings_review_status,
        optimization_review_status=optimization_review_status,
        version_id=version_entry["version_id"],
        version_no=version_entry["version_no"],
        version_label=version_entry["label"],
        latest_version_id=latest_id,
        version_count=versions_summary["version_count"],
        has_versions=versions_summary["has_versions"],
        is_latest_version=(version_entry["version_id"] == latest_id),
        versions_summary=versions_summary["versions"],
    )


def get_project_info(project_id: str, *, version_id: Optional[str] = None) -> Optional[dict]:
    """Прочитать raw project_info.json.

    При `version_id` (или активном bind_version) пытается прочитать
    `project_info.json` из папки версии; fallback — корневой info проекта.
    """
    proj_dir = resolve_project_dir(project_id)
    target_vid = version_service.resolve_effective_version_id(
        proj_dir, project_id, version_id,
    )
    try:
        version_dir = version_service.get_version_dir(proj_dir, project_id, target_vid)
    except version_service.VersionNotFoundError:
        return None

    if is_projects_v2_version_dir(version_dir):
        info = load_version_project_info(version_dir)
        if info:
            return info
    version_info = version_dir / "project_info.json"
    if version_info.exists():
        info = _load_json(version_info)
        if info:
            return info
    # Fallback — корневой info (legacy V1).
    return _load_json(proj_dir / "project_info.json")


def save_project_info(
    project_id: str, data: dict, *, version_id: Optional[str] = None,
) -> bool:
    """Сохранить project_info.json.

    При `version_id` (или активном bind_version) пишет в папку версии;
    fallback — корневой info проекта (legacy V1).
    """
    root_dir = resolve_project_dir(project_id)
    path = root_dir / "project_info.json"
    target_vid = version_service.resolve_effective_version_id(
        root_dir, project_id, version_id,
    )
    try:
        version_dir = version_service.get_version_dir(root_dir, project_id, target_vid)
        # Пишем в версию только если её папка реально существует (для legacy
        # V1 version_dir == root_dir, поведение не меняется).
        if version_dir.exists():
            path = version_dir / "project_info.json"
    except version_service.VersionNotFoundError:
        pass
    # Шаг 6A: v2-primary ветка активна ТОЛЬКО при WRITE_MODE=projects_v2_primary
    # (в проде НЕ включена). v2 первичен, legacy — fail-soft архив. В режимах
    # legacy/dual_write_shadow выполняется прежний путь ниже (без изменений).
    from backend.app.services.storage import storage_write_facade as _swf
    if _swf.v2_is_primary():
        from backend.app.services.storage.v2_primary_wiring import (
            save_project_info_v2_primary as _save_v2_primary,
        )
        return _save_v2_primary(
            project_id, data, version_id=target_vid,
            legacy_root=root_dir, legacy_path=path,
        )

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        return False

    # Step 9/10 dual-write canary: shadow-зеркало проекта в v2 после успешной
    # legacy-записи project_info.json (no-op в legacy, fail-soft). try/except —
    # чтобы путь `return True` оставался байт-идентичным прежнему поведению.
    try:
        _swf.shadow_mirror_project_path_safe(root_dir)
    except Exception:
        pass

    return True


def _write_v2_doc_section(doc_dir: Path, section: str) -> None:
    """Записать поле ``section`` в project_info всех версий v2-документа (in-place).

    Не создаёт ничего нового — только обновляет уже существующие
    ``versions/*/version.json`` (ключ ``project_info``) и
    ``versions/*/01_input/project_info.json``.
    """
    for vj in doc_dir.glob("versions/*/version.json"):
        try:
            d = json.loads(vj.read_text(encoding="utf-8"))
            pi = d.get("project_info")
            if isinstance(pi, dict):
                pi["section"] = section
                vj.write_text(
                    json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8"
                )
        except Exception:
            pass
    for pij in doc_dir.glob("versions/*/01_input/project_info.json"):
        try:
            d = json.loads(pij.read_text(encoding="utf-8"))
            d["section"] = section
            pij.write_text(
                json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8"
            )
        except Exception:
            pass




def _load_v2_document_json(doc_dir: Path) -> dict:
    try:
        data = json.loads((doc_dir / "document.json").read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _dump_v2_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")


def _legacy_unit_from_project_path(project_path: Path) -> tuple[Path, Path]:
    path = Path(project_path)
    if path.is_file():
        path = path.parent
    if path.parent.name.endswith(version_service.CONTAINER_SUFFIX):
        return path.parent, path
    return path, path


def _replace_path_prefix(value, old_base: Path, new_base: Path):
    if not isinstance(value, str) or not value:
        return value
    try:
        rel = Path(value).relative_to(old_base)
    except ValueError:
        return value
    return str(new_base / rel)


def _resolve_v2_legacy_project_path(project_id: str, doc_dir: Path) -> Path | None:
    dj = _load_v2_document_json(doc_dir)
    raw = str(dj.get("legacy_project_path") or "").strip()
    if raw:
        path = Path(raw)
        if path.exists():
            return path
    try:
        return resolve_project_dir(project_id, must_exist=True)
    except ProjectNotResolvedError:
        return None
    except Exception as exc:
        logger.warning(
            "[projects_v2] legacy path resolve failed for %s: %s",
            project_id,
            exc,
        )
        return None


def _plan_legacy_discipline_move(project_id: str, doc_dir: Path, target: str) -> dict | None:
    legacy_project_path = _resolve_v2_legacy_project_path(project_id, doc_dir)
    if legacy_project_path is None or not legacy_project_path.exists():
        return None
    source_unit, old_project_path = _legacy_unit_from_project_path(legacy_project_path)
    if not source_unit.exists():
        return None
    source_disc_dir = source_unit.parent
    target_disc_dir = source_disc_dir.parent / target
    target_unit = target_disc_dir / source_unit.name
    try:
        rel = old_project_path.relative_to(source_unit)
    except ValueError:
        rel = Path()
    new_project_path = target_unit / rel
    return {
        "source_unit": source_unit,
        "source_disc": source_disc_dir.name,
        "target_disc_dir": target_disc_dir,
        "target_unit": target_unit,
        "old_project_path": old_project_path,
        "new_project_path": new_project_path,
        "needs_move": source_disc_dir.name != target,
    }


def _ensure_legacy_move_has_no_conflict(plan: dict | None, target: str) -> None:
    if not plan or not plan.get("needs_move"):
        return
    target_unit = Path(plan["target_unit"])
    if target_unit.exists():
        raise ValueError(
            f"В legacy-разделе '{target}' уже есть проект '{target_unit.name}'"
        )


def _update_document_json_legacy_paths(doc_dir: Path, plan: dict) -> None:
    dj_path = doc_dir / "document.json"
    dj = _load_v2_document_json(doc_dir)
    if not dj:
        return
    old_unit = Path(plan["source_unit"])
    new_unit = Path(plan["target_unit"])
    dj["legacy_project_path"] = str(plan["new_project_path"])
    for key in ("legacy_folder_path", "legacy_path"):
        if key in dj:
            dj[key] = _replace_path_prefix(dj[key], old_unit, new_unit)
    versions = dj.get("versions")
    if isinstance(versions, list):
        for version in versions:
            if not isinstance(version, dict):
                continue
            for key in ("legacy_project_path", "legacy_folder_path", "legacy_path"):
                if key in version:
                    version[key] = _replace_path_prefix(version[key], old_unit, new_unit)
    _dump_v2_json(dj_path, dj)

    for vj_path in doc_dir.glob("versions/*/version.json"):
        try:
            vj = json.loads(vj_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(vj, dict):
            continue
        changed = False
        for key in ("legacy_project_path", "legacy_folder_path", "legacy_path"):
            if key in vj:
                updated = _replace_path_prefix(vj[key], old_unit, new_unit)
                if updated != vj[key]:
                    vj[key] = updated
                    changed = True
        if changed:
            _dump_v2_json(vj_path, vj)


def _write_v2_doc_discipline(doc_dir: Path, target: str) -> None:
    dj_path = doc_dir / "document.json"
    try:
        dj = _load_v2_document_json(doc_dir)
        if dj:
            dj["discipline"] = target
            _dump_v2_json(dj_path, dj)
    except Exception:
        pass


def _apply_legacy_discipline_move(project_id: str, doc_dir: Path, plan: dict | None) -> None:
    if not plan:
        return
    if not plan.get("needs_move"):
        _update_document_json_legacy_paths(doc_dir, plan)
        return
    try:
        target_disc_dir = Path(plan["target_disc_dir"])
        target_unit = Path(plan["target_unit"])
        target_disc_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(plan["source_unit"]), str(target_unit))
        _update_document_json_legacy_paths(doc_dir, plan)
        invalidate_project_cache()
    except Exception as exc:
        logger.warning(
            "[projects_v2] legacy folder move failed for %s: %s",
            project_id,
            exc,
        )

def _move_v2_document_discipline(project_id: str, section: str) -> bool:
    # v2 discipline is the folder name. Move the v2 document first, then move
    # the matching legacy unit so future re-migration cannot recreate old-disc duplicates.
    target = (section or "").strip()
    if not target:
        return False
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    adapter = ProjectsV2Adapter()
    if not adapter.is_available():
        return False
    doc = adapter.find_document_by_project_id(project_id)
    if not doc:
        return False
    doc_dir = Path(doc["doc_dir"])
    legacy_plan = _plan_legacy_discipline_move(project_id, doc_dir, target)
    _ensure_legacy_move_has_no_conflict(legacy_plan, target)

    current_disc = doc_dir.parent.parent.name
    if current_disc != target:
        object_dir = doc_dir.parents[3]
        target_docs = object_dir / "disciplines" / target / "documents"
        target_dir = target_docs / doc_dir.name
        if target_dir.exists():
            raise ValueError(
                f"В разделе '{target}' уже есть проект '{doc_dir.name}'"
            )
        target_docs.mkdir(parents=True, exist_ok=True)
        shutil.move(str(doc_dir), str(target_dir))
        doc_dir = target_dir

    _write_v2_doc_discipline(doc_dir, target)
    _write_v2_doc_section(doc_dir, target)
    _apply_legacy_discipline_move(project_id, doc_dir, legacy_plan)
    return True


def set_project_section(project_id: str, section: str) -> dict:
    """Сменить дисциплину проекта.

    Legacy: пишет поле ``section`` в project_info.json (дисциплина определяется
    этим полем). Под v2-primary ФИЗИЧЕСКИ переносит документ в папку
    ``disciplines/<section>/`` (read_canary группирует по папке) и пишет section
    прямо в project_info перенесённого документа.

    ВАЖНО: под v2-primary при успешном переносе НЕ вызываем legacy
    ``save_project_info`` — его v2-target резолвится из legacy-папки (которая
    осталась в старой дисциплине) и заскаффолдил бы ПУСТОЙ документ-дубль в
    старом разделе (баг 2026-06-23, проект ОВ1.1-ПА).

    Для незарегистрированных проектов (папка с PDF, без project_info.json)
    создаёт минимальный project_info.json с указанным разделом.
    """
    info = get_project_info(project_id)
    if not info:
        proj_dir = resolve_project_dir(project_id)
        if not proj_dir.exists():
            raise ValueError(f"Папка проекта '{project_id}' не найдена")
        pdf_files = sorted(p.name for p in proj_dir.glob("*.pdf") if p.is_file())
        info = {
            "project_id": project_id,
            "name": project_id,
            "section": section,
            "description": "",
            "pdf_file": pdf_files[0] if pdf_files else "",
            "pdf_files": pdf_files,
        }
    else:
        info["section"] = section
    try:
        from backend.app.services.storage import storage_write_facade as _swf
        _v2_primary = _swf.v2_is_primary()
    except Exception:
        _v2_primary = False
    if _v2_primary:
        # Перенос сам обновляет document.json + project_info.section. Конфликт/
        # ошибка → ValueError (section не записан, без рассинхрона).
        if _move_v2_document_discipline(project_id, section):
            invalidate_project_cache()
            return info
        # v2-документ не найден (legacy-only) → обычная legacy-запись ниже.
        invalidate_project_cache()
    if not save_project_info(project_id, info):
        raise ValueError(f"Не удалось сохранить project_info.json для '{project_id}'")
    # #78: создание project_info.json для голой папки может поменять классификацию
    # проекта в iter_project_dirs — сбрасываем кеш.
    invalidate_project_cache()
    return info


def _get_pipeline_status(output_dir: Path, *, project_id: Optional[str] = None) -> PipelineStatus:
    """Определить статус конвейера.

    Приоритет: pipeline_log.json > файловая проверка (fallback).

    `project_id` нужен для корректной проверки `pipeline_manager.is_running`:
    для V2 `output_dir.parent.name` == "v2", не настоящий project_id, поэтому
    имя папки использовать нельзя.
    """
    status = PipelineStatus()
    gemma_state = evaluate_gemma_enrichment(output_dir.parent)
    gemma_migration = detect_gemma_migration_state(output_dir.parent, gemma_state=gemma_state)

    # 1. Попытка прочитать pipeline_log.json (персистентный лог этапов)
    log = _load_pipeline_log(output_dir)
    if log and "stages" in log:
        stages = log["stages"]
        # Маппинг: ключ в pipeline_log → поле PipelineStatus
        mapping = {
            "crop_blocks": "crop_blocks",
            "gemma_enrichment": "gemma_enrichment",
            "text_analysis": "text_analysis",
            "block_analysis": "blocks_analysis",
            "block_retry": "block_retry",
            "findings_merge": "findings",
            "findings_critic": "findings_critic",
            "findings_corrector": "findings_corrector",
            "norm_verify": "norms_verified",
            "optimization": "optimization",
            "optimization_critic": "optimization_critic",
            "optimization_corrector": "optimization_corrector",
            "debt_control": "debt_control",
            "decision_carryover": "decision_carryover",
            "excel": "excel",
            # Legacy aliases
            "prepare": "crop_blocks",
            "tile_audit": "blocks_analysis",
            "main_audit": "findings",
        }
        valid_statuses = ("done", "error", "partial", "running", "skipped", "interrupted")
        # Маппинг: ключ pipeline_log → файл-индикатор завершения
        output_files = {
            "crop_blocks": f"{GEMMA_BLOCKS_DIRNAME}/index.json",
            "gemma_enrichment": "gemma_enrichment_summary.json",
            "text_analysis": "01_text_analysis.json",
            "block_analysis": "02_blocks_analysis.json",
            "findings_merge": "03_findings.json",
            "findings_critic": "03_findings_review.json",
            "findings_corrector": "03_findings.json",
            "norm_verify": "03a_norms_verified.json",
            "optimization": "optimization.json",
            "optimization_critic": "optimization_review.json",
            "optimization_corrector": "optimization.json",
            "debt_control": "migrated_findings_report.json",
            "decision_carryover": "decision_carryover_report.json",
            # Legacy aliases
            "prepare": f"{GEMMA_BLOCKS_DIRNAME}/index.json",
            "tile_audit": "02_blocks_analysis.json",
            "main_audit": "03_findings.json",
        }
        for log_key, field in mapping.items():
            stage_info = stages.get(log_key, {})
            s = stage_info.get("status", "pending")
            if s in valid_statuses:
                # "interrupted" (рестарт сервера) → показывать как "error"
                if s == "interrupted":
                    s = "error"
                # Защита: если "running" но нет активного job → считать "error"
                if s == "running":
                    from backend.app.pipeline.manager import pipeline_manager
                    # Для V2 output_dir.parent.name == "v2" и не равен
                    # project_id — поэтому используем явно переданный.
                    proj_id = project_id or output_dir.parent.name
                    if not pipeline_manager.is_running(proj_id):
                        s = "error"
                if log_key == "gemma_enrichment":
                    if gemma_migration.get("migration_required"):
                        s = "migration_required"
                    elif gemma_state.get("ready"):
                        s = "partial" if gemma_state.get("status") in {"partial_allowed", "partial"} else "done"
                    elif s in ("done", "partial", "skipped"):
                        s = "error"
                    setattr(status, field, s)
                    continue
                # Кросс-валидация: если "error" но выходной файл существует → "done"
                if s == "error":
                    out_file = output_files.get(log_key)
                    if out_file and (output_dir / out_file).exists():
                        fsize = (output_dir / out_file).stat().st_size
                        if fsize > 100:
                            s = "done"
                setattr(status, field, s)
        return status

    # 2. Fallback: логика по файлам (для проектов без pipeline_log.json)
    blocks_index = gemma_blocks_index_path(output_dir.parent)
    if blocks_index.exists():
        status.crop_blocks = "done"

    if gemma_migration.get("migration_required"):
        status.gemma_enrichment = "migration_required"
    elif gemma_state.get("ready"):
        status.gemma_enrichment = "partial" if gemma_state.get("status") in {"partial_allowed", "partial"} else "done"
    elif gemma_state.get("status") not in {"missing_blocks", "missing_md", "missing"}:
        status.gemma_enrichment = "error"

    if (output_dir / "01_text_analysis.json").exists():
        status.text_analysis = "done"

    if (output_dir / "02_blocks_analysis.json").exists():
        status.blocks_analysis = "done"
    elif list(output_dir.glob("block_batch_*.json")):
        status.blocks_analysis = "partial"

    if (output_dir / "03_findings.json").exists():
        status.findings = "done"

    if (output_dir / "03a_norms_verified.json").exists():
        status.norms_verified = "done"
    elif (output_dir / "norm_checks.json").exists():
        status.norms_verified = "partial"

    if (output_dir / "optimization.json").exists():
        status.optimization = "done"

    if (output_dir / "migrated_findings_report.json").exists():
        status.debt_control = "done"

    if (output_dir / "decision_carryover_report.json").exists():
        status.decision_carryover = "done"

    return status


def _load_pipeline_log(output_dir: Path) -> Optional[dict]:
    """Прочитать pipeline_log.json."""
    return _load_json(output_dir / "pipeline_log.json")


# Порядок и человеко-понятные названия этапов конвейера
_PIPELINE_STAGE_ORDER = [
    ("crop_blocks", "Кроп блоков"),
    ("gemma_enrichment", GEMMA_STAGE_LABEL),
    ("text_analysis", "Анализ текста"),
    ("block_analysis", "Анализ блоков"),
    ("block_retry", "Retry нечитаемых блоков"),
    ("findings_merge", "Свод замечаний"),
    ("findings_critic", "Critic замечаний"),
    ("findings_corrector", "Corrector замечаний"),
    ("norm_verify", "Верификация норм"),
    ("optimization", "Оптимизация"),
    ("optimization_critic", "Critic оптимизации"),
    ("optimization_corrector", "Corrector оптимизации"),
    ("debt_control", "Контроль долгов"),
    ("decision_carryover", "Перенос вердиктов"),
    ("excel", "Excel-отчёт"),
]

def _get_stage_order(pipeline_version: str = "legacy") -> list[tuple[str, str]]:
    """Вернуть список (key, label) этапов конвейера.

    По флагу PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED блоки (+retry) идут ПЕРЕД текстом.
    """
    from backend.app.core import config as cfg
    if not getattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False):
        return _PIPELINE_STAGE_ORDER
    order = [p for p in _PIPELINE_STAGE_ORDER if p[0] != "text_analysis"]
    text_tuple = next(p for p in _PIPELINE_STAGE_ORDER if p[0] == "text_analysis")
    anchor_keys = {"block_retry", "block_analysis"}
    insert_at = max(i for i, p in enumerate(order) if p[0] in anchor_keys) + 1
    order.insert(insert_at, text_tuple)
    return order


def _build_pipeline_issues(output_dir: Path, pipeline_version: str = "legacy") -> list[str]:
    """Извлечь проблемы конвейера для индикатора на дашборде.

    Проверяет:
    - Этапы с ошибками (error/interrupted)
    - Critic/Corrector пропущены при наличии findings
    - Нормы/оптимизация не запускались
    """
    issues = []
    # Миграция Gemma schema v2 не показывается как pipeline_issue на дашборде:
    # старые проекты (Qwen/legacy) считаются рабочими, новые проверяются через Gemma.

    log = _load_pipeline_log(output_dir)
    if not log or "stages" not in log:
        return issues

    stages = log["stages"]
    stage_order = _get_stage_order(pipeline_version)

    # Этапы с ошибками
    _labels = dict(stage_order)
    for key, label in stage_order:
        info = stages.get(key, {})
        s = info.get("status", "")
        if s in ("error", "interrupted"):
            short_err = info.get("error", "")
            if short_err and len(short_err) > 80:
                short_err = short_err[:77] + "..."
            issues.append(f"{label}: {short_err}" if short_err else f"{label}: ошибка")

    # Findings есть, но critic/corrector не запускались
    has_findings = (output_dir / "03_findings.json").exists()
    findings_key = "findings_merge"
    if has_findings:
        if "findings_critic" not in stages and findings_key in stages:
            issues.append("Critic замечаний: не запускался")
        # Corrector пропущен при наличии проблем в review
        review_path = output_dir / "03_findings_review.json"
        if review_path.exists() and "findings_corrector" not in stages:
            try:
                import json
                rd = json.loads(review_path.read_text(encoding="utf-8"))
                verdicts = rd.get("meta", {}).get("verdicts", {})
                total_pass = verdicts.get("pass", 0)
                total_reviewed = rd.get("meta", {}).get("total_reviewed", 0)
                if total_reviewed > total_pass:
                    issues.append(f"Corrector: пропущен ({total_reviewed - total_pass} проблем)")
            except Exception:
                pass

    # Нормы не запускались
    if has_findings and "norm_verify" not in stages and findings_key in stages:
        issues.append("Верификация норм: не запускалась")

    return issues


def _normalize_crop_blocks_status(
    output_dir: Path,
    stages: dict,
) -> tuple[str, str]:
    """Нормализовать статус crop_blocks.

    Источники истины (по убыванию приоритета):
      1) pipeline_log.crop_blocks.status == "done" → done
      2) legacy pipeline_log.prepare.status == "done" → done
      3) существующий _output/blocks_gemma_100/index.json → done
      4) raw status из лога (running/error/partial/...) или pending
    """
    info = stages.get("crop_blocks") or {}
    legacy = stages.get("prepare") or {}
    message = info.get("message") or legacy.get("message") or ""
    raw_status = info.get("status") or ""

    if raw_status == "done":
        return "done", message
    if legacy.get("status") == "done":
        return "done", message

    blocks_index = gemma_blocks_index_path(output_dir.parent)
    if blocks_index.exists():
        try:
            if blocks_index.stat().st_size > 10:
                return "done", message
        except OSError:
            pass

    if raw_status:
        return raw_status, message
    return "pending", message


def _build_gemma_done_message(
    *,
    blocks_ok: int,
    blocks_total: int,
    blocks_failed: int,
    high_detail_skipped_large: int,
) -> str:
    """Сформировать пользовательское message для status=done.

    Учитывает high_detail_skipped_large_block — он не понижает статус,
    но достоин упоминания, чтобы пользователь знал, что часть блоков прошла
    через fallback base 100 DPI.
    """
    parts = [f"Готово: {blocks_ok}/{blocks_total} блоков обработано, {blocks_failed} упали."]
    if high_detail_skipped_large > 0:
        suffix = "блок" if high_detail_skipped_large == 1 else "блоков"
        if high_detail_skipped_large == 1:
            parts.append(
                "Один блок не прошёл high-detail 300 DPI из-за safety cutoff, "
                "использован базовый профиль gemma_100_base."
            )
        else:
            parts.append(
                f"{high_detail_skipped_large} {suffix} не прошли high-detail 300 DPI "
                "из-за safety cutoff, использован базовый профиль gemma_100_base."
            )
    return " ".join(parts)


def _build_gemma_partial_message(
    *,
    blocks_ok: int,
    blocks_total: int,
    blocks_failed: int,
    uncovered_block_ids: list,
) -> str:
    """Сформировать пользовательское message для status=partial.

    partial означает реальные пропуски: failed > 0 или uncovered != []. Сюда
    же попадает legacy-кейс с partial из pipeline_log, если есть failed.
    """
    parts = [f"Выполнено с предупреждениями: {blocks_ok}/{blocks_total} блоков, {blocks_failed} упали."]
    if uncovered_block_ids:
        preview = ", ".join(str(b) for b in uncovered_block_ids[:5])
        more = "" if len(uncovered_block_ids) <= 5 else f" (и ещё {len(uncovered_block_ids) - 5})"
        parts.append(f"Есть непокрытые блоки: {preview}{more}.")
    return " ".join(parts)


def _normalize_gemma_enrichment_status(
    output_dir: Path,
    stages: dict,
) -> tuple[str, str, str]:
    """Нормализовать статус gemma_enrichment.

    Возвращает (status, user_message, raw_message). raw_message — исходный
    `pipeline_log.stages.gemma_enrichment.message` (может быть пустым); UI и
    тесты используют его как debug/detail.original_message. user_message —
    переформулированное под текущий статус сообщение для пользователя.

    Логика статусов:
      - migration_required (detect_gemma_migration_state) → migration_required
      - evaluate_gemma_enrichment(...).ready == True:
          * blocks_ok >= blocks_total и failed_blocks == 0 и нет uncovered → done
          * иначе → partial
      - log status=partial и detail.blocks_ok == detail.blocks_total и
        detail.blocks_failed == 0 → done
      - log status=partial и detail.blocks_failed > 0 → partial
      - в остальном — raw status из лога (или pending)
    """
    info = stages.get("gemma_enrichment") or {}
    raw_status = info.get("status") or ""
    raw_message = info.get("message", "")
    detail = info.get("detail") or {}

    gemma_state = evaluate_gemma_enrichment(output_dir.parent)
    gemma_migration = detect_gemma_migration_state(output_dir.parent, gemma_state=gemma_state)

    if gemma_migration.get("migration_required"):
        return "migration_required", raw_message, raw_message

    if gemma_state.get("ready"):
        blocks_ok = int(gemma_state.get("blocks_ok") or 0)
        blocks_total = int(gemma_state.get("blocks_total") or 0)
        uncovered = list(gemma_state.get("uncovered_block_ids") or [])
        high_detail_skipped_large = int(gemma_state.get("high_detail_skipped_large") or 0)
        # blocks_failed exposed только через сводку — читаем напрямую.
        summary_path = output_dir / "gemma_enrichment_summary.json"
        summary_failed = 0
        if summary_path.exists():
            try:
                with open(summary_path, "r", encoding="utf-8") as f:
                    sdata = json.load(f)
                summary_failed = int(sdata.get("blocks_failed") or 0)
                if not high_detail_skipped_large:
                    high_detail_skipped_large = int(sdata.get("high_detail_skipped_large") or 0)
            except (OSError, json.JSONDecodeError, ValueError):
                summary_failed = 0
        if (
            blocks_ok >= blocks_total
            and summary_failed == 0
            and not uncovered
        ):
            user_message = _build_gemma_done_message(
                blocks_ok=blocks_ok,
                blocks_total=blocks_total,
                blocks_failed=summary_failed,
                high_detail_skipped_large=high_detail_skipped_large,
            )
            return "done", user_message, raw_message
        user_message = _build_gemma_partial_message(
            blocks_ok=blocks_ok,
            blocks_total=blocks_total,
            blocks_failed=summary_failed,
            uncovered_block_ids=uncovered,
        )
        return "partial", user_message, raw_message

    if raw_status == "partial" and isinstance(detail, dict):
        blocks_ok = detail.get("blocks_ok")
        blocks_total = detail.get("blocks_total")
        blocks_failed = detail.get("blocks_failed")
        if (
            isinstance(blocks_ok, int)
            and isinstance(blocks_total, int)
            and isinstance(blocks_failed, int)
        ):
            if blocks_ok == blocks_total and blocks_failed == 0:
                user_message = _build_gemma_done_message(
                    blocks_ok=blocks_ok,
                    blocks_total=blocks_total,
                    blocks_failed=0,
                    high_detail_skipped_large=0,
                )
                return "done", user_message, raw_message
            if blocks_failed > 0:
                user_message = _build_gemma_partial_message(
                    blocks_ok=blocks_ok,
                    blocks_total=blocks_total,
                    blocks_failed=blocks_failed,
                    uncovered_block_ids=list(detail.get("uncovered_block_ids") or []),
                )
                return "partial", user_message, raw_message

    if raw_status:
        return raw_status, raw_message, raw_message
    return "pending", raw_message, raw_message


# Legacy/alternative ключи pipeline_log → канонический stage_key.
# При сборке pipeline_summary, если в pipeline_log нет канонического ключа,
# но есть один из alias — берём его статус/message.
_PIPELINE_STAGE_ALIASES: dict[str, tuple[str, ...]] = {
    "crop_blocks": ("prepare",),
    "block_analysis": ("v4_extraction", "tile_audit"),
    "findings_merge": ("v4_formatter", "main_audit"),
}

# Артефакты на ФС, доказывающие что этап выполнен.
# Путь относительно `_output/`. Если файл/папка существует и не пустой —
# статус этапа можно поднять до done.
_PIPELINE_STAGE_ARTIFACTS: dict[str, tuple[str, ...]] = {
    "crop_blocks": (f"{GEMMA_BLOCKS_DIRNAME}/index.json",),
    "text_analysis": ("01_text_analysis.json",),
    "block_analysis": ("02_blocks_analysis.json",),
    "findings_merge": ("03_findings.json",),
    "findings_critic": ("03_findings_review.json",),
    # corrector обновляет тот же 03_findings.json + оставляет pre_review бэкап
    "findings_corrector": ("03_findings.json",),
    "norm_verify": ("03a_norms_verified.json", "norm_checks.json"),
    "optimization": ("optimization.json",),
    "optimization_critic": ("optimization_review.json",),
    "optimization_corrector": ("optimization.json",),
    "debt_control": ("migrated_findings_report.json",),
    "decision_carryover": ("decision_carryover_report.json",),
}

# Канонический порядок индексов для downstream-проверок. Если индекс этапа i
# меньше индекса этапа j и j завершён, и есть артефакт для i — i тоже done.
_DOWNSTREAM_DEPENDENCY: dict[str, tuple[str, ...]] = {
    # findings_critic done → findings_merge done (corrector тоже зависит от merge)
    "findings_merge": ("findings_critic", "findings_corrector"),
    # findings_corrector done → findings_critic done
    "findings_critic": ("findings_corrector",),
    # norm_verify done → findings_merge done (нормы строятся из findings)
    # Не выводим, потому что norm_verify может запускаться параллельно.
    # block_analysis done подтверждает text_analysis: text используется
    # для построения батчей анализа блоков.
    "text_analysis": ("block_analysis", "findings_merge"),
    # block_analysis done подтверждается findings_merge (мердж читает blocks).
    "block_analysis": ("findings_merge",),
    # optimization_corrector done → optimization_critic done
    "optimization_critic": ("optimization_corrector",),
    # optimization_critic done → optimization done
    "optimization": ("optimization_critic", "optimization_corrector"),
    # gemma_enrichment — legacy: если block_analysis/findings уже done без
    # Gemma, значит проект использовал старый Qwen-конвейер.
    "gemma_enrichment": ("block_analysis", "findings_merge"),
}

# Fallback message по терминальному статусу, если в логе message пустой.
_STATUS_FALLBACK_MESSAGE: dict[str, str] = {
    "done": "Готово",
    "partial": "Выполнено с предупреждениями",
    "skipped": "Пропущено",
    "running": "Выполняется…",
    "error": "Ошибка",
    "interrupted": "Прервано",
    "migration_required": "Требуется миграция",
    "pending": "",
}

# Ключи, по которым определяется, что в pipeline_log записан legacy v4 или
# pre-Gemma запуск. Используется для классификации pending → skipped.
_LEGACY_PIPELINE_MARKERS: tuple[str, ...] = (
    "v4_extraction", "v4_memory", "v4_candidates", "v4_formatter",
    "main_audit", "qwen_enrichment",
)

# Этапы, которые в legacy v4/pre-Gemma конвейере не существовали и не должны
# показываться как pending для уже завершённых старых аудитов. Эти этапы
# превращаются в "skipped" с понятным сообщением, если присутствует legacy-
# маркер или последующий обязательный этап уже done.
#
# gemma_enrichment вынесен в отдельный блок (специальная нормализация), здесь
# перечисляем дополнительные этапы, появившиеся вместе/после Gemma:
#   - block_retry — добавлен после внедрения Gemma OCR retry-логики;
#     в v4 чёткого retry-этапа не было.
_LEGACY_OPTIONAL_STAGES: dict[str, str] = {
    "block_retry": (
        "Пропущено: legacy-аудит не использовал retry нечитаемых блоков."
    ),
}


def _has_legacy_marker(stages: dict) -> bool:
    """Есть ли в pipeline_log хотя бы один legacy v4/pre-Gemma маркер."""
    return any(stages.get(k) for k in _LEGACY_PIPELINE_MARKERS)


def _artifact_exists(output_dir: Path, rel: str) -> bool:
    """Проверить, что артефакт существует и не пустой."""
    p = output_dir / rel
    try:
        if not p.exists():
            return False
        if p.is_file():
            return p.stat().st_size > 10
        if p.is_dir():
            # для папок (например, blocks_gemma_100) считаем существование
            # самого индекс-файла, который уже проверяет вызывающий код.
            return True
    except OSError:
        return False
    return False


def _has_any_artifact(output_dir: Path, key: str) -> bool:
    """Хотя бы один артефакт этапа существует на ФС."""
    for rel in _PIPELINE_STAGE_ARTIFACTS.get(key, ()):
        if _artifact_exists(output_dir, rel):
            return True
    return False


def _stage_info_with_aliases(stages: dict, key: str) -> tuple[dict, str | None]:
    """Вернуть (info, alias_used_or_none).

    Если канонический ключ есть — возвращаем его. Иначе ищем alias.
    """
    info = stages.get(key)
    if info:
        return info, None
    for alias in _PIPELINE_STAGE_ALIASES.get(key, ()):
        alias_info = stages.get(alias)
        if alias_info:
            return alias_info, alias
    return {}, None


def _downstream_done(
    stages: dict,
    key: str,
    inferred_status: dict[str, str],
) -> bool:
    """Есть ли downstream-этап в терминальном done/partial-состоянии.

    inferred_status — уже посчитанные статусы для предыдущих этапов в текущем
    проходе. Сюда же подтягиваются alias-этапы.
    """
    downstream_keys = _DOWNSTREAM_DEPENDENCY.get(key, ())
    if key == "text_analysis":
        # В порядке block→text «block_analysis done» больше НЕ подтверждает text
        # (блоки идут первыми). Оставляем только реальную зависимость findings_merge.
        from backend.app.core import config as cfg
        if getattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False):
            downstream_keys = tuple(k for k in downstream_keys if k != "block_analysis")
    if not downstream_keys:
        return False
    for dk in downstream_keys:
        # 1) inferred_status уже даёт ответ
        s = inferred_status.get(dk)
        if s in ("done", "partial"):
            return True
        # 2) raw pipeline_log
        info, _ = _stage_info_with_aliases(stages, dk)
        if info.get("status") in ("done", "partial"):
            return True
    return False


def _normalize_pipeline_stage_status(
    output_dir: Path,
    key: str,
    stages: dict,
    inferred_status: dict[str, str],
) -> tuple[str, str, str | None, str | None]:
    """Универсальный нормализатор статуса этапа.

    Возвращает (status, user_message, raw_message, alias_used).

    Логика по приоритету:
      1) crop_blocks → _normalize_crop_blocks_status (старый специальный).
      2) gemma_enrichment → _normalize_gemma_enrichment_status (старый).
      3) Прямая запись в pipeline_log для канонического ключа.
      4) Запись в pipeline_log для legacy alias.
      5) Артефакт на ФС → done (со сгенерированным message).
      6) Downstream-этап done → done (со сгенерированным message).
      7) pending (без message).
    """
    if key == "crop_blocks":
        status, normalized_message = _normalize_crop_blocks_status(output_dir, stages)
        return status, normalized_message or "", None, None
    if key == "gemma_enrichment":
        status, user_message, raw_message = _normalize_gemma_enrichment_status(
            output_dir, stages,
        )
        # Legacy v4-проект без gemma_100: gemma_state.ready=False и
        # migration_required=False (нет даже blocks_gemma_100). При этом
        # downstream этапы (block_analysis, findings_merge) уже done.
        # Для UI это не «незавершённый этап» (○), а «пропущенный» (—):
        # этап был не нужен в legacy pipeline. Признак legacy: либо
        # downstream done, либо в pipeline_log стоят legacy-маркеры
        # v4_extraction / v4_formatter / main_audit / qwen_enrichment.
        if status == "pending" and (
            _has_legacy_marker(stages)
            or _downstream_done(stages, "gemma_enrichment", inferred_status)
        ):
            status = "skipped"
            if not user_message:
                user_message = (
                    "Пропущено: legacy-аудит выполнен до внедрения "
                    "Gemma OCR enrichment."
                )
        return status, user_message, raw_message, None

    info, alias_used = _stage_info_with_aliases(stages, key)
    raw_status = info.get("status") or ""
    raw_message = info.get("message") or ""

    if raw_status:
        # Если терминальный статус — пропускаем сразу.
        if raw_status in ("done", "partial", "skipped", "error", "interrupted",
                          "running", "migration_required"):
            return raw_status, raw_message, None, alias_used

    # Артефакт-based inference.
    if _has_any_artifact(output_dir, key):
        # Подбираем дружелюбный message.
        msg = raw_message or "Готово (обнаружен артефакт)"
        return "done", msg, raw_message or None, alias_used

    # decision_carryover появился позже остальных этапов: у уже завершённых
    # аудитов нет ни записи в pipeline_log, ни отчёта. Если Excel (финальный
    # этап) done — аудит выполнен до внедрения переноса, показываем skipped.
    if key in ("decision_carryover", "debt_control"):
        excel_status = inferred_status.get("excel") or (
            (stages.get("excel") or {}).get("status") or ""
        )
        if excel_status in ("done", "partial") or _has_legacy_marker(stages):
            return (
                "skipped",
                "Пропущено: аудит выполнен до внедрения этого этапа.",
                raw_message or None,
                alias_used,
            )

    # Downstream-based inference.
    if _downstream_done(stages, key, inferred_status):
        msg = raw_message or "Готово (определено по последующему этапу)"
        return "done", msg, raw_message or None, alias_used

    # Legacy-skipped inference: этапы, которых не было в v4/pre-Gemma конвейере.
    # Если есть legacy-маркер (v4_extraction / qwen_enrichment / …) —
    # этап в этом аудите никогда не запускался и не должен оставаться pending.
    if key in _LEGACY_OPTIONAL_STAGES and _has_legacy_marker(stages):
        legacy_msg = _LEGACY_OPTIONAL_STAGES[key]
        msg = raw_message or legacy_msg
        return "skipped", msg, raw_message or None, alias_used

    # raw status есть, но не входит в известный набор → отдаём как есть.
    if raw_status:
        return raw_status, raw_message, None, alias_used

    return "pending", raw_message, None, alias_used


def _build_pipeline_summary(output_dir: Path, pipeline_version: str = "legacy") -> list[dict]:
    """Собрать детальное саммари конвейера из pipeline_log.json.

    Возвращает ВСЕ этапы конвейера. Если этап ещё не запускался —
    возвращает его со статусом "pending".

    Источники истины (по убыванию приоритета):
      1) pipeline_log.<key>.status (терминальный) → как есть.
      2) pipeline_log.<alias>.status для legacy aliases (prepare, v4_extraction,
         tile_audit, main_audit, …).
      3) Артефакт на ФС (_PIPELINE_STAGE_ARTIFACTS) → done.
      4) Downstream-этап done → done (например, findings_critic done означает
         что findings_merge тоже done).
      5) pending.

    Для crop_blocks и gemma_enrichment действуют специальные нормализаторы
    с расширенной семантикой (см. _normalize_crop_blocks_status и
    _normalize_gemma_enrichment_status).

    Возвращает список dict:
      {key, label, status, message, duration_sec, error, raw_message?}
    """
    log = _load_pipeline_log(output_dir)
    stages = log.get("stages", {}) if log else {}

    # Предпроход: посчитать статусы по pipeline_log + alias + артефактам,
    # чтобы _downstream_done мог смотреть и в "будущие" этапы. Без предпрохода
    # gemma_enrichment не узнает что block_analysis done через v4_extraction.
    prelim: dict[str, str] = {}
    for key, _label in _get_stage_order(pipeline_version):
        info, _ = _stage_info_with_aliases(stages, key)
        raw_s = info.get("status") or ""
        if raw_s:
            prelim[key] = raw_s
        elif _has_any_artifact(output_dir, key):
            prelim[key] = "done"

    result = []
    inferred_status: dict[str, str] = dict(prelim)
    for key, label in _get_stage_order(pipeline_version):
        info, alias_used = _stage_info_with_aliases(stages, key)
        status, user_message, raw_message, _alias = _normalize_pipeline_stage_status(
            output_dir, key, stages, inferred_status,
        )
        inferred_status[key] = status
        message = user_message or ""

        # Вычислить длительность только если в логе есть метки времени.
        duration_sec = None
        started = info.get("started_at")
        completed = info.get("completed_at") or info.get("interrupted_at")
        if started and completed:
            try:
                from datetime import datetime
                t0 = datetime.fromisoformat(started)
                t1 = datetime.fromisoformat(completed)
                duration_sec = round((t1 - t0).total_seconds())
            except Exception:
                pass

        entry = {
            "key": key,
            "label": label,
            "status": status,
        }
        # Минимальная запись возможна только когда статус pending И не было
        # ни лога, ни сгенерированного message (нормализатор может вернуть
        # объяснительный message даже для pending — например, legacy v4).
        if not info and status == "pending" and not message:
            result.append(entry)
            continue
        # Гарантируем непустой message для терминальных статусов, чтобы UI не
        # показывал «пустую» строку. Fallback применяется только если другие
        # источники message пустые.
        if not message and status in _STATUS_FALLBACK_MESSAGE:
            fallback = _STATUS_FALLBACK_MESSAGE.get(status, "")
            if fallback:
                message = fallback
        if message:
            entry["message"] = message
        if raw_message and raw_message != message:
            entry["raw_message"] = raw_message
        if alias_used:
            entry["raw_stage_key"] = alias_used
        if duration_sec is not None:
            entry["duration_sec"] = duration_sec
        if status in ("error", "interrupted") and info.get("error"):
            entry["error"] = info["error"]

        result.append(entry)
    return result


def scan_unregistered_folders() -> list[dict]:
    """Найти папки в projects/, которые содержат PDF, но не имеют project_info.json."""
    result = []
    for project_id, entry in iter_project_dirs():
        info_path = entry / "project_info.json"
        if info_path.exists():
            continue

        pdf_files = list(entry.glob("*.pdf"))
        md_files = list(entry.glob("*_document.md")) + list(entry.glob("*.md"))
        md_files = list({f.name: f for f in md_files}.values())

        if not pdf_files:
            continue

        result.append({
            "folder": project_id,
            "pdf_files": [f.name for f in pdf_files],
            "md_files": [f.name for f in md_files],
            "pdf_size_mb": round(pdf_files[0].stat().st_size / 1024 / 1024, 1),
        })

    return result


def scan_external_folder(folder_path: str) -> list[dict]:
    """Сканировать внешнюю папку — найти подпапки с PDF.

    Ищет PDF-файлы в самой папке и в подпапках (1 уровень).
    """
    result = []
    target = Path(folder_path)
    if not target.exists() or not target.is_dir():
        return result

    # Собрать кандидатов: сама папка + подпапки
    candidates = [target]
    for sub in sorted(target.iterdir()):
        if sub.is_dir() and not sub.name.startswith("_"):
            candidates.append(sub)

    for entry in candidates:
        pdf_files = list(entry.glob("*.pdf"))
        if not pdf_files:
            continue
        md_files = list(entry.glob("*_document.md")) + list(entry.glob("*.md"))
        md_files = list({f.name: f for f in md_files}.values())

        result.append({
            "folder": entry.name,
            "full_path": str(entry),
            "pdf_files": [f.name for f in pdf_files],
            "md_files": [f.name for f in md_files],
            "pdf_size_mb": round(pdf_files[0].stat().st_size / 1024 / 1024, 1),
        })

    return result


# ── Browser folder upload (Добавить проект → Из папки на компьютере) ──────────
# Инженер загружает папку проекта со своего компьютера через сайт. Браузер не
# отдаёт абсолютный путь, поэтому мы получаем список (имя_файла, байты) и сами
# раскладываем их в legacy projects/, генерируем project_info.json (клиентский
# не доверяем) и запускаем dual_write_shadow зеркало.

_ALLOWED_UPLOAD_EXTS = {".pdf", ".md", ".json", ".html", ".htm"}


class UploadFolderError(ValueError):
    """Невалидный запрос загрузки папки (маппится в HTTP 422)."""


class UploadFolderConflict(FileExistsError):
    """Проект с таким именем уже существует (маппится в HTTP 409)."""


def _safe_upload_basename(name: str) -> str:
    """Безопасный basename без path-traversal.

    Браузер кладёт в webkitRelativePath относительный путь (`folder/sub/file.pdf`).
    Берём только basename, отбрасываем директории, запрещаем `..`/абсолютные/NUL.
    """
    raw = (name or "").replace("\\", "/")
    base = os.path.basename(raw).strip()
    if not base or base in (".", "..") or "/" in base or "\x00" in base:
        raise UploadFolderError(f"Небезопасное имя файла: {name!r}")
    return base


def _v2_document_exists(object_id: str, project_name: str) -> bool:
    """Best-effort проверка дубля в projects_v2 (по object_id + document_code).

    Никогда не бросает: недоступный/сбойный v2 → False (legacy-проверка
    остаётся авторитетной).
    """
    try:
        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
        adapter = ProjectsV2Adapter()
        if not adapter.is_available():
            return False
        return adapter.find_document(project_name, object_id=object_id) is not None
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _classify_upload_files(files: list[tuple[str, bytes]]) -> dict:
    """(имя, байты)[] → {pdfs, mds, results, ocrs, ignored}. Бросает UploadFolderError
    на небезопасном имени (path-traversal). Общая логика для save и precheck."""
    pdfs: list[tuple[str, bytes]] = []
    mds: list[tuple[str, bytes]] = []
    results: list[tuple[str, bytes]] = []
    ocrs: list[tuple[str, bytes]] = []
    ignored: list[str] = []
    for fname, data in files:
        safe = _safe_upload_basename(fname)  # бросает на traversal
        ext = os.path.splitext(safe)[1].lower()
        low = safe.lower()
        if ext not in _ALLOWED_UPLOAD_EXTS:
            ignored.append(safe)
            continue
        if ext == ".pdf":
            pdfs.append((safe, data))
        elif ext == ".md":
            mds.append((safe, data))
        elif ext == ".json" and low.endswith("_result.json"):
            results.append((safe, data))
        elif ext in (".html", ".htm") and low.endswith("_ocr.html"):
            ocrs.append((safe, data))
        else:
            ignored.append(safe)
    return {"pdfs": pdfs, "mds": mds, "results": results, "ocrs": ocrs, "ignored": ignored}


def _compute_upload_fingerprint(cls: dict) -> dict:
    """pdf_sha256 + bundle_fingerprint. bundle учитывает pdf/md/result/ocr и их
    sha256 (имя+роль+хэш) — `*_ocr.html` входит в отпечаток."""
    files_manifest: list[dict] = []
    pdf_sha: Optional[str] = None
    for role, items in (("pdf", cls["pdfs"]), ("md", cls["mds"]),
                        ("result", cls["results"]), ("ocr", cls["ocrs"])):
        for name, data in items:
            h = _sha256_bytes(data)
            files_manifest.append({"role": role, "name": name, "sha256": h, "size": len(data)})
            if role == "pdf" and pdf_sha is None:
                pdf_sha = h
    canon = "\n".join(sorted(f"{m['role']}:{m['name']}:{m['sha256']}" for m in files_manifest))
    bundle_fp = hashlib.sha256(canon.encode("utf-8")).hexdigest() if files_manifest else None
    return {"pdf_sha256": pdf_sha, "bundle_fingerprint": bundle_fp, "files": files_manifest}


def _normalize_name_for_similarity(name: str) -> str:
    """Нормализация имени проекта для детекта похожих (снимает ревизии/копии/даты)."""
    n = (name or "").strip().lower()
    n = re.sub(r"\(main\)$", "", n)
    n = re.sub(r"\.pdf$", "", n)
    n = re.sub(r"^\d{2}\.\d{2}\.\d{2}_", "", n)            # дата-префикс
    n = re.sub(r"\s*\((?:изм\.?\s*\d+|\d+)\)", "", n)      # (1) (2) (Изм.1)
    n = re.sub(r"\s*v\d+$", "", n)                          # V2
    n = re.sub(r"_в\d+$", "", n)                            # _в2
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _scan_object_fingerprints(obj_projects_dir) -> dict:
    """Скан project_info.json объекта → индексы fingerprint'ов + нормализованных имён.

    Только legacy (авторитетно, зеркалится в v2). Старые проекты без fingerprint
    участвуют лишь в name-индексе (checksum-дедуп — forward-looking).
    """
    pdf_idx: dict[str, list[str]] = {}
    bundle_idx: dict[str, list[str]] = {}
    names_list: list[dict] = []
    try:
        for pi in Path(obj_projects_dir).rglob("project_info.json"):
            try:
                info = json.loads(pi.read_text(encoding="utf-8"))
            except Exception:
                continue
            pid = info.get("project_id") or pi.parent.name
            nm = info.get("name") or pi.parent.name
            sect = info.get("section") or ""
            ps = info.get("pdf_sha256")
            bf = info.get("bundle_fingerprint")
            if ps:
                pdf_idx.setdefault(ps, []).append(pid)
            if bf:
                bundle_idx.setdefault(bf, []).append(pid)
            names_list.append({"pid": pid, "discipline": sect,
                               "norm": _normalize_name_for_similarity(nm)})
    except Exception:
        pass
    return {"pdf": pdf_idx, "bundle": bundle_idx, "names_list": names_list}


# error-коды precheck, означающие «нельзя загрузить вообще» (не дубль)
_PRECHECK_ERROR_CODES = {
    "no_pdf", "multiple_pdf", "bad_name", "bad_discipline",
    "object_not_found", "unsafe_filename",
}


def _suggested_version_label(obj_projects_dir, object_id: str, target_pid: str) -> str:
    """«V{N+1}» для target-проекта (best-effort, для подсказки в UI)."""
    try:
        from backend.app.services.common import version_service as _vs
        tdir = resolve_project_dir(target_pid, object_id=object_id)
        summ = _vs.get_versions_summary(tdir, target_pid)
        return f"V{int(summ.get('version_count', 1)) + 1}"
    except Exception:
        return "V2"


def precheck_uploaded_project_folder(*, object_id: str, discipline: Optional[str] = None,
                                     project_name: str,
                                     files: list[tuple[str, bytes]],
                                     folder_name: Optional[str] = None) -> dict:
    """Dry-run проверка загрузки папки — НИЧЕГО не пишет. Возвращает verdict
    (ready/warning/duplicate/error) + авто-дисциплину + предложение версии.

    Дисциплина: если не передана — определяется (имя папки → имя PDF → текст
    document.md → fallback EOM) через discipline_service. Дубли проверяются под
    эффективной дисциплиной. Предложение версии: точное совпадение
    нормализованного имени с существующим проектом того же раздела.
    """
    from backend.app.services.common.object_service import (
        get_object_by_id, get_projects_dir_for,
    )
    from backend.app.services.common import discipline_service as _ds

    provided_discipline = (discipline or "").strip()
    project_name = (project_name or "").strip()
    blocks: list[dict] = []
    warnings: list[dict] = []

    obj = get_object_by_id(object_id)
    obj_dir = get_projects_dir_for(object_id) if obj else None

    try:
        cls = _classify_upload_files(files)
    except UploadFolderError as e:
        blocks.append({"code": "unsafe_filename", "message": str(e)})
        cls = {"pdfs": [], "mds": [], "results": [], "ocrs": [], "ignored": []}
    fp = _compute_upload_fingerprint(cls)

    # --- авто-определение дисциплины -----------------------------------------
    pdf_name = cls["pdfs"][0][0] if cls["pdfs"] else ""
    doc_text = ""
    if cls["mds"]:
        try:
            doc_text = cls["mds"][0][1].decode("utf-8", "ignore")[:8000]
        except Exception:
            doc_text = ""
    det = _ds.detect_discipline_detailed(folder_name or "", pdf_name, doc_text)
    detected_discipline = det["code"]
    effective_discipline = provided_discipline or detected_discipline

    name_invalid = (not project_name or project_name.startswith("_")
                    or any(s in project_name for s in ("/", "\\", "..", "\x00")))
    disc_invalid = (not effective_discipline or effective_discipline.startswith("_")
                    or any(s in effective_discipline for s in ("/", "\\", "..", "\x00")))

    npdf = len(cls["pdfs"])
    if npdf == 0:
        blocks.append({"code": "no_pdf", "message": "В папке не найден PDF. Нужен ровно один PDF проекта."})
    elif npdf > 1:
        blocks.append({"code": "multiple_pdf", "message": "В папке найдено несколько PDF. Оставьте один PDF на проект."})
    if name_invalid:
        blocks.append({"code": "bad_name", "message": "Недопустимое название проекта (пустое, с '_' или спецсимволами)."})
    if disc_invalid:
        blocks.append({"code": "bad_discipline", "message": "Недопустимая дисциплина."})
    if obj is None:
        blocks.append({"code": "object_not_found", "message": f"Объект не найден: {object_id!r}"})

    project_id = (f"{effective_discipline}/{project_name}"
                  if (project_name and effective_discipline and not name_invalid and not disc_invalid)
                  else None)
    normalized_project_name = _normalize_name_for_similarity(project_name)
    suggested_target = None
    suggested_target_name = None
    suggested_version_label = None

    if obj_dir is not None and project_id:
        dest = obj_dir / effective_discipline / project_name
        if dest.exists() and (dest / "project_info.json").exists():
            blocks.append({"code": "legacy_name_exists", "message": f"Проект «{project_id}» уже существует в projects/."})
        if _v2_document_exists(object_id, project_name):
            blocks.append({"code": "v2_name_exists", "message": f"Проект «{project_id}» уже существует в projects_v2."})

        idx = _scan_object_fingerprints(obj_dir)
        bf = fp.get("bundle_fingerprint")
        ps = fp.get("pdf_sha256")
        if bf and bf in idx["bundle"]:
            dup = idx["bundle"][bf]
            blocks.append({"code": "bundle_exact_duplicate",
                           "message": f"Точный комплект уже загружался: {', '.join(dup[:3])}"})
        elif ps and ps in idx["pdf"]:
            dup = idx["pdf"][ps]
            warnings.append({"code": "pdf_checksum_duplicate",
                             "message": f"Такой PDF уже загружался: {', '.join(dup[:3])}"})
        # совпадение нормализованного имени в том же разделе → предложение версии
        sim = [r["pid"] for r in idx["names_list"]
               if r["discipline"] == effective_discipline
               and r["norm"] == normalized_project_name and r["pid"] != project_id]
        if sim:
            suggested_target = sim[0]
            suggested_target_name = suggested_target.split("/")[-1]
            suggested_version_label = _suggested_version_label(obj_dir, object_id, suggested_target)
            warnings.append({"code": "similar_name",
                             "message": f"Похоже на новую версию проекта: {', '.join(sim[:3])}"})

    if blocks:
        status = "error" if any(b["code"] in _PRECHECK_ERROR_CODES for b in blocks) else "duplicate"
    elif warnings:
        status = "warning"
    else:
        status = "ready"

    return {
        "project_id": project_id, "object_id": object_id,
        "discipline": effective_discipline,
        "detected_discipline": detected_discipline,
        "discipline_source": det["source"], "discipline_reason": det["reason"],
        "discipline_was_provided": bool(provided_discipline),
        "project_name": project_name,
        "normalized_project_name": normalized_project_name,
        "suggested_target_project": suggested_target,
        "suggested_target_name": suggested_target_name,
        "suggested_version_label": suggested_version_label,
        "pdf_sha256": fp.get("pdf_sha256"), "bundle_fingerprint": fp.get("bundle_fingerprint"),
        "pdf_count": npdf, "pdf_name": (cls["pdfs"][0][0] if cls["pdfs"] else None),
        "has_md": bool(cls["mds"]), "has_result": bool(cls["results"]), "has_ocr": bool(cls["ocrs"]),
        "ignored_files": cls["ignored"],
        "status": status, "blocks": blocks, "warnings": warnings,
        "bundle_warnings": _upload_bundle_warnings(bool(cls["mds"]), bool(cls["results"]), bool(cls["ocrs"])),
    }


def _save_uploaded_as_new_version(*, object_id: str, discipline: str,
                                  target_project_id: str,
                                  files: list[tuple[str, bytes]]) -> dict:
    """Загрузить папку как НОВУЮ ВЕРСИЮ существующего проекта.

    Переиспользует проверенный `create_version_from_existing_files`: in-memory
    байты пишутся во временную папку, оттуда копируются в новую версию target.
    Валидация (target существует, тот же раздел) — внутри version_service.
    Привязка к объекту обеспечивается object-bound резолвером. Orphan не
    создаётся (нет source-проекта). После — пере-зеркаливание target в v2.
    """
    from backend.app.services.common import version_service as _vs

    target_project_id = (target_project_id or "").strip()
    if not target_project_id:
        raise UploadFolderError("Не указан target_project_id для режима new_version")

    cls = _classify_upload_files(files)
    if len(cls["pdfs"]) == 0:
        raise UploadFolderError("В папке не найден PDF. Нужен ровно один PDF проекта.")
    if len(cls["pdfs"]) > 1:
        raise UploadFolderError("В папке найдено несколько PDF. Оставьте один PDF на проект.")

    # object-bound резолвер: target обязан быть в ЭТОМ объекте (иначе not found)
    def _resolver(pid, **kw):
        return resolve_project_dir(pid, object_id=object_id)

    with tempfile.TemporaryDirectory(prefix="upload_ver_") as tmp:
        tmpd = Path(tmp)
        def _w(items):
            out = []
            for name, data in items:
                p = tmpd / name
                p.write_bytes(data)
                out.append(str(p))
            return out
        pdf_paths = _w(cls["pdfs"])
        md_paths = _w(cls["mds"])
        result_paths = _w(cls["results"])
        ocr_paths = _w(cls["ocrs"])
        try:
            res = _vs.create_version_from_existing_files(
                target_project_id,
                candidate_files={
                    "pdf": pdf_paths[0],
                    "md": md_paths[0] if md_paths else None,
                    "result_json": result_paths[0] if result_paths else None,
                    "extra": ocr_paths,  # *_ocr.html едет в версию
                },
                expected_section=discipline or None,
                comment="Загружено как версия из «Из папки на компьютере»",
                source="upload_folder_modal",
                allowed_roots=[tmpd],
                resolve_project_dir_fn=_resolver,
            )
        except _vs.VersionFileConflictError as e:
            raise UploadFolderConflict(str(e))
        except _vs.VersionFileError as e:
            raise UploadFolderError(str(e))
        except ValueError as e:
            # несовпадение раздела target и т.п.
            raise UploadFolderError(str(e))

    # пере-зеркалить target в v2 (project_info версии правится после mirror) —
    # как в merge_project_as_version; no-op в legacy, fail-soft.
    #
    # В projects_v2-primary версия уже записана напрямую в v2
    # (create_version_from_existing_files), и повторный mirror ИЗ legacy затёр бы
    # её в document.json → VersionNotFoundError. Зеркалим только в legacy/shadow.
    if not _vs._projects_v2_context_enabled():
        try:
            from backend.app.services.storage import storage_write_facade as _swf
            _swf.shadow_mirror_project_id_safe(target_project_id)
        except Exception:
            pass

    return {
        "mode": "new_version",
        "project_id": target_project_id,
        "name": target_project_id.split("/")[-1],
        "section": discipline,
        "object_id": object_id,
        "version": res.get("version"),
        "version_id": (res.get("version") or {}).get("version_id"),
        "saved_files": res.get("saved", []),
        "warnings": res.get("warnings", []),
        "versions_summary": res.get("versions_summary"),
        "has_md": bool(cls["mds"]), "has_result": bool(cls["results"]), "has_ocr": bool(cls["ocrs"]),
    }


def save_uploaded_project_folder(*, object_id: str, discipline: str,
                                 project_name: str,
                                 files: list[tuple[str, bytes]],
                                 description: str = "",
                                 upload_mode: str = "new_project",
                                 target_project_id: Optional[str] = None) -> dict:
    """Сохранить папку проекта, загруженную инженером через браузер.

    Args:
        object_id: id объекта (резолвится в его projects_dir).
        discipline: код дисциплины (EOM/OV/…), служит подпапкой.
        project_name: имя проекта = basename папки версии (project_id без слеша).
        files: список (имя_файла, байты). project_info.json игнорируется
               (генерируем сами). Принимаются только pdf/md/json(*_result)/html(*_ocr).
        description: опциональное описание.
        upload_mode: `new_project` (default) или `new_version`.
        target_project_id: для `new_version` — проект-основание (в этом объекте).

    Returns: словарь с project_id/saved_files/ignored_files/has_* и project_info
        (new_project) либо version/versions_summary (new_version).

    Raises:
        UploadFolderError (→422): нет/несколько PDF, кривое имя/дисциплина,
            небезопасное имя файла, объект не найден, раздел target не совпал.
        UploadFolderConflict (→409): проект уже есть в legacy/v2 или версия-дубль.
        FileNotFoundError (→404): target для new_version не найден.
    """
    from backend.app.services.common.object_service import (
        get_object_by_id, get_projects_dir_for,
    )

    discipline = (discipline or "").strip()
    project_name = (project_name or "").strip()

    if upload_mode == "new_version":
        # резолв объекта (для проверки существования) делается внутри резолвера
        if get_object_by_id(object_id) is None:
            raise UploadFolderError(f"Объект не найден: {object_id!r}")
        return _save_uploaded_as_new_version(
            object_id=object_id, discipline=discipline,
            target_project_id=target_project_id or "", files=files,
        )

    # --- валидация имён (path-traversal / служебные префиксы) -----------------
    if not project_name:
        raise UploadFolderError("Не указано название проекта")
    if project_name.startswith("_"):
        raise UploadFolderError("Название проекта не может начинаться с '_'")
    if any(s in project_name for s in ("/", "\\", "..", "\x00")):
        raise UploadFolderError("Недопустимое название проекта")
    if not discipline:
        raise UploadFolderError("Не указана дисциплина")
    if any(s in discipline for s in ("/", "\\", "..", "\x00")) or discipline.startswith("_"):
        raise UploadFolderError("Недопустимая дисциплина")

    # --- резолв объекта ------------------------------------------------------
    obj = get_object_by_id(object_id)
    if obj is None:
        raise UploadFolderError(f"Объект не найден: {object_id!r}")
    obj_projects_dir = get_projects_dir_for(object_id)
    if obj_projects_dir is None:
        raise UploadFolderError(f"Не удалось определить папку объекта: {object_id!r}")

    # --- классификация файлов (project_info.json и прочее — игнор) ------------
    cls = _classify_upload_files(files)
    pdfs, mds, results, ocrs, ignored = (
        cls["pdfs"], cls["mds"], cls["results"], cls["ocrs"], cls["ignored"]
    )

    if len(pdfs) == 0:
        raise UploadFolderError("В папке не найден PDF. Нужен ровно один PDF проекта.")
    if len(pdfs) > 1:
        raise UploadFolderError(
            "В папке найдено несколько PDF. Выберите папку одного проекта."
        )

    fp = _compute_upload_fingerprint(cls)

    # --- проверка дубля (legacy авторитетно + best-effort v2). Повторяется и
    #     здесь (не только в precheck) для защиты от race condition. -----------
    project_id = f"{discipline}/{project_name}"
    dest = obj_projects_dir / discipline / project_name
    if dest.exists() and (dest / "project_info.json").exists():
        raise UploadFolderConflict(f"Проект '{project_id}' уже существует в projects/")
    if _v2_document_exists(object_id, project_name):
        raise UploadFolderConflict(f"Проект '{project_id}' уже существует в projects_v2")
    # точный bundle-дубль (тот же комплект файлов) — hard block
    bf = fp.get("bundle_fingerprint")
    if bf:
        idx = _scan_object_fingerprints(obj_projects_dir)
        if bf in idx["bundle"]:
            dup = ", ".join(idx["bundle"][bf][:3])
            raise UploadFolderConflict(f"Точный комплект уже загружался: {dup}")

    # --- запись в legacy (авторитетно, первым) -------------------------------
    dest.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []

    def _write_all(items: list[tuple[str, bytes]]) -> None:
        for safe, data in items:
            (dest / safe).write_bytes(data)
            saved.append(safe)

    _write_all(pdfs)
    _write_all(mds)
    _write_all(results)
    _write_all(ocrs)

    md_names = [n for n, _ in mds]
    md_doc = next((n for n in md_names if n.lower().endswith("_document.md")), None)
    md_primary = md_doc or (md_names[0] if md_names else None)

    info: dict = {
        "project_id": project_id,
        "name": project_name,
        "section": discipline,
        "description": description or "",
        "pdf_file": pdfs[0][0],
        "pdf_files": [n for n, _ in pdfs],
        "object_id": object_id,
        "source": "upload-folder",
        # fingerprint для дедупа (precheck сканирует именно эти поля)
        "pdf_sha256": fp.get("pdf_sha256"),
        "bundle_fingerprint": fp.get("bundle_fingerprint"),
        "tile_config": {},
    }
    if md_names:
        info["md_file"] = md_primary
        info["md_files"] = md_names

    (dest / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    # input_manifest.json — расширенный отпечаток (per-file sha256). Зеркалится в
    # v2 01_input вместе с прочими input-файлами.
    manifest = {
        "schema_version": 1,
        "source": "upload-folder",
        "project_id": project_id,
        "object_id": object_id,
        "pdf_sha256": fp.get("pdf_sha256"),
        "bundle_fingerprint": fp.get("bundle_fingerprint"),
        "files": fp.get("files", []),
    }
    (dest / "input_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (dest / "_output").mkdir(exist_ok=True)

    # --- dual_write_shadow зеркало (no-op в legacy, fail-soft) ----------------
    # *_ocr.html попадёт в v2 01_input/02_work автоматически: find_input_quad
    # распознаёт _ocr.html. try/except гарантирует, что сбой v2 не ломает legacy.
    try:
        from backend.app.services.storage import storage_write_facade as _swf
        _swf.shadow_mirror_project_path_safe(dest)
    except Exception:
        pass

    return {
        "project_id": project_id,
        "name": project_name,
        "section": discipline,
        "object_id": object_id,
        "dest": str(dest),
        "saved_files": saved,
        "ignored_files": ignored,
        "has_pdf": True,
        "has_md": bool(md_names),
        "has_result": bool(results),
        "has_ocr": bool(ocrs),
        "warnings": _upload_bundle_warnings(bool(md_names), bool(results), bool(ocrs)),
        "project_info": info,
    }


def _upload_bundle_warnings(has_md: bool, has_result: bool, has_ocr: bool) -> list[str]:
    """Человекочитаемые предупреждения о недостающих (не блокирующих) файлах."""
    warns: list[str] = []
    if not has_md:
        warns.append("Не найден *_document.md — текстовый анализ потребует OCR/Chandra.")
    if not has_result:
        warns.append("Не найден *_result.json — кроп блоков потребует подготовки.")
    if not has_ocr:
        warns.append("Не найден *_ocr.html — text_evidence будет ограничен.")
    return warns


def register_external_project(source_path: str, pdf_file: str,
                              pdf_files: list[str] | None = None,
                              md_file: Optional[str] = None,
                              md_files: list[str] | None = None,
                              name: Optional[str] = None, section: str = "EOM",
                              description: str = "") -> dict:
    """Скопировать проект из внешней папки в projects/ и создать project_info.json.

    Копирует PDF и MD файлы (не всю папку), создаёт project_info.json.
    """
    source = Path(source_path)
    if not source.exists():
        raise ValueError(f"Папка '{source_path}' не найдена")

    folder_name = name or source.name
    dest = _get_projects_dir() / folder_name
    if dest.exists() and (dest / "project_info.json").exists():
        raise ValueError(f"Проект '{folder_name}' уже существует в projects/")

    dest.mkdir(parents=True, exist_ok=True)

    # Нормализуем списки
    all_pdfs = pdf_files or [pdf_file]
    all_pdfs = [p for p in all_pdfs if p]
    all_mds = md_files or ([md_file] if md_file else [])
    all_mds = [m for m in all_mds if m]

    # Копируем все PDF
    for pf in all_pdfs:
        src_pdf = source / pf
        if not src_pdf.exists():
            raise ValueError(f"PDF файл '{pf}' не найден в '{source_path}'")
        shutil.copy2(str(src_pdf), str(dest / pf))

    # Копируем все MD
    for mf in all_mds:
        src_md = source / mf
        if src_md.exists():
            shutil.copy2(str(src_md), str(dest / mf))

    # Копируем *_result.json (нужен для blocks.py crop)
    for rj in source.glob("*_result.json"):
        shutil.copy2(str(rj), str(dest / rj.name))

    # Копируем *_ocr.html (нужен для text_evidence; фикс 2026-06-17 — раньше не
    # копировался). v2-зеркало подхватит его автоматически (find_input_quad
    # распознаёт _ocr.html и кладёт в 01_input/02_work).
    for oh in source.glob("*_ocr.html"):
        shutil.copy2(str(oh), str(dest / oh.name))

    # Создаём project_info.json
    project_id = folder_name
    info = {
        "project_id": project_id,
        "name": project_id,
        "section": section,
        "description": description,
        "pdf_file": all_pdfs[0],
        "pdf_files": all_pdfs,
        "source_path": str(source),
        "tile_config": {},
    }
    if all_mds:
        info["md_file"] = all_mds[0]
        info["md_files"] = all_mds

    output_dir = dest / "_output"
    output_dir.mkdir(exist_ok=True)

    info_path = dest / "project_info.json"
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    # Step 9/10 dual-write canary: после успешной legacy-записи зеркалим проект в
    # projects_v2 (no-op в режиме legacy, fail-soft — никогда не ломает legacy).
    # try/except гарантирует байт-идентичность legacy даже при сбое импорта хука.
    try:
        from backend.app.services.storage import storage_write_facade as _swf
        _swf.shadow_mirror_project_path_safe(dest)
    except Exception:
        pass

    return info


def register_project(folder: str, pdf_file: str, pdf_files: list[str] | None = None,
                     md_file: Optional[str] = None, md_files: list[str] | None = None,
                     name: Optional[str] = None, section: str = "EOM",
                     description: str = "") -> dict:
    """Создать project_info.json для папки из projects/.

    Args:
        folder: имя папки в projects/
        pdf_file: основной PDF-файл (обратная совместимость)
        pdf_files: все PDF-файлы (если несколько)
        md_file: основной MD-файл (опционально)
        md_files: все MD-файлы (если несколько)
        name: название проекта
        section: раздел проекта
        description: описание
    """
    proj_dir = resolve_project_dir(folder)
    if not proj_dir.exists():
        raise ValueError(f"Папка '{folder}' не найдена в projects/")

    # Нормализуем списки PDF
    all_pdfs = pdf_files or [pdf_file]
    all_pdfs = [p for p in all_pdfs if p]  # убрать пустые
    if not all_pdfs:
        raise ValueError("Не указан ни один PDF файл")

    for pf in all_pdfs:
        if not (proj_dir / pf).exists():
            raise ValueError(f"PDF файл '{pf}' не найден в папке '{folder}'")

    # Нормализуем списки MD
    all_mds = md_files or ([md_file] if md_file else [])
    all_mds = [m for m in all_mds if m]
    for mf in all_mds:
        if not (proj_dir / mf).exists():
            raise ValueError(f"MD файл '{mf}' не найден в папке '{folder}'")

    project_id = name or folder
    info = {
        "project_id": project_id,
        "name": project_id,
        "section": section,
        "description": description,
        "pdf_file": all_pdfs[0],
        "pdf_files": all_pdfs,
        "tile_config": {},
    }
    if all_mds:
        info["md_file"] = all_mds[0]
        info["md_files"] = all_mds

    # Создаём _output папку
    output_dir = proj_dir / "_output"
    output_dir.mkdir(exist_ok=True)

    # Сохраняем project_info.json
    info_path = proj_dir / "project_info.json"
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    # Step 9/10 dual-write canary: legacy-first, затем shadow-зеркало в v2
    # (no-op в legacy, fail-soft).
    try:
        from backend.app.services.storage import storage_write_facade as _swf
        _swf.shadow_mirror_project_path_safe(proj_dir)
    except Exception:
        pass

    # #78: новый project_info.json мог перевести голую папку в статус проекта —
    # сбрасываем кеш списка проектов.
    invalidate_project_cache()
    return info


# ── projects_v2 arm для clean (read-cutover делает legacy-очистку невидимой) ──

# Generated/runtime подпапки версии в projects_v2, отвечающие за состояние аудита
# и pipeline status (то, что читает UI при v2-read). Source (01_input) и
# метаданные (version.json) сюда НЕ входят — они сохраняются.
_V2_GENERATED_VERSION_DIRS = ("02_work", "03_analysis", "04_review", "05_export", "99_service")


def _resolve_v2_version_id(adapter, doc_dir, current, legacy_version_id):
    """legacy version_id (`v1`) → v2-форма (`v001`). Неизвестный → current.

    Зеркалит read_canary._resolve_version, чтобы clean целился ТОЧНО в ту версию,
    из которой UI читает статус.
    """
    try:
        vids = [v.get("version_id") for v in adapter.list_versions(doc_dir)]
    except Exception:
        vids = []
    r = str(legacy_version_id or "").strip().lower()
    if r and r in vids:
        return r
    if r.startswith("v") and r[1:].isdigit():
        cand = "v%03d" % int(r[1:])
        if cand in vids:
            return cand
    return current


def _clean_projects_v2_artifacts(project_id: str, legacy_version_id: Optional[str],
                                 *, object_id: Optional[str] = None) -> dict:
    """Очистить generated/runtime артефакты версии проекта в projects_v2.

    Срабатывает ТОЛЬКО когда включён v2-read default
    (`AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED`) — т.е. когда UI читает статус из
    projects_v2 и legacy-очистка визуально невидима. Иначе — no-op (поведение
    как раньше: чистится только legacy).

    Удаляет (backup-move в `projects_v2/_system/clean_backups/` ПЕРЕД удалением):
      `versions/<vid>/{02_work,03_analysis,04_review,05_export,99_service}`
    Сохраняет: `01_input` (source), `version.json`, doc-метаданные
    (`document.json`, `current_version.txt`, `versions/`), соседние версии.

    Безопасность: path-safe (target строго внутри
    `projects_v2/objects/.../versions/<vid>/`), version-scoped, fail-soft (любая
    ошибка → warning, не исключение — legacy-очистка не должна падать из-за v2).
    """
    out = {
        "v2_attempted": False, "v2_cleaned": False,
        "v2_doc_dir": None, "v2_version_id": None,
        "v2_removed": [], "v2_backup": None, "warnings": [],
    }
    try:
        from backend.app.services.storage import read_canary
    except Exception:
        return out  # v2-read слой отсутствует (legacy-only ветка) → no-op
    try:
        if not read_canary.default_read_enabled():
            return out  # v2-read не активен → UI читает legacy, v2 не трогаем

        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
        adapter = ProjectsV2Adapter()
        if not adapter.is_available():
            out["warnings"].append("projects_v2 storage недоступно")
            return out
        out["v2_attempted"] = True

        if object_id is None:
            try:
                from backend.app.services.common import object_service
                object_id = object_service.get_current_id()
            except Exception:
                object_id = None

        # Маппинг как в read_canary: document_code = basename(project_id) − «(main)».
        document_code = Path(project_id).name.replace("(main)", "").strip()
        doc = adapter.find_document(document_code, object_id=object_id)
        if doc is None and object_id is not None:
            # Мягкий fallback (как read_canary без object_id) — первое совпадение.
            doc = adapter.find_document(document_code, object_id=None)
        if doc is None:
            out["warnings"].append(f"projects_v2: документ '{document_code}' не найден")
            return out

        doc_dir = Path(doc["doc_dir"])
        v2vid = _resolve_v2_version_id(
            adapter, doc_dir, doc.get("current_version"), legacy_version_id,
        )
        if not v2vid:
            out["warnings"].append("projects_v2: версия не определена")
            return out
        vdir = adapter.version_dir(doc_dir, v2vid)
        out["v2_doc_dir"] = str(doc_dir)
        out["v2_version_id"] = v2vid

        # --- safety guards (деструктив) ---
        objects_root = adapter.objects_root.resolve()
        try:
            vdir_res = vdir.resolve()
        except Exception:
            out["warnings"].append("projects_v2: не удалось резолвить version_dir")
            return out
        # (1) version_dir строго внутри projects_v2/objects/
        if not str(vdir_res).startswith(str(objects_root) + os.sep):
            out["warnings"].append("projects_v2: version_dir вне objects-root — пропуск (safety)")
            return out
        # (2) это именно .../versions/<vid> (не doc root, не вся versions/)
        if vdir.parent.name != "versions":
            out["warnings"].append("projects_v2: неожиданная структура version_dir — пропуск (safety)")
            return out
        if not vdir.exists():
            out["warnings"].append(f"projects_v2: version_dir '{v2vid}' отсутствует")
            return out

        # --- backup-move generated подпапок ---
        removed: list[str] = []
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_root = (adapter.v2_root / "_system" / "clean_backups"
                       / f"{ts}_{document_code}_{v2vid}")
        for name in _V2_GENERATED_VERSION_DIRS:
            d = vdir / name
            if not d.exists():
                continue
            # (3) каждый target строго внутри version_dir (анти-traversal)
            try:
                if not str(d.resolve()).startswith(str(vdir_res) + os.sep):
                    out["warnings"].append(f"projects_v2: '{name}' вне version_dir — пропуск (safety)")
                    continue
            except Exception:
                continue
            try:
                backup_root.mkdir(parents=True, exist_ok=True)
                shutil.move(str(d), str(backup_root / name))
                removed.append(name)
            except Exception as e:
                out["warnings"].append(f"projects_v2: не удалось убрать '{name}': {e}")

        out["v2_removed"] = removed
        out["v2_backup"] = str(backup_root) if removed else None
        out["v2_cleaned"] = bool(removed)

        try:
            print(
                f"[clean v2] project_id={project_id} discipline={doc.get('discipline')} "
                f"document={document_code} version={v2vid} legacy_version={legacy_version_id} "
                f"doc_dir={doc_dir} removed={removed} backup={out['v2_backup']} "
                f"warnings={out['warnings']}"
            )
        except Exception:
            pass
        return out
    except Exception as e:
        # Fail-soft: v2-arm не должен валить legacy-очистку.
        out["warnings"].append(f"projects_v2 clean failed: {e}")
        return out


def _clean_project_data_v2_primary(
    project_id: str, *, version_id: Optional[str] = None, _confirmed: bool = False,
) -> dict:
    """projects_v2-primary clean with mandatory backup + confirmation."""
    if not _confirmed:
        raise ValueError("Для очистки projects_v2 требуется явное подтверждение _confirmed=True")

    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.storage_write_facade import StorageWriteFacade, V2Target
    from backend.app.services.storage.v2_primary_wiring import (
        backup_version_before_destructive,
        guard_destructive_v2_primary,
        record_destructive_confirmation,
    )

    facade = StorageWriteFacade()
    v2_root = facade.v2_root()
    if v2_root is None:
        raise ValueError("projects_v2 root не настроен")

    adapter = ProjectsV2Adapter(v2_root)
    doc = adapter.find_document_by_project_id(project_id)
    if doc is None:
        raise ValueError(f"Проект '{project_id}' не найден в projects_v2")
    target_vid = adapter.resolve_version_id(doc, version_id)
    if not target_vid:
        raise ValueError(f"Версия '{version_id}' проекта '{project_id}' не найдена в projects_v2")

    target = V2Target(
        object_folder=doc["object_folder"],
        discipline=doc["discipline"],
        document_code=doc["document_code"],
        version_id=target_vid,
    )
    version_dir = target.version_dir(v2_root)
    if not version_dir.is_dir():
        raise ValueError(f"Версия '{target_vid}' проекта '{project_id}' не найдена в projects_v2")

    backup_id = backup_version_before_destructive(target, v2_root, "clean_project_data")
    record_destructive_confirmation(
        target, v2_root, op="clean_project_data", backup_id=backup_id, project_id=project_id,
    )
    guard_destructive_v2_primary(
        "clean_project_data", confirmed=True, backup_id=backup_id,
    )

    result = {
        "deleted_files": 0,
        "deleted_dirs": 0,
        "freed_mb": 0.0,
        "version_id": target.vid_disk(),
        "backup_id": backup_id,
    }
    total_size = 0

    analysis_dir = version_dir / "03_analysis"
    if analysis_dir.exists():
        for f in analysis_dir.rglob("*"):
            if f.is_file():
                total_size += f.stat().st_size
                result["deleted_files"] += 1
            elif f.is_dir():
                result["deleted_dirs"] += 1
        shutil.rmtree(analysis_dir)
    (analysis_dir / "latest").mkdir(parents=True, exist_ok=True)

    vj_path = version_dir / "version.json"
    if vj_path.exists():
        try:
            vj = json.loads(vj_path.read_text(encoding="utf-8"))
        except Exception:
            vj = None
        if isinstance(vj, dict):
            info = vj.get("project_info")
            if isinstance(info, dict):
                for field in [
                    "tile_config_source", "text_source",
                    "md_page_classification", "text_extraction_quality",
                    "tile_quality",
                ]:
                    info.pop(field, None)
                info["tile_config"] = {}
                vj["project_info"] = info
                vj_path.write_text(json.dumps(vj, ensure_ascii=False, indent=2), encoding="utf-8")
                result["project_info_reset"] = True

    result["freed_mb"] = round(total_size / 1024 / 1024, 1)
    return result


def restore_clean_backup(project_id: str, *, backup_id: str, version_id: Optional[str] = None) -> dict:
    """Восстановить v2-primary версию из destructive backup."""
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.storage_write_facade import StorageWriteFacade, V2Target
    from backend.app.services.storage.v2_primary_wiring import restore_from_backup_id, v2_primary_enabled

    if not v2_primary_enabled():
        raise RuntimeError("restore-clean доступен только в projects_v2_primary")

    v2_root = StorageWriteFacade().v2_root()
    if v2_root is None:
        raise FileNotFoundError("projects_v2 root не настроен")
    adapter = ProjectsV2Adapter(v2_root)
    doc = adapter.find_document_by_project_id(project_id)
    if doc is None:
        raise FileNotFoundError(f"Проект '{project_id}' не найден в projects_v2")
    target_vid = adapter.resolve_version_id(doc, version_id)
    if not target_vid:
        raise FileNotFoundError(f"Версия '{version_id}' проекта '{project_id}' не найдена в projects_v2")
    target = V2Target(
        object_folder=doc["object_folder"],
        discipline=doc["discipline"],
        document_code=doc["document_code"],
        version_id=target_vid,
    )
    return restore_from_backup_id(target, v2_root, backup_id)


def clean_project_data(project_id: str, *, version_id: Optional[str] = None, _confirmed: bool = False) -> dict:
    """Очистить все результаты аудита, сохранив только исходные документы.

    Сохраняет (исходные файлы пользователя):
    - *.pdf
    - *_document.md (и другие *.md)
    - *_result.json (OCR-результат для кропа блоков)
    - *_annotation.json (OCR-аннотации)
    - *_ocr.html (OCR-визуализация)
    - project_info.json (сбрасывается до минимума)

    Удаляет всё остальное:
    - Папку _output/ целиком
    - client.log, extracted_text.txt и другие генерируемые файлы

    Очищаются данные ТОЛЬКО указанной версии (`version_id`; при None —
    активной/latest). Папка `_output/` лежит внутри папки версии, поэтому
    другие версии проекта не затрагиваются.

    Returns:
        dict с описанием удалённого
    """
    from backend.app.services.storage.storage_write_facade import v2_is_primary
    if v2_is_primary():
        return _clean_project_data_v2_primary(
            project_id, version_id=version_id, _confirmed=_confirmed,
        )

    # Legacy/dual_write_shadow branch: прежняя очистка `_output/` без v2-деструктива.
    from backend.app.services.storage.v2_primary_wiring import guard_destructive_v2_primary
    guard_destructive_v2_primary("clean_project_data")
    root_dir = resolve_project_dir(project_id)
    if not root_dir.exists():
        raise ValueError(f"Проект '{project_id}' не найден")

    # Папка конкретной версии (V1 → корень проекта; старшие → братская папка
    # версии в контейнере). Так очистка не задевает соседние версии.
    target_vid = version_service.resolve_effective_version_id(
        root_dir, project_id, version_id,
    )
    try:
        proj_dir = version_service.get_version_dir(root_dir, project_id, target_vid)
    except version_service.VersionNotFoundError:
        proj_dir = root_dir
    if not proj_dir.exists():
        raise ValueError(
            f"Версия '{target_vid}' проекта '{project_id}' не найдена"
        )

    result = {"deleted_files": 0, "deleted_dirs": 0, "freed_mb": 0.0}
    total_size = 0

    # Исходные файлы — НЕ удаляем
    def is_source_file(f: Path) -> bool:
        name = f.name.lower()
        if name == "project_info.json":
            return True
        if name.endswith(".pdf"):
            return True
        if name.endswith(".md"):
            return True
        if name.endswith("_result.json"):
            return True
        if name.endswith("_annotation.json"):
            return True
        if name.endswith("_ocr.html"):
            return True
        return False

    # 1. Удаляем _output/ целиком
    output_dir = proj_dir / "_output"
    if output_dir.exists():
        for f in output_dir.rglob("*"):
            if f.is_file():
                total_size += f.stat().st_size
                result["deleted_files"] += 1
            elif f.is_dir():
                result["deleted_dirs"] += 1
        shutil.rmtree(output_dir)

    # 2. Удаляем все генерируемые файлы в корне проекта
    for f in proj_dir.iterdir():
        if f.is_file() and not is_source_file(f):
            total_size += f.stat().st_size
            result["deleted_files"] += 1
            f.unlink()

    result["freed_mb"] = round(total_size / 1024 / 1024, 1)

    # 3. Сбрасываем авто-поля в project_info.json (той же версии)
    info = get_project_info(project_id, version_id=target_vid)
    if info:
        auto_fields = [
            "tile_config_source", "text_source",
            "md_page_classification", "text_extraction_quality",
            "tile_quality",
        ]
        for field in auto_fields:
            info.pop(field, None)
        info["tile_config"] = {}
        save_project_info(project_id, info, version_id=target_vid)
        result["project_info_reset"] = True
    result["version_id"] = target_vid

    # 4. Пересоздаём пустую _output/
    output_dir.mkdir(exist_ok=True)

    # 5. projects_v2 arm: при включённом v2-read default UI читает статус из
    #    projects_v2, поэтому одной legacy-очистки недостаточно (визуально «ничего
    #    не очистилось»). Дочищаем generated/runtime артефакты ТОЙ ЖЕ версии в v2.
    #    Fail-soft: не влияет на успех legacy-очистки. object_id резолвится из
    #    активного объекта внутри helper'а.
    result["legacy_cleaned"] = True
    v2 = _clean_projects_v2_artifacts(project_id, target_vid)
    result["v2_attempted"] = v2.get("v2_attempted", False)
    result["v2_cleaned"] = v2.get("v2_cleaned", False)
    for k in ("v2_doc_dir", "v2_version_id", "v2_removed", "v2_backup"):
        if v2.get(k):
            result[k] = v2[k]
    result["warnings"] = v2.get("warnings", [])

    return result


def _load_json(path: Path) -> Optional[dict]:
    """Безопасное чтение JSON-файла."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return None


# ─── Document (MD) Viewer ─────────────────────────────────────

_document_cache: dict[str, dict] = {}  # {project_id: {ts, data}}
_DOCUMENT_CACHE_TTL = 60  # секунд

_PAGE_RE = re.compile(r'^## СТРАНИЦА (\d+)', re.MULTILINE)
_SHEET_INFO_RE = re.compile(r'^\*\*Лист:\*\*\s*(.+)$', re.MULTILINE)
_SHEET_NAME_RE = re.compile(r'^\*\*Наименование листа:\*\*\s*(.+)$', re.MULTILINE)


def _parse_image_block(text: str) -> dict:
    """Парсинг метаданных IMAGE-блока."""
    result = {}
    # Тип и оси из первой строки: **[ИЗОБРАЖЕНИЕ]** | Тип: XXX | Оси: YYY
    first_line = text.split('\n')[0] if text else ''
    m = re.search(r'\|\s*Тип:\s*(.+?)(?:\s*\||$)', first_line)
    if m:
        result['image_type'] = m.group(1).strip()
    m = re.search(r'\|\s*Оси:\s*(.+?)(?:\s*\||$)', first_line)
    if m:
        result['axes'] = m.group(1).strip()

    for field, pattern in [
        ('brief', r'^\*\*Краткое описание:\*\*\s*(.+)$'),
        ('description', r'^\*\*Описание:\*\*\s*(.+)$'),
        ('text_on_drawing', r'^\*\*Текст на чертеже:\*\*\s*(.+)$'),
        ('entities', r'^\*\*Сущности:\*\*\s*(.+)$'),
    ]:
        m = re.search(pattern, text, re.MULTILINE)
        if m:
            result[field] = m.group(1).strip()
    return result


def parse_md_document(project_id: str, *, version_id: Optional[str] = None) -> Optional[dict]:
    """Парсинг MD-файла проекта по страницам и блокам (для нужной версии).

    Возвращает: {project_id, md_file, total_pages, pages: [{page_num, sheet_info, sheet_label, blocks: [...]}]}
    """
    # Резолвим версию ПЕРЕД кэшированием, чтобы кеш ключался по реальному
    # version_id, а не по строке "latest". Иначе при смене latest_version_id
    # кеш продолжит отдавать данные старой версии.
    proj_dir = resolve_project_dir(project_id)
    try:
        effective_vid = version_service.resolve_effective_version_id(
            proj_dir, project_id, version_id,
        )
        version_dir = version_service.get_version_dir(proj_dir, project_id, effective_vid)
    except version_service.VersionNotFoundError:
        return None

    cache_key = f"{project_id}::{effective_vid}"
    cached = _document_cache.get(cache_key)
    if cached and (time.time() - cached['ts']) < _DOCUMENT_CACHE_TTL:
        return cached['data']

    info = get_project_info(project_id, version_id=effective_vid)
    if not info:
        return None
    md_file_name = info.get("md_file")
    try:
        sources = resolve_version_source_files(version_dir, project_id, project_info=info)
        md_path = sources.md_path
        if md_path is not None:
            try:
                md_file_name = str(md_path.relative_to(version_dir))
            except ValueError:
                md_file_name = md_path.name
    except Exception:
        md_path = None
    if md_path is None:
        if not md_file_name:
            return None
        md_path = version_dir / md_file_name
    if not md_path.exists():
        return None

    try:
        md_text = md_path.read_text(encoding='utf-8')
    except Exception:
        return None

    result = parse_md_text(md_text, project_id=project_id, md_file=md_file_name)
    if result is None:
        return None

    _document_cache[cache_key] = {"ts": time.time(), "data": result}
    return result


def parse_md_text(md_text: str, *, project_id: str, md_file: str) -> Optional[dict]:
    """Чистый парсер MD-текста по страницам/блокам (без резолва путей/кеша).

    Выделено из parse_md_document, чтобы тот же парсер можно было применить к MD
    из projects_v2 (read canary), гарантируя идентичный контракт. Возвращает None,
    если в тексте нет ни одного маркера `## СТРАНИЦА N`.
    """
    page_splits = list(_PAGE_RE.finditer(md_text))
    if not page_splits:
        return None

    pages = []
    for i, match in enumerate(page_splits):
        page_num = int(match.group(1))
        start = match.end()
        end = page_splits[i + 1].start() if i + 1 < len(page_splits) else len(md_text)
        page_text = md_text[start:end]

        # Метаданные страницы
        sheet_info = None
        sheet_label = None
        m = _SHEET_INFO_RE.search(page_text)
        if m:
            sheet_info = m.group(1).strip()
        m = _SHEET_NAME_RE.search(page_text)
        if m:
            sheet_label = m.group(1).strip()

        # Разбиваем на блоки
        block_matches = list(BLOCK_HEADER_RE.finditer(page_text))
        blocks = []
        for j, bm in enumerate(block_matches):
            block_type = bm.group("type")  # TEXT или IMAGE
            block_id = bm.group("id").strip()
            b_start = bm.end()
            b_end = block_matches[j + 1].start() if j + 1 < len(block_matches) else len(page_text)
            block_content = page_text[b_start:b_end].strip()

            block = {"block_id": block_id, "type": block_type}
            if block_type == "TEXT":
                block["content"] = block_content
            else:
                block.update(_parse_image_block(block_content))
                # Сохраняем и raw content для полноты
                block["content"] = block_content
            blocks.append(block)

        text_blocks = sum(1 for b in blocks if b['type'] == 'TEXT')
        image_blocks = sum(1 for b in blocks if b['type'] == 'IMAGE')

        pages.append({
            "page_num": page_num,
            "sheet_info": sheet_info,
            "sheet_label": sheet_label,
            "text_blocks": text_blocks,
            "image_blocks": image_blocks,
            "blocks": blocks,
        })

    return {
        "project_id": project_id,
        "md_file": md_file,
        "total_pages": len(pages),
        "pages": pages,
    }


def get_document_page(
    project_id: str,
    page_num: int,
    *,
    version_id: Optional[str] = None,
) -> Optional[dict]:
    """Получить данные одной страницы MD-документа."""
    doc = parse_md_document(project_id, version_id=version_id)
    if not doc:
        return None
    for page in doc['pages']:
        if page['page_num'] == page_num:
            return {
                "project_id": project_id,
                "page_num": page['page_num'],
                "sheet_info": page['sheet_info'],
                "sheet_label": page['sheet_label'],
                "blocks": page['blocks'],
            }
    return None
