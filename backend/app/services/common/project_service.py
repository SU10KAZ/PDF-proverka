"""
Сервис для работы с проектами.
Сканирование, чтение project_info.json, определение статуса конвейера.
"""
import contextvars
import json
import os
import re
import shutil
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


# TTL-кеш для iter_project_dirs (30 сек)
_PROJECT_DIRS_CACHE: list[tuple[str, Path]] = []
_PROJECT_DIRS_CACHE_TIME: float = 0.0
_PROJECT_DIRS_TTL: float = 30.0


def invalidate_project_cache() -> None:
    """Сбросить TTL-кеш `iter_project_dirs`.

    Вызывать после операций, которые меняют состав папок в `PROJECTS_DIR`:
    добавление/удаление/переименование проектов (например, после merge-as-version
    удаления source-папки). Без этого `/api/projects` будет ~30 сек показывать
    устаревший список.
    """
    global _PROJECT_DIRS_CACHE, _PROJECT_DIRS_CACHE_TIME
    _PROJECT_DIRS_CACHE = []
    _PROJECT_DIRS_CACHE_TIME = 0.0


def iter_project_dirs(force: bool = False) -> list[tuple[str, Path]]:
    """Рекурсивно найти все папки проектов (включая подпапки-группы).

    Возвращает [(project_id, path), ...] где project_id = имя папки.
    Проект = папка с project_info.json или PDF-файлами.
    Подпапка-группа (OV/, EOM/ и т.д.) = папка без project_info.json и без PDF.

    Кеш обновляется раз в 30 секунд (или force=True).
    """
    global _PROJECT_DIRS_CACHE, _PROJECT_DIRS_CACHE_TIME

    now = time.time()
    if not force and _PROJECT_DIRS_CACHE and (now - _PROJECT_DIRS_CACHE_TIME) < _PROJECT_DIRS_TTL:
        return _PROJECT_DIRS_CACHE

    results: list[tuple[str, Path]] = []
    projects_dir = _get_projects_dir()
    if not projects_dir.exists():
        return results
    for entry in sorted(projects_dir.iterdir()):
        if not entry.is_dir() or entry.name.startswith("_"):
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
                if sub.is_dir() and not sub.name.startswith("_"):
                    results.append((sub.name, sub))

    _PROJECT_DIRS_CACHE = results
    _PROJECT_DIRS_CACHE_TIME = now
    return results


def resolve_project_dir(
    project_id: str,
    *,
    object_id: Optional[str] = None,
    strict: bool = False,
) -> Path:
    """Найти папку проекта по ID.

    Порядок:
      1) Если передан `object_id` — резолвим в рамках projects_dir ЭТОГО объекта.
      2) Иначе если установлен ContextVar-binding — в рамках привязанного объекта.
      3) Иначе — старое поведение (через current_id / default).

    strict=True: если project_id существует в НЕСКОЛЬКИХ объектах и scope
    (object_id / binding) не задан — поднимаем `AmbiguousProjectError`. По
    умолчанию strict=False, чтобы не ломать существующие read-эндпоинты.
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
    # Поиск в подпапках (1 уровень)
    for subdir in projects_dir.iterdir():
        if subdir.is_dir() and not subdir.name.startswith("_"):
            candidate = subdir / project_id
            if candidate.exists():
                return candidate
    return direct  # fallback


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


def list_projects() -> list[ProjectStatus]:
    """Получить список всех проектов с их статусом."""
    hidden = _load_hidden_projects()
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
    version_info_path = version_dir / "project_info.json"
    root_info_path = proj_dir / "project_info.json"
    info_path = version_info_path if version_info_path.exists() else root_info_path
    if not info_path.exists():
        return None

    info = _load_json(info_path)
    if not info:
        return None

    output_dir = version_dir / "_output"
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

    text_path = output_dir / "extracted_text.txt"
    has_text = text_path.exists() and text_path.stat().st_size > 0
    text_size_kb = round(text_path.stat().st_size / 1024, 1) if has_text else 0.0

    # MD-файл (структурированный текст из внешнего OCR)
    md_file_name = info.get("md_file")
    has_md = False
    md_size_kb = 0.0
    if md_file_name:
        md_path = version_dir / md_file_name
        if md_path.exists() and md_path.stat().st_size > 0:
            has_md = True
            md_size_kb = round(md_path.stat().st_size / 1024, 1)
    # Основной текстовый источник аудита: только Markdown PDF representation.
    # extracted_text.txt может отображаться как артефакт, но не используется
    # как fallback для Stage 01.
    text_source = "md" if has_md else "none"

    # OCR result.json (от OCR-сервера) — в папке версии
    has_ocr = bool(list(version_dir.glob("*_result.json")))

    # OCR-блоки (кропнутые image-блоки) — в папке версии
    block_count = 0
    block_errors = 0
    block_expected = 0
    blocks_index = gemma_blocks_index_path(version_dir)
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
        pdf_files=[pf for pf in pdf_files if (proj_dir / pf).exists()],
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

    version_info = version_dir / "project_info.json"
    if version_info.exists():
        info = _load_json(version_info)
        if info:
            return info
    # Fallback — корневой info (legacy V1).
    return _load_json(proj_dir / "project_info.json")


def save_project_info(project_id: str, data: dict) -> bool:
    """Сохранить project_info.json."""
    path = resolve_project_dir(project_id) / "project_info.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def set_project_section(project_id: str, section: str) -> dict:
    """Сменить дисциплину проекта (только метаданные, без перемещения файлов).

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
    if not save_project_info(project_id, info):
        raise ValueError(f"Не удалось сохранить project_info.json для '{project_id}'")
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
    ("excel", "Excel-отчёт"),
]

def _get_stage_order(pipeline_version: str = "legacy") -> list[tuple[str, str]]:
    """Вернуть список (key, label) этапов конвейера."""
    return _PIPELINE_STAGE_ORDER


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

    return info


def clean_project_data(project_id: str) -> dict:
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

    Returns:
        dict с описанием удалённого
    """
    proj_dir = resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise ValueError(f"Проект '{project_id}' не найден")

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

    # 3. Сбрасываем авто-поля в project_info.json
    info = get_project_info(project_id)
    if info:
        auto_fields = [
            "tile_config_source", "text_source",
            "md_page_classification", "text_extraction_quality",
            "tile_quality",
        ]
        for field in auto_fields:
            info.pop(field, None)
        info["tile_config"] = {}
        save_project_info(project_id, info)
        result["project_info_reset"] = True

    # 4. Пересоздаём пустую _output/
    output_dir.mkdir(exist_ok=True)

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
    if not md_file_name:
        return None

    md_path = version_dir / md_file_name
    if not md_path.exists():
        return None

    try:
        md_text = md_path.read_text(encoding='utf-8')
    except Exception:
        return None

    # Разбиваем по страницам
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

    result = {
        "project_id": project_id,
        "md_file": md_file_name,
        "total_pages": len(pages),
        "pages": pages,
    }

    _document_cache[cache_key] = {"ts": time.time(), "data": result}
    return result


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
