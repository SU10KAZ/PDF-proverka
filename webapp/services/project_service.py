"""
Сервис для работы с проектами.
Сканирование, чтение project_info.json, определение статуса конвейера.
"""
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Optional
from datetime import datetime

from webapp.config import PROJECTS_DIR, SEVERITY_CONFIG
from webapp.models.project import (
    ProjectInfo, ProjectStatus, PipelineStatus, TextExtractionQuality,
)

# TTL-кеш для iter_project_dirs (30 сек)
_PROJECT_DIRS_CACHE: list[tuple[str, Path]] = []
_PROJECT_DIRS_CACHE_TIME: float = 0.0
_PROJECT_DIRS_TTL: float = 30.0


def iter_project_dirs(force: bool = False) -> list[tuple[str, Path]]:
    """Рекурсивно найти все папки проектов (включая подпапки-группы).

    Возвращает [(project_id, path), ...] где project_id = имя папки.
    Проект = папка с project_info.json или PDF-файлами.
    Подпапка-группа (OV/, EM/ и т.д.) = папка без project_info.json и без PDF.

    Кеш обновляется раз в 30 секунд (или force=True).
    """
    global _PROJECT_DIRS_CACHE, _PROJECT_DIRS_CACHE_TIME

    now = time.time()
    if not force and _PROJECT_DIRS_CACHE and (now - _PROJECT_DIRS_CACHE_TIME) < _PROJECT_DIRS_TTL:
        return _PROJECT_DIRS_CACHE

    results: list[tuple[str, Path]] = []
    if not PROJECTS_DIR.exists():
        return results
    for entry in sorted(PROJECTS_DIR.iterdir()):
        if not entry.is_dir() or entry.name.startswith("_"):
            continue
        if (entry / "project_info.json").exists() or list(entry.glob("*.pdf")):
            results.append((entry.name, entry))
        else:
            # Подпапка-группа — заходим внутрь (1 уровень)
            for sub in sorted(entry.iterdir()):
                if sub.is_dir() and not sub.name.startswith("_"):
                    results.append((sub.name, sub))

    _PROJECT_DIRS_CACHE = results
    _PROJECT_DIRS_CACHE_TIME = now
    return results


def resolve_project_dir(project_id: str) -> Path:
    """Найти папку проекта по ID (имя папки), с поиском в подпапках."""
    direct = PROJECTS_DIR / project_id
    if direct.exists():
        return direct
    # Если PROJECTS_DIR не существует — не падаем, возвращаем direct path
    if not PROJECTS_DIR.exists():
        return direct
    # Поиск в подпапках (1 уровень)
    for subdir in PROJECTS_DIR.iterdir():
        if subdir.is_dir() and not subdir.name.startswith("_"):
            candidate = subdir / project_id
            if candidate.exists():
                return candidate
    return direct  # fallback


def list_projects() -> list[ProjectStatus]:
    """Получить список всех проектов с их статусом."""
    projects = []
    for project_id, entry in iter_project_dirs():
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


def get_project_status(project_id: str) -> Optional[ProjectStatus]:
    """Получить полный статус одного проекта."""
    proj_dir = resolve_project_dir(project_id)
    if not proj_dir.exists():
        return None

    info_path = proj_dir / "project_info.json"
    if not info_path.exists():
        return None

    info = _load_json(info_path)
    if not info:
        return None

    output_dir = proj_dir / "_output"
    pdf_file = info.get("pdf_file", "document.pdf")
    pdf_files = info.get("pdf_files", [pdf_file])
    pdf_path = proj_dir / pdf_file

    # Проверяем наличие файлов — суммируем размеры всех PDF
    has_pdf = pdf_path.exists()
    pdf_size_mb = 0.0
    for pf in pdf_files:
        pp = proj_dir / pf
        if pp.exists():
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
        md_path = proj_dir / md_file_name
        if md_path.exists() and md_path.stat().st_size > 0:
            has_md = True
            md_size_kb = round(md_path.stat().st_size / 1024, 1)
    # Определяем основной текстовый источник
    if has_md:
        text_source = "md"
    elif has_text:
        text_source = "extracted_text"
    else:
        text_source = "none"

    # OCR result.json (от OCR-сервера)
    has_ocr = bool(list(proj_dir.glob("*_result.json")))

    # OCR-блоки (кропнутые image-блоки)
    block_count = 0
    block_errors = 0
    block_expected = 0
    blocks_index = output_dir / "blocks" / "index.json"
    if blocks_index.exists():
        bi = _load_json(blocks_index)
        if bi:
            block_count = bi.get("total_blocks", 0)
            block_errors = bi.get("errors", 0)
            block_expected = bi.get("total_expected", 0)

    # Pipeline status
    pipeline = _get_pipeline_status(output_dir)

    # Замечания
    findings_count = 0
    findings_by_severity = {}
    audit_date = None
    findings_path = output_dir / "03_findings.json"
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

    # Детальное саммари конвейера
    pipeline_summary = _build_pipeline_summary(output_dir)
    pipeline_issues = _build_pipeline_issues(output_dir)

    return ProjectStatus(
        project_id=project_id,
        name=info.get("name", project_id),
        description=info.get("description", ""),
        section=info.get("section", "EM"),
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
    )


def get_project_info(project_id: str) -> Optional[dict]:
    """Прочитать raw project_info.json."""
    path = resolve_project_dir(project_id) / "project_info.json"
    return _load_json(path)


def save_project_info(project_id: str, data: dict) -> bool:
    """Сохранить project_info.json."""
    path = resolve_project_dir(project_id) / "project_info.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def _get_pipeline_status(output_dir: Path) -> PipelineStatus:
    """Определить статус конвейера.

    Приоритет: pipeline_log.json > файловая проверка (fallback).
    """
    status = PipelineStatus()

    # 1. Попытка прочитать pipeline_log.json (персистентный лог этапов)
    log = _load_pipeline_log(output_dir)
    if log and "stages" in log:
        stages = log["stages"]
        # Маппинг: ключ в pipeline_log → поле PipelineStatus
        mapping = {
            "crop_blocks": "crop_blocks",
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
            "crop_blocks": "blocks/index.json",
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
            "prepare": "blocks/index.json",
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
                    from webapp.services.pipeline_service import pipeline_manager
                    proj_id = output_dir.parent.name
                    if not pipeline_manager.is_running(proj_id):
                        s = "error"
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
    blocks_index = output_dir / "blocks" / "index.json"
    if blocks_index.exists():
        status.crop_blocks = "done"

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


def _build_pipeline_issues(output_dir: Path) -> list[str]:
    """Извлечь проблемы конвейера для индикатора на дашборде.

    Проверяет:
    - Этапы с ошибками (error/interrupted)
    - Critic/Corrector пропущены при наличии findings
    - Нормы/оптимизация не запускались
    """
    log = _load_pipeline_log(output_dir)
    if not log or "stages" not in log:
        return []

    stages = log["stages"]
    issues = []

    # Этапы с ошибками
    _labels = dict(_PIPELINE_STAGE_ORDER)
    for key, label in _PIPELINE_STAGE_ORDER:
        info = stages.get(key, {})
        s = info.get("status", "")
        if s in ("error", "interrupted"):
            short_err = info.get("error", "")
            if short_err and len(short_err) > 40:
                short_err = short_err[:37] + "..."
            issues.append(f"{label}: {short_err}" if short_err else f"{label}: ошибка")

    # Findings есть, но critic/corrector не запускались
    has_findings = (output_dir / "03_findings.json").exists()
    if has_findings:
        if "findings_critic" not in stages and "findings_merge" in stages:
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
    if has_findings and "norm_verify" not in stages and "findings_merge" in stages:
        issues.append("Верификация норм: не запускалась")

    # Оптимизация не запускалась (не ошибка, но информация)
    # — не добавляем, чтобы не шуметь

    return issues


def _build_pipeline_summary(output_dir: Path) -> list[dict]:
    """Собрать детальное саммари конвейера из pipeline_log.json.

    Возвращает ВСЕ этапы конвейера. Если этап ещё не запускался —
    возвращает его со статусом "pending".

    Возвращает список dict:
      {key, label, status, message, duration_sec, error}
    """
    log = _load_pipeline_log(output_dir)
    stages = log.get("stages", {}) if log else {}

    result = []
    for key, label in _PIPELINE_STAGE_ORDER:
        info = stages.get(key)
        if not info:
            result.append({"key": key, "label": label, "status": "pending"})
            continue
        status = info.get("status", "pending")
        message = info.get("message", "")

        # Вычислить длительность
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
        if message:
            entry["message"] = message
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
                              name: Optional[str] = None, section: str = "EM",
                              description: str = "") -> dict:
    """Скопировать проект из внешней папки в projects/ и создать project_info.json.

    Копирует PDF и MD файлы (не всю папку), создаёт project_info.json.
    """
    source = Path(source_path)
    if not source.exists():
        raise ValueError(f"Папка '{source_path}' не найдена")

    folder_name = name or source.name
    dest = PROJECTS_DIR / folder_name
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
                     name: Optional[str] = None, section: str = "EM",
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
_BLOCK_RE = re.compile(r'^### BLOCK \[(TEXT|IMAGE)\]: (.+)$', re.MULTILINE)
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


def parse_md_document(project_id: str) -> Optional[dict]:
    """Парсинг MD-файла проекта по страницам и блокам.

    Возвращает: {project_id, md_file, total_pages, pages: [{page_num, sheet_info, sheet_label, blocks: [...]}]}
    """
    # Проверяем кэш
    cached = _document_cache.get(project_id)
    if cached and (time.time() - cached['ts']) < _DOCUMENT_CACHE_TTL:
        return cached['data']

    info = get_project_info(project_id)
    if not info:
        return None
    md_file_name = info.get("md_file")
    if not md_file_name:
        return None

    md_path = resolve_project_dir(project_id) / md_file_name
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
        block_matches = list(_BLOCK_RE.finditer(page_text))
        blocks = []
        for j, bm in enumerate(block_matches):
            block_type = bm.group(1)  # TEXT или IMAGE
            block_id = bm.group(2).strip()
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

    _document_cache[project_id] = {"ts": time.time(), "data": result}
    return result


def get_document_page(project_id: str, page_num: int) -> Optional[dict]:
    """Получить данные одной страницы MD-документа."""
    doc = parse_md_document(project_id)
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
