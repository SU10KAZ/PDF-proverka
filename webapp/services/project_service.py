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
    pdf_path = proj_dir / pdf_file

    # Проверяем наличие файлов
    has_pdf = pdf_path.exists()
    pdf_size_mb = round(pdf_path.stat().st_size / 1024 / 1024, 1) if has_pdf else 0.0

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

    # Тайлы
    tiles_dir = output_dir / "tiles"
    tile_count = 0
    tile_pages = 0
    if tiles_dir.exists():
        page_dirs = [d for d in tiles_dir.iterdir() if d.is_dir() and d.name.startswith("page_")]
        tile_pages = len(page_dirs)
        for pd in page_dirs:
            tile_count += len(list(pd.glob("*.png")))

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

    return ProjectStatus(
        project_id=project_id,
        name=info.get("name", project_id),
        description=info.get("description", ""),
        section=info.get("section", "EM"),
        object=info.get("object"),
        has_pdf=has_pdf,
        pdf_size_mb=pdf_size_mb,
        has_extracted_text=has_text,
        text_size_kb=text_size_kb,
        has_md_file=has_md,
        md_file_name=md_file_name if has_md else None,
        md_file_size_kb=md_size_kb,
        text_source=text_source,
        has_tiles=tile_count > 0,
        tile_count=tile_count,
        tile_pages=tile_pages,
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


def get_tile_pages(project_id: str) -> list[dict]:
    """Получить список страниц с тайлами."""
    tiles_dir = resolve_project_dir(project_id) / "_output" / "tiles"
    if not tiles_dir.exists():
        return []

    pages = []
    for page_dir in sorted(tiles_dir.iterdir()):
        if not page_dir.is_dir() or not page_dir.name.startswith("page_"):
            continue

        page_num = page_dir.name.replace("page_", "")
        tiles = sorted([f.name for f in page_dir.glob("*.png")])

        # Определяем размер сетки из имён файлов
        rows = set()
        cols = set()
        for t in tiles:
            # page_07_r1c2.png → r=1, c=2
            parts = t.replace(".png", "").split("_")
            for p in parts:
                if p.startswith("r") and "c" in p:
                    rc = p.split("c")
                    rows.add(int(rc[0][1:]))
                    cols.add(int(rc[1]))

        # Попробуем прочитать index.json
        index_path = page_dir / "index.json"
        index_data = _load_json(index_path) if index_path.exists() else None

        pages.append({
            "page_num": page_num,
            "tile_count": len(tiles),
            "rows": max(rows) if rows else 0,
            "cols": max(cols) if cols else 0,
            "tiles": tiles,
            "index": index_data,
        })

    return pages


def get_tile_path(project_id: str, page_num: str, row: int, col: int) -> Optional[Path]:
    """Получить путь к PNG-файлу тайла."""
    page_dir = resolve_project_dir(project_id) / "_output" / "tiles" / f"page_{page_num}"
    tile_file = page_dir / f"page_{page_num}_r{row}c{col}.png"
    if tile_file.exists():
        return tile_file
    return None


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
            "findings_merge": "findings",
            "norm_verify": "norms_verified",
            "optimization": "optimization",
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
            "norm_verify": "03a_norms_verified.json",
            "optimization": "optimization.json",
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
                              md_file: Optional[str] = None,
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

    # Копируем PDF
    src_pdf = source / pdf_file
    if not src_pdf.exists():
        raise ValueError(f"PDF файл '{pdf_file}' не найден в '{source_path}'")
    shutil.copy2(str(src_pdf), str(dest / pdf_file))

    # Копируем MD если есть
    if md_file:
        src_md = source / md_file
        if src_md.exists():
            shutil.copy2(str(src_md), str(dest / md_file))

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
        "pdf_file": pdf_file,
        "source_path": str(source),
        "tile_config": {},
    }
    if md_file:
        info["md_file"] = md_file

    output_dir = dest / "_output"
    output_dir.mkdir(exist_ok=True)

    info_path = dest / "project_info.json"
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    return info


def register_project(folder: str, pdf_file: str, md_file: Optional[str] = None,
                     name: Optional[str] = None, section: str = "EM",
                     description: str = "") -> dict:
    """Создать project_info.json для папки из projects/.

    Args:
        folder: имя папки в projects/
        pdf_file: имя PDF-файла внутри папки
        md_file: имя MD-файла (опционально)
        name: название проекта (если не задано — используется имя папки)
        section: раздел проекта (EM по умолчанию)
        description: описание

    Returns:
        dict с project_info или raises ValueError
    """
    proj_dir = resolve_project_dir(folder)
    if not proj_dir.exists():
        raise ValueError(f"Папка '{folder}' не найдена в projects/")

    pdf_path = proj_dir / pdf_file
    if not pdf_path.exists():
        raise ValueError(f"PDF файл '{pdf_file}' не найден в папке '{folder}'")

    # Проверяем MD-файл если указан
    if md_file:
        md_path = proj_dir / md_file
        if not md_path.exists():
            raise ValueError(f"MD файл '{md_file}' не найден в папке '{folder}'")

    project_id = name or folder
    info = {
        "project_id": project_id,
        "name": project_id,
        "section": section,
        "description": description,
        "pdf_file": pdf_file,
        "tile_config": {},
    }
    if md_file:
        info["md_file"] = md_file

    # Создаём _output папку
    output_dir = proj_dir / "_output"
    output_dir.mkdir(exist_ok=True)

    # Сохраняем project_info.json
    info_path = proj_dir / "project_info.json"
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    return info


def get_tile_analysis(project_id: str) -> Optional[dict]:
    """Агрегация данных анализа тайлов из tile_batch_*.json.

    Возвращает словарь {tile_name: {label, summary, key_values_read, findings}}
    для быстрого O(1) lookup на фронтенде по имени тайла.
    """
    output_dir = resolve_project_dir(project_id) / "_output"
    if not output_dir.exists():
        return None

    batch_files = sorted(output_dir.glob("tile_batch_*.json"))
    if not batch_files:
        return None

    tiles_map = {}
    for batch_file in batch_files:
        data = _load_json(batch_file)
        if not data:
            continue
        for tile_info in data.get("tiles_reviewed", []):
            tile_name = tile_info.get("tile", "")
            if not tile_name:
                continue
            tiles_map[tile_name] = {
                "tile": tile_name,
                "page": tile_info.get("page"),
                "label": tile_info.get("label", ""),
                "summary": tile_info.get("summary", ""),
                "key_values_read": tile_info.get("key_values_read", []),
                "findings": tile_info.get("findings", []),
            }

    return {
        "project_id": project_id,
        "total_analyzed": len(tiles_map),
        "tiles": tiles_map,
    }


def get_page_analysis(project_id: str, page_num: int) -> Optional[dict]:
    """Полный анализ одной страницы: page_summary + тайлы.

    Приоритет: 02_tiles_analysis.json → fallback на tile_batch_*.json.
    """
    output_dir = resolve_project_dir(project_id) / "_output"
    if not output_dir.exists():
        return None

    page_summary = None
    page_tiles = []

    # 1. Ищем в 02_tiles_analysis.json (мерженый)
    merged_path = output_dir / "02_tiles_analysis.json"
    merged = _load_json(merged_path)
    if merged:
        for ps in merged.get("page_summaries", []):
            if ps.get("page") == page_num:
                page_summary = ps
                break
        for tile in merged.get("tiles_reviewed", []):
            if tile.get("page") == page_num:
                page_tiles.append(tile)
    else:
        # 2. Fallback: агрегация из tile_batch_*.json
        batch_files = sorted(output_dir.glob("tile_batch_*.json"))
        partial_summaries = []
        for bf in batch_files:
            data = _load_json(bf)
            if not data:
                continue
            for ps in data.get("page_summaries", []):
                if ps.get("page") == page_num:
                    partial_summaries.append(ps)
            for tile in data.get("tiles_reviewed", []):
                if tile.get("page") == page_num:
                    page_tiles.append(tile)

        if partial_summaries:
            page_summary = _simple_merge_page_summaries(partial_summaries, page_num)

    if not page_summary and not page_tiles:
        return None

    return {
        "project_id": project_id,
        "page": page_num,
        "page_summary": page_summary,
        "tiles": page_tiles,
    }


def get_all_page_summaries(project_id: str) -> Optional[dict]:
    """Все page_summaries проекта (без full_text_content — для списка).

    Приоритет: 02_tiles_analysis.json → fallback на tile_batch_*.json.
    """
    output_dir = resolve_project_dir(project_id) / "_output"
    if not output_dir.exists():
        return None

    summaries = []

    # 1. Ищем в 02_tiles_analysis.json
    merged_path = output_dir / "02_tiles_analysis.json"
    merged = _load_json(merged_path)
    if merged and merged.get("page_summaries"):
        summaries = merged["page_summaries"]
    else:
        # 2. Fallback: агрегация из tile_batch_*.json
        batch_files = sorted(output_dir.glob("tile_batch_*.json"))
        partial_map = {}  # {page_num: [parts]}
        for bf in batch_files:
            data = _load_json(bf)
            if not data:
                continue
            for ps in data.get("page_summaries", []):
                pn = ps.get("page", 0)
                if pn not in partial_map:
                    partial_map[pn] = []
                partial_map[pn].append(ps)

        for pn in sorted(partial_map.keys()):
            merged_ps = _simple_merge_page_summaries(partial_map[pn], pn)
            summaries.append(merged_ps)

    if not summaries:
        return None

    # Убираем full_text_content для лёгкости ответа
    light_summaries = []
    for s in summaries:
        light = {k: v for k, v in s.items() if k != "full_text_content"}
        light_summaries.append(light)

    return {
        "project_id": project_id,
        "page_summaries": light_summaries,
    }


def _simple_merge_page_summaries(parts: list, page_num: int) -> dict:
    """Простое слияние partial page_summaries (on-the-fly, без id_map)."""
    if len(parts) == 1:
        result = dict(parts[0])
        result["is_partial"] = False
        return result

    parts_sorted = sorted(parts, key=lambda p: min(p.get("rows_covered", [0])))

    sheet_type = "other"
    sheet_type_label = "Прочее"
    for p in parts_sorted:
        if p.get("sheet_type") and p["sheet_type"] != "other":
            sheet_type = p["sheet_type"]
            sheet_type_label = p.get("sheet_type_label", sheet_type)
            break

    all_rows = set()
    rows_total = 0
    for p in parts_sorted:
        all_rows.update(p.get("rows_covered", []))
        rows_total = max(rows_total, p.get("rows_total", 0))

    text_parts = [p.get("full_text_content", "") for p in parts_sorted if p.get("full_text_content")]
    seen_kv = set()
    key_values = []
    for p in parts_sorted:
        for kv in p.get("key_values", []):
            if kv not in seen_kv:
                seen_kv.add(kv)
                key_values.append(kv)

    findings = []
    seen_f = set()
    for p in parts_sorted:
        for fid in p.get("findings_on_page", []):
            if fid not in seen_f:
                seen_f.add(fid)
                findings.append(fid)

    summaries = [p.get("summary", "") for p in parts_sorted if p.get("summary")]

    return {
        "page": page_num,
        "sheet_type": sheet_type,
        "sheet_type_label": sheet_type_label,
        "is_partial": False,
        "rows_covered": sorted(all_rows),
        "rows_total": rows_total,
        "full_text_content": "\n".join(text_parts),
        "key_values": key_values,
        "findings_on_page": findings,
        "tile_count": sum(p.get("tile_count", 0) for p in parts_sorted),
        "summary": " ".join(summaries),
    }


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
