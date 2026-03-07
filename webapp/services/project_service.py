"""
Сервис для работы с проектами.
Сканирование, чтение project_info.json, определение статуса конвейера.
"""
import json
import os
import shutil
from pathlib import Path
from typing import Optional
from datetime import datetime

from webapp.config import PROJECTS_DIR, SEVERITY_CONFIG
from webapp.models.project import (
    ProjectInfo, ProjectStatus, PipelineStatus, TextExtractionQuality,
)


def list_projects() -> list[ProjectStatus]:
    """Получить список всех проектов с их статусом."""
    projects = []
    if not PROJECTS_DIR.exists():
        return projects

    for entry in sorted(PROJECTS_DIR.iterdir()):
        if not entry.is_dir():
            continue
        info_path = entry / "project_info.json"
        if not info_path.exists():
            # Проект без конфигурации — показываем как неподготовленный
            pdf_files = list(entry.glob("*.pdf"))
            if not pdf_files:
                continue  # Пустая папка — пропускаем
            projects.append(ProjectStatus(
                project_id=entry.name,
                name=entry.name,
                description="(не подготовлен — нет project_info.json)",
                has_pdf=True,
                pdf_size_mb=round(pdf_files[0].stat().st_size / 1024 / 1024, 1),
            ))
            continue

        status = get_project_status(entry.name)
        if status:
            projects.append(status)

    return projects


def get_project_status(project_id: str) -> Optional[ProjectStatus]:
    """Получить полный статус одного проекта."""
    proj_dir = PROJECTS_DIR / project_id
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

    # Пакеты тайлов
    total_batches = 0
    completed_batches = 0
    batches_path = output_dir / "tile_batches.json"
    if batches_path.exists():
        bdata = _load_json(batches_path)
        if bdata:
            total_batches = bdata.get("total_batches", len(bdata.get("batches", [])))
            # Считаем завершённые
            for i in range(1, total_batches + 1):
                batch_file = output_dir / f"tile_batch_{i:03d}.json"
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
        last_audit_date=audit_date,
        total_batches=total_batches,
        completed_batches=completed_batches,
    )


def get_project_info(project_id: str) -> Optional[dict]:
    """Прочитать raw project_info.json."""
    path = PROJECTS_DIR / project_id / "project_info.json"
    return _load_json(path)


def save_project_info(project_id: str, data: dict) -> bool:
    """Сохранить project_info.json."""
    path = PROJECTS_DIR / project_id / "project_info.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def get_tile_pages(project_id: str) -> list[dict]:
    """Получить список страниц с тайлами."""
    tiles_dir = PROJECTS_DIR / project_id / "_output" / "tiles"
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
    page_dir = PROJECTS_DIR / project_id / "_output" / "tiles" / f"page_{page_num}"
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
            "prepare": "init",
            "text_analysis": "text_analysis",
            "tile_audit": "tiles_analysis",
            "main_audit": "findings",
            "norm_verify": "norms_verified",
            "optimization": "optimization",
        }
        valid_statuses = ("done", "error", "partial", "running", "skipped")
        # Маппинг: поле pipeline_log → файл-индикатор завершения
        output_files = {
            "main_audit": "03_findings.json",
            "tile_audit": "02_tiles_analysis.json",
            "norm_verify": "03a_norms_verified.json",
            "text_analysis": "01_text_analysis.json",
            "prepare": "00_init.json",
            "optimization": "optimization.json",
        }
        for log_key, field in mapping.items():
            stage_info = stages.get(log_key, {})
            s = stage_info.get("status", "pending")
            if s in valid_statuses:
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

    # 2. Fallback: старая логика по файлам (для проектов без pipeline_log.json)
    if (output_dir / "00_init.json").exists():
        status.init = "done"

    if (output_dir / "01_text_analysis.json").exists():
        status.text_analysis = "done"

    if (output_dir / "02_tiles_analysis.json").exists():
        status.tiles_analysis = "done"
    elif list(output_dir.glob("tile_batch_*.json")):
        status.tiles_analysis = "partial"

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
    if not PROJECTS_DIR.exists():
        return result

    for entry in sorted(PROJECTS_DIR.iterdir()):
        if not entry.is_dir():
            continue
        info_path = entry / "project_info.json"
        if info_path.exists():
            continue  # Уже зарегистрирован

        # Ищем PDF и MD файлы
        pdf_files = list(entry.glob("*.pdf"))
        md_files = list(entry.glob("*_document.md")) + list(entry.glob("*.md"))
        # Убираем дубликаты (если *_document.md также *.md)
        md_files = list({f.name: f for f in md_files}.values())

        if not pdf_files:
            continue  # Нет PDF — не проект

        result.append({
            "folder": entry.name,
            "pdf_files": [f.name for f in pdf_files],
            "md_files": [f.name for f in md_files],
            "pdf_size_mb": round(pdf_files[0].stat().st_size / 1024 / 1024, 1),
        })

    return result


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
    proj_dir = PROJECTS_DIR / folder
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
    output_dir = PROJECTS_DIR / project_id / "_output"
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
    output_dir = PROJECTS_DIR / project_id / "_output"
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
    output_dir = PROJECTS_DIR / project_id / "_output"
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
    """Очистить все результаты аудита, сохранив PDF, MD и project_info.json.

    Удаляет:
    - Всю папку _output/ (тайлы, JSON-этапы, батчи, логи, отчёты)

    Сбрасывает в project_info.json:
    - tile_config → {}
    - tile_config_source, text_source, md_page_classification,
      text_extraction_quality, tile_quality → удаляются

    Сохраняет:
    - PDF файл(ы)
    - MD файл(ы)
    - project_info.json (минимальная конфигурация)

    Returns:
        dict с описанием удалённого
    """
    proj_dir = PROJECTS_DIR / project_id
    if not proj_dir.exists():
        raise ValueError(f"Проект '{project_id}' не найден")

    result = {"deleted_files": 0, "deleted_dirs": 0, "freed_mb": 0.0}

    # 1. Удаляем _output/
    output_dir = proj_dir / "_output"
    if output_dir.exists():
        # Подсчитаем размер
        total_size = 0
        for f in output_dir.rglob("*"):
            if f.is_file():
                total_size += f.stat().st_size
                result["deleted_files"] += 1
            elif f.is_dir():
                result["deleted_dirs"] += 1
        result["freed_mb"] = round(total_size / 1024 / 1024, 1)

        shutil.rmtree(output_dir)

    # 2. Сбрасываем авто-поля в project_info.json
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

    # 3. Пересоздаём пустую _output/
    output_dir.mkdir(exist_ok=True)

    return result


def _load_json(path: Path) -> Optional[dict]:
    """Безопасное чтение JSON-файла."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return None
