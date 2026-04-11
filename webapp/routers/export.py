"""
REST API для экспорта отчётов.
"""
import io
import json
import os
import zipfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional

from webapp.config import BASE_DIR
from webapp.services import excel_service
from webapp.services.project_service import resolve_project_dir

router = APIRouter(prefix="/api/export", tags=["export"])


class ExcelSectionRequest(BaseModel):
    section: str
    project_ids: list[str]


@router.post("/excel")
async def generate_excel(report_type: str = "all"):
    """Генерация Excel-отчёта. report_type: findings | optimization | all"""
    if report_type not in ("findings", "optimization", "all"):
        raise HTTPException(400, f"Неверный тип отчёта: {report_type}")
    success, result = await excel_service.generate_excel(report_type=report_type)
    if success:
        filename = os.path.basename(result)
        return {"status": "ok", "file": filename, "path": result}
    else:
        raise HTTPException(500, f"Ошибка генерации Excel: {result}")


@router.post("/excel/section")
async def generate_section_excel(req: ExcelSectionRequest):
    """Генерация Excel-отчёта для одного раздела."""
    project_dirs = []
    for pid in req.project_ids:
        try:
            d = resolve_project_dir(pid)
            project_dirs.append(str(d))
        except Exception:
            continue
    if not project_dirs:
        raise HTTPException(400, "Нет проектов с данными в этом разделе")
    success, result = await excel_service.generate_excel(
        report_type="all",
        project_dirs=project_dirs,
    )
    if success:
        filename = os.path.basename(result)
        return {"status": "ok", "file": filename, "path": result}
    else:
        raise HTTPException(500, f"Ошибка генерации Excel: {result}")


@router.get("/download/{filename}")
async def download_file(filename: str):
    """Скачать файл отчёта."""
    from webapp.config import REPORTS_DIR
    # Ищем в REPORTS_DIR (отчет/), затем в BASE_DIR
    filepath = REPORTS_DIR / filename
    if not filepath.exists():
        filepath = BASE_DIR / filename
    if not filepath.exists():
        raise HTTPException(404, f"Файл '{filename}' не найден")
    if not str(filepath.resolve()).startswith(str(BASE_DIR.resolve())):
        raise HTTPException(403, "Доступ запрещён")
    return FileResponse(
        str(filepath),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


@router.get("/audit-package/{project_id:path}")
async def download_audit_package(project_id: str):
    """Скачать ZIP-пакет аудита для обсуждения в любой нейронке."""
    project_dir = resolve_project_dir(project_id)
    output_dir = project_dir / "_output"

    # Проверяем что есть хоть какие-то результаты аудита
    findings_file = output_dir / "03_findings.json"
    if not findings_file.exists():
        raise HTTPException(404, "Аудит не завершён — нет файла 03_findings.json")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # --- project_info.json ---
        pi = project_dir / "project_info.json"
        if pi.exists():
            zf.write(str(pi), "project_info.json")

        # --- MD-файл (основной текст документа) ---
        md_files = list(project_dir.glob("*_document.md"))
        for md in md_files:
            zf.write(str(md), md.name)

        # --- JSON-файлы конвейера ---
        pipeline_files = [
            ("01_text_analysis.json", "01_text_analysis.json"),
            ("02_blocks_analysis.json", "02_blocks_analysis.json"),
            ("03_findings.json", "03_findings.json"),
            ("03_findings_review.json", "03_findings_review.json"),
            ("norm_checks.json", "norm_checks.json"),
            ("optimization.json", "optimization.json"),
            ("optimization_review.json", "optimization_review.json"),
            ("document_graph.json", "document_graph.json"),
        ]
        for fname, arcname in pipeline_files:
            fpath = output_dir / fname
            if fpath.exists():
                zf.write(str(fpath), arcname)

        # --- Индекс блоков (без PNG — экономия места) ---
        blocks_dir = output_dir / "blocks"
        if blocks_dir.exists():
            index_file = blocks_dir / "index.json"
            if index_file.exists():
                zf.write(str(index_file), "blocks/index.json")

        # --- История обсуждений ---
        disc_dir = output_dir / "discussions"
        if disc_dir.exists():
            for disc_file in sorted(disc_dir.glob("*.json")):
                zf.write(str(disc_file), f"discussions/{disc_file.name}")

        # --- README.md с инструкцией для LLM ---
        readme = _build_audit_readme(project_dir, output_dir)
        zf.writestr("README.md", readme)

    buf.seek(0)
    # Имя из project_info.json → name, fallback на project_id
    project_name = project_id
    pi_path = project_dir / "project_info.json"
    if pi_path.exists():
        try:
            pi_data = json.loads(pi_path.read_text(encoding="utf-8"))
            project_name = pi_data.get("name", project_id)
        except Exception:
            pass
    safe_name = project_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
    filename = f"audit_package_{safe_name}.zip"

    # RFC 5987: filename* для кириллицы, filename для ASCII fallback
    from urllib.parse import quote
    ascii_fallback = "audit_package.zip"
    encoded_name = quote(filename)
    content_disp = f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": content_disp},
    )


def _build_audit_readme(project_dir: Path, output_dir: Path) -> str:
    """Генерирует README.md с описанием пакета аудита для LLM."""
    # Прочитать project_info
    pi_path = project_dir / "project_info.json"
    project_name = project_dir.name
    section = ""
    description = ""
    if pi_path.exists():
        try:
            pi = json.loads(pi_path.read_text(encoding="utf-8"))
            project_name = pi.get("name", project_name)
            section = pi.get("section", "")
            description = pi.get("description", "")
        except Exception:
            pass

    # Подсчёт замечаний
    findings_summary = ""
    findings_path = output_dir / "03_findings.json"
    if findings_path.exists():
        try:
            data = json.loads(findings_path.read_text(encoding="utf-8"))
            findings = data if isinstance(data, list) else data.get("findings", [])
            total = len(findings)
            by_severity = {}
            by_category = {}
            for f in findings:
                sev = f.get("severity", "N/A")
                cat = f.get("category", "N/A")
                by_severity[sev] = by_severity.get(sev, 0) + 1
                by_category[cat] = by_category.get(cat, 0) + 1
            sev_str = ", ".join(f"{k}: {v}" for k, v in sorted(by_severity.items()))
            cat_str = ", ".join(f"{k}: {v}" for k, v in sorted(by_category.items()))
            findings_summary = f"- Всего замечаний: **{total}**\n- По критичности: {sev_str}\n- По категориям: {cat_str}"
        except Exception:
            findings_summary = "- (не удалось прочитать)"

    # Наличие оптимизации
    has_optimization = (output_dir / "optimization.json").exists()

    # Список файлов
    files_desc = """
| Файл | Описание |
|------|----------|
| `README.md` | Этот файл — описание пакета и инструкции |
| `project_info.json` | Метаданные проекта (название, раздел, дисциплина) |
| `*_document.md` | Полный текст документа (OCR из PDF) |
| `document_graph.json` | Структура документа: текст и блоки по страницам |
| `01_text_analysis.json` | Этап 1: анализ текста (таблицы, нормативные ссылки) |
| `02_blocks_analysis.json` | Этап 2: анализ чертежей (описание каждого блока) |
| `03_findings.json` | Этап 3: **все замечания аудита** (основной файл) |
| `03_findings_review.json` | Вердикты критика по каждому замечанию |
| `norm_checks.json` | Проверка актуальности нормативных документов |
| `optimization.json` | Предложения по оптимизации (если есть) |
| `optimization_review.json` | Вердикты критика по оптимизации |
| `blocks/index.json` | Индекс блоков (page, ocr_label, size) — PNG не включены |
| `discussions/*.json` | История обсуждений (если были) |
"""

    readme = f"""# Пакет аудита: {project_name}

**Раздел:** {section}
**Описание:** {description}
**Дата выгрузки:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

## Сводка

{findings_summary}
{"- Оптимизация: есть" if has_optimization else "- Оптимизация: не проводилась"}

## Файлы в архиве

{files_desc}

## Как использовать

1. **Загрузите файлы в чат с LLM** (Claude, ChatGPT, Gemini и др.)
2. Начните с `03_findings.json` — это основной файл с замечаниями
3. Для контекста подключите `document_graph.json` или `*_document.md`
4. Описания чертежей в `02_blocks_analysis.json` (PNG не включены для экономии места)

## Примеры вопросов для LLM

- "Проанализируй замечание F-003 и скажи, обоснованно ли оно"
- "Какие критические замечания связаны с кабельной продукцией?"
- "Проверь, актуальна ли норма СП 256.1325800.2016"
- "Сравни замечания с вердиктами критика из findings_review"
- "Предложи формулировку ответа проектировщику на замечание F-012"

## Структура замечания (03_findings.json)

Каждое замечание содержит:
- `id` — уникальный номер (F-001, F-002...)
- `severity` — критичность (КРИТИЧЕСКОЕ, ЭКОНОМИЧЕСКОЕ, РЕКОМЕНДАТЕЛЬНОЕ и др.)
- `category` — категория (cable, lighting, protection и др.)
- `problem` / `description` — суть проблемы
- `norm` — ссылка на нормативный документ
- `solution` — рекомендация по исправлению
- `page` — страница PDF, `sheet` — лист из штампа
- `evidence` — привязка к блокам-чертежам

## Нормативная база РФ

Замечания привязаны к нормативным документам РФ (СП, ГОСТ, ПУЭ).
Статус каждой нормы проверен в `norm_checks.json` (действует / заменён / отменён).
"""
    return readme
