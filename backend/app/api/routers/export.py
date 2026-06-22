"""
REST API для экспорта отчётов.
"""
import io
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional

from backend.app.core.config import BASE_DIR
import backend.app.services.export.excel_service as excel_service
from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.storage.projects_v2_source_resolver import (
    load_version_project_info,
    resolve_version_source_files,
)

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
    from backend.app.core.config import REPORTS_DIR
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



def _project_info_from_v2_version(version_dir: Path) -> dict:
    info = load_version_project_info(version_dir)
    return info if isinstance(info, dict) else {}


async def _download_audit_package_v2(project_id: str, version_id: Optional[str] = None):
    """projects_v2-primary ZIP export. READ-ONLY, no legacy fallback inside branch."""
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.v2_primary_wiring import v2_source_pdf

    adapter = ProjectsV2Adapter()
    doc = adapter.find_document_by_project_id(project_id)
    if doc is None:
        raise HTTPException(404, f"Документ '{project_id}' не найден в projects_v2")
    vid = adapter.resolve_version_id(doc, version_id)
    if not vid:
        raise HTTPException(404, f"Версия '{version_id}' не найдена в projects_v2")
    doc_dir = Path(doc["doc_dir"])
    version_dir = adapter.version_dir(doc_dir, vid)
    output_dir = adapter.latest_dir(doc_dir, vid)

    findings_file = output_dir / "03_findings.json"
    if not findings_file.exists():
        raise HTTPException(404, "Аудит не завершён — нет файла 03_findings.json")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        project_info = _project_info_from_v2_version(version_dir)
        if project_info:
            zf.writestr("project_info.json", json.dumps(project_info, ensure_ascii=False, indent=2))
        elif (version_dir / "version.json").exists():
            zf.write(str(version_dir / "version.json"), "version.json")

        pdf_path = v2_source_pdf(project_id, vid)
        if pdf_path and pdf_path.exists():
            zf.write(str(pdf_path), pdf_path.name)

        work_md = version_dir / "02_work" / "document.md"
        if work_md.exists():
            zf.write(str(work_md), "document.md")
        for name in adapter.input_files(doc_dir, vid):
            if name.lower().endswith(".md"):
                md = version_dir / "01_input" / name
                if md.exists() and (not work_md.exists() or md.name != work_md.name):
                    zf.write(str(md), md.name)

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

        for blocks_name in ("blocks", "blocks_gemma_100", "blocks_stage02_100"):
            index_file = output_dir / blocks_name / "index.json"
            if index_file.exists():
                zf.write(str(index_file), "blocks/index.json")
                break

        disc_dir = output_dir / "discussions"
        if disc_dir.exists():
            for disc_file in sorted(disc_dir.glob("*.json")):
                zf.write(str(disc_file), f"discussions/{disc_file.name}")

        tmp_xlsx = None
        try:
            from backend.app.core.config import GENERATE_EXCEL_SCRIPT
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_xlsx = tmp.name
            env = {
                **os.environ,
                "AUDIT_NO_OPEN": "1",
                "AUDIT_VERSION_DIR": str(version_dir),
                "AUDIT_OUTPUT_DIR": str(output_dir),
            }
            result = subprocess.run(
                [sys.executable, str(GENERATE_EXCEL_SCRIPT),
                 str(output_dir), "--out", tmp_xlsx, "--no-summary"],
                capture_output=True, timeout=60, env=env,
            )
            if result.returncode == 0 and os.path.exists(tmp_xlsx) and os.path.getsize(tmp_xlsx) > 0:
                zf.write(tmp_xlsx, "audit_report.xlsx")
        except Exception as e:
            print(f"[audit-package:v2] Excel generation failed: {e}")
        finally:
            if tmp_xlsx and os.path.exists(tmp_xlsx):
                os.unlink(tmp_xlsx)

        er = output_dir / "expert_review.json"
        if er.exists():
            zf.write(str(er), "expert_review.json")

        readme = _build_audit_readme(version_dir, output_dir)
        zf.writestr("README.md", readme)

    buf.seek(0)
    project_name = project_info.get("name", doc["document_code"]) if project_info else doc["document_code"]
    safe_name = project_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
    filename = f"audit_package_{safe_name}.zip"
    from urllib.parse import quote
    ascii_fallback = "audit_package.zip"
    encoded_name = quote(filename)
    content_disp = f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": content_disp},
    )

@router.get("/audit-package/{project_id:path}")
async def download_audit_package(project_id: str, version_id: Optional[str] = None):
    """Скачать ZIP-пакет аудита для обсуждения в любой нейронке."""
    from backend.app.services.storage.storage_write_facade import v2_is_primary
    if v2_is_primary():
        return await _download_audit_package_v2(project_id, version_id)

    project_dir = resolve_project_dir(project_id)
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    output_dir = version_dir / "_output"

    # Проверяем что есть хоть какие-то результаты аудита
    findings_file = output_dir / "03_findings.json"
    if not findings_file.exists():
        raise HTTPException(404, "Аудит не завершён — нет файла 03_findings.json")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # --- project_info.json (версия, fallback на корень логического проекта) ---
        project_info = load_version_project_info(version_dir)
        if project_info:
            zf.writestr("project_info.json", json.dumps(project_info, ensure_ascii=False, indent=2))
        else:
            pi = version_dir / "project_info.json"
            if not pi.exists():
                pi = project_dir / "project_info.json"
            if pi.exists():
                zf.write(str(pi), "project_info.json")

        # --- MD-файл (основной текст документа) ---
        try:
            sources = resolve_version_source_files(version_dir, project_id, project_info=project_info)
            md_files = list(sources.md_paths)
        except Exception:
            md_files = list(version_dir.glob("*_document.md"))
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

        # --- Excel-таблица замечаний и оптимизаций (со столбцами Решение / Причина отклонения) ---
        tmp_xlsx = None
        try:
            from backend.app.core.config import GENERATE_EXCEL_SCRIPT
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_xlsx = tmp.name
            env = {
                **os.environ,
                "AUDIT_NO_OPEN": "1",
                "AUDIT_VERSION_DIR": str(version_dir),
                "AUDIT_OUTPUT_DIR": str(output_dir),
            }
            result = subprocess.run(
                [sys.executable, str(GENERATE_EXCEL_SCRIPT),
                 str(version_dir), "--out", tmp_xlsx, "--no-summary"],
                capture_output=True, timeout=60, env=env,
            )
            if result.returncode == 0 and os.path.exists(tmp_xlsx) and os.path.getsize(tmp_xlsx) > 0:
                zf.write(tmp_xlsx, "audit_report.xlsx")
        except Exception as e:
            print(f"[audit-package] Excel generation failed: {e}")
        finally:
            if tmp_xlsx and os.path.exists(tmp_xlsx):
                os.unlink(tmp_xlsx)

        # --- expert_review.json (решения эксперта, если есть) ---
        er = output_dir / "expert_review.json"
        if er.exists():
            zf.write(str(er), "expert_review.json")

        # --- README.md с инструкцией для LLM ---
        readme = _build_audit_readme(version_dir, output_dir)
        zf.writestr("README.md", readme)

    buf.seek(0)
    # Имя из project_info.json → name, fallback на project_id
    project_name = project_id
    pi_path = version_dir / "project_info.json"
    if not pi_path.exists():
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
    pi = _project_info_from_v2_version(project_dir)
    if not pi and pi_path.exists():
        try:
            pi = json.loads(pi_path.read_text(encoding="utf-8"))
        except Exception:
            pi = {}
    if pi:
        project_name = pi.get("name", project_name)
        section = pi.get("section", "")
        description = pi.get("description", "")

    # Подсчёт замечаний
    findings_summary = ""
    findings_path = output_dir / "03_findings.json"
    if findings_path.exists():
        try:
            data = json.loads(findings_path.read_text(encoding="utf-8"))
            findings = data if isinstance(data, list) else data.get("findings", [])
            coverage = {} if isinstance(data, list) else data.get("analysis_coverage") or data.get("meta", {}).get("analysis_coverage") or {}
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
            cov_summary = coverage.get("summary") or {}
            if cov_summary:
                findings_summary += (
                    "\n- Непокрытые блоки Gemma enrichment: "
                    f"**{cov_summary.get('gemma_uncovered_count', 0)}**"
                    "\n- Ошибки single-block анализа: "
                    f"**{cov_summary.get('single_block_failed_count', 0)}**"
                    "\n- Блоки, исключённые из полноценного анализа: "
                    f"**{cov_summary.get('excluded_from_full_analysis_count', 0)}**"
                )
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
| `audit_report.xlsx` | **Excel-таблица замечаний и оптимизаций** — со столбцами РЕШЕНИЕ и ПРИЧИНА ОТКЛОНЕНИЯ |
| `expert_review.json` | Решения эксперта (если были приняты ранее) |
"""

    readme = f"""# Пакет аудита: {project_name}

**Раздел:** {section}
**Описание:** {description}
**Дата выгрузки:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

## Сводка

{findings_summary}
{"- Оптимизация: есть" if has_optimization else "- Оптимизация: не проводилась"}

## Непокрытые блоки Gemma enrichment / ошибки single-block

Если в сводке выше есть ненулевые значения, см. `03_findings.json` → `analysis_coverage.sections`:
- `Непокрытые блоки Gemma enrichment`
- `Ошибки single-block анализа`
- `Блоки, исключённые из полноценного анализа`

## Файлы в архиве

{files_desc}

## Как использовать

1. **Загрузите файлы в чат с LLM** (Claude, ChatGPT, Gemini и др.)
2. Начните с `03_findings.json` — это основной файл с замечаниями
3. Для контекста подключите `document_graph.json` или `*_document.md`
4. Описания чертежей в `02_blocks_analysis.json` (PNG не включены для экономии места)

## Таблица решений (audit_report.xlsx)

Excel-файл содержит полную таблицу замечаний и оптимизаций со столбцами:
- **Решение эксперта** — заполните: "Принято" или "Отклонено"
- **Причина отклонения** — обязательно при отклонении

После заполнения загрузите Excel обратно на платформу: **Дашборд → База знаний → Загрузить решения**

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
