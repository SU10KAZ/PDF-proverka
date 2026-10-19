"""
REST API для экспорта отчётов.
"""
import asyncio
import io
import json
import os
import threading
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import re

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional

from backend.app.core.access import require_admin_dep
import backend.app.services.export.excel_service as excel_service
from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    TEXT_ANALYSIS_FILENAME,
    resolve_existing,
)
from backend.app.services.storage.projects_v2_source_resolver import (
    load_version_project_info,
    resolve_version_source_files,
)

router = APIRouter(prefix="/api/export", tags=["export"])


class ExcelSectionRequest(BaseModel):
    section: str
    project_ids: list[str]


# Имя файла отчёта в REPORTS_DIR: без каталогов, без ведущей точки, только xlsx.
_REPORT_NAME_RE = re.compile(r"^[\w][\w.\-]*\.xlsx$", re.UNICODE)


@router.post("/excel", dependencies=[Depends(require_admin_dep)])
async def generate_excel(report_type: str = "all"):
    """Генерация Excel-отчёта. report_type: findings | optimization | all"""
    if report_type not in ("findings", "optimization", "all"):
        raise HTTPException(400, f"Неверный тип отчёта: {report_type}")
    success, result = await excel_service.generate_excel(report_type=report_type)
    if success:
        # Абсолютный путь сервера наружу не отдаём.
        return {"status": "ok", "file": os.path.basename(result)}
    else:
        raise HTTPException(500, "Ошибка генерации Excel")


@router.post("/excel/section", dependencies=[Depends(require_admin_dep)])
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


@router.get("/download/{filename}", dependencies=[Depends(require_admin_dep)])
async def download_file(filename: str):
    """Скачать сформированный Excel-отчёт из REPORTS_DIR.

    Раньше при отсутствии файла в REPORTS_DIR искали в корне приложения
    (BASE_DIR) с проверкой `startswith`, и любой вошедший мог скачать
    файлы верхнего уровня релиза (.mcp.json, CLAUDE.md, код, а в чекауте —
    .env). Теперь — только REPORTS_DIR, только *.xlsx, без каталогов.
    """
    from backend.app.core.config import REPORTS_DIR
    if not _REPORT_NAME_RE.match(filename or ""):
        raise HTTPException(404, "Файл не найден")
    reports_root = REPORTS_DIR.resolve()
    filepath = (reports_root / filename).resolve()
    if filepath.parent != reports_root or not filepath.is_file():
        raise HTTPException(404, "Файл не найден")
    return FileResponse(
        str(filepath),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


def _project_info_from_v2_version(version_dir: Path) -> dict:
    info = load_version_project_info(version_dir)
    return info if isinstance(info, dict) else {}


def _resolve_expert_review(version_dir: Path, output_dir: Path) -> Optional[Path]:
    """Найти expert_review.json (решения эксперта + перенос вердиктов).

    В projects_v2 файл лежит в `<version_dir>/04_review/`, а output_dir
    указывает на `03_analysis/latest`, где его нет. Перебираем канонические
    пути (совпадает с knowledge_base_service._review_paths), иначе решения и
    причины «Возможный повтор» не попадают в пакет выгрузки.
    """
    for cand in (
        version_dir / "04_review" / "expert_review.json",
        version_dir / "03_analysis" / "latest" / "expert_review.json",
        output_dir / "expert_review.json",
        version_dir / "_output" / "expert_review.json",
    ):
        if cand.exists():
            return cand
    return None


def _resolve_discussions_dir(version_dir: Path, output_dir: Path) -> Optional[Path]:
    """Найти папку обсуждений (история чатов по замечаниям).

    discussion_service хранит их в СТАБИЛЬНОМ `<version_dir>/_output/discussions`,
    а output_dir в projects_v2 указывает на `03_analysis/latest`, где их нет.
    Проверяем оба варианта, возвращаем первый существующий, иначе None.
    """
    for cand in (
        version_dir / "_output" / "discussions",
        output_dir / "discussions",
    ):
        if cand.is_dir():
            return cand
    return None


# Не больше двух сборок пакета одновременно: сборка читает исходный PDF и
# артефакты целиком и запускает генератор Excel. Семафор потоковый — сборка
# идёт в потоке, а не в event loop.
_PACKAGE_SLOTS = threading.BoundedSemaphore(2)


class PackageInconsistent(RuntimeError):
    """Артефакты версии менялись во время сборки (идёт новый анализ)."""


def _cleanup_stale_packages(max_age_sec: int = 3600) -> None:
    """Удалить временные архивы, оставшиеся после обрыва скачивания
    (BackgroundTask удаления может не выполниться при разрыве соединения)."""
    import time as _time

    root = Path(tempfile.gettempdir())
    now = _time.time()
    for pattern in ("audit_package_*.zip", "audit_report_*.xlsx"):
        for path in root.glob(pattern):
            try:
                if now - path.stat().st_mtime > max_age_sec:
                    path.unlink()
            except OSError:
                pass


def _stat_signature(paths: list[Path]) -> list[tuple[str, int, int]]:
    out = []
    for path in paths:
        try:
            st = path.stat()
            out.append((str(path), st.st_mtime_ns, st.st_size))
        except OSError:
            out.append((str(path), -1, -1))
    return out


def _run_excel_generator(version_dir: Path, output_dir: Path, out_path: str) -> bool:
    """Единственный подпроцесс выгрузки: детерминированный generate_excel_report.py
    с фиксированными аргументами. Модели и анализа нет."""
    from backend.app.core.config import GENERATE_EXCEL_SCRIPT

    env = {
        **os.environ,
        "AUDIT_NO_OPEN": "1",
        "AUDIT_VERSION_DIR": str(version_dir),
        "AUDIT_OUTPUT_DIR": str(output_dir),
    }
    result = subprocess.run(
        [sys.executable, str(GENERATE_EXCEL_SCRIPT), str(output_dir), "--out", out_path, "--no-summary"],
        capture_output=True, timeout=60, env=env,
    )
    return result.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0


def _build_package_v2_file(adapter, doc: dict, vid: str, project_id: str, *,
                           include_discussions: bool) -> tuple[str, dict]:
    """Собрать ZIP во временный файл с уникальным именем. → (путь, project_info).

    Согласованность: набор включаемых файлов фиксируется до сборки и
    сверяется после (mtime/size); при расхождении — одна повторная сборка,
    затем PackageInconsistent (→ 409). Состав задан явным списком.
    """
    from backend.app.services.storage.v2_primary_wiring import v2_source_pdf

    doc_dir = Path(doc["doc_dir"])
    version_dir = adapter.version_dir(doc_dir, vid)
    output_dir = adapter.latest_dir(doc_dir, vid)
    project_info = _project_info_from_v2_version(version_dir)

    files: list[tuple[Path, str]] = []
    if not project_info and (version_dir / "version.json").exists():
        files.append((version_dir / "version.json", "version.json"))
    pdf_path = v2_source_pdf(project_id, vid, object_id=doc.get("object_id"))
    if pdf_path and pdf_path.exists():
        files.append((pdf_path, pdf_path.name))
    work_md = version_dir / "02_work" / "document.md"
    if work_md.exists():
        files.append((work_md, "document.md"))
    for name in adapter.source_input_files(doc_dir, vid):
        if name.lower().endswith(".md"):
            md = version_dir / "01_input" / name
            if md.exists() and (not work_md.exists() or md.name != work_md.name):
                files.append((md, md.name))
    # (source_filename, arcname): источник через resolve_existing, в ZIP —
    # под КАНОНИЧЕСКИМ именем.
    for fname, arcname in (
        (TEXT_ANALYSIS_FILENAME, TEXT_ANALYSIS_FILENAME),
        (BLOCKS_ANALYSIS_FILENAME, BLOCKS_ANALYSIS_FILENAME),
        ("03_findings.json", "03_findings.json"),
        ("03_findings_review.json", "03_findings_review.json"),
        ("norm_checks.json", "norm_checks.json"),
        ("optimization.json", "optimization.json"),
        ("optimization_review.json", "optimization_review.json"),
        ("document_graph.json", "document_graph.json"),
    ):
        fpath = resolve_existing(output_dir, fname)
        if fpath.exists():
            files.append((fpath, arcname))
    for blocks_name in ("blocks", "blocks_gemma_100", "blocks_stage02_100"):
        index_file = output_dir / blocks_name / "index.json"
        if index_file.exists():
            files.append((index_file, "blocks/index.json"))
            break
    if include_discussions:
        # История чатов с моделью — только администратору (план, Д-3).
        disc_dir = _resolve_discussions_dir(version_dir, output_dir)
        if disc_dir is not None:
            for disc_file in sorted(disc_dir.glob("*.json")):
                files.append((disc_file, f"discussions/{disc_file.name}"))
    er = _resolve_expert_review(version_dir, output_dir)
    if er is not None:
        files.append((er, "expert_review.json"))

    _cleanup_stale_packages()
    watched = [path for path, _ in files]
    for _attempt in range(2):
        before = _stat_signature(watched)
        fd, zip_path = tempfile.mkstemp(prefix="audit_package_", suffix=".zip")
        os.close(fd)
        tmp_xlsx = None
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                if project_info:
                    zf.writestr("project_info.json", json.dumps(project_info, ensure_ascii=False, indent=2))
                for path, arcname in files:
                    zf.write(str(path), arcname)
                try:
                    fd_x, tmp_xlsx = tempfile.mkstemp(prefix="audit_report_", suffix=".xlsx")
                    os.close(fd_x)
                    if _run_excel_generator(version_dir, output_dir, tmp_xlsx):
                        zf.write(tmp_xlsx, "audit_report.xlsx")
                except Exception as exc:  # noqa: BLE001 — пакет без Excel лучше, чем без пакета
                    print(f"[audit-package:v2] Excel generation failed: {exc}")
                zf.writestr("README.md", _build_audit_readme(version_dir, output_dir))
        except BaseException:
            os.unlink(zip_path)
            raise
        finally:
            if tmp_xlsx and os.path.exists(tmp_xlsx):
                os.unlink(tmp_xlsx)
        if _stat_signature(watched) == before:
            return zip_path, project_info
        os.unlink(zip_path)
    raise PackageInconsistent("артефакты версии менялись во время сборки")


async def _download_audit_package_v2(project_id: str, version_id: Optional[str] = None):
    """projects_v2-primary ZIP export. READ-ONLY, no legacy fallback inside branch."""
    from starlette.background import BackgroundTask

    from backend.app.core.access import read_only_request
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    adapter = ProjectsV2Adapter()
    doc = adapter.find_document_by_project_id(project_id)
    if doc is None:
        raise HTTPException(404, f"Документ '{project_id}' не найден в projects_v2")
    vid = adapter.resolve_version_id(doc, version_id)
    if not vid:
        raise HTTPException(404, f"Версия '{version_id}' не найдена в projects_v2")
    output_dir = adapter.latest_dir(Path(doc["doc_dir"]), vid)
    if not (output_dir / "03_findings.json").exists():
        raise HTTPException(404, "Аудит не завершён — нет файла 03_findings.json")

    from backend.app.pipeline.manager import pipeline_manager

    if pipeline_manager.is_running(project_id, vid):
        raise HTTPException(409, {"code": "processing",
                                  "message": "Идёт обработка версии — архив будет доступен после завершения."})
    if not _PACKAGE_SLOTS.acquire(blocking=False):
        raise HTTPException(429, {"code": "busy",
                                  "message": "Сейчас собираются другие архивы. Повторите через минуту."})
    try:
        zip_path, project_info = await asyncio.to_thread(
            _build_package_v2_file, adapter, doc, vid, project_id,
            include_discussions=not read_only_request(),
        )
    except PackageInconsistent as exc:
        raise HTTPException(409, {"code": "processing",
                                  "message": "Результаты версии обновляются — повторите позже."}) from exc
    finally:
        _PACKAGE_SLOTS.release()

    project_name = project_info.get("name", doc["document_code"]) if project_info else doc["document_code"]
    safe_name = project_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
    filename = f"audit_package_{safe_name}.zip"
    from urllib.parse import quote
    content_disp = f"attachment; filename=\"audit_package.zip\"; filename*=UTF-8''{quote(filename)}"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        headers={"Content-Disposition": content_disp},
        background=BackgroundTask(os.unlink, zip_path),
    )


@router.get("/audit-package/{project_id:path}")
async def download_audit_package(project_id: str, version_id: Optional[str] = None):
    """Скачать ZIP-пакет аудита для обсуждения в любой нейронке."""
    from backend.app.services.storage.v2_primary_wiring import v2_primary_enabled
    if v2_primary_enabled():
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
        # pi — путь к project_info.json; вычисляем БЕЗУСЛОВНО, т.к. он нужен и
        # ниже для _resolve_version_pdf (иначе в ветке project_info из v2
        # переменная осталась бы неопределённой → UnboundLocalError).
        pi = version_dir / "project_info.json"
        if not pi.exists():
            pi = project_dir / "project_info.json"
        project_info = load_version_project_info(version_dir)
        if project_info:
            zf.writestr("project_info.json", json.dumps(project_info, ensure_ascii=False, indent=2))
        elif pi.exists():
            zf.write(str(pi), "project_info.json")

        # --- PDF этой версии (источник истины) ---
        pdf_path = _resolve_version_pdf(version_dir, pi)
        if pdf_path is not None:
            zf.write(str(pdf_path), pdf_path.name)

        # --- MD-файл (основной текст документа) ---
        try:
            sources = resolve_version_source_files(version_dir, project_id, project_info=project_info)
            md_files = list(sources.md_paths)
        except Exception:
            md_files = list(version_dir.glob("*_document.md"))
        for md in md_files:
            zf.write(str(md), md.name)

        # --- JSON-файлы конвейера ---
        # (source_filename, arcname): для переименованных артефактов источник
        # резолвится через resolve_existing (legacy или новое имя на диске), а в
        # ZIP всегда кладём под КАНОНИЧЕСКИМ именем.
        pipeline_files = [
            (TEXT_ANALYSIS_FILENAME, TEXT_ANALYSIS_FILENAME),
            (BLOCKS_ANALYSIS_FILENAME, BLOCKS_ANALYSIS_FILENAME),
            ("03_findings.json", "03_findings.json"),
            ("03_findings_review.json", "03_findings_review.json"),
            ("norm_checks.json", "norm_checks.json"),
            ("optimization.json", "optimization.json"),
            ("optimization_review.json", "optimization_review.json"),
            ("document_graph.json", "document_graph.json"),
        ]
        for fname, arcname in pipeline_files:
            fpath = resolve_existing(output_dir, fname)
            if fpath.exists():
                zf.write(str(fpath), arcname)

        # --- Индекс блоков (без PNG — экономия места) ---
        blocks_dir = output_dir / "blocks"
        if blocks_dir.exists():
            index_file = blocks_dir / "index.json"
            if index_file.exists():
                zf.write(str(index_file), "blocks/index.json")

        # --- История обсуждений ---
        disc_dir = _resolve_discussions_dir(version_dir, output_dir)
        if disc_dir is not None:
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
        er = _resolve_expert_review(version_dir, output_dir)
        if er is not None:
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


def _resolve_version_pdf(version_dir: Path, pi_path: Path) -> Optional[Path]:
    """Найти PDF обрабатывавшейся версии.

    Приоритет: `pdf_file` из project_info.json → одиночный *.pdf-файл в папке
    версии. Возвращает None, если PDF не найден.
    """
    # 1. Явный pdf_file из метаданных
    if pi_path.exists():
        try:
            info = json.loads(pi_path.read_text(encoding="utf-8"))
            pdf_file = (info.get("pdf_file") or "").strip()
            if pdf_file:
                cand = version_dir / pdf_file
                if cand.is_file():
                    return cand
        except Exception:
            pass

    # 2. Fallback: *.pdf-файлы в папке версии (исключаем папки `<name>.pdf/`)
    pdfs = sorted(p for p in version_dir.glob("*.pdf") if p.is_file())
    if pdfs:
        return pdfs[0]
    return None


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
| `*.pdf` | Исходный PDF этой версии — источник истины |
| `*_document.md` | Полный текст документа (OCR из PDF) |
| `document_graph.json` | Структура документа: текст и блоки по страницам |
| `02_text_analysis.json` | Этап 1: анализ текста (таблицы, нормативные ссылки) |
| `01_blocks_analysis.json` | Этап 2: анализ чертежей (описание каждого блока) |
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
4. Описания чертежей в `01_blocks_analysis.json` (PNG не включены для экономии места)

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
