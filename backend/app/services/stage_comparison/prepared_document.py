"""Детерминированная модель подготовленного PDF для сравнения стадий.

Модуль намеренно ничего не распознаёт и не сопоставляет. Он объединяет уже
полученные при подготовке артефакты в один JSON:

``document.pdf + blocks.json + document.md + ocr.html -> PreparedDocument``.

``blocks.json`` остаётся источником геометрии, а ``results_md`` — единственной
точкой разбора Markdown/штампов/описаний изображений. Исходные артефакты не
изменяются: PreparedDocument пишется только в ``03_analysis`` версии.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from backend.app.services.common.blocks_json import load_blocks_json
from backend.app.services.common.results_md import ResultsMdDocument, parse_results_md

from . import stage_storage
from .block_semantic_type import classify_block_semantic_type


SCHEMA_VERSION = 1
MODEL_KIND = "stage_comparison_prepared_document"
PREPARED_DIRNAME = "prepared_comparison"
PREPARED_FILENAME = "prepared_document.json"


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _relative_to_version(path: Path | None, version_dir: Path) -> str | None:
    if path is None:
        return None
    try:
        return str(path.relative_to(version_dir))
    except ValueError:
        return str(path)


def prepared_document_path(version_dir: str | Path) -> Path:
    return Path(version_dir) / "03_analysis" / "latest" / PREPARED_DIRNAME / PREPARED_FILENAME


def _finite_bbox(value: Any) -> list[float]:
    if not isinstance(value, (list, tuple)) or len(value) < 4:
        return []
    try:
        return [float(value[i]) for i in range(4)]
    except (TypeError, ValueError):
        return []


def _valid_norm_bbox(value: Any) -> tuple[list[float], list[str]]:
    bbox = _finite_bbox(value)
    warnings: list[str] = []
    if len(bbox) != 4 or not all(math.isfinite(item) for item in bbox):
        return [], ["coords_norm_missing_or_non_finite"]
    x0, y0, x1, y1 = bbox
    if not (0 <= x0 <= 1 and 0 <= y0 <= 1 and 0 <= x1 <= 1 and 0 <= y1 <= 1):
        warnings.append("coords_norm_out_of_range")
    if x1 <= x0 or y1 <= y0:
        warnings.append("coords_norm_degenerate")
    return bbox, warnings


def _page_source_metrics(pdf_path: Path | None) -> tuple[dict[int, dict], list[str]]:
    """Дешёвые объективные признаки страницы непосредственно из PDF.

    В геометрии используем PyMuPDF ``page.rect``: это видимая страница после
    /Rotate, то есть та же система, в которой приходят ``coords_norm`` портала.
    """
    if pdf_path is None or not pdf_path.is_file():
        return {}, ["source_pdf_missing"]
    try:
        import fitz
    except Exception as exc:  # pragma: no cover - only when dependency absent
        return {}, [f"pymupdf_unavailable: {exc}"]

    out: dict[int, dict] = {}
    warnings: list[str] = []
    try:
        source = fitz.open(str(pdf_path))
    except Exception as exc:
        return {}, [f"source_pdf_unreadable: {exc}"]
    try:
        for index, page in enumerate(source):
            page_w, page_h = float(page.rect.width), float(page.rect.height)
            try:
                text = page.get_text("text") or ""
                words = page.get_text("words") or []
            except Exception:
                text, words = "", []
            try:
                drawings_count = len(page.get_drawings())
            except Exception:
                drawings_count = 0
            image_count = 0
            image_area = 0.0
            try:
                seen_xrefs: set[int] = set()
                for image in page.get_images(full=True):
                    xref = int(image[0])
                    # Один xref может быть размещён несколько раз; считаем
                    # прямоугольники размещения, а не только уникальные images.
                    if xref in seen_xrefs and not page.get_image_rects(xref):
                        continue
                    seen_xrefs.add(xref)
                    rects = page.get_image_rects(xref, transform=True)
                    image_count += len(rects)
                    image_area += sum(max(0.0, rect.width * rect.height) for rect in rects)
            except Exception:
                pass
            page_area = max(1.0, page_w * page_h)
            out[index] = {
                "page_size": {"width": round(page_w, 6), "height": round(page_h, 6)},
                "rotation": int(page.rotation or 0),
                "media_box": [round(float(v), 6) for v in page.mediabox],
                "crop_box": [round(float(v), 6) for v in page.cropbox],
                "source_metrics": {
                    "pdf_text_characters": len(text),
                    "pdf_words": len(words),
                    "drawing_objects": drawings_count,
                    "image_placements": image_count,
                    # Сумма, а не union: это простой показатель, в котором
                    # перекрытые изображения могут дать значение > 1.
                    "image_area_ratio_sum_capped": round(min(1.0, image_area / page_area), 6),
                },
                "pdf_text": text.strip(),
            }
    finally:
        source.close()
    return out, warnings


def _source_type(metrics: dict) -> str:
    """Неглубокая классификация по сохранённым объективным признакам."""
    text = int(metrics.get("pdf_text_characters") or 0)
    drawings = int(metrics.get("drawing_objects") or 0)
    images = int(metrics.get("image_placements") or 0)
    image_ratio = float(metrics.get("image_area_ratio_sum_capped") or 0)
    if drawings > 0 and image_ratio < 0.08:
        return "vector"
    if drawings == 0 and images > 0 and image_ratio >= 0.5:
        return "scan"
    if drawings > 0 and images > 0:
        return "mixed"
    if text == 0 and drawings == 0 and images == 0:
        return "suspected_broken_vector"
    return "mixed"


def _page_meta(blocks_data: dict) -> dict[int, dict]:
    result: dict[int, dict] = {}
    for raw in blocks_data.get("pages") or []:
        if not isinstance(raw, dict):
            continue
        try:
            index = int(raw.get("page_index"))
        except (TypeError, ValueError):
            continue
        result[index] = dict(raw)
    return result


def _md_page_map(md: ResultsMdDocument) -> dict[int, Any]:
    return {page.number: page for page in md.pages}


def _graphic_description(md_block: Any | None) -> dict | None:
    if md_block is None or not md_block.is_image:
        return None
    sections = dict(md_block.image_sections or {})
    meta = dict(md_block.image_meta or {})
    return {
        "type": meta.get("type") or None,
        "axes": meta.get("axes") or None,
        "zone": meta.get("zone") or None,
        "level": meta.get("level") or None,
        "summary": sections.get("summary") or None,
        "description": sections.get("description") or None,
        "entities": sections.get("entities") or None,
        "verification": sections.get("verification") or None,
    }


def _block_for_prepared(
    raw: dict,
    *,
    md_block: Any | None,
    pdf_page: dict | None,
    page_index: int,
    page_number: int,
) -> dict:
    block_id = _clean(raw.get("block_id") or raw.get("id"))
    raw_type = _clean(raw.get("block_type") or raw.get("type")).lower() or "unknown"
    block_type = "stamp" if raw_type == "stamp" else raw_type
    norm_bbox, bbox_warnings = _valid_norm_bbox(raw.get("coords_norm") or raw.get("bbox_norm"))
    pdf_bbox: list[float] | None = None
    if norm_bbox and pdf_page:
        size = pdf_page.get("page_size") or {}
        width, height = float(size.get("width") or 0), float(size.get("height") or 0)
        if width > 0 and height > 0:
            pdf_bbox = [
                round(norm_bbox[0] * width, 6), round(norm_bbox[1] * height, 6),
                round(norm_bbox[2] * width, 6), round(norm_bbox[3] * height, 6),
            ]
        else:
            bbox_warnings.append("pdf_page_size_missing")

    stamp = dict(md_block.stamp) if md_block is not None else {}
    text = md_block.body if md_block is not None else ""
    raw_for_semantic = {
        "block_type": "image" if block_type == "stamp" else block_type,
        "category_code": "stamp" if block_type == "stamp" else raw.get("category_code"),
        "coords_norm": norm_bbox,
        "crop_url": raw.get("crop_url"),
        "stamp_data": {
            "document_code": stamp.get("code"), "sheet_number": stamp.get("sheet"),
            "sheet_name": stamp.get("name"), "stage": stamp.get("stage"),
            "organization": stamp.get("organization"),
        } if stamp else None,
        "ocr_text": text,
    }
    # blocks.json уже однозначно маркирует основную надпись. Не даём
    # эвристике текста понизить её до обычного text (бывает в старых MD).
    semantic_type = "stamp" if block_type == "stamp" else classify_block_semantic_type(raw_for_semantic)
    return {
        "block_id": block_id,
        "source_block_id": block_id,
        "type": block_type,
        "semantic_type": semantic_type,
        "page": page_number,
        "page_index": page_index,
        "bbox_pdf_visual": pdf_bbox,
        "normalized_bbox": norm_bbox or None,
        "coordinate_system": {
            "origin": "top_left_visible_page",
            "x_axis": "right",
            "y_axis": "down",
            "rotation_applied": True,
        },
        "polygon_normalized": raw.get("polygon_points") or raw.get("polygon_points_norm") or None,
        "text": text or None,
        "crop": {
            "crop_url": _clean(raw.get("crop_url")) or None,
            "local_file": _clean(raw.get("image_file")) or None,
            "can_render_from_source_pdf": bool(norm_bbox and pdf_bbox),
        },
        "stamp": stamp or None,
        "graphic_description": _graphic_description(md_block),
        "entities": (_graphic_description(md_block) or {}).get("entities"),
        "verification": (_graphic_description(md_block) or {}).get("verification"),
        "quality": {
            "coordinates_valid": not bbox_warnings,
            "warnings": bbox_warnings,
            "md_block_available": md_block is not None,
        },
        "source": {
            "blocks_json": dict(raw),
            "markdown": {
                "ordinal": md_block.ordinal,
                "header_line": md_block.header_line,
                "body_start_line": md_block.body_start_line,
            } if md_block is not None else None,
        },
    }


def _document_stamp(md: ResultsMdDocument, pages: list[dict]) -> dict:
    stamp = dict(md.stamp or {})
    # Metadata before first page is authoritative; fill gaps from the first
    # page/block rather than guessing them from the filename.
    for page in pages:
        for block in page.get("blocks") or []:
            block_stamp = block.get("stamp") or {}
            for key in ("code", "stage", "object", "organization", "revisions"):
                if not stamp.get(key) and block_stamp.get(key):
                    stamp[key] = block_stamp[key]
            if stamp:
                break
        if stamp:
            break
    return stamp


def build_prepared_document(
    version_dir: str | Path,
    *,
    stage_name: str | None = None,
    object_metadata: dict | None = None,
) -> dict:
    """Собрать модель документа из одной versioned comparison-версии.

    Отсутствующие optional artifacts не являются ошибкой: документ и страницы
    всё равно возвращаются с quality/warnings.
    """
    version_dir = Path(version_dir)
    work = version_dir / "02_work"
    pdf_path = work / "document.pdf"
    if not pdf_path.is_file():
        pdf_path = None
    md_path = work / "document.md"
    if not md_path.is_file():
        md_path = None
    blocks_path = work / "blocks.json"
    if not blocks_path.is_file():
        blocks_path = None
    html_path = work / "ocr.html"
    if not html_path.is_file():
        html_path = None

    blocks_data = load_blocks_json(blocks_path) if blocks_path else None
    if not blocks_data:
        blocks_data = {"pages": [], "blocks": []}
    md = ResultsMdDocument()
    if md_path:
        md = parse_results_md(md_path.read_text(encoding="utf-8", errors="replace"))

    pdf_pages, pdf_warnings = _page_source_metrics(pdf_path)
    raw_pages = _page_meta(blocks_data)
    md_pages = _md_page_map(md)
    raw_blocks_by_page: dict[int, list[dict]] = {}
    for raw in blocks_data.get("blocks") or []:
        if not isinstance(raw, dict):
            continue
        try:
            index = int(raw.get("page_index"))
        except (TypeError, ValueError):
            continue
        raw_blocks_by_page.setdefault(index, []).append(raw)

    page_indices = set(pdf_pages) | set(raw_pages) | set(raw_blocks_by_page)
    page_indices |= {number - 1 for number in md_pages if number > 0}
    pages: list[dict] = []
    unmatched_md_block_ids: set[str] = {block.block_id for block in md.blocks}
    for index in sorted(page_indices):
        page_number = index + 1
        pdf_page = pdf_pages.get(index)
        md_page = md_pages.get(page_number)
        blocks: list[dict] = []
        for raw in raw_blocks_by_page.get(index, []):
            block_id = _clean(raw.get("block_id") or raw.get("id"))
            md_block = md.blocks_by_id().get(block_id)
            if md_block is not None:
                unmatched_md_block_ids.discard(block_id)
            blocks.append(_block_for_prepared(
                raw, md_block=md_block, pdf_page=pdf_page,
                page_index=index, page_number=page_number,
            ))

        # MD может содержать блок без записи в blocks.json. Сохраняем его как
        # block с отсутствующей геометрией, а не теряем дорогой OCR/описание.
        for md_block in (md_page.blocks if md_page else []):
            if md_block.block_id not in unmatched_md_block_ids:
                continue
            unmatched_md_block_ids.discard(md_block.block_id)
            blocks.append(_block_for_prepared(
                {"block_id": md_block.block_id, "block_type": md_block.block_type},
                md_block=md_block, pdf_page=pdf_page,
                page_index=index, page_number=page_number,
            ))

        stamp_candidates = [block.get("stamp") or {} for block in blocks]
        stamp = next((item for item in stamp_candidates if item), {})
        metrics = (pdf_page or {}).get("source_metrics") or {}
        page_warnings: list[str] = []
        if not pdf_page:
            page_warnings.append("pdf_page_missing")
        if not blocks:
            page_warnings.append("blocks_missing")
        if not stamp:
            page_warnings.append("stamp_missing")
        if any(not block["quality"]["coordinates_valid"] for block in blocks):
            page_warnings.append("invalid_block_coordinates")
        graphic_blocks = [block for block in blocks if block.get("graphic_description")]
        text_blocks = [block for block in blocks if block.get("type") == "text"]
        page_text_parts = [block["text"] for block in text_blocks if block.get("text")]
        pages.append({
            "pdf_page": page_number,
            "page_index": index,
            "sheet_number": stamp.get("sheet") or None,
            "sheet_name": stamp.get("name") or None,
            "stamp": stamp or None,
            "page_size": (pdf_page or {}).get("page_size"),
            "rotation": (pdf_page or {}).get("rotation"),
            "media_box": (pdf_page or {}).get("media_box"),
            "crop_box": (pdf_page or {}).get("crop_box"),
            "source_type": _source_type(metrics) if pdf_page else "suspected_broken_vector",
            "source_metrics": metrics,
            "text": {
                "from_blocks": "\n\n".join(page_text_parts),
                "pdf_extracted_text": (pdf_page or {}).get("pdf_text") or "",
                "block_ids": [block["block_id"] for block in text_blocks if block.get("text")],
            },
            "blocks": blocks,
            "quality": {
                "stamp_available": bool(stamp),
                "text_available": bool(page_text_parts or (pdf_page or {}).get("pdf_text")),
                "blocks_available": bool(blocks),
                "vector_available": bool(metrics.get("drawing_objects")),
                "graphic_descriptions_available": bool(graphic_blocks),
                "coordinates_valid": bool(blocks) and not any(
                    not block["quality"]["coordinates_valid"] for block in blocks),
                "warnings": page_warnings,
            },
        })

    document_json = _read_json(version_dir.parent.parent / "document.json")
    version_json = _read_json(version_dir / "version.json")
    stamp = _document_stamp(md, pages)
    all_blocks = [block for page in pages for block in page["blocks"]]
    source_types = Counter(page["source_type"] for page in pages)
    block_types = Counter(block["type"] for block in all_blocks)
    global_warnings = list(pdf_warnings)
    if not blocks_path:
        global_warnings.append("blocks_json_missing")
    if not md_path:
        global_warnings.append("document_md_missing")
    if len(pdf_pages) and len(pdf_pages) != len(pages):
        global_warnings.append("page_sources_count_mismatch")
    if unmatched_md_block_ids:
        global_warnings.append(f"markdown_blocks_outside_page_map: {len(unmatched_md_block_ids)}")

    object_metadata = object_metadata or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": MODEL_KIND,
        "coordinate_convention": {
            "normalized_bbox": "[x0,y0,x1,y1] in 0..1, top-left of visible rotated page",
            "bbox_pdf_visual": "PyMuPDF page.rect coordinates, y grows down, /Rotate applied",
            "crop_roundtrip": "use source PDF + page_index + normalized_bbox through pdf_crop.extract_block_crop",
        },
        "document": {
            "source_pdf": _relative_to_version(pdf_path, version_dir),
            "filename": pdf_path.name if pdf_path else None,
            "code": stamp.get("code") or document_json.get("document_code") or version_dir.parent.name,
            "stage": stage_name or stamp.get("stage") or None,
            "object": stamp.get("object") or object_metadata.get("display_name") or object_metadata.get("legacy_name") or None,
            "organization": stamp.get("organization") or None,
            "revisions": stamp.get("revisions") or None,
            "page_count": len(pages),
            "version_id": version_json.get("version_id") or version_dir.name,
            "available_sources": {
                "source_pdf": pdf_path is not None,
                "markdown": md_path is not None,
                "blocks_json": blocks_path is not None,
                "ocr_html": html_path is not None,
            },
            "source_artifacts": {
                "pdf": _relative_to_version(pdf_path, version_dir),
                "markdown": _relative_to_version(md_path, version_dir),
                "blocks_json": _relative_to_version(blocks_path, version_dir),
                "ocr_html": _relative_to_version(html_path, version_dir),
            },
        },
        "summary": {
            "pages": len(pages),
            "blocks": len(all_blocks),
            "block_types": dict(sorted(block_types.items())),
            "source_types": dict(sorted(source_types.items())),
            "pages_with_sheet_number": sum(1 for page in pages if page.get("sheet_number")),
            "pages_with_sheet_name": sum(1 for page in pages if page.get("sheet_name")),
            "pages_with_insufficient_data": sum(1 for page in pages if len(page["quality"]["warnings"]) >= 2),
            "warnings_count": len(global_warnings) + sum(len(page["quality"]["warnings"]) for page in pages),
        },
        "pages": pages,
        "warnings": global_warnings,
    }


def write_prepared_document(path: str | Path, document: dict) -> Path:
    """Атомарно записать stable JSON без времени сборки и случайных полей."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise
    return path


def build_and_write_prepared_document(
    version_dir: str | Path,
    *,
    stage_name: str | None = None,
    object_metadata: dict | None = None,
) -> tuple[dict, Path]:
    version_dir = Path(version_dir)
    document = build_prepared_document(
        version_dir, stage_name=stage_name, object_metadata=object_metadata,
    )
    return document, write_prepared_document(prepared_document_path(version_dir), document)


def build_stage_prepared_documents(stage_dir: str | Path) -> list[tuple[dict, Path]]:
    """Построить PreparedDocument для current-версии каждого документа стадии."""
    stage_dir = Path(stage_dir)
    stage_meta = _read_json(stage_dir / "stage.json")
    object_meta = _read_json(stage_dir.parent / "object.json")
    stage_name = _clean(stage_meta.get("stage")) or stage_dir.name
    result: list[tuple[dict, Path]] = []
    for item in stage_storage.iter_current_documents(stage_dir):
        result.append(build_and_write_prepared_document(
            item["version_dir"], stage_name=stage_name, object_metadata=object_meta,
        ))
    return result


def _report_cell(value: Any) -> str:
    """Безопасная ячейка Markdown-таблицы; отчёт не теряет исходный JSON."""
    text = _clean(value).replace("|", "\\|").replace("\n", " ")
    return text or "—"


def write_prepared_diagnostic_report(
    comparison_dir: str | Path,
    prepared_documents: list[dict],
) -> Path:
    """Записать короткую человекочитаемую сверку PreparedDocument.

    Это диагностический результат объектного уровня, поэтому он лежит рядом со
    стадиями, а не среди входных PDF и не в общем временном каталоге.
    """
    comparison_dir = Path(comparison_dir)
    report_path = comparison_dir / "diagnostics" / "prepared_documents.md"
    rows: list[str] = []
    for document in prepared_documents:
        meta = document.get("document") or {}
        code = _report_cell(meta.get("code"))
        stage = _report_cell(meta.get("stage"))
        for page in document.get("pages") or []:
            blocks = page.get("blocks") or []
            metrics = page.get("source_metrics") or {}
            vector = (
                f"txt {metrics.get('pdf_text_characters', 0)}, "
                f"words {metrics.get('pdf_words', 0)}, "
                f"draw {metrics.get('drawing_objects', 0)}, "
                f"img {metrics.get('image_placements', 0)}"
            )
            quality = page.get("quality") or {}
            warnings = ", ".join(quality.get("warnings") or [])
            quality_text = "ok" if not warnings else warnings
            rows.append(
                "| " + " | ".join([
                    _report_cell(page.get("pdf_page")),
                    _report_cell(page.get("sheet_number")),
                    _report_cell(page.get("sheet_name")),
                    code,
                    _report_cell(page.get("source_type")),
                    str(sum(1 for block in blocks if block.get("type") == "text")),
                    str(sum(1 for block in blocks if block.get("graphic_description"))),
                    _report_cell(vector),
                    _report_cell(quality_text),
                ]) + " |"
            )
    lines = [
        "# Диагностика PreparedDocument",
        "",
        "Собрано только из существующих `document.pdf`, `blocks.json`, "
        "`document.md` и `ocr.html`; OCR, сопоставление и сравнение не запускались.",
        "",
        "| PDF page | Sheet | Name | Code | Type | Text blocks | Graphic blocks | Vector metrics | Quality |",
        "| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
        *rows,
        "",
    ]
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(lines)
    fd, temp_name = tempfile.mkstemp(prefix=report_path.name + ".", suffix=".tmp", dir=report_path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_name, report_path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise
    return report_path


__all__ = [
    "SCHEMA_VERSION", "MODEL_KIND", "PREPARED_DIRNAME", "PREPARED_FILENAME",
    "prepared_document_path", "build_prepared_document", "write_prepared_document",
    "build_and_write_prepared_document", "build_stage_prepared_documents",
    "write_prepared_diagnostic_report",
]
