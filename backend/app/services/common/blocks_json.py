"""Синтез канонического result.json из `*_blocks.json` портала vibe.

Портал с 2026-07-16 отдаёт геометрию блоков файлом `<имя>_blocks.json`
(schema_version 1): pages[] (page_index/width_px/height_px/rotation) и
blocks[] (block_id/ordinal/page_index/block_type text|image|stamp/
shape_type/coords_norm/polygon_points/crop_url). Пайплайн же читает
канонический result.json старого портала:

    {"pdf_path": …, "pages": [{"page_number": 1-based, "width": px,
     "height": px, "blocks": [{"id", "page_index" 0-based, "coords_px",
     "coords_norm", "block_type" text|image, "category_code" ("stamp"),
     "shape_type", "polygon_points[_norm]", "ocr_text", "ocr_json",
     "crop_url", "created_at", "source"}]}]}

Здесь — детерминированный маппинг нового формата в старый (без нейросетей):
- координаты: coords_norm (top-left ВИЗУАЛЬНОЙ ориентации, см.
  docs/new_upload_format.md) × (width_px, height_px) → coords_px;
- штампы: block_type "stamp" → "image" + category_code "stamp" (как в старом
  формате), ocr_json собирается из per-block **Stamp:** MD той же страницы;
- тексты блоков: body из *_results.md по block_id (если MD передан);
- синтезированный файл помечается "source": "blocks_json_v1" — при появлении
  настоящего *_result.json от портала рабочая копия просто перезаписывается
  им без перестройки архитектуры.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from backend.app.services.common.results_md import ResultsMdDocument

# Маркер происхождения синтезированного result.json
SYNTH_SOURCE = "blocks_json_v1"


def load_blocks_json(path) -> Optional[dict]:
    """Прочитать и минимально проверить `*_blocks.json` (None при браке)."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if not isinstance(data.get("pages"), list) or not isinstance(data.get("blocks"), list):
        return None
    return data


def _scale_coords(coords_norm, width: int, height: int) -> list[int]:
    """[x0,y0,x1,y1] normalized → пиксели страницы (int, с клампом в границы)."""
    if not coords_norm or len(coords_norm) < 4:
        return [0, 0, 0, 0]
    x0, y0, x1, y1 = coords_norm[:4]
    return [
        max(0, min(width, round(float(x0) * width))),
        max(0, min(height, round(float(y0) * height))),
        max(0, min(width, round(float(x1) * width))),
        max(0, min(height, round(float(y1) * height))),
    ]


def _scale_polygon(points, width: int, height: int) -> list[list[int]]:
    out: list[list[int]] = []
    for pt in points or []:
        if not pt or len(pt) < 2:
            continue
        out.append([
            max(0, min(width, round(float(pt[0]) * width))),
            max(0, min(height, round(float(pt[1]) * height))),
        ])
    return out


def _created_at(created: Optional[str]) -> str:
    """MD «2026-07-07 15:22:34 UTC» → старый вид «2026-07-07 15:22:34»."""
    if not created:
        return ""
    return created.removesuffix(" UTC").strip()


def _stamp_ocr_json(stamp: dict) -> dict:
    """ocr_json штампа в старой схеме из per-block **Stamp:** нового MD."""
    return {
        "document_code": stamp.get("code") or None,
        "project_name": None,
        "sheet_name": stamp.get("name") or None,
        "stage": stamp.get("stage") or None,
        "sheet_number": stamp.get("sheet") or None,
        "total_sheets": None,
        "organization": stamp.get("organization") or None,
    }


def build_result_json(
    blocks_data: dict,
    md_doc: Optional[ResultsMdDocument] = None,
    *,
    pdf_name: Optional[str] = None,
) -> dict:
    """Собрать канонический result.json из blocks.json (+ тексты из MD).

    md_doc (разобранный *_results.md той же генерации) обогащает блоки:
    text/image получают ocr_text = body блока по block_id, штампы — ocr_json
    из штампа блоков своей страницы. Без md_doc геометрия всё равно полная.
    """
    md_blocks = md_doc.blocks_by_id() if md_doc is not None else {}
    md_sheets = md_doc.sheet_map() if md_doc is not None else {}

    pages_meta: dict[int, dict] = {}
    for pg in blocks_data.get("pages", []):
        try:
            idx = int(pg.get("page_index"))
        except (TypeError, ValueError):
            continue
        pages_meta[idx] = {
            "page_number": idx + 1,
            "width": int(pg.get("width_px") or 0),
            "height": int(pg.get("height_px") or 0),
            "rotation": int(pg.get("rotation") or 0),
            "blocks": [],
        }

    for blk in blocks_data.get("blocks", []):
        try:
            page_index = int(blk.get("page_index"))
        except (TypeError, ValueError):
            continue
        page = pages_meta.get(page_index)
        if page is None:
            # страница вне pages[] — создаём заглушку без размеров
            page = pages_meta[page_index] = {
                "page_number": page_index + 1,
                "width": 0, "height": 0, "rotation": 0, "blocks": [],
            }
        width, height = page["width"], page["height"]

        raw_type = (blk.get("block_type") or "").lower()
        is_stamp = raw_type == "stamp"
        block_id = blk.get("block_id") or ""
        md_block = md_blocks.get(block_id)

        out: dict = {
            "id": block_id,
            "page_index": page_index,
            "coords_px": _scale_coords(blk.get("coords_norm"), width, height),
            "coords_norm": list(blk.get("coords_norm") or []),
            # старый формат: штамп = image + category_code "stamp"
            "block_type": "image" if is_stamp else raw_type,
            "source": SYNTH_SOURCE,
            "shape_type": blk.get("shape_type") or "rectangle",
            "ocr_text": "",
            "created_at": _created_at(md_block.created if md_block else None),
            "crop_url": blk.get("crop_url") or "",
        }
        if blk.get("ordinal") is not None:
            out["ordinal"] = blk.get("ordinal")
        if blk.get("polygon_points"):
            out["polygon_points_norm"] = blk["polygon_points"]
            out["polygon_points"] = _scale_polygon(blk["polygon_points"], width, height)
        if is_stamp:
            out["category_code"] = "stamp"
            sheet_info = md_sheets.get(page_index + 1) or {}
            stamp_src = {}
            # штампов нет в MD — берём **Stamp:** любого блока этой страницы
            if md_doc is not None:
                pg_obj = md_doc.page(page_index + 1)
                if pg_obj and pg_obj.blocks:
                    stamp_src = pg_obj.blocks[0].stamp
            ocr_json = _stamp_ocr_json(stamp_src)
            if not ocr_json.get("sheet_name") and sheet_info.get("name"):
                ocr_json["sheet_name"] = sheet_info["name"]
            if not ocr_json.get("sheet_number") and sheet_info.get("sheet"):
                ocr_json["sheet_number"] = sheet_info["sheet"]
            out["ocr_json"] = ocr_json
            # graph_builder._extract_sheet_info читает НОМЕР ЛИСТА из
            # block["stamp_data"] (канонический ключ старого портала), а не из
            # ocr_json. Без этого sheet_no_raw страниц графа остаётся None и
            # столбец «Лист/Раздел» в Excel пустой. Эмитим stamp_data тоже.
            out["stamp_data"] = ocr_json
            out["ocr_text"] = json.dumps(ocr_json, ensure_ascii=False, indent=2)
        elif md_block is not None:
            out["ocr_text"] = md_block.body
            if md_block.is_image:
                # структурная мета IMAGE-блока (Type/Axes/Zone/Level + секции)
                meta = dict(md_block.image_meta)
                if md_block.image_sections:
                    meta["sections"] = dict(md_block.image_sections)
                if meta:
                    out["ocr_json"] = meta

        page["blocks"].append(out)

    doc_name = pdf_name or blocks_data.get("document_name") or ""
    return {
        "pdf_path": doc_name,
        "source": SYNTH_SOURCE,
        "synthesized_from": "blocks_json",
        "blocks_json_generated_at": blocks_data.get("generated_at"),
        "document_id": blocks_data.get("document_id"),
        "coordinate_space": blocks_data.get("coordinate_space"),
        "pages": [pages_meta[k] for k in sorted(pages_meta)],
    }


def ensure_result_json_for_version(version_dir) -> bool:
    """У версии есть 02_work/result.json — или он синтезирован из blocks.json.

    Единая точка «самолечения» для версий, загруженных любым путём (новый
    проект / новая версия / миграция) до подключения синтеза в их контур.
    True — result.json есть (был или создан). Fail-soft, идемпотентно.
    """
    try:
        version_dir = Path(version_dir)
        work = version_dir / "02_work"
        dst = work / "result.json"
        if dst.is_file():
            return True
        inp = version_dir / "01_input"
        bj = work / "blocks.json"
        if not bj.is_file():
            cands = sorted(inp.glob("*_blocks.json")) if inp.is_dir() else []
            if not cands:
                return False
            bj = cands[0]
        md = work / "document.md"
        if not md.is_file():
            mds = sorted(inp.glob("*_results.md")) if inp.is_dir() else []
            md = mds[0] if mds else None
        return synthesize_result_json_file(bj, dst, md)
    except Exception:
        return False


def synthesize_result_json_file(
    blocks_json_path,
    result_json_path,
    md_path=None,
    *,
    pdf_name: Optional[str] = None,
) -> bool:
    """Синтезировать result.json из blocks.json (+MD) на диске.

    Возвращает True при успехе; никогда не бросает (fail-soft: приём
    комплекта не должен падать из-за брака геометрии). Существующий
    НЕсинтезированный result.json не перезаписывается.
    """
    try:
        blocks_data = load_blocks_json(blocks_json_path)
        if blocks_data is None:
            return False
        dst = Path(result_json_path)
        if dst.exists():
            try:
                existing = json.loads(dst.read_text(encoding="utf-8"))
                if isinstance(existing, dict) and existing.get("source") != SYNTH_SOURCE:
                    return False  # настоящий result.json портала — не трогаем
            except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                return False
        md_doc = None
        if md_path is not None and Path(md_path).is_file():
            from backend.app.services.common.results_md import (
                is_results_md_text, parse_results_md,
            )
            text = Path(md_path).read_text(encoding="utf-8", errors="replace")
            if is_results_md_text(text):
                md_doc = parse_results_md(text)
        result = build_result_json(blocks_data, md_doc, pdf_name=pdf_name)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        return True
    except Exception:
        return False
