"""Консервативная детерминированная проверка идентичности уже сопоставленных листов.

Здесь нет выравнивания, поиска областей изменений или LLM. ``identical``
выдаётся исключительно при согласии нескольких независимых слоёв PDF и рендера.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
_WS = re.compile(r"\s+")


def _digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _norm_text(value: Any) -> str:
    return _WS.sub(" ", unicodedata.normalize("NFKC", str(value or "")).strip())


def _round(value: Any, digits: int = 3) -> float:
    return round(float(value), digits)


def _primitive(value: Any) -> Any:
    """JSON-вариант PyMuPDF primitive, без порядковых номеров объектов."""
    if isinstance(value, float):
        return _round(value)
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if hasattr(value, "x") and hasattr(value, "y"):
        return ["point", _round(value.x), _round(value.y)]
    if all(hasattr(value, attr) for attr in ("x0", "y0", "x1", "y1")):
        return ["rect", _round(value.x0), _round(value.y0), _round(value.x1), _round(value.y1)]
    if isinstance(value, dict):
        return {str(key): _primitive(item) for key, item in sorted(value.items()) if key != "seqno"}
    if isinstance(value, (list, tuple)):
        return [_primitive(item) for item in value]
    return str(value)


def _status(*, equal: bool | None, left: str | None = None, right: str | None = None, detail: str | None = None) -> dict:
    return {
        "status": "equal" if equal is True else "different" if equal is False else "unavailable",
        "left_digest": left, "right_digest": right, "detail": detail,
    }


def _prepared_block_signature(page: dict | None) -> tuple[str | None, int]:
    if not isinstance(page, dict) or not isinstance(page.get("blocks"), list):
        return None, 0
    rows = []
    for block in page["blocks"]:
        bbox = block.get("normalized_bbox") or []
        try:
            box = [_round(value, 4) for value in bbox[:4]] if len(bbox) == 4 else None
        except (TypeError, ValueError):
            box = None
        rows.append([str(block.get("type") or "unknown"), box])
    return _digest(sorted(rows)), len(rows)


def _page_evidence(document, page_number: int) -> dict:
    page = document[page_number - 1]
    width, height = float(page.rect.width), float(page.rect.height)
    geometry = [round(width, 4), round(height, 4), int(page.rotation or 0)]
    raw_text = _norm_text(page.get_text("text"))
    words = []
    for item in page.get_text("words") or []:
        x0, y0, x1, y1, text = item[:5]
        words.append([_round(x0 / max(width, 1), 4), _round(y0 / max(height, 1), 4), _round(x1 / max(width, 1), 4), _round(y1 / max(height, 1), 4), _norm_text(text)])
    words.sort()
    text = None if not raw_text and not words else {"content": _digest(raw_text), "geometry": _digest(words)}

    drawings = []
    for drawing in page.get_drawings() or []:
        # Сортировка removes sequence/object ordering; path primitives remain
        # intact, as their order can affect drawing itself.
        drawings.append(_primitive(drawing))
    drawings.sort(key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False, separators=(",", ":")))
    vector = None if not drawings else _digest(drawings)

    images = []
    for image in page.get_images(full=True) or []:
        xref = int(image[0])
        try:
            # Хешируем декодированные пиксели, а не поток объекта PDF:
            # разные изображения иногда сериализуются одинаковым wrapper-ом.
            import fitz
            pix = fitz.Pixmap(document, xref)
            content = hashlib.sha256(bytes(pix.samples)).hexdigest()
            # Без transform=True: в этом режиме API возвращает (Rect, Matrix),
            # а здесь нужна именно геометрия размещения Rect.
            rects = page.get_image_rects(xref)
            for rect in rects:
                images.append([content, _round(rect.x0 / max(width, 1), 4), _round(rect.y0 / max(height, 1), 4), _round(rect.x1 / max(width, 1), 4), _round(rect.y1 / max(height, 1), 4)])
        except Exception:
            images.append([f"unreadable:{xref}"])
    images.sort()
    image_digest = None if not images else _digest(images)

    # Строгий raster-confirmation. Разный export может дать different и тогда
    # лист уйдёт дальше; один render никогда не создаёт identical сам по себе.
    try:
        import fitz
        pixmap = page.get_pixmap(matrix=fitz.Matrix(0.25, 0.25), colorspace=fitz.csGRAY, alpha=False)
        render = _digest([pixmap.width, pixmap.height, bytes(pixmap.samples).hex()])
    except Exception:
        render = None
    return {"geometry": geometry, "text": text, "vector": vector, "images": image_digest, "render": render}


def _compare_evidence(left: dict, right: dict, left_prepared: dict | None, right_prepared: dict | None) -> tuple[dict, list[str]]:
    signals: dict[str, dict] = {}
    reasons: list[str] = []
    geometry_equal = left["geometry"] == right["geometry"]
    signals["page_geometry"] = _status(equal=geometry_equal, left=_digest(left["geometry"]), right=_digest(right["geometry"]))
    if not geometry_equal:
        reasons.append("page_geometry_different")

    for key, label in (("text", "text"), ("vector", "vector"), ("images", "images"), ("render", "render")):
        left_value, right_value = left.get(key), right.get(key)
        if key == "text":
            if left_value is None and right_value is None:
                signals["text"] = _status(equal=None, detail="no_extractable_text")
                signals["text_geometry"] = _status(equal=None, detail="no_extractable_text")
            elif left_value is None or right_value is None:
                signals["text"] = _status(equal=False, detail="text_available_only_on_one_side")
                signals["text_geometry"] = _status(equal=False, detail="text_available_only_on_one_side")
                reasons.append("text_different")
            else:
                text_equal = left_value["content"] == right_value["content"]
                word_equal = left_value["geometry"] == right_value["geometry"]
                signals["text"] = _status(equal=text_equal, left=left_value["content"], right=right_value["content"])
                signals["text_geometry"] = _status(equal=word_equal, left=left_value["geometry"], right=right_value["geometry"])
                if not text_equal or not word_equal:
                    reasons.append("text_or_text_geometry_different")
            continue
        if left_value is None and right_value is None:
            signals[label] = _status(equal=None, detail=f"no_{label}_layer")
        elif left_value is None or right_value is None:
            signals[label] = _status(equal=False, left=left_value, right=right_value, detail=f"{label}_available_only_on_one_side")
            reasons.append(f"{label}_different")
        else:
            same = left_value == right_value
            signals[label] = _status(equal=same, left=left_value, right=right_value)
            if not same:
                reasons.append(f"{label}_different")

    left_blocks, left_count = _prepared_block_signature(left_prepared)
    right_blocks, right_count = _prepared_block_signature(right_prepared)
    if left_blocks is None or right_blocks is None:
        signals["blocks"] = _status(equal=None, detail="prepared_blocks_missing")
    else:
        same = left_blocks == right_blocks
        signals["blocks"] = _status(equal=same, left=left_blocks, right=right_blocks, detail=f"count:{left_count}/{right_count}")
        if not same:
            reasons.append("prepared_block_structure_different")
    return signals, reasons


def evaluate_page_identity(left_pdf, right_pdf, left_page_number: int, right_page_number: int, *, left_prepared: dict | None, right_prepared: dict | None) -> dict:
    """Проверить одну уже принятую пару. Никакого geometric alignment."""
    try:
        import fitz
        left_document, right_document = fitz.open(str(left_pdf)), fitz.open(str(right_pdf))
    except Exception as exc:
        return {"left_page": left_page_number, "right_page": right_page_number, "status": "uncertain", "confidence": 0.0, "reason": f"pdf_open_failed:{exc}"}
    try:
        if not (1 <= left_page_number <= left_document.page_count and 1 <= right_page_number <= right_document.page_count):
            return {"left_page": left_page_number, "right_page": right_page_number, "status": "uncertain", "confidence": 0.0, "reason": "page_out_of_range"}
        left, right = _page_evidence(left_document, left_page_number), _page_evidence(right_document, right_page_number)
    except Exception as exc:
        return {"left_page": left_page_number, "right_page": right_page_number, "status": "uncertain", "confidence": 0.0, "reason": f"evidence_failed:{exc}"}
    finally:
        left_document.close(); right_document.close()
    signals, reasons = _compare_evidence(left, right, left_prepared, right_prepared)
    different = any(signal["status"] == "different" for signal in signals.values())
    available_content = sum(signals[name]["status"] == "equal" for name in ("text", "vector", "images"))
    render_equal = signals["render"]["status"] == "equal"
    if different:
        status, confidence, reason = "needs_comparison", 1.0, "; ".join(reasons)
    elif render_equal and available_content >= 1 and signals["page_geometry"]["status"] == "equal" and signals["blocks"]["status"] == "equal":
        status, confidence, reason = "identical", 1.0, "canonical_content_layers_and_render_match"
    else:
        status, confidence, reason = "uncertain", 0.0, "insufficient_independent_evidence_for_identical"
    return {"left_page": left_page_number, "right_page": right_page_number, "status": status, "confidence": confidence, "signals": signals, "reason": reason}


def _page_map(document: dict) -> dict[int, dict]:
    return {int(page.get("pdf_page")): page for page in document.get("pages") or [] if page.get("pdf_page") is not None}


def evaluate_sheet_identity(left_document: dict, right_document: dict, *, left_pdf, right_pdf, sheet_matching: dict, alignment_items: list[dict] | None = None) -> dict:
    """Проверить matched + ручные пары; uncertain alignment намеренно исключён."""
    pairs: list[dict] = []
    seen: set[tuple[int, int]] = set()
    for match in sheet_matching.get("matches") or []:
        if match.get("status") == "matched":
            key = (int(match["left_page"]), int(match["right_page"]))
            pairs.append({"left_page": key[0], "right_page": key[1], "source": "sheet_matcher"}); seen.add(key)
    for item in alignment_items or []:
        if str(item.get("mode") or "") != "manual" or item.get("left_page") is None or item.get("right_page") is None:
            continue
        key = (int(item["left_page"]), int(item["right_page"]))
        if key not in seen:
            pairs.append({"left_page": key[0], "right_page": key[1], "source": "manual_alignment"}); seen.add(key)
    left_pages, right_pages = _page_map(left_document), _page_map(right_document)
    results = []
    for pair in sorted(pairs, key=lambda item: (item["left_page"], item["right_page"])):
        result = evaluate_page_identity(left_pdf, right_pdf, pair["left_page"], pair["right_page"], left_prepared=left_pages.get(pair["left_page"]), right_prepared=right_pages.get(pair["right_page"]))
        result["source"] = pair["source"]
        results.append(result)
    return {
        "schema_version": SCHEMA_VERSION, "kind": "stage_comparison_sheet_identity",
        "settings": {"llm_used": False, "overlay_used": False, "alignment_used": False, "false_identical_policy": "strict_conjunction"},
        "input": {"left": left_document.get("document") or {}, "right": right_document.get("document") or {}},
        "items": results,
        "summary": {status: sum(item["status"] == status for item in results) for status in ("identical", "needs_comparison", "uncertain")},
    }


def _atomic_write(path: Path, payload: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except OSError: pass
        raise
    return path


def write_sheet_identity_report(directory: str | Path, report: dict) -> tuple[Path, Path]:
    directory = Path(directory)
    json_path, md_path = directory / "sheet_identity.json", directory / "sheet_identity.md"
    _atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = ["# Проверка идентичности сопоставленных листов", "", "`identical` выдан только при совпадении канонических слоёв и raster render. Все остальные листы остаются для следующего этапа.", "", "| V2 page | V3 page | Status | Text | Vector | Images | Render | Reason |", "| ---: | ---: | --- | --- | --- | --- | --- | --- |"]
    for item in report.get("items") or []:
        signal = item.get("signals") or {}
        lines.append("| " + " | ".join([str(item.get("left_page")), str(item.get("right_page")), item.get("status") or "—", (signal.get("text") or {}).get("status", "—"), (signal.get("vector") or {}).get("status", "—"), (signal.get("images") or {}).get("status", "—"), (signal.get("render") or {}).get("status", "—"), str(item.get("reason") or "—").replace("|", "\\|")]) + " |")
    lines.extend(["", "## Счётчики", "", *[f"- {key}: {value}" for key, value in (report.get("summary") or {}).items()], ""])
    _atomic_write(md_path, "\n".join(lines))
    return json_path, md_path


__all__ = ["evaluate_page_identity", "evaluate_sheet_identity", "write_sheet_identity_report"]
