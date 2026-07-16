"""EV2 извлечение значений таблиц (Путь Б) — заполнение key_values_read.

Цель: дать кросс-блоку и зрению ТЕКСТ содержимого таблиц (ведомости/спецификации/
экспликации), а не только описание (label). Тогда поиск по марке/значению работает.

Стратегия (офлайн-first):
  1. Определить блоки-таблицы по маркерам label/sheet (таблица/ведомость/спец/экспл).
  2. Попробовать офлайн текст-слой PDF блока через block_pdf_source.extract_block_text_layer
     (pdfplumber/PyMuPDF/result.json) — БЕЗ нейросети.
  3. Если текст-слоя нет (типично для CAD-чертежей без вектор-текста) — пометить
     status="needs_vision" (добивается зрением в Фазе 4).

НЕ переписывает 01_blocks_analysis.json на диске — результат кешируется отдельно.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_TABLE_MARKERS = ("таблица", "спецификац", "экспликац", "ведомост",
                  "условные обознач", "перечень", "schedule")


@dataclass
class TableExtraction:
    block_id: str
    status: str = "empty"        # extracted | needs_vision | empty
    source: str = ""             # result_json | pymupdf | pdfplumber | none
    values: list = field(default_factory=list)   # ["марка: значение", ...] / строки
    text: str = ""


def is_table_block(block: dict) -> bool:
    txt = f"{block.get('label') or ''} {block.get('sheet') or ''}".lower()
    if block.get("sheet_type") in ("table_legend", "table_or_schedule"):
        return True
    return any(m in txt for m in _TABLE_MARKERS)


def identify_table_blocks(blocks_analysis: dict) -> list:
    out = []
    for b in (blocks_analysis.get("blocks") or blocks_analysis.get("block_analyses") or []):
        if is_table_block(b):
            out.append(str(b.get("block_id", "")).replace("block_", ""))
    return [b for b in out if b]


def _find_result_json_text(output_dir: Path, block_id: str) -> Optional[str]:
    """Поискать готовый pdfplumber/result-текст блока в output_dir (если есть)."""
    for name in (f"result_{block_id}.json", f"{block_id}.result.json"):
        p = output_dir / name
        if p.is_file():
            try:
                return p.read_text(encoding="utf-8")
            except OSError:
                pass
    return None


def extract_table_values(block: dict, output_dir: Path) -> TableExtraction:
    """Best-effort офлайн-извлечение значений таблицы. Без PDF/текст-слоя → needs_vision."""
    bid = str(block.get("block_id", "")).replace("block_", "")
    te = TableExtraction(block_id=bid)
    # уже есть key_values_read в анализе? используем
    kvr = block.get("key_values_read") or []
    if kvr:
        te.status, te.source, te.values = "extracted", "blocks_analysis", [str(x) for x in kvr]
        return te
    # офлайн text-layer
    try:
        from backend.app.services.stage_comparison.block_pdf_source import (
            extract_block_text_layer,
        )
        rj = _find_result_json_text(output_dir, bid)
        if rj:
            tl = extract_block_text_layer(result_json_text=rj, prefer_result_json=True)
            if getattr(tl, "ok", False) and getattr(tl, "text", "").strip():
                te.status, te.source = "extracted", getattr(tl, "source", "result_json")
                te.text = tl.text[:3000]
                te.values = _rows_from_words(getattr(tl, "words", []))
                return te
    except Exception:
        pass
    # нет офлайн-источника → отдать зрению
    te.status = "needs_vision"
    return te


def _rows_from_words(words: list) -> list:
    """Сгруппировать слова с bbox в строки таблицы (по Y), вернуть строки текстом."""
    if not words:
        return []
    rows: dict = {}
    for w in words:
        bbox = w.get("bbox") or [0, 0, 0, 0]
        y = round(float(bbox[1]) / 6) if len(bbox) > 1 else 0
        rows.setdefault(y, []).append((float(bbox[0]) if bbox else 0, str(w.get("text", ""))))
    out = []
    for y in sorted(rows):
        cells = [t for _, t in sorted(rows[y])]
        line = " ".join(cells).strip()
        if line:
            out.append(line)
    return out[:60]


def build_key_values_cache(output_dir: Path) -> dict:
    """Прогнать по всем блокам-таблицам проекта, вернуть {block_id: TableExtraction-dict}."""
    ba_path = output_dir / "01_blocks_analysis.json"
    if not ba_path.is_file():
        return {}
    ba = json.loads(ba_path.read_text(encoding="utf-8"))
    out = {}
    for b in (ba.get("blocks") or ba.get("block_analyses") or []):
        if not is_table_block(b):
            continue
        te = extract_table_values(b, output_dir)
        out[te.block_id] = {"status": te.status, "source": te.source,
                            "values": te.values, "text": te.text}
    return out
