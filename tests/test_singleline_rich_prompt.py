"""test_singleline_rich_prompt — флаг SINGLELINE_RICH_PROMPT (rich-разметка в Stage 02).

Проверяет `build_singleline_prompt` (compact vs rich), `resolve_singleline_prompt`
(по version_dir с 02_work/result.json + document.pdf) и наличие флага в config.
Данные в projects/ (вне git) → skip-if-missing. Платный API НЕ вызывается.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    build_singleline_prompt,
    resolve_singleline_prompt,
)

ROOT = Path(__file__).resolve().parents[1]


def _find_result_json():
    env = os.environ.get("SINGLELINE_K1_RESULT_JSON")
    cands = [Path(env)] if env else []
    cands += sorted(ROOT.glob("projects/**/13АВ-РД-ЭМ-К1*result.json"))
    return next((rj for rj in cands if rj.exists()), None)


def _sibling_pdf(rj: Path):
    pdf = rj.with_name(rj.name.replace("_result.json", ".pdf"))
    if pdf.exists():
        return pdf
    pdfs = sorted(rj.parent.glob("*.pdf"))
    return pdfs[0] if pdfs else None


RJ = _find_result_json()
PDF = _sibling_pdf(RJ) if RJ else None

pytestmark = pytest.mark.skipif(
    not (RJ and PDF),
    reason="нет данных проекта 13АВ-РД-ЭМ-К1 (projects/ в .gitignore)",
)


def _scheme_vt():
    """Вектор-текст листа ВРУ-К1.2 начало (с QF3.*, без 3-сегментных)."""
    d = json.loads(RJ.read_text(encoding="utf-8"))
    best, sc = None, -1
    for pg in d.get("pages", []):
        for b in pg.get("blocks", []):
            vt = b.get("pdfplumber_text") or ""
            if "К1.2." in vt and re.search(r"QF3\.\d+", vt) and not re.search(r"QF\d+\.\d+\.\d+", vt):
                s = len(set(re.findall(r"QF3\.\d+", vt)))
                if s > sc:
                    sc, best = s, vt
    return best


def test_flag_exists_and_is_bool():
    from backend.app.core import config
    assert isinstance(config.SINGLELINE_RICH_PROMPT_ENABLED, bool)


def test_build_singleline_prompt_compact_vs_rich():
    vt = _scheme_vt()
    assert vt
    compact = build_singleline_prompt(PDF, vt, rich=False, block_id="B", page=10)
    rich = build_singleline_prompt(PDF, vt, rich=True, block_id="B", page=10)
    assert compact and rich
    # компактный = render_graph_for_prompt (без 8 разделов эталона)
    assert "## Структура схемы" in compact
    assert "## 6. Таблица проверки трансформаторов тока" not in compact
    # rich = полная эталонная разметка (есть отходящие линии + ТТ + примечания)
    assert "## 4. Отходящие линии" in rich
    assert "## 6. Таблица проверки трансформаторов тока" in rich
    assert "## 7. Примечания" in rich
    # оба несут задачу и заголовок блока
    for p in (compact, rich):
        assert p.startswith("# Блок B | страница PDF 10")
        assert "верни findings[]" in p
    # rich заметно объёмнее (больше контекста)
    assert len(rich) > len(compact)


def test_build_singleline_prompt_none_for_non_scheme():
    # текст без feeder-якорей → не схема → None (не ломаем не-схемные блоки)
    assert build_singleline_prompt(PDF, "просто текст без схемы", rich=True) is None


def test_resolve_singleline_prompt_via_version_dir(tmp_path):
    """resolve_singleline_prompt читает 02_work/result.json + document.pdf по version_dir."""
    vt = _scheme_vt()
    assert vt
    work = tmp_path / "02_work"
    work.mkdir()
    (work / "result.json").write_text(
        json.dumps({"pages": [{"blocks": [{"id": "BLK", "pdfplumber_text": vt}]}]}, ensure_ascii=False),
        encoding="utf-8")
    (work / "document.pdf").symlink_to(PDF)

    rich = resolve_singleline_prompt(tmp_path, "BLK", 10, rich=True)
    compact = resolve_singleline_prompt(tmp_path, "BLK", 10, rich=False)
    assert rich and "## 6. Таблица проверки трансформаторов тока" in rich
    assert compact and "## Структура схемы" in compact
    # неизвестный блок → None
    assert resolve_singleline_prompt(tmp_path, "NOPE", 10, rich=True) is None
    # нет 02_work → None
    assert resolve_singleline_prompt(tmp_path / "empty", "BLK", 10, rich=True) is None
