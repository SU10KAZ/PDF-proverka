"""Роутер источника блока для Stage 02 — «вместо Gemma подаём точный текст чертежа».

Решение Андрея 2026-07-07: «вместо Gemma везде делаем сырые данные, а если это
однолинейки — делаем структурированные вектографом; в дальнейшем — свой
структурированный профиль для каждого типа графического блока».

Развилка на блок (3 ветки):
  1) однолинейная расчётная схема И гейт Вектографа пройден → ПОЛНЫЙ структурированный
     рендер (`render_graph_etalon_markdown`): связи QF↔код↔кабель↔потребитель привязаны
     геометрией колонок (мис-привязки соседства нет);
  2) есть содержательный вектор-слой (иначе) → СЫРОЙ вектор-текст блока (полигон-клип):
     100% полнота, 0 галлюцинаций OCR; связи домысливает LLM по соседству;
  3) вектор-слоя нет (скан/растр — клип тоньше порога) → None → Gemma-описание +
     изображение остаются как есть. Это ОБЯЗАТЕЛЬНЫЙ fallback: на сканах вектор-текста
     физически нет, «выбросить Gemma везде» без него = пустое описание блока.

Источник вектор-текста — полигон-клип из PDF по `document_graph.json` (НЕ `pdfplumber_text`
из result.json, который у многих проектов пуст — см. память проекта).

Всё за флагом `BLOCK_SOURCE_ROUTER_ENABLED` (default OFF), fail-soft: любая ошибка/нехватка
данных → (None, <kind>) → прод-поведение (Gemma) не меняется.

Будущее (роадмап graphic_block_extraction): для ВК/ОВ/СС/планов — свои структурированные
профили уровня Вектографа; сейчас структурируется только однолинейка (пилот).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Tuple

from .singleline_graph_geometry import (
    _clip_words_to_bbox,
    _clip_words_to_polygon,
    build_singleline_graph,
    evaluate_vectograf_gate,
    render_graph_etalon_markdown,
)

# Тоньше → считаем, что вектор-слоя нет (скан/растр) → fallback на Gemma.
_MIN_VECTOR_CHARS = 40

_TASK = (
    "## Задача:\n"
    "Выше — ТОЧНЫЙ текст блока (встроенный текст чертежа / структурный разбор вектор-слоя, "
    "это приоритетный источник ЧИСЕЛ, марок, сечений и связей). Найди проблемы и верни "
    "findings[]. Только проблемы. Не описывай что видишь. Если всё корректно — пустой массив."
)


def _locate(output_dir) -> Tuple[Optional[Path], Optional[Path]]:
    """(pdf_path, document_graph_path) по output_dir.

    PDF ищем вверх по родителям (до 4 уровней): legacy (project/_output → PDF в project/),
    V2 (<version_dir>/_output → 02_work/document.pdf) и v2-primary, где output_dir =
    <version_dir>/03_analysis/runs/<run_id> или .../latest — PDF лежит в
    <version_dir>/02_work на 2-3 уровня выше (один od.parent его НЕ находил: роутер и
    предикат пропуска Gemma молча выключались на всех v2-primary прогонах)."""
    od = Path(output_dir)
    dg = od / "document_graph.json"
    dgp = dg if dg.exists() else None
    parents = list(od.parents)[:4]
    for vd in parents:
        for cand in (vd / "02_work" / "document.pdf", vd / "document.pdf"):
            if cand.exists():
                return cand, dgp
    for vd in parents:
        work = vd / "02_work"
        if work.is_dir():
            cands = sorted(work.glob("*.pdf"))
            if cands:
                return cands[0], dgp
    cands = sorted(od.parent.glob("*.pdf"))
    if cands:
        return cands[0], dgp
    return None, dgp


def _extract_block(pdf_path: Path, dg: dict, block_id: str):
    """(page_text, block_text, bbox_norm, polygon_norm, page_pdf) для image-блока. Один open PDF."""
    import fitz  # локально, как в остальных block_grounding

    from .md_mirror_reconcile import _block_text

    doc = fitz.open(str(pdf_path))
    try:
        for p in dg.get("pages", []):
            pi = p.get("page_index", p.get("page"))
            if pi is None or pi >= doc.page_count:
                continue
            for b in p.get("image_blocks", []):
                bid = b.get("id") or b.get("block_id")
                if str(bid) != str(block_id):
                    continue
                page = doc[pi]
                pw, ph = float(page.rect.width), float(page.rect.height)
                words = page.get_text("words")
                poly = b.get("polygon_points_norm")
                bbox = b.get("coords_norm")
                clipped = (
                    _clip_words_to_polygon(words, poly, pw, ph)
                    if poly
                    else _clip_words_to_bbox(words, bbox, pw, ph)
                )
                return page.get_text(), _block_text(clipped), bbox, poly, (pi or 0) + 1
        return None
    finally:
        doc.close()


def vector_covered_block_ids(output_dir) -> dict:
    """{block_id: вектор-текст} для image-блоков с ГОДНЫМ вектор-слоем (≥ _MIN_VECTOR_CHARS).

    ОБЩИЙ предикат с `resolve_block_source`: тот же полигон-клип, тот же порог. Блок попадает
    сюда ТОГДА И ТОЛЬКО ТОГДА, когда роутер на Stage 02 отдаст ему вектор-текст (не gemma_fallback).
    Используется гейтом пропуска стадии Gemma: эти блоки можно НЕ гонять через Gemma — роутер и так
    подаст их точный текст. Один open PDF (батч). fail-soft → {}.

    ВАЖНО: значение (вектор-текст) кладётся в placeholder-enrichment пропущенного блока —
    страховка на случай, если роутер на Stage 02 всё же fail-soft'нётся: блок не станет слепым.
    """
    try:
        import fitz  # локально, как в остальных block_grounding

        from .md_mirror_reconcile import _block_text

        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return {}
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        doc = fitz.open(str(pdf))
        try:
            out: dict = {}
            for p in dg.get("pages", []):
                pi = p.get("page_index", p.get("page"))
                if pi is None or pi >= doc.page_count:
                    continue
                page = doc[pi]
                pw, ph = float(page.rect.width), float(page.rect.height)
                words = page.get_text("words")
                for b in p.get("image_blocks", []):
                    bid = b.get("id") or b.get("block_id")
                    if not bid:
                        continue
                    poly = b.get("polygon_points_norm")
                    clipped = (
                        _clip_words_to_polygon(words, poly, pw, ph)
                        if poly
                        else _clip_words_to_bbox(words, b.get("coords_norm"), pw, ph)
                    )
                    txt = _block_text(clipped)
                    if len((txt or "").strip()) >= _MIN_VECTOR_CHARS:
                        out[str(bid)] = txt
            return out
        finally:
            doc.close()
    except Exception:
        return {}


def resolve_block_source(
    output_dir, block_id: str, page, *, panel_hint: str = "ВРУ"
) -> Tuple[Optional[str], str]:
    """user_text для Stage 02 из вектор-слоя, либо None (оставить Gemma+изображение).

    Возвращает (text_or_None, source_kind). source_kind:
      structured_singleline | raw_vector | gemma_fallback | no_sources | block_not_found | error.
    fail-soft: при любой проблеме → (None, kind) и прод-путь (Gemma) сохраняется.
    """
    try:
        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return None, "no_sources"
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        ex = _extract_block(pdf, dg, str(block_id))
        if ex is None:
            return None, "block_not_found"
        page_text, block_text, bbox, poly, page_pdf = ex
        if len((block_text or "").strip()) < _MIN_VECTOR_CHARS:
            return None, "gemma_fallback"  # скан/растр — вектор-слоя нет

        head = f"# Блок {block_id} | страница PDF {page or page_pdf}\n\n"

        # (1) однолинейка + гейт → полный структурированный рендер
        graph = None
        try:
            graph = build_singleline_graph(
                pdf, page_text, panel_hint=panel_hint, bbox_norm=bbox, polygon_norm=poly
            )
        except Exception:
            graph = None
        if graph and evaluate_vectograf_gate(graph).get("use"):
            etalon = render_graph_etalon_markdown(graph)
            if etalon and len(etalon) > 200:
                return head + etalon + "\n\n" + _TASK, "structured_singleline"

        # (2) иначе → сырой вектор-текст блока (полный, без потерь)
        body = (
            "## Точный текст блока из вектор-слоя PDF (встроенный текст чертежа, без ошибок OCR):\n"
            f"```\n{block_text}\n```\n"
        )
        return head + body + "\n" + _TASK, "raw_vector"
    except Exception:
        return None, "error"
