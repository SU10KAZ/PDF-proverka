"""block_grounding/runner.py — стадия Value Grounding (Phase 1, офлайн).

run_block_grounding_stage(ctx, *, force) -> StageResult

Читает result.json (per-block ocr_text=gemma + pdfplumber_text=вектор), сверяет значения,
пишет _output/block_grounding_summary.json с корректировками (В4.0→В40) и метриками.
Не трогает gemma-выход. Управляется флагом BLOCK_VALUE_GROUNDING_ENABLED (OFF → no-op).
"""
from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from backend.app.pipeline.stage_result import StageResult
from backend.app.pipeline.stages.block_grounding.grounding import (
    extract_concrete_classes,
    ground_block,
    vector_usable,
)

if TYPE_CHECKING:
    from backend.app.pipeline.context import PipelineStageContext

STAGE_LABEL = "Block Value Grounding"
SUMMARY_NAME = "block_grounding_summary.json"
_qwen_lock: asyncio.Lock | None = None


def _get_qwen_lock() -> asyncio.Lock:
    global _qwen_lock
    if _qwen_lock is None:
        _qwen_lock = asyncio.Lock()
    return _qwen_lock


def _find_result_json(project_dir: Path, output_dir: Optional[Path]) -> Optional[Path]:
    """Найти result.json (Chandra) с per-block ocr_text/pdfplumber_text."""
    candidates = [
        project_dir / "02_work" / "result.json",
        project_dir / "result.json",
    ]
    if output_dir:
        candidates += [output_dir / "result.json", output_dir.parent / "02_work" / "result.json"]
    for p in candidates:
        if p.is_file():
            return p
    hits = list(project_dir.glob("**/result.json"))
    return hits[0] if hits else None


def _find_source_pdf(project_dir: Path) -> Optional[Path]:
    """Исходный PDF для рендера тайлов (Phase 2)."""
    for p in (project_dir / "02_work" / "document.pdf", project_dir / "document.pdf"):
        if p.is_file():
            return p
    for sub in ("01_input", "02_work", "."):
        hits = list((project_dir / sub).glob("*.pdf")) if (project_dir / sub).exists() else []
        if hits:
            return hits[0]
    return None


def _select_qwen_blocks(candidates: list, *, tile_min: int, crop_min: int, max_blocks: int) -> list:
    """Выбрать блоки Phase 2: крупные (тайлинг) приоритетно, затем средние (кроп). Cap = max_blocks.

    ``mode`` проставляется по ширине. ``crop_min<=0`` → режим crop выключен (только тайлинг крупных).
    Чистая функция (без I/O) — тестируется отдельно.
    """
    tiled = sorted((dict(c, mode="tiled") for c in candidates
                    if c.get("width", 0) >= tile_min),
                   key=lambda c: -c.get("width", 0))
    medium = []
    if crop_min and crop_min > 0:
        medium = sorted((dict(c, mode="crop") for c in candidates
                         if crop_min <= c.get("width", 0) < tile_min),
                        key=lambda c: -c.get("width", 0))
    return (tiled + medium)[:max_blocks]


def _iter_blocks(result: dict):
    """Перебрать блоки с контекстом страницы: (block, page_number, page_px(w,h))."""
    if isinstance(result.get("pages"), list):
        for p in result["pages"]:
            pn = p.get("page_number")
            ppx = (p.get("width"), p.get("height"))
            for b in (p.get("blocks") or []):
                if isinstance(b, dict):
                    yield b, pn, ppx
    for b in (result.get("blocks") or []):
        if isinstance(b, dict):
            yield b, (b.get("page") or b.get("page_index")), (b.get("page_width"), b.get("page_height"))


def run_block_grounding(project_dir: Path, output_dir: Path) -> dict:
    """Чистая обработка (без ctx) — удобна для тестов/CLI."""
    rp = _find_result_json(project_dir, output_dir)
    if not rp:
        return {"status": "no_result_json", "blocks_total": 0}
    try:
        result = json.loads(rp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "result_json_error", "error": str(exc)[:200], "blocks_total": 0}

    all_blocks = list(_iter_blocks(result))  # (block, page_number, page_px)

    # Проход 1: классы бетона из вектор-слоя ВСЕХ блоков → истина уровня документа
    # (ловит «В4.0» на CAD-блоке без своего вектора, если «В40» есть в другом блоке).
    doc_classes: set = set()
    for b, _pn, _ppx in all_blocks:
        pp = b.get("pdfplumber_text") or ""
        if vector_usable(pp):
            doc_classes |= extract_concrete_classes(pp)

    blocks_out = []
    qwen_candidates = []  # image-блоки без годного вектора → кандидаты на Phase 2 (qwen)
    n_total = n_image = n_vector = n_corrected = 0
    field_counter: Counter = Counter()
    recalls = []
    # Проход 2: grounding каждого блока. Доменное правило класса валидно для любого типа;
    # вектор-grounding/recall считаем содержательным на image-блоках.
    for b, pn, ppx in all_blocks:
        btype = b.get("block_type")
        n_total += 1
        if btype == "image":
            n_image += 1
        g = ground_block(b.get("ocr_text") or "", b.get("pdfplumber_text") or "", doc_classes=doc_classes)
        if g["vector_usable"]:
            n_vector += 1
        if g["corrections"]:
            n_corrected += 1
            for c in g["corrections"]:
                field_counter[c["field"]] += 1
        if g["gemma_number_recall_vs_vector"] is not None:
            recalls.append(g["gemma_number_recall_vs_vector"])
        bid = b.get("id") or b.get("block_id")
        if g["corrections"] or g["vector_usable"]:
            blocks_out.append({"block_id": bid, "page": pn, "block_type": btype, **g})
        # кандидат Phase 2: image без годного вектора, есть координаты/размер страницы
        co = b.get("coords_px")
        if (btype == "image" and not g["vector_usable"] and co and pn and ppx[0] and ppx[1]):
            width = co[2] - co[0]
            qwen_candidates.append({
                "block_id": bid, "page": pn, "coords_px": co,
                "page_px": [ppx[0], ppx[1]], "width": width,
                "mode": "tiled",  # фактический mode (tiled/crop) выберет стадия по порогу
            })

    avg_recall = round(sum(recalls) / len(recalls), 3) if recalls else None
    summary = {
        "stage": "block_value_grounding",
        "schema_version": 1,
        "source_result_json": str(rp),
        "blocks_total": n_total,
        "image_blocks": n_image,
        "blocks_vector_grounded": n_vector,
        "blocks_with_corrections": n_corrected,
        "document_concrete_classes": sorted(doc_classes),
        "corrections_by_field": dict(field_counter),
        "gemma_avg_number_recall_vs_vector": avg_recall,
        "blocks": blocks_out,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / SUMMARY_NAME).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["status"] = "done"
    summary["qwen_candidates"] = qwen_candidates  # для Phase 2 (в файл не пишем)
    return summary


def _augment_summary_with_qwen(output_dir: Path, qwen_results: list) -> None:
    """Дописать результаты Phase 2 (qwen) в block_grounding_summary.json."""
    p = output_dir / SUMMARY_NAME
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    by_id = {b.get("block_id"): b for b in data.get("blocks", [])}
    n_filled = 0
    for r in qwen_results:
        if r.get("error") or not r.get("values"):
            continue
        n_filled += 1
        rec = by_id.get(r["block_id"])
        gv = {"qwen_values": r["values"], "qwen_tiles": r.get("tiles")}
        if rec is None:
            data.setdefault("blocks", []).append({
                "block_id": r["block_id"], "block_type": "image",
                "value_source": r["source"], "value_confidence": "medium",
                "grounded_values": gv, "corrections": [],
            })
        else:
            rec["value_source"] = r["source"]
            rec.setdefault("grounded_values", {}).update(gv)
    data["qwen_phase2"] = {
        "blocks_attempted": len(qwen_results),
        "blocks_filled": n_filled,
        "errors": sum(1 for r in qwen_results if r.get("error")),
    }
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


async def run_block_grounding_stage(ctx: "PipelineStageContext", *, force: bool = False) -> StageResult:
    """Стадия Value Grounding. OFF по умолчанию (флаг) → no-op skip."""
    import asyncio
    from backend.app.core.config import BLOCK_VALUE_GROUNDING_ENABLED
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import gemma_output_root

    if not BLOCK_VALUE_GROUNDING_ENABLED:
        return StageResult.ok(skipped=True, status="disabled")

    pid = ctx.project_id
    project_dir = ctx.project_dir
    output_dir = getattr(ctx, "output_dir", None) or gemma_output_root(project_dir)
    ctx.update_pipeline_log("block_value_grounding", "running")
    await ctx.log(f"═══ {STAGE_LABEL} ═══")

    try:
        summary = await asyncio.to_thread(run_block_grounding, project_dir, Path(output_dir))
    except Exception as exc:  # fail-soft: усиление не должно ронять пайплайн
        msg = f"{STAGE_LABEL}: {exc}"
        await ctx.log(msg, "warn")
        ctx.update_pipeline_log("block_value_grounding", "error", error=msg)
        return StageResult.ok(skipped=True, status="error_soft")

    if summary.get("status") in {"no_result_json", "result_json_error"}:
        await ctx.log(f"  {STAGE_LABEL}: нет result.json/вектор-слоя — пропуск", "info")
        ctx.update_pipeline_log("block_value_grounding", "done",
                                message="нет вектор-данных", detail={"status": summary.get("status")})
        return StageResult.ok(skipped=True, status=summary.get("status"))

    msg = (f"grounded {summary['blocks_vector_grounded']}/{summary['blocks_total']} блоков, "
           f"корректировок в {summary['blocks_with_corrections']} "
           f"({summary.get('corrections_by_field') or {}})")
    await ctx.log(f"  ✓ {msg}")

    # ── Phase 2 (qwen-тайлинг для КРУПНЫХ no-vector блоков) — gated, дорого/ngrok ──
    qwen_filled = 0
    from backend.app.core.config import (
        BLOCK_VALUE_GROUNDING_QWEN_ENABLED, BLOCK_VALUE_GROUNDING_QWEN_MIN_WIDTH,
        BLOCK_VALUE_GROUNDING_QWEN_CROP_MIN_WIDTH, BLOCK_VALUE_GROUNDING_QWEN_MAX_BLOCKS,
        BLOCK_VALUE_GROUNDING_QWEN_MODEL,
    )
    selected = _select_qwen_blocks(
        summary.get("qwen_candidates") or [],
        tile_min=BLOCK_VALUE_GROUNDING_QWEN_MIN_WIDTH,
        crop_min=BLOCK_VALUE_GROUNDING_QWEN_CROP_MIN_WIDTH,
        max_blocks=BLOCK_VALUE_GROUNDING_QWEN_MAX_BLOCKS)
    if BLOCK_VALUE_GROUNDING_QWEN_ENABLED and selected:
        try:
            from backend.app.pipeline.stages.block_grounding.qwen_grounding import run_qwen_grounding
            pdf = _find_source_pdf(project_dir)
            if pdf is None:
                await ctx.log("  Phase 2: исходный PDF не найден — пропуск qwen", "warn")
            else:
                n_tiled = sum(1 for c in selected if c.get("mode") == "tiled")
                n_crop = len(selected) - n_tiled
                await ctx.log(f"  Phase 2 (qwen): {len(selected)} no-vector блоков "
                              f"(тайлинг {n_tiled}, кроп {n_crop})")
                render_dir = Path(output_dir) / "block_grounding_qwen_tiles"
                # Qwen остаётся opt-in и сериализуется своим независимым lock.
                async with _get_qwen_lock():
                    qres = await run_qwen_grounding(
                        selected, pdf, model=BLOCK_VALUE_GROUNDING_QWEN_MODEL,
                        max_blocks=BLOCK_VALUE_GROUNDING_QWEN_MAX_BLOCKS, render_dir=render_dir)
                await asyncio.to_thread(_augment_summary_with_qwen, Path(output_dir), qres)
                qwen_filled = sum(1 for r in qres if r.get("values"))
                await ctx.log(f"  ✓ Phase 2: заполнено {qwen_filled}/{len(qres)} блоков qwen-значениями")
        except Exception as exc:  # fail-soft
            await ctx.log(f"  Phase 2 (qwen) пропущен (soft): {exc}", "warn")

    ctx.update_pipeline_log("block_value_grounding", "done", message=msg, detail={
        "blocks_total": summary["blocks_total"],
        "blocks_vector_grounded": summary["blocks_vector_grounded"],
        "blocks_with_corrections": summary["blocks_with_corrections"],
        "qwen_blocks_filled": qwen_filled,
    })
    return StageResult.ok(status="done", **{
        "blocks_total": summary["blocks_total"],
        "blocks_corrected": summary["blocks_with_corrections"],
        "qwen_blocks_filled": qwen_filled,
    })
