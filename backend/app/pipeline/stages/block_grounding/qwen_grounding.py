"""Phase 2 — qwen тайлинг/точечный кроп для блоков БЕЗ вектор-слоя.

Доказано (experiments/.../exp_tiling.py): на больших схемах одиночный рендер ужимает текст
до нечитаемого (qwen 0%), а тайлинг (читаемые куски + merge) даёт ~90%. Здесь — production-порт:
рендер региона из PDF (fitz) → qwen по тайлам/кропу → объединение значений.

Дорого и ngrok-bound → вызывается только под флагом BLOCK_VALUE_GROUNDING_QWEN_ENABLED,
ТОЛЬКО для крупных no-vector блоков, с жёстким cap и отдельной сериализацией.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

_PROMPT = (
    "Это фрагмент строительного чертежа/схемы. Перечисли ВСЕ видимые числа, размеры, отметки, "
    "токи, мощности, марки автоматов/кабелей, классы, коды — дословно, как написано. "
    "Ответь ТОЛЬКО JSON: {\"values\": [\"...\"]}"
)

# Параметры тайлинга (px страницы); подтверждены экспериментом 0%→90%.
TILE_PX = 2600
TILE_OVERLAP = 350
MAX_TILES = 48
TILE_LONG_SIDE = 1700
CROP_LONG_SIDE = 2200


def _render_region(pdf_path: Path, page_number: int, coords_px, page_px,
                   *, long_side: int, out_path: Path) -> Optional[Path]:
    """Чёткий рендер прямоугольной области страницы из векторного PDF."""
    import fitz
    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        idx = max(0, int(page_number) - 1)
        if idx >= doc.page_count:
            return None
        page = doc[idx]
        w_pt, h_pt = page.rect.width, page.rect.height
        pw, ph = page_px
        if not pw or not ph:
            return None
        x0, y0, x1, y1 = coords_px
        clip = fitz.Rect(x0 / pw * w_pt, y0 / ph * h_pt, x1 / pw * w_pt, y1 / ph * h_pt)
        if clip.width < 1 or clip.height < 1:
            return None
        zoom = max(1.0, min(long_side / max(clip.width, clip.height), 12.0))
        pix = page.get_pixmap(clip=clip, matrix=fitz.Matrix(zoom, zoom))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(out_path))
        return out_path
    except Exception:
        return None
    finally:
        doc.close()


def tile_grid(coords_px, *, tile: int = TILE_PX, overlap: int = TILE_OVERLAP,
              max_tiles: int = MAX_TILES) -> list:
    """Сетка перекрывающихся тайлов внутри bbox блока (px страницы)."""
    x0, y0, x1, y1 = [int(v) for v in coords_px]
    step = max(1, tile - overlap)
    xs = list(range(x0, x1, step)) or [x0]
    ys = list(range(y0, y1, step)) or [y0]
    tiles = []
    for tx in xs:
        for ty in ys:
            tiles.append([tx, ty, min(tx + tile, x1), min(ty + tile, y1)])
    return tiles[:max_tiles]


def _extract_values(res) -> list:
    """Достать список значений из ответа qwen (parsed → fallback regex)."""
    if res is None:
        return []
    p = getattr(res, "parsed", None)
    if isinstance(p, dict) and isinstance(p.get("values"), list):
        return [str(v) for v in p["values"] if str(v).strip()]
    raw = getattr(res, "full_raw_response", "") or getattr(res, "raw_response_excerpt", "") or ""
    m = re.search(r'"values"\s*:\s*\[(.*?)\]', raw, re.DOTALL)
    if m:
        try:
            return [str(v) for v in json.loads("[" + m.group(1) + "]") if str(v).strip()]
        except Exception:
            return re.findall(r'"([^"]{1,40})"', m.group(1))
    return []


async def qwen_read_block(pdf_path: Path, page: int, coords_px, page_px, *,
                          model: str, mode: str, render_dir: Path) -> dict:
    """Прочитать блок qwen: mode='tiled' (большой) или 'crop' (точечный)."""
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local

    if mode == "tiled":
        tiles = tile_grid(coords_px)
        vals: set = set()
        ok_tiles = 0
        for i, tc in enumerate(tiles):
            png = _render_region(pdf_path, page, tc, page_px,
                                 long_side=TILE_LONG_SIDE, out_path=render_dir / f"t{i}.png")
            if not png:
                continue
            res = await describe_image_local(png, _PROMPT, model=model)
            v = _extract_values(res)
            if v:
                ok_tiles += 1
                vals |= set(v)
        return {"source": "qwen_tiled", "tiles": len(tiles), "ok_tiles": ok_tiles,
                "values": sorted(vals)}

    png = _render_region(pdf_path, page, coords_px, page_px,
                         long_side=CROP_LONG_SIDE, out_path=render_dir / "crop.png")
    if not png:
        return {"source": "qwen_crop", "tiles": 0, "values": []}
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local
    res = await describe_image_local(png, _PROMPT, model=model)
    return {"source": "qwen_crop", "tiles": 1, "values": sorted(set(_extract_values(res)))}


async def run_qwen_grounding(candidates: list, pdf_path: Path, *, model: str,
                             max_blocks: int, render_dir: Path,
                             progress_cb=None) -> list:
    """Прогнать qwen по списку кандидатов (уже отфильтрованных/отсортированных). Cap = max_blocks."""
    results = []
    capped = candidates[:max_blocks]
    for i, c in enumerate(capped, 1):
        try:
            r = await qwen_read_block(
                pdf_path, c["page"], c["coords_px"], c["page_px"],
                model=model, mode=c.get("mode", "tiled"),
                render_dir=render_dir / str(c["block_id"]))
            results.append({"block_id": c["block_id"], "width": c.get("width"), **r})
        except Exception as exc:
            results.append({"block_id": c["block_id"], "error": str(exc)[:160]})
        if progress_cb:
            try:
                await progress_cb(i, len(capped), results[-1])
            except Exception:
                pass
    return results
