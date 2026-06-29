"""High-res image-only re-check спорных кандидатов аудита.

Идея: на исходном мелком кропе (gemma_100, ~430×700) qwen не различает глифы
(В40 vs В4.0) и опирается на OCR-текст, который мог сам ошибиться. Здесь мы:
  1) рендерим блок ИЗ ЛОКАЛЬНОГО PDF на высоком разрешении (вектор → чётко),
  2) подаём qwen ТОЛЬКО картинку (gemma-OCR НЕ даём),
  3) просим прочитать спорное значение посимвольно с изображения.

Кандидат, у которого вердикт «эксперт ошибся» при этом ОТВАЛИВАЕТСЯ (no/cannot_tell),
скорее всего был OCR-артефактом. Кто остаётся «yes» — надёжнее.

Read-only: PDF только читаем; рендеры — во временную папку.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF

from .audit_rejected import iter_alia_rejected, load_version_finding

_RENDER_DIR = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/"
                   "dbee36d1-1d74-4bf6-9212-08c364fd8b0f/scratchpad/highres_blocks")


def _result_path(output_dir: Path) -> Optional[Path]:
    vd = output_dir.parent.parent  # versions/vNNN
    for p in (vd / "02_work" / "result.json", vd / "01_input" / "result.json"):
        if p.is_file():
            return p
    return None


def _pdf_path(output_dir: Path) -> Optional[Path]:
    vd = output_dir.parent.parent
    for p in (vd / "02_work" / "document.pdf",):
        if p.is_file():
            return p
    cands = list((vd / "01_input").glob("*.pdf"))
    return cands[0] if cands else None


def block_coords_index(output_dir: Path) -> dict:
    """block_id -> (page_number, coords_px[x0,y0,x1,y1], page_w_px, page_h_px)."""
    rp = _result_path(output_dir)
    if not rp:
        return {}
    try:
        res = json.loads(rp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    idx = {}
    for page in res.get("pages", []):
        pw, ph = page.get("width"), page.get("height")
        pn = page.get("page_number")
        for b in page.get("blocks", []):
            bid = b.get("id") or b.get("block_id")
            co = b.get("coords_px")
            if bid and co and pw and ph and pn:
                idx[bid] = (pn, co, pw, ph)
    return idx


def render_block_highres(pdf_path: Path, page_number: int, coords_px, page_px,
                         *, long_side: int, out_path: Path) -> Optional[Path]:
    """Чёткий рендер области блока из векторного PDF на нужный long_side (px)."""
    try:
        doc = fitz.open(str(pdf_path))
        page = doc[page_number - 1]
        W_pt, H_pt = page.rect.width, page.rect.height
        pw, ph = page_px
        x0, y0, x1, y1 = coords_px
        clip = fitz.Rect(x0 / pw * W_pt, y0 / ph * H_pt, x1 / pw * W_pt, y1 / ph * H_pt)
        if clip.width < 1 or clip.height < 1:
            return None
        zoom = long_side / max(clip.width, clip.height)
        zoom = max(1.0, min(zoom, 12.0))
        pix = page.get_pixmap(clip=clip, matrix=fitz.Matrix(zoom, zoom))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(out_path))
        doc.close()
        return out_path
    except Exception:
        return None


def _evidence_block_ids(finding: dict) -> list:
    ids = []
    for e in (finding.get("evidence") or []):
        if isinstance(e, dict) and e.get("block_id"):
            ids.append(e["block_id"])
    for bid in (finding.get("related_block_ids") or []):
        if bid not in ids:
            ids.append(bid)
    return ids


_PROMPT = """Ты эксперт по строительной проектной документации. ИИ выдвинул замечание по чертежу, а ЧЕЛОВЕК-ЭКСПЕРТ его ОТКЛОНИЛ. Проверь, не ошибся ли ЭКСПЕРТ, ВНИМАТЕЛЬНО прочитав значения ПРЯМО С ИЗОБРАЖЕНИЯ.

Это чёткий рендер фрагмента из PDF. НЕ полагайся на чужой OCR — читай символы сам, посимвольно.

Замечание ИИ:
{problem}

Обоснование, по которому ЭКСПЕРТ отклонил замечание:
{reason}

Найди на изображении значение, вокруг которого спор, и прочитай его точно (например класс бетона «В40» или «В4.0»; размер; марку).

Правила:
- expert_wrong="yes" ТОЛЬКО если с изображения ЧЁТКО видно значение/факт, ПРЯМО опровергающий обоснование эксперта.
- Если значение не видно или не уверен в прочтении — expert_wrong="cannot_tell".
- Если прочитанное с картинки СОГЛАСУЕТСЯ с обоснованием эксперта — expert_wrong="no".
- КРИТИЧЕСКИ ВАЖНО: если эксперт ссылается на ОБЩИЕ УКАЗАНИЯ, другие листы/разделы,
  спецификации/ведомости, типовые узлы, или что вопрос ПРИНЯТ ЭКСПЕРТИЗОЙ / является формальным
  некритичным недочётом — а этих данных НЕТ на показанном фрагменте — отвечай "cannot_tell" (НЕ "yes").
  Отсутствие данных на ОДНОМ фрагменте НЕ значит, что эксперт ошибся, если он ссылается на их
  наличие в другом месте. Не объявляй эксперта неправым только потому, что чего-то нет на этом блоке.

Ответь ТОЛЬКО одним JSON:
{{"value_read":"что именно прочитал с картинки","expert_wrong":"yes|no|cannot_tell","explanation":"кратко на русском"}}
"""


def _ordered_block_ids(ctx, finding) -> list:
    """Блоки в порядке релевантности: реранжированные ctx.blocks, затем evidence finding'а."""
    ids = []
    for b in (getattr(ctx, "blocks", None) or []):
        bid = getattr(b, "block_id", None)
        if bid and bid not in ids:
            ids.append(bid)
    for bid in _evidence_block_ids(finding):
        if bid not in ids:
            ids.append(bid)
    return ids


async def recheck_one_async(rec, *, model: str, long_side: int = 2200) -> Optional[dict]:
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local
    from .context import load_context_from_dir
    from .extract import _parse

    finding = load_version_finding(rec.output_dir, rec.item_id)
    if not finding:
        return None
    finding = {**finding, "id": rec.item_id}
    ctx = load_context_from_dir(rec.output_dir, finding, section=rec.discipline)
    idx = block_coords_index(rec.output_dir)
    pdf = _pdf_path(rec.output_dir)

    # 1) лучший блок с координатами → high-res рендер из PDF
    png = None
    used_block = None
    source = "none"
    if pdf:
        for bid in _ordered_block_ids(ctx, finding):
            if bid in idx:
                pn, co, pw, ph = idx[bid]
                out = _RENDER_DIR / f"{rec.document}_{rec.item_id}_{bid}.png"
                png = render_block_highres(pdf, pn, co, (pw, ph), long_side=long_side, out_path=out)
                if png:
                    used_block, source = bid, "highres"
                    break
    # 2) фолбэк: gemma-кроп (тот же image-only промпт, всё равно без OCR-текста)
    if not png and ctx and ctx.primary_png:
        png = Path(ctx.primary_png)
        if png.is_file():
            used_block, source = "primary_png", "gemma_fallback"
        else:
            png = None
    # 3) совсем нет картинки → консервативно cannot_tell (не ошибка)
    if not png:
        return {
            "discipline": rec.discipline, "document": rec.document, "version": rec.version,
            "item_id": rec.item_id, "used_block": None, "source": "no_image",
            "problem": (finding.get("problem") or "")[:200],
            "rejection_reason": rec.rejection_reason[:200],
            "value_read": "", "highres_verdict": "cannot_tell",
            "explanation": "нет рендеримого блока/PDF", "still_expert_wrong": False,
        }

    prompt = _PROMPT.format(
        problem=(finding.get("problem") or finding.get("description") or "")[:600],
        reason=rec.rejection_reason[:600])
    try:
        res = await describe_image_local(png, prompt, model=model)
    except Exception as exc:
        return {"item_id": rec.item_id, "document": rec.document, "error": str(exc)}
    obj = _parse((res.full_raw_response or res.raw_response_excerpt or "").strip()) or {}
    verdict = str(obj.get("expert_wrong", "cannot_tell")).strip().lower()
    quote = str(obj.get("value_read", "")).strip()
    # GUARD: «yes» только при непустом прочтении с картинки
    still = verdict == "yes" and bool(quote)
    return {
        "discipline": rec.discipline, "document": rec.document, "version": rec.version,
        "item_id": rec.item_id, "used_block": used_block, "source": source,
        "png_size": png.stat().st_size if png and png.is_file() else 0,
        "problem": (finding.get("problem") or "")[:200],
        "rejection_reason": rec.rejection_reason[:200],
        "value_read": quote[:160],
        "highres_verdict": verdict,
        "explanation": str(obj.get("explanation", ""))[:240],
        "still_expert_wrong": still,
    }


OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"


def _select_recs(disciplines, mode: str, limit: int = 0) -> list:
    """mode='all' → ВСЕ needs_vision (из triageA.json, как в первом прогоне);
       mode='candidates' → только кандидаты reason-aware."""
    key_set = set()
    if mode == "all":
        tri = json.loads((OUT_DIR / "triageA.json").read_text(encoding="utf-8"))
        for q in tri.get("vision_queue", []):
            if q["discipline"] in disciplines:
                key_set.add((q["document"], q["version"], q["item_id"]))
    else:
        for disc in disciplines:
            f = OUT_DIR / f"visionB_{disc}_reasonaware.json"
            if f.is_file():
                for c in json.loads(f.read_text(encoding="utf-8")).get("candidates", []):
                    key_set.add((c["document"], c["version"], c["item_id"]))
    recs = []
    for disc in disciplines:
        for r in iter_alia_rejected(disc):
            if (r.document, r.version, r.item_id) in key_set:
                recs.append(r)
    if limit:
        recs = recs[:limit]
    return recs


async def _run_async(disciplines, *, model: str, long_side: int, mode: str = "candidates",
                     limit: int = 0) -> int:
    import time
    from collections import Counter
    from . import ngrok_guard

    recs = _select_recs(disciplines, mode, limit)
    print(f"[recheck] mode={mode} к прогону: {len(recs)} ({disciplines})", flush=True)

    ngrok_guard.preflight(require_idle=False)
    results = []
    with ngrok_guard.LocalLLMLock(owner="ev2", note=f"highres_{mode}"):
        t0 = time.time()
        for i, r in enumerate(recs, 1):
            try:
                res = await recheck_one_async(r, model=model, long_side=long_side)
            except Exception as exc:
                res = {"item_id": r.item_id, "document": r.document, "error": str(exc)}
            if res:
                results.append(res)
            if i % 10 == 0:
                kept = sum(1 for x in results if x.get("still_expert_wrong"))
                print(f"  …{i}/{len(recs)} ({time.time()-t0:.0f}с) 'yes'={kept}", flush=True)

    verdicts = Counter(x.get("highres_verdict") for x in results if "highres_verdict" in x)
    sources = Counter(x.get("source") for x in results if x.get("source"))
    errs = sum(1 for x in results if x.get("error"))
    tag = "all" if mode == "all" else "candidates"
    prefix = "visionB2" if mode == "all" else "recheck_highres"
    out = OUT_DIR / f"{prefix}_{'_'.join(disciplines)}.json"
    out.write_text(json.dumps({
        "disciplines": disciplines, "mode": mode, "model": model, "long_side": long_side,
        "rechecked": len(results), "errors": errs,
        "highres_verdicts": dict(verdicts), "sources": dict(sources),
        "candidates": [x for x in results if x.get("still_expert_wrong")],
        "all": results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[recheck] перепроверено {len(results)} | ошибок {errs} | mode={mode}")
    print(f"[recheck] вердикты: {dict(verdicts)} | источники: {dict(sources)}")
    print(f"[recheck] кандидатов 'эксперт ошибся': {verdicts.get('yes',0)}")
    print(f"[recheck] отчёт: {out}")
    return 0


if __name__ == "__main__":
    import argparse, asyncio, sys as _sys
    ROOT = Path(__file__).resolve().parents[2]
    _sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--disciplines", default="KJ,TX")
    ap.add_argument("--mode", choices=("all", "candidates"), default="candidates",
                    help="all = ВСЕ needs_vision; candidates = только кандидаты reason-aware")
    ap.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    ap.add_argument("--long-side", type=int, default=2200)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    raise SystemExit(asyncio.run(_run_async(
        [d.strip() for d in a.disciplines.split(",") if d.strip()],
        model=a.model, long_side=a.long_side, mode=a.mode, limit=a.limit)))
