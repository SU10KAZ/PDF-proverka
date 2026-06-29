"""Тест 3 Track A: gemma vs qwen high-res — recall числовых токенов против pdfplumber-эталона.

Берём image-блоки, у которых ЕСТЬ векторный текст-слой (pdfplumber = независимый эталон).
Для каждого: gemma OCR (готовый ocr_text), qwen на high-res рендере из PDF (транскрипция значений).
Считаем, кто полнее/точнее ловит числа эталона. Один блок = честный head-to-head.
"""
from __future__ import annotations

import glob
import json
import re
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from experiments.evidence_agent_v2.highres_recheck import render_block_highres
from experiments.evidence_agent_v2.extract import _parse
from experiments.evidence_agent_v2 import ngrok_guard

BASE = ROOT / "projects_v2" / "objects" / "214_Alia_ASTERUS" / "disciplines"
RENDER = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/"
              "dbee36d1-1d74-4bf6-9212-08c364fd8b0f/scratchpad/exp_blocks")
N = 30
MODEL = "qwen/qwen3.6-35b-a3b"

_PROMPT = (
    "Это фрагмент строительного чертежа. Перечисли ВСЕ видимые на нём ЧИСЛА, размеры, "
    "отметки, марки, коды, классы — дословно, как написано. Не интерпретируй, просто считай. "
    "Ответь ТОЛЬКО JSON: {\"values\": [\"...\", \"...\"]}"
)


def nums(s):
    return set(re.findall(r"\d{2,}", (s or "").replace(" ", "").replace(",", ".")))


def _collect():
    cands = []
    for rp in glob.glob(str(BASE / "*/documents/*/versions/*/02_work/result.json")):
        vdir = Path(rp).parent.parent
        pdf = vdir / "02_work" / "document.pdf"
        if not pdf.is_file():
            continue
        try:
            d = json.load(open(rp, encoding="utf-8"))
        except Exception:
            continue
        disc = Path(rp).parts[Path(rp).parts.index("disciplines") + 1]
        for p in d.get("pages", []):
            pw, ph, pn = p.get("width"), p.get("height"), p.get("page_number")
            if not (pw and ph and pn):
                continue
            for b in p.get("blocks", []):
                pp = (b.get("pdfplumber_text") or "").strip()
                g = (b.get("ocr_text") or "").strip()
                if (b.get("block_type") == "image" and len(pp) > 40
                        and len(nums(pp)) >= 5 and b.get("coords_px") and g):
                    cands.append({"disc": disc, "pdf": pdf, "page": pn, "coords": b["coords_px"],
                                  "ppx": (pw, ph), "pp": pp, "gemma": g, "id": b.get("id")})
        if len(cands) > 600:
            break
    return cands


async def main_async():
    cands = _collect()
    # ровная выборка по дисциплинам
    by = {}
    for c in cands:
        by.setdefault(c["disc"], []).append(c)
    sample = []
    i = 0
    while len(sample) < N and any(by.values()):
        for disc in list(by):
            if by[disc]:
                sample.append(by[disc].pop())
                if len(sample) >= N:
                    break
        i += 1
        if i > 1000:
            break
    print(f"[exp] выборка {len(sample)} блоков из {len(cands)} кандидатов", flush=True)

    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local
    ngrok_guard.preflight(require_idle=False)
    results = []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="exp_gemma_qwen"):
        t0 = time.time()
        for i, c in enumerate(sample, 1):
            gt = nums(c["pp"])
            gemma_n = nums(c["gemma"])
            out = RENDER / f"b{i}.png"
            png = render_block_highres(c["pdf"], c["page"], c["coords"], c["ppx"],
                                       long_side=2200, out_path=out)
            qwen_n = set()
            qraw = ""
            if png:
                try:
                    r = await describe_image_local(png, _PROMPT, model=MODEL)
                    qraw = (r.full_raw_response or r.raw_response_excerpt or "")
                    obj = _parse(qraw) or {}
                    vals = obj.get("values", []) if isinstance(obj, dict) else []
                    qwen_n = nums(" ".join(str(v) for v in vals)) | nums(qraw)
                except Exception as e:
                    qraw = f"ERR {e}"
            rec = {
                "disc": c["disc"], "id": c["id"], "gt": len(gt),
                "gemma_found": len(gt & gemma_n), "qwen_found": len(gt & qwen_n),
                "gemma_recall": round(len(gt & gemma_n) / max(1, len(gt)), 2),
                "qwen_recall": round(len(gt & qwen_n) / max(1, len(gt)), 2),
            }
            results.append(rec)
            if i % 5 == 0:
                print(f"  …{i}/{len(sample)} ({time.time()-t0:.0f}с)", flush=True)

    gtT = sum(r["gt"] for r in results)
    gm = sum(r["gemma_found"] for r in results)
    qw = sum(r["qwen_found"] for r in results)
    out = ROOT / "experiments/evidence_agent_v2/results/audit_alia/exp_gemma_vs_qwen.json"
    out.write_text(json.dumps({"n": len(results), "gt_total": gtT,
                               "gemma_recall": round(gm / max(1, gtT), 3),
                               "qwen_recall": round(qw / max(1, gtT), 3),
                               "per_block": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n=== ТЕСТ 3A: gemma vs qwen (эталон pdfplumber, {len(results)} блоков) ===")
    print(f"чисел эталона всего: {gtT}")
    print(f"gemma recall: {gm/max(1,gtT)*100:.0f}%  ({gm}/{gtT})")
    print(f"qwen  recall: {qw/max(1,gtT)*100:.0f}%  ({qw}/{gtT})")
    won = sum(1 for r in results if r["qwen_recall"] > r["gemma_recall"])
    lost = sum(1 for r in results if r["qwen_recall"] < r["gemma_recall"])
    print(f"qwen лучше на {won} блоках, хуже на {lost}, равно на {len(results)-won-lost}")
    print(f"[exp] отчёт: {out}")


if __name__ == "__main__":
    import asyncio
    raise SystemExit(asyncio.run(main_async()))
