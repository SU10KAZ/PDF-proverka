"""Тест 3C: НАСТОЯЩИЕ большие однолинейные электросхемы (ВРУ/ГРЩ).
gemma vs qwen single-render vs pdfplumber (вектор = эталон). Гипотеза: на гигантских схемах
одиночный рендер ужимает текст до нечитаемого → qwen ≈ 0; pdfplumber имеет всё."""
from __future__ import annotations
import glob, json, re, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from experiments.evidence_agent_v2.highres_recheck import render_block_highres
from experiments.evidence_agent_v2.extract import _parse
from experiments.evidence_agent_v2 import ngrok_guard

BASE = ROOT / "projects_v2" / "objects" / "214_Alia_ASTERUS" / "disciplines"
RENDER = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/"
              "dbee36d1-1d74-4bf6-9212-08c364fd8b0f/scratchpad/exp_sl")
MODEL = "qwen/qwen3.6-35b-a3b"
TRUE = re.compile(r"однолинейн|схема\s+(ВРУ|ГРЩ|щит|электроснабж)|вводно-распределительн|расчётн\w*\s+ток|автоматическ\w+\s+выключател", re.I)
_PROMPT = ("Это однолинейная электрическая схема. Перечисли ВСЕ видимые числа, токи, мощности, "
           "марки автоматов/кабелей, обозначения. Ответь ТОЛЬКО JSON: {\"values\": [\"...\"]}")


def nums(s):
    return set(re.findall(r"\d{2,}", (s or "").replace(" ", "").replace(",", ".")))


def collect(n=8):
    out = []
    for rp in glob.glob(str(BASE / "*/documents/*/versions/*/02_work/result.json")):
        vdir = Path(rp).parent.parent
        pdf = vdir / "02_work" / "document.pdf"
        if not pdf.is_file():
            continue
        disc = Path(rp).parts[Path(rp).parts.index("disciplines") + 1]
        if disc not in ("EOM", "SS"):
            continue
        try:
            d = json.load(open(rp, encoding="utf-8"))
        except Exception:
            continue
        for p in d.get("pages", []):
            pw, ph, pn = p.get("width"), p.get("height"), p.get("page_number")
            if not (pw and ph and pn):
                continue
            for b in p.get("blocks", []):
                if b.get("block_type") != "image" or not b.get("coords_px"):
                    continue
                g = (b.get("ocr_text") or ""); pp = (b.get("pdfplumber_text") or "").strip()
                c = b["coords_px"]; w = c[2] - c[0]
                if TRUE.search(g) and w > 8000 and len(nums(pp)) > 200:
                    out.append({"pdf": pdf, "page": pn, "coords": c, "ppx": (pw, ph),
                                "pp": pp, "gemma": g, "id": b.get("id"), "doc": Path(rp).parts[Path(rp).parts.index("documents")+1], "w": w})
                    if len(out) >= n:
                        return out
    return out


async def main():
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local
    items = collect(8)
    print(f"[SL] настоящих однолинейных к тесту: {len(items)}", flush=True)
    ngrok_guard.preflight(require_idle=False)
    rows = []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="exp_sl"):
        for i, c in enumerate(items, 1):
            gt = nums(c["pp"]); gemma_n = nums(c["gemma"])
            png = render_block_highres(c["pdf"], c["page"], c["coords"], c["ppx"],
                                       long_side=2200, out_path=RENDER / f"sl{i}.png")
            qn = set()
            if png:
                try:
                    r = await describe_image_local(png, _PROMPT, model=MODEL)
                    obj = _parse(r.full_raw_response or r.raw_response_excerpt or "") or {}
                    vals = obj.get("values", []) if isinstance(obj, dict) else []
                    qn = nums(" ".join(map(str, vals)))
                except Exception:
                    pass
            rows.append({"doc": c["doc"], "id": c["id"], "w": c["w"], "gt": len(gt),
                         "gemma_recall": round(len(gt & gemma_n) / max(1, len(gt)), 2),
                         "qwen_recall": round(len(gt & qn) / max(1, len(gt)), 2)})
            print(f"  sl{i} {c['doc']} {c['w']}px: эталон={len(gt)} gemma={rows[-1]['gemma_recall']} qwen={rows[-1]['qwen_recall']}", flush=True)
    gt = sum(r["gt"] for r in rows)
    gm = sum(r["gt"] * r["gemma_recall"] for r in rows)
    qw = sum(r["gt"] * r["qwen_recall"] for r in rows)
    out = ROOT / "experiments/evidence_agent_v2/results/audit_alia/exp_single_line.json"
    out.write_text(json.dumps({"n": len(rows), "gt": gt, "gemma_recall": round(gm/max(1,gt),3),
                               "qwen_recall": round(qw/max(1,gt),3),
                               "pdfplumber_recall": 1.0, "rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n=== ТЕСТ 3C: большие однолинейные ({len(rows)} схем, ~{sum(r['w'] for r in rows)//max(1,len(rows))}px) ===")
    print(f"эталон чисел: {gt}")
    print(f"gemma recall:      {gm/max(1,gt)*100:.0f}%")
    print(f"qwen recall:       {qw/max(1,gt)*100:.0f}%  (одиночный рендер, ужатие → текст теряется)")
    print(f"pdfplumber recall: 100%  (вектор-слой = эталон, всё на месте — OCR не нужен)")
    print(f"[SL] отчёт: {out}")


if __name__ == "__main__":
    import asyncio
    raise SystemExit(asyncio.run(main()))
