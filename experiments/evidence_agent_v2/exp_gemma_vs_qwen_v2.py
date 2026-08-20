"""Тест 3 расширенный: gemma vs qwen high-res по двум категориям.
  general     — обычные CAD-блоки (плотная статистика, 100 шт)
  single_line — БОЛЬШИЕ однолинейные электросхемы (EOM, 15 шт): проверка, ломается ли
                одиночный high-res рендер на огромных листах (нужен ли тайлинг)
Эталон везде — pdfplumber (вектор). Один блок = честный head-to-head.
"""
from __future__ import annotations
import glob, json, re, time, sys
from pathlib import Path
from collections import defaultdict
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from experiments.evidence_agent_v2.highres_recheck import render_block_highres
from experiments.evidence_agent_v2.extract import _parse
from experiments.evidence_agent_v2 import ngrok_guard

BASE = ROOT / "projects_v2" / "objects" / "214_Alia_ASTERUS" / "disciplines"
RENDER = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/"
              "dbee36d1-1d74-4bf6-9212-08c364fd8b0f/scratchpad/exp_v2")
MODEL = "qwen/qwen3.6-35b-a3b"
N_GEN, N_SL = 100, 15
SL = re.compile(r"однолинейн|ВРУ|ГРЩ|вводно-распред|автоматическ.{0,10}выключател|расчётн.{0,5}ток|кВт|питающ|щит", re.I)
_PROMPT = ("Это фрагмент строительного чертежа. Перечисли ВСЕ видимые числа, размеры, отметки, "
           "марки, классы, коды — дословно. Ответь ТОЛЬКО JSON: {\"values\": [\"...\"]}")


def nums(s):
    return set(re.findall(r"\d{2,}", (s or "").replace(" ", "").replace(",", ".")))


def collect():
    gen, sl = [], []
    for rp in glob.glob(str(BASE / "*/documents/*/versions/*/02_work/result.json")):
        vdir = Path(rp).parent.parent
        pdf = vdir / "02_work" / "document.pdf"
        if not pdf.is_file():
            continue
        disc = Path(rp).parts[Path(rp).parts.index("disciplines") + 1]
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
                pp = (b.get("pdfplumber_text") or "").strip()
                g = (b.get("ocr_text") or "").strip()
                c = b["coords_px"]; w = c[2] - c[0]; h = c[3] - c[1]
                rec = {"pdf": pdf, "page": pn, "coords": c, "ppx": (pw, ph),
                       "pp": pp, "gemma": g, "id": b.get("id"), "disc": disc, "w": w, "h": h}
                if len(pp) > 40 and len(nums(pp)) >= 5 and g:
                    if w < 3000 and h < 3000:
                        gen.append(rec)
                    elif disc == "EOM" and w > 1500 and h > 1000 and SL.search(g) and len(pp) > 200:
                        sl.append(rec)
        if len(gen) > 800 and len(sl) > 60:
            break
    return gen, sl


async def run_cat(name, items, describe):
    rows = []
    for i, c in enumerate(items, 1):
        gt = nums(c["pp"]); gemma_n = nums(c["gemma"])
        png = render_block_highres(c["pdf"], c["page"], c["coords"], c["ppx"],
                                   long_side=2200, out_path=RENDER / f"{name}{i}.png")
        qn = set()
        if png:
            try:
                r = await describe(png, _PROMPT, model=MODEL)
                obj = _parse(r.full_raw_response or r.raw_response_excerpt or "") or {}
                vals = obj.get("values", []) if isinstance(obj, dict) else []
                qn = nums(" ".join(map(str, vals)))
            except Exception:
                pass
        rows.append({"gt": len(gt), "gm": len(gt & gemma_n), "qw": len(gt & qn),
                     "w": c["w"], "h": c["h"]})
        if i % 10 == 0:
            print(f"  [{name}] …{i}/{len(items)}", flush=True)
    return rows


async def main():
    from backend.app.services.common.local_vision_provider import describe_image_local
    gen, sl = collect()
    # ровная выборка general по дисциплинам
    by = defaultdict(list)
    for c in gen:
        by[c["disc"]].append(c)
    gsel = []
    while len(gsel) < N_GEN and any(by.values()):
        for dsc in list(by):
            if by[dsc] and len(gsel) < N_GEN:
                gsel.append(by[dsc].pop())
    ssel = sl[:N_SL]
    print(f"[exp2] general={len(gsel)} (из {len(gen)}), single_line={len(ssel)} (из {len(sl)})", flush=True)

    ngrok_guard.preflight(require_idle=False)
    with ngrok_guard.LocalLLMLock(owner="ev2", note="exp2"):
        t0 = time.time()
        gen_rows = await run_cat("gen", gsel, describe_image_local)
        sl_rows = await run_cat("sl", ssel, describe_image_local)
        print(f"  всего {time.time()-t0:.0f}с", flush=True)

    def summ(rows):
        gt = sum(r["gt"] for r in rows); gm = sum(r["gm"] for r in rows); qw = sum(r["qw"] for r in rows)
        return gt, gm, qw, gm / max(1, gt), qw / max(1, gt)
    out = ROOT / "experiments/evidence_agent_v2/results/audit_alia/exp_gemma_vs_qwen_v2.json"
    res = {}
    for name, rows in (("general", gen_rows), ("single_line", sl_rows)):
        gt, gm, qw, gr, qr = summ(rows)
        avg_w = sum(r["w"] for r in rows) // max(1, len(rows))
        res[name] = {"n": len(rows), "gt": gt, "gemma_recall": round(gr, 3),
                     "qwen_recall": round(qr, 3), "avg_width_px": avg_w}
        print(f"\n=== {name} ({len(rows)} блоков, ср.ширина {avg_w}px) ===")
        print(f"  эталон чисел: {gt}")
        print(f"  gemma recall: {gr*100:.0f}%   qwen recall: {qr*100:.0f}%")
    out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[exp2] отчёт: {out}")


if __name__ == "__main__":
    import asyncio
    raise SystemExit(asyncio.run(main()))
