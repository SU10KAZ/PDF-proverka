"""Исследование: как поднять qwen до ~100% на больших однолинейных схемах через ТАЙЛИНГ.
Сравнение: qwen single-render (0%) vs qwen+тайлинг vs pdfplumber (100%). Эталон — pdfplumber.
"""
from __future__ import annotations
import glob, json, re, math, sys, time
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
              "dbee36d1-1d74-4bf6-9212-08c364fd8b0f/scratchpad/exp_tile")
MODEL = "qwen/qwen3.6-35b-a3b"
TRUE = re.compile(r"однолинейн|схема\s+(ВРУ|ГРЩ|щит|электроснабж)|вводно-распределительн|расчётн\w*\s+ток|автоматическ\w+\s+выключател", re.I)
_PROMPT = ("Это фрагмент однолинейной электросхемы. Перечисли ВСЕ видимые числа, токи (А), "
           "мощности (кВт), марки автоматов/кабелей, обозначения — дословно. "
           "Ответь ТОЛЬКО JSON: {\"values\": [\"...\"]}")
TILE = 2600        # размер тайла в px страницы
OVERLAP = 350
MAX_TILES = 48


def nums(s):
    return set(re.findall(r"\d{2,}", (s or "").replace(" ", "").replace(",", ".")))


def grid(coords):
    x0, y0, x1, y1 = coords
    xs = list(range(int(x0), int(x1), TILE - OVERLAP)) or [int(x0)]
    ys = list(range(int(y0), int(y1), TILE - OVERLAP)) or [int(y0)]
    tiles = []
    for tx in xs:
        for ty in ys:
            tiles.append([tx, ty, min(tx + TILE, x1), min(ty + TILE, y1)])
    return tiles[:MAX_TILES]


def collect(n=2):
    out = []
    for rp in glob.glob(str(BASE / "*/documents/*/versions/*/02_work/result.json")):
        vdir = Path(rp).parent.parent
        pdf = vdir / "02_work" / "document.pdf"
        if not pdf.is_file():
            continue
        disc = Path(rp).parts[Path(rp).parts.index("disciplines") + 1]
        if disc != "EOM":
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
                if TRUE.search(g) and w > 12000 and len(pp) > 1500:
                    out.append({"pdf": pdf, "page": pn, "coords": c, "ppx": (pw, ph),
                                "pp": pp, "gemma": g, "id": b.get("id"),
                                "doc": Path(rp).parts[Path(rp).parts.index("documents") + 1], "w": w})
                    if len(out) >= n:
                        return out
    return out


async def main():
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local
    items = collect(2)
    print(f"[tile] схем к тесту: {len(items)}", flush=True)
    ngrok_guard.preflight(require_idle=False)
    rows = []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="exp_tiling"):
        for k, c in enumerate(items, 1):
            gt = nums(c["pp"]); gemma_n = nums(c["gemma"])
            # baseline single-render
            png = render_block_highres(c["pdf"], c["page"], c["coords"], c["ppx"], long_side=2200, out_path=RENDER / f"s{k}.png")
            single = set()
            if png:
                try:
                    r = await describe_image_local(png, _PROMPT, model=MODEL)
                    obj = _parse(r.full_raw_response or r.raw_response_excerpt or "") or {}
                    single = nums(" ".join(map(str, obj.get("values", []) if isinstance(obj, dict) else [])))
                except Exception:
                    pass
            # tiled
            tiles = grid(c["coords"])
            tiled = set()
            t0 = time.time()
            for ti, tc in enumerate(tiles):
                tp = render_block_highres(c["pdf"], c["page"], tc, c["ppx"], long_side=1700, out_path=RENDER / f"t{k}_{ti}.png")
                if not tp:
                    continue
                try:
                    r = await describe_image_local(tp, _PROMPT, model=MODEL)
                    obj = _parse(r.full_raw_response or r.raw_response_excerpt or "") or {}
                    tiled |= nums(" ".join(map(str, obj.get("values", []) if isinstance(obj, dict) else [])))
                except Exception:
                    pass
            row = {"doc": c["doc"], "w": c["w"], "gt": len(gt), "tiles": len(tiles),
                   "gemma": round(len(gt & gemma_n) / max(1, len(gt)), 2),
                   "single": round(len(gt & single) / max(1, len(gt)), 2),
                   "tiled": round(len(gt & tiled) / max(1, len(gt)), 2),
                   "sec": round(time.time() - t0)}
            rows.append(row)
            print(f"  схема{k} {c['doc']} {c['w']}px эталон={len(gt)} | gemma={row['gemma']} "
                  f"single={row['single']} ТАЙЛИНГ={row['tiled']} ({row['tiles']} тайлов, {row['sec']}с)", flush=True)
    out = ROOT / "experiments/evidence_agent_v2/results/audit_alia/exp_tiling.json"
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    gt = sum(r["gt"] for r in rows)
    print(f"\n=== ИТОГ: тайлинг на больших однолинейных ({len(rows)} схем) ===")
    for m in ("gemma", "single", "tiled"):
        v = sum(r["gt"] * r[m] for r in rows) / max(1, gt)
        print(f"  {m:8}: {v*100:.0f}%")
    print(f"  pdfplumber: 100%")


if __name__ == "__main__":
    import asyncio
    raise SystemExit(asyncio.run(main()))
