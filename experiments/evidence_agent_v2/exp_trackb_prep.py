"""Track B: подготовка CAD-блоков (без pdfplumber-эталона) для ручной сверки.
Рендерит high-res + прогоняет qwen-транскрипцию + сохраняет gemma-текст.
Дальше человек (Claude) читает рендеры глазами и судит, кто точнее на КОНКРЕТНЫХ значениях.
"""
from __future__ import annotations
import glob, json, re, time, sys
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
              "dbee36d1-1d74-4bf6-9212-08c364fd8b0f/scratchpad/trackb")
MODEL = "qwen/qwen3.6-35b-a3b"
N = 4
_PROMPT = ("Это фрагмент строительного чертежа. Перечисли ВСЕ видимые числа, размеры, отметки, "
           "марки, классы, коды — дословно. Ответь ТОЛЬКО JSON: {\"values\": [\"...\"]}")

# ищем CAD-блоки БЕЗ текст-слоя, но с содержательным gemma (классы/марки/размеры)
KEYWORD = re.compile(r"бетон|В\d|класс|марк|Кр-|Ф\w|перемычк|анкер|EI|КМ\d|шаг|ø|Ø|d=\d", re.I)
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
    doc = Path(rp).parts[Path(rp).parts.index("documents") + 1]
    for p in d.get("pages", []):
        pw, ph, pn = p.get("width"), p.get("height"), p.get("page_number")
        if not (pw and ph and pn):
            continue
        for b in p.get("blocks", []):
            pp = (b.get("pdfplumber_text") or "").strip()
            g = (b.get("ocr_text") or "").strip()
            if (b.get("block_type") == "image" and len(pp) < 20 and KEYWORD.search(g)
                    and b.get("coords_px") and 400 < (b["coords_px"][2]-b["coords_px"][0]) < 3000):
                cands.append({"disc": disc, "doc": doc, "pdf": pdf, "page": pn,
                              "coords": b["coords_px"], "ppx": (pw, ph), "gemma": g, "id": b.get("id")})
    if len(cands) > 200:
        break

# выборка по разным дисциплинам
by = {}
for c in cands:
    by.setdefault(c["disc"], []).append(c)
sample = []
while len(sample) < N and any(by.values()):
    for disc in list(by):
        if by[disc] and len(sample) < N:
            sample.append(by[disc].pop())

import asyncio
async def go():
    from backend.app.services.common.local_vision_provider import describe_image_local
    ngrok_guard.preflight(require_idle=False)
    out_rows = []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="trackb"):
        for i, c in enumerate(sample, 1):
            png = render_block_highres(c["pdf"], c["page"], c["coords"], c["ppx"],
                                       long_side=2400, out_path=RENDER / f"b{i}.png")
            qvals = []
            if png:
                try:
                    r = await describe_image_local(png, _PROMPT, model=MODEL)
                    obj = _parse(r.full_raw_response or r.raw_response_excerpt or "") or {}
                    qvals = obj.get("values", []) if isinstance(obj, dict) else []
                except Exception as e:
                    qvals = [f"ERR {e}"]
            out_rows.append({"i": i, "disc": c["disc"], "doc": c["doc"], "id": c["id"],
                             "png": str(png), "gemma": c["gemma"][:1500],
                             "qwen_values": qvals})
            print(f"  b{i}: {c['disc']}/{c['doc']} {c['id']} → {png}", flush=True)
    (RENDER / "trackb_data.json").write_text(json.dumps(out_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nГОТОВО: {len(out_rows)} блоков, данные в {RENDER/'trackb_data.json'}")

asyncio.run(go())
