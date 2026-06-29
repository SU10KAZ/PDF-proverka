"""Вариант A: бесплатный локальный фильтр кандидатов калиброванным промптом.

Перегоняет ТОЛЬКО кандидатов (graphic из visionB2_*.json + text из visionTEXT_*.json)
калиброванной qwen3.6-35b с правилом «инфо в общих указаниях/др.листах/принято экспертизой
→ cannot_tell». Сжимает 1039 в чистый шорт-лист. 0 токенов подписки (локально).

  python -m experiments.evidence_agent_v2.filter_candidates
"""
from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path

from .audit_rejected import iter_alia_rejected
from .highres_recheck import recheck_one_async
from .text_recheck import recheck_text_one

OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"
MODEL = "qwen/qwen3.6-35b-a3b"


def _load_candidates(glob_pat: str) -> list:
    keys = []
    for f in sorted(OUT_DIR.glob(glob_pat)):
        d = json.loads(f.read_text(encoding="utf-8"))
        for c in d.get("candidates", []):
            keys.append((c["discipline"], c["document"], c["version"], c["item_id"]))
    return keys


def _rec_lookup(disciplines):
    lut = {}
    for disc in disciplines:
        for r in iter_alia_rejected(disc):
            lut[(r.discipline, r.document, r.version, r.item_id)] = r
    return lut


async def main_async() -> int:
    from . import ngrok_guard

    g_keys = _load_candidates("visionB2_*.json")
    t_keys = _load_candidates("visionTEXT_*.json")
    disciplines = sorted({k[0] for k in g_keys + t_keys})
    print(f"[filter] graphic кандидатов: {len(g_keys)} | text: {len(t_keys)} | дисц: {disciplines}", flush=True)
    lut = _rec_lookup(disciplines)

    ngrok_guard.preflight(require_idle=False)
    g_results, t_results = [], []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="filter_candidates"):
        # 1) графика — калиброванный high-res image-only
        t0 = time.time()
        for i, k in enumerate(g_keys, 1):
            r = lut.get(k)
            if not r:
                continue
            try:
                res = await recheck_one_async(r, model=MODEL)
            except Exception as exc:
                res = {"item_id": k[3], "document": k[1], "error": str(exc)[:120]}
            if res:
                res["kind"] = "graphic"
                g_results.append(res)
            if i % 25 == 0:
                kept = sum(1 for x in g_results if x.get("still_expert_wrong"))
                print(f"  graphic …{i}/{len(g_keys)} ({time.time()-t0:.0f}с) выжило={kept}", flush=True)
        # 2) текст — калиброванный текстовый промпт
        t1 = time.time()
        for i, k in enumerate(t_keys, 1):
            r = lut.get(k)
            if not r:
                continue
            try:
                res = await recheck_text_one(r, model=MODEL)
            except Exception as exc:
                res = {"item_id": k[3], "document": k[1], "error": str(exc)[:120]}
            if res:
                res["kind"] = "text"
                t_results.append(res)
            if i % 25 == 0:
                kept = sum(1 for x in t_results if x.get("expert_maybe_wrong"))
                print(f"  text …{i}/{len(t_keys)} ({time.time()-t1:.0f}с) выжило={kept}", flush=True)

    g_surv = [x for x in g_results if x.get("still_expert_wrong")]
    t_surv = [x for x in t_results if x.get("expert_maybe_wrong")]
    out = OUT_DIR / "filter_candidates_result.json"
    out.write_text(json.dumps({
        "model": MODEL,
        "graphic_in": len(g_keys), "graphic_survived": len(g_surv),
        "text_in": len(t_keys), "text_survived": len(t_surv),
        "graphic_verdicts": dict(Counter(x.get("highres_verdict") for x in g_results if "highres_verdict" in x)),
        "text_verdicts": dict(Counter(x.get("expert_wrong_verdict") for x in t_results if "expert_wrong_verdict" in x)),
        "graphic_survivors": g_surv, "text_survivors": t_surv,
        "graphic_all": g_results, "text_all": t_results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[filter] ГРАФИКА: было {len(g_keys)} → выжило {len(g_surv)} "
          f"(снято {len(g_keys)-len(g_surv)})")
    print(f"[filter] ТЕКСТ: было {len(t_keys)} → выжило {len(t_surv)} "
          f"(снято {len(t_keys)-len(t_surv)})")
    print(f"[filter] ИТОГО шорт-лист: {len(g_surv)+len(t_surv)} (было {len(g_keys)+len(t_keys)})")
    print(f"[filter] отчёт: {out}")
    return 0


if __name__ == "__main__":
    import asyncio, sys
    ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    raise SystemExit(asyncio.run(main_async()))
