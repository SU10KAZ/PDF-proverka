#!/usr/bin/env python3
"""CLI аудита отклонённых замечаний Алии — «где ошибся эксперт» (read-only).

  # Фаза A — офлайн-триаж всех (без нейросети, можно при работающем Cursor)
  python -m experiments.evidence_agent_v2.run_audit --phase triage

  # Фаза B — vision-пилот одной дисциплины (окно без Cursor)
  python -m experiments.evidence_agent_v2.run_audit --phase vision --discipline TX

  # Фаза C — отчёт
  python -m experiments.evidence_agent_v2.run_audit --phase report
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from experiments.evidence_agent_v2 import audit_rejected as ar

OUT_DIR = ROOT / "experiments" / "evidence_agent_v2" / "results" / "audit_alia"


def phase_triage(discipline=None) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    by_disc_cat = defaultdict(Counter)
    queue = []   # needs_vision
    offline_candidates = []
    t0 = time.time()
    n = 0
    for rec in ar.iter_alia_rejected(discipline):
        row = ar.triage_offline(rec)
        by_disc_cat[row["discipline"]][row["category"]] += 1
        if row["category"] == "needs_vision":
            queue.append({k: row[k] for k in ("discipline", "document", "version", "item_id")})
        elif row["category"] == "offline_accept_candidate":
            offline_candidates.append(row)
        n += 1
        if n % 500 == 0:
            print(f"  …{n} обработано ({time.time()-t0:.0f}с)", flush=True)
    report = {
        "generated_at": datetime.now().isoformat(),
        "object": "214_Alia_ASTERUS",
        "total_rejected": n,
        "by_discipline_category": {d: dict(c) for d, c in by_disc_cat.items()},
        "needs_vision_count": len(queue),
        "offline_accept_candidates": offline_candidates,
        "vision_queue": queue,
    }
    out = OUT_DIR / "triageA.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[триаж] всего отклонённых: {n}")
    for d, c in sorted(by_disc_cat.items()):
        print(f"  {d:5} {dict(c)}")
    print(f"[триаж] needs_vision: {len(queue)} | offline_accept_candidates: {len(offline_candidates)}")
    print(f"[триаж] отчёт: {out}")
    return 0


async def _phase_vision_async(discipline: str, model: str, limit: int, reason_aware: bool = True) -> int:
    from experiments.evidence_agent_v2 import ngrok_guard
    if not discipline:
        print("vision-фаза требует --discipline", file=sys.stderr)
        return 1
    print("[vision] preflight…")
    ngrok_guard.preflight(require_idle=False)

    recs = [r for r in ar.iter_alia_rejected(discipline)]
    print(f"[vision] {discipline}: {len(recs)} отклонённых; триаж → needs_vision…")
    # офлайн-фильтр до vision
    vis_recs = []
    for r in recs:
        row = ar.triage_offline(r)
        if row["category"] == "needs_vision":
            vis_recs.append(r)
    if limit:
        vis_recs = vis_recs[:limit]
    print(f"[vision] к vision-прогону: {len(vis_recs)}")

    results, candidates = [], []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="audit_vision"):
        t0 = time.time()
        for i, r in enumerate(vis_recs, 1):
            try:
                if reason_aware:
                    res = await ar.audit_vision_reasonaware_async(r, model=model)
                else:
                    res = await ar.audit_vision_async(r, model=model, runs=1)
            except Exception as exc:
                res = {"item_id": r.item_id, "document": r.document, "error": str(exc)}
            if res:
                results.append(res)
                if res.get("expert_maybe_wrong"):
                    candidates.append(res)
            if i % 10 == 0:
                print(f"  …{i}/{len(vis_recs)} ({time.time()-t0:.0f}с), кандидатов={len(candidates)}",
                      flush=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    suffix = "_reasonaware" if reason_aware else ""
    out = OUT_DIR / f"visionB_{discipline}{suffix}.json"
    out.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(), "discipline": discipline, "model": model,
        "audited": len(results), "candidates_count": len(candidates),
        "candidates": sorted(candidates, key=lambda x: -x.get("confidence", 0)),
        "all_results": results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[vision] проверено: {len(results)} | кандидатов «эксперт мог ошибиться»: {len(candidates)}")
    print(f"[vision] отчёт: {out}")
    return 0


def phase_report() -> int:
    triage = json.loads((OUT_DIR / "triageA.json").read_text(encoding="utf-8")) if (OUT_DIR / "triageA.json").is_file() else {}
    ra_files = sorted(OUT_DIR.glob("visionB_*_reasonaware.json"))
    vision_files = ra_files or sorted(OUT_DIR.glob("visionB_*.json"))
    lines = ["# Аудит отклонённых замечаний Алии — кандидаты «эксперт мог ошибиться»", ""]
    lines.append(f"Сгенерировано: {datetime.now().isoformat()}")
    lines.append("\n> Это КАНДИДАТЫ на человеческую перепроверку, НЕ вердикты. Верификатор сам может")
    lines.append("> ошибаться в чтении чертежа; он не видит смежные листы/норм-контекст эксперта.\n")
    lines.append("> **Методология (reason-aware):** модель оценивает не «верно ли замечание», а")
    lines.append("> *опровергает ли чертёж КОНКРЕТНОЕ обоснование эксперта* — только при дословной")
    lines.append("> цитате против причины. Это снизило ложные кандидаты с ~61% (наивный accept-сигнал)")
    lines.append("> до ~12% (TX-пилот: 10 из 80). Наивный сигнал отброшен как недостоверный.\n")
    if triage:
        lines.append(f"## Триаж (офлайн): всего отклонённых {triage.get('total_rejected')}")
        for d, c in sorted(triage.get("by_discipline_category", {}).items()):
            lines.append(f"- **{d}**: {c}")
        offc = triage.get("offline_accept_candidates", [])
        if offc:
            lines.append(f"\n### Офлайн-кандидаты (норма/кросс-блок, без нейросети): {len(offc)}")
            for r in offc[:30]:
                lines.append(f"- [{r['discipline']}/{r['document']} {r['item_id']}] norm={r.get('norm_kind')} xb={r.get('xb_kind')}")
                lines.append(f"  - замечание: {r.get('problem','')}")
                lines.append(f"  - отклонено: {r.get('rejection_reason','')}")
    for vf in vision_files:
        data = json.loads(vf.read_text(encoding="utf-8"))
        cands = data.get("candidates", [])
        ra = "reasonaware" in vf.name
        lines.append(f"\n## Vision: {data.get('discipline')} — кандидатов {len(cands)} из {data.get('audited')}"
                     + (" (reason-aware)" if ra else ""))
        for c in cands:
            lines.append(f"### [{c['discipline']}/{c['document']} {c['item_id']}]")
            lines.append(f"- **Замечание ИИ:** {c.get('problem','')}")
            lines.append(f"- **Эксперт отклонил:** {c.get('rejection_reason','')}")
            if ra:
                lines.append(f"- **Опровергающая цитата с чертежа:** «{c.get('contradicting_quote','')}»")
                lines.append(f"- **Почему эксперт мог ошибиться:** {c.get('explanation','')}")
            else:
                lines.append(f"- **Верификатор:** {c.get('verifier_decision')} / {c.get('reason','')}")
                if c.get("evidence_quote"):
                    lines.append(f"- **Цитата:** «{c['evidence_quote']}»")
                lines.append(f"- **Флаг:** {c.get('why_flag','')}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / "EXPERT_AUDIT_ALIA.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[отчёт] {out}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--phase", choices=("triage", "vision", "report"), required=True)
    p.add_argument("--discipline", default=None, help="напр. TX, AR, KJ …")
    p.add_argument("--object", default="alia")
    p.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    p.add_argument("--limit", type=int, default=0, help="ограничить число vision-кейсов")
    p.add_argument("--no-reason-aware", action="store_true", help="старый сигнал (accept), не reason-aware")
    a = p.parse_args()
    if a.phase == "triage":
        return phase_triage(a.discipline)
    if a.phase == "vision":
        return asyncio.run(_phase_vision_async(a.discipline, a.model, a.limit,
                                               reason_aware=not a.no_reason_aware))
    return phase_report()


if __name__ == "__main__":
    raise SystemExit(main())
