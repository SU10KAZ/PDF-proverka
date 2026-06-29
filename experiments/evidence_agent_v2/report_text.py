"""Агрегатор отчёта по текстовым замечаниям (visionTEXT_*.json → EXPERT_AUDIT_ALIA_TEXT.md)."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"


def main() -> int:
    files = sorted(OUT_DIR.glob("visionTEXT_*.json"))
    lines = ["# Аудит Алии — ТЕКСТОВЫЕ замечания: кандидаты «эксперт мог ошибиться»", ""]
    lines.append(f"Сгенерировано: {datetime.now().isoformat()}")
    lines.append("\n> Модель: qwen/qwen3.6-35b-a3b (локально, 0 токенов подписки). Reason-aware по")
    lines.append("> норме/тексту. Кандидаты = на ручную перепроверку, не вердикты. Документ не всегда")
    lines.append("> приложен к замечанию → фактические споры часто cannot_tell (консервативно).\n")

    tot_all = tot_cand = 0
    tot_verd = Counter()
    rows = []
    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        disc = "_".join(d.get("disciplines", []))
        a = d.get("rechecked", 0)
        cands = d.get("candidates", [])
        rows.append((disc, a, len(cands)))
        tot_all += a; tot_cand += len(cands)
        tot_verd.update(d.get("verdicts", {}))

    lines.append("## Сводка по дисциплинам\n")
    lines.append("| Дисц | Прогнано | Кандидатов |")
    lines.append("|---|---|---|")
    for disc, a, c in rows:
        lines.append(f"| {disc} | {a} | {c} |")
    lines.append(f"| **ИТОГО** | {tot_all} | {tot_cand} |")
    lines.append(f"\nВердикты всего: {dict(tot_verd)}\n")

    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        disc = "_".join(d.get("disciplines", []))
        cands = d.get("candidates", [])
        if not cands:
            continue
        lines.append(f"\n## {disc} — кандидатов {len(cands)}")
        for c in cands:
            flag = f" [{c['norm_flag']}]" if c.get("norm_flag") else ""
            lines.append(f"### [{c['document']} {c['item_id']}]{flag}")
            lines.append(f"- **Замечание:** {c.get('problem','')}")
            lines.append(f"- **Норма:** {c.get('norm','') or '—'}")
            lines.append(f"- **Эксперт отклонил:** {c.get('rejection_reason','')}")
            lines.append(f"- **Довод против эксперта:** {c.get('key_point','')}")
            lines.append(f"- **Пояснение:** {c.get('explanation','')}")

    out = OUT_DIR / "EXPERT_AUDIT_ALIA_TEXT.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[text-отчёт] прогнано {tot_all} | кандидатов {tot_cand}")
    print(f"[text-отчёт] {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
