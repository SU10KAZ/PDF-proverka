"""Сравнение «было/стало»: первый прогон (gemma-OCR + мелкий кроп, reason-aware) против
нового (high-res image-only). Read-only — только читает артефакты, пишет отчёт.

OLD: results/audit_alia/visionB_<DISC>_reasonaware.json → all_results[].expert_wrong_verdict
NEW: results/audit_alia/visionB2_<DISC>.json            → all[].highres_verdict
Ключ замечания: (document, version, item_id).
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"

DISCIPLINES = ["OV", "KM", "EOM", "SS", "KJ", "TX", "AR"]


def _norm(v: str) -> str:
    """yes остаётся yes; no/cannot_tell/прочее → 'no' (не-кандидат)."""
    return "yes" if str(v).strip().lower() == "yes" else "no"


def _load_old(disc: str) -> dict:
    f = OUT_DIR / f"visionB_{disc}_reasonaware.json"
    if not f.is_file():
        return {}
    data = json.loads(f.read_text(encoding="utf-8"))
    out = {}
    for x in data.get("all_results", []):
        if "expert_wrong_verdict" not in x:
            continue
        key = (x.get("document"), x.get("version"), x.get("item_id"))
        out[key] = x
    return out


def _load_new(disc: str) -> dict:
    f = OUT_DIR / f"visionB2_{disc}.json"
    if not f.is_file():
        return {}
    data = json.loads(f.read_text(encoding="utf-8"))
    out = {}
    for x in data.get("all", []):
        if "highres_verdict" not in x:
            continue
        key = (x.get("document"), x.get("version"), x.get("item_id"))
        out[key] = x
    return out


def main() -> int:
    lines = ["# Аудит Алии — было/стало: gemma-OCR vs high-res image-only", ""]
    lines.append(f"Сгенерировано: {datetime.now().isoformat()}")
    lines.append("\n> **Было** — первый прогон (reason-aware на gemma-OCR + мелкий кроп 431×709).")
    lines.append("> **Стало** — high-res image-only (рендер блока из PDF, qwen без gemma-текста).")
    lines.append("> Кандидат = вердикт «эксперт ошибся» (yes). Кандидаты = на ручную перепроверку.\n")

    totals = Counter()
    table = []
    removed_examples, new_examples = [], []
    for disc in DISCIPLINES:
        old, new = _load_old(disc), _load_new(disc)
        if not new:
            continue
        common = set(old) & set(new)
        m = Counter()
        for key in common:
            o, n = _norm(old[key].get("expert_wrong_verdict")), _norm(new[key].get("highres_verdict"))
            m[(o, n)] += 1
            if o == "yes" and n == "no":
                removed_examples.append((disc, key, old[key], new[key]))
            elif o == "no" and n == "yes":
                new_examples.append((disc, key, old[key], new[key]))
        old_cand = sum(1 for k in common if _norm(old[k].get("expert_wrong_verdict")) == "yes")
        new_cand = sum(1 for k in common if _norm(new[k].get("highres_verdict")) == "yes")
        table.append((disc, len(common), old_cand, new_cand,
                      m[("yes", "yes")], m[("yes", "no")], m[("no", "yes")]))
        for k, v in m.items():
            totals[k] += v

    lines.append("## Сводка по дисциплинам\n")
    lines.append("| Дисц | Сравнено | Было канд. | Стало канд. | yes→yes | yes→no (снято) | no→yes (новые) |")
    lines.append("|---|---|---|---|---|---|---|")
    tot = [0, 0, 0, 0, 0, 0]
    for disc, n, oc, nc, yy, yn, ny in table:
        lines.append(f"| {disc} | {n} | {oc} | {nc} | {yy} | {yn} | {ny} |")
        for i, v in enumerate((n, oc, nc, yy, yn, ny)):
            tot[i] += v
    lines.append(f"| **ИТОГО** | {tot[0]} | {tot[1]} | {tot[2]} | {tot[3]} | {tot[4]} | {tot[5]} |")

    lines.append("\n## Что это значит")
    lines.append(f"- **yes→no (снято {tot[4]})** — кандидаты первого алгоритма, не подтвердившиеся на чёткой")
    lines.append("  картинке: вероятные OCR-артефакты (как В4.0). Эксперт по ним скорее прав.")
    lines.append(f"- **no→yes (новые {tot[5]})** — замечания, которые первый алгоритм заглушил OCR-шумом, а")
    lines.append("  high-res выявил как реальные споры. Их раньше пропускали.")
    lines.append(f"- **yes→yes ({tot[3]})** — устояли в обоих → самые надёжные кандидаты.")

    lines.append("\n## Примеры СНЯТЫХ (yes→no) — вероятные ложные первого алгоритма")
    for disc, key, o, n in removed_examples[:12]:
        lines.append(f"### [{disc}/{key[0]} {key[2]}]")
        lines.append(f"- Замечание: {(o.get('problem') or '')[:170]}")
        lines.append(f"- Эксперт: {(o.get('rejection_reason') or '')[:150]}")
        lines.append(f"- high-res прочитал: {(n.get('value_read') or '')[:120]} → {n.get('explanation','')[:140]}")

    lines.append("\n## Примеры НОВЫХ (no→yes) — выявлено high-res, раньше пропускали")
    for disc, key, o, n in new_examples[:12]:
        lines.append(f"### [{disc}/{key[0]} {key[2]}]")
        lines.append(f"- Замечание: {(n.get('problem') or '')[:170]}")
        lines.append(f"- Эксперт: {(n.get('rejection_reason') or '')[:150]}")
        lines.append(f"- high-res прочитал: {(n.get('value_read') or '')[:120]} → {n.get('explanation','')[:140]}")

    out = OUT_DIR / "EXPERT_AUDIT_ALIA_BEFORE_AFTER.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[было/стало] всего сравнено {tot[0]} | yes→yes {tot[3]} | yes→no {tot[4]} | no→yes {tot[5]}")
    print(f"[было/стало] отчёт: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
