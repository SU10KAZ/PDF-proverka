"""Финальный шорт-лист после калиброванного фильтра (вариант A).
Ранжир по критичности (severity + темы) + группировка. Read-only."""
from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

from .audit_rejected import iter_alia_rejected, load_version_finding

OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"

SAFETY = re.compile(r"эвакуац|мгн|маломобильн|инвалид|доступн|пожар|огнестойк|противопожар|дымоудал", re.I)
STRUCT = re.compile(r"несущ|защитн.{0,3}слой|армир|обрушени|нагрузк|прочност|анкер|закладн|бетон|колонн|перемычк", re.I)
ARITH = re.compile(r"не сход|не замкн|сумм|\d+\s*[x×]\s*\d+|размерн|габарит|расхожд|\bвместо\b|≠", re.I)


def _enrich():
    res = json.loads((OUT_DIR / "filter_candidates_result.json").read_text(encoding="utf-8"))
    items = []
    for x in res.get("graphic_survivors", []):
        items.append({**x, "kind": "graphic", "quote": x.get("value_read", "")})
    for x in res.get("text_survivors", []):
        items.append({**x, "kind": "text", "quote": x.get("key_point", "")})
    # severity из findings
    lut = {}
    discs = {x["discipline"] for x in items if x.get("discipline")}
    for disc in discs:
        for r in iter_alia_rejected(disc):
            lut[(r.discipline, r.document, r.version, r.item_id)] = r.output_dir
    for x in items:
        od = lut.get((x.get("discipline"), x.get("document"), x.get("version"), x.get("item_id")))
        sev = ""
        if od:
            f = load_version_finding(od, x["item_id"])
            if f:
                sev = str(f.get("severity", ""))
        x["_sev"] = sev
    return items


def _score(x):
    s = 0
    txt = (x.get("problem", "") + " " + x.get("explanation", "") + " " + x.get("rejection_reason", ""))
    sev = x.get("_sev", "").upper()
    if "КРИТИ" in sev:
        s += 8
    elif "ЭКСПЛУАТА" in sev:
        s += 4
    elif "ЭКОНОМ" in sev:
        s += 2
    if SAFETY.search(txt):
        s += 10
    if STRUCT.search(txt):
        s += 6
    if ARITH.search(txt):
        s += 4
    if re.search(r"\d", x.get("quote", "")):
        s += 3
    return s


def main() -> int:
    items = _enrich()
    for x in items:
        x["_score"] = _score(x)
    items.sort(key=lambda y: -y["_score"])

    L = ["# Аудит Алии — ФИНАЛЬНЫЙ ШОРТ-ЛИСТ «эксперт мог ошибиться»", ""]
    L.append(f"Сгенерировано: {datetime.now().isoformat()}")
    L.append(f"\n**{len(items)} кандидатов** после калиброванного фильтра (из 1039 сырых). Локально,")
    L.append("0 токенов подписки. Калибровка убрала ложный паттерн «инфо в общих указаниях/др.листах/")
    L.append("принято экспертизой». Это ЛИДЫ на ручную перепроверку экспертом, НЕ вердикты.\n")
    g = sum(1 for x in items if x["kind"] == "graphic")
    L.append(f"- графика: {g} | текст: {len(items)-g}")
    L.append(f"- severity: {dict(Counter(x.get('_sev','') for x in items))}\n")

    L.append("## ТОП-40 по критичности\n")
    for i, x in enumerate(items[:40], 1):
        L.append(f"### {i}. [{x.get('discipline')}/{x.get('document')} {x['item_id']}] "
                 f"({x.get('_sev','')}, {x['kind']}, score {x['_score']})")
        L.append(f"- **Замечание:** {(x.get('problem') or '')[:200]}")
        L.append(f"- **Эксперт отклонил:** {(x.get('rejection_reason') or '')[:180]}")
        if x["kind"] == "graphic":
            L.append(f"- **Прочитано с чертежа:** {(x.get('value_read') or '')[:140]}")
        else:
            L.append(f"- **Довод против эксперта:** {(x.get('key_point') or '')[:160]}")
        L.append(f"- **Пояснение:** {(x.get('explanation') or '')[:180]}")

    L.append(f"\n## Остальные {max(0,len(items)-40)} — кратко\n")
    for x in items[40:]:
        L.append(f"- [{x.get('discipline')}/{x.get('document')} {x['item_id']}] "
                 f"({x.get('_sev','')}, {x['kind']}): {(x.get('problem') or '')[:90]} "
                 f"→ {(x.get('quote') or '')[:60]}")

    out = OUT_DIR / "EXPERT_AUDIT_ALIA_SHORTLIST.md"
    out.write_text("\n".join(L), encoding="utf-8")
    json.dump(items, open(OUT_DIR / "shortlist_ranked.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(f"[шорт-лист] {len(items)} кандидатов (графика {g}, текст {len(items)-g})")
    print(f"[шорт-лист] severity: {dict(Counter(x.get('_sev','') for x in items))}")
    print(f"[шорт-лист] {out}")
    return 0


if __name__ == "__main__":
    import sys
    ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    raise SystemExit(main())
