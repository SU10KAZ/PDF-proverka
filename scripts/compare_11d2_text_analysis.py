#!/usr/bin/env python3
"""11D.2 §15-17 — разбор артефакта этапа и сверка с 11D.

Печатает ТОЛЬКО агрегаты и флаги наличия тем. Тексты замечаний заказчика
наружу не выводятся: сравнение ведётся по счётчикам и признакам.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

NEW = Path(sys.argv[1])
OLD = Path(sys.argv[2])
OUT = Path(sys.argv[3])

ALLOWED_SEVERITY = {
    "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

THEMES = {
    "pue_7_35": r"7\.35",
    "osup": r"\bОСУП\b|основн\w*\s+систем\w*\s+уравнивания\s+потенциал",
    "dsup": r"\bДСУП\b|дополнительн\w*\s+систем\w*\s+уравнивания\s+потенциал",
    "grounding_system_type": r"TN-C-S|TN-S|TN-C\b|\bTT\b|\bIT\b|тип\w*\s+систем\w*\s+заземлени|систем\w*\s+заземлени",
}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def finding_text(f: dict) -> str:
    parts = []
    for k, v in f.items():
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, (list, dict)):
            parts.append(json.dumps(v, ensure_ascii=False))
    return "\n".join(parts)


def analyse(payload: dict, label: str) -> dict:
    findings = payload.get("text_findings") or []
    sev = Counter((f.get("severity") or "<нет>") for f in findings)
    cats = Counter((f.get("category") or "<нет>") for f in findings)
    ids = [f.get("id") for f in findings]
    blob = "\n".join(finding_text(f) for f in findings)
    refs = payload.get("normative_refs_found") or []
    refs_blob = json.dumps(refs, ensure_ascii=False)
    all_blob = blob + "\n" + refs_blob

    # Какие нормативные документы упомянуты (номера, не тексты).
    norm_docs = sorted(set(
        re.findall(r"(?:СП|ГОСТ\s*Р?|СНиП|ПУЭ|СанПиН|ФЗ)[\s ]?[\d\.\-]*\d", all_blob)
    ))

    return {
        "label": label,
        "text_findings": len(findings),
        "severity_histogram": dict(sorted(sev.items(), key=lambda kv: -kv[1])),
        "unknown_severity_values": sorted(set(sev) - ALLOWED_SEVERITY),
        "all_severity_in_enum": set(sev) <= ALLOWED_SEVERITY,
        "categories": dict(sorted(cats.items(), key=lambda kv: -kv[1])),
        "normative_refs_found": len(refs),
        "findings_with_norm": sum(1 for f in findings if f.get("norm")),
        "findings_with_norm_quote": sum(1 for f in findings if f.get("norm_quote")),
        "findings_with_source": sum(1 for f in findings if f.get("source")),
        "items_verified_from_blocks": len(payload.get("items_verified_from_blocks") or []),
        "project_params_keys": sorted((payload.get("project_params") or {}).keys()),
        "top_level_keys": sorted(payload.keys()),
        "text_source": payload.get("text_source"),
        "stage": payload.get("stage"),
        "ids_unique": len(set(ids)) == len(ids),
        "ids_count": len(ids),
        "duplicate_ids": [i for i, c in Counter(ids).items() if c > 1],
        "malformed_findings": [
            {"index": i, "missing": [k for k in ("id", "severity", "finding") if not f.get(k)]}
            for i, f in enumerate(findings)
            if not all(f.get(k) for k in ("id", "severity", "finding"))
        ],
        "norm_docs_mentioned": norm_docs,
        "themes": {
            name: {
                "present": bool(re.search(rx, all_blob, re.IGNORECASE)),
                "findings_hit": [
                    {"id": f.get("id"), "severity": f.get("severity")}
                    for f in findings
                    if re.search(rx, finding_text(f), re.IGNORECASE)
                ],
            }
            for name, rx in THEMES.items()
        },
    }


new = load(NEW)
old = load(OLD)
a_new = analyse(new, "11D.2 (после правки 11D.1)")
a_old = analyse(old, "11D (до правки)")

report = {
    "kind": "11D.2 §16-17 — severity, контракт и сравнение с 11D",
    "content_in_report": False,
    "content_note": "Только счётчики, значения перечислений и флаги наличия тем. Текстов замечаний нет.",
    "new_run": a_new,
    "old_run_11d": a_old,
    "delta": {
        "text_findings": a_new["text_findings"] - a_old["text_findings"],
        "normative_refs_found": a_new["normative_refs_found"] - a_old["normative_refs_found"],
        "severity_before": a_old["severity_histogram"],
        "severity_after": a_new["severity_histogram"],
        "categories_new_only": sorted(set(a_new["categories"]) - set(a_old["categories"])),
        "categories_gone": sorted(set(a_old["categories"]) - set(a_new["categories"])),
        "categories_shared": sorted(set(a_new["categories"]) & set(a_old["categories"])),
        "norm_docs_new_only": sorted(set(a_new["norm_docs_mentioned"]) - set(a_old["norm_docs_mentioned"])),
        "norm_docs_gone": sorted(set(a_old["norm_docs_mentioned"]) - set(a_new["norm_docs_mentioned"])),
        "themes": {
            name: {
                "11d": a_old["themes"][name]["present"],
                "11d2": a_new["themes"][name]["present"],
                "11d_severities": sorted({x["severity"] for x in a_old["themes"][name]["findings_hit"]}),
                "11d2_severities": sorted({x["severity"] for x in a_new["themes"][name]["findings_hit"]}),
            }
            for name in THEMES
        },
    },
    "pass_criteria_16": {
        "all_severity_in_enum": a_new["all_severity_in_enum"],
        "unknown_severity_values": a_new["unknown_severity_values"],
        "no_duplicate_ids": a_new["ids_unique"],
        "no_malformed_findings": a_new["malformed_findings"] == [],
        "text_source_is_md": a_new["text_source"] == "md",
        "note": "Число КРИТИЧЕСКИХ критерием НЕ является (§16, §20).",
    },
}
OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(report, ensure_ascii=False, indent=2))
