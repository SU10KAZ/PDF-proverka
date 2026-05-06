"""Intake для норм, не покрытых status_index.json.

Принимает один из источников:
    --findings <path>   путь к 03_findings.json (или совместимому файлу)
    --list <path>       JSON-массив строк ИЛИ txt с одной нормой на строку
    --codes c1,c2,...   inline список через запятую

Прогоняет каждую норму через get_norm_status() и собирает unresolved случаи.

Выходы (под tools/):
    missing_norms_report.json   — полная статистика + discovered + unresolved
    missing_norms_queue.json    — actionable список для human review
    missing_norms_queue.md      — human-readable версия queue

Запуск:
    python3 tools/intake_missing_norms.py --findings /path/to/03_findings.json
    python3 tools/intake_missing_norms.py --list missing.json
    python3 tools/intake_missing_norms.py --codes "ГОСТ 00000-9999,СО 153-34"
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from norms_api import (  # noqa: E402
    detect_family,
    get_norm_status,
    is_supported_family,
    is_vault_hosted_family,
)

OUT_REPORT = HERE / "missing_norms_report.json"
OUT_QUEUE_JSON = HERE / "missing_norms_queue.json"
OUT_QUEUE_MD = HERE / "missing_norms_queue.md"


# Тот же набор regex'ов, что использует PDF-проверка для извлечения ссылок,
# продублирован здесь, чтобы intake работал автономно от первого проекта.
_NORM_PATTERNS = [
    r"СП\s+[\d\.]+\.\d{7}\.\d{4}",
    r"СП\s+\d+\.\d+\.\d{4}",
    r"ГОСТ\s+(?:Р\s+)?(?:IEC\s+)?(?:МЭК\s+)?[\d\.\-]+(?:\-\d{4})?",
    r"ПУЭ[\s\-]*[67]?",
    r"СНиП\s+[\d\.\-\*]+",
    r"ВСН\s+[\d\-]+",
    r"ФЗ[\s\-]*\d+",
    r"\d+-ФЗ",
    r"ПП\s+РФ\s+[№]?\s*\d+",
    r"СО\s+[\d\.\-]+",
    r"РД\s+[\d\.\-]+",
    r"МДС\s+[\d\.\-]+",
]
_NORM_REGEX = re.compile("|".join(f"({p})" for p in _NORM_PATTERNS), re.IGNORECASE)


def _extract_from_text(text: str) -> list[str]:
    if not text:
        return []
    out: set[str] = set()
    for tup in _NORM_REGEX.findall(text):
        if isinstance(tup, tuple):
            for m in tup:
                if m and m.strip():
                    out.add(m.strip())
        elif tup and tup.strip():
            out.add(tup.strip())
    return sorted(out)


# ---------- входные форматы ----------


def _collect_from_findings(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    findings = data.get("findings") or data.get("items") or []
    norms: dict[str, dict] = {}
    for f in findings:
        fid = f.get("id", "?")
        norm_field = f.get("norm") or ""
        problem = f.get("finding") or f.get("problem") or ""
        recommendation = f.get("recommendation") or f.get("solution") or ""
        cited_from_field = [norm_field.strip()] if norm_field.strip() else []
        cited = cited_from_field + _extract_from_text(f"{norm_field}\n{problem}\n{recommendation}")
        for raw in set(cited):
            key = re.sub(r"\s+", " ", raw).strip()
            if not key:
                continue
            bucket = norms.setdefault(
                key,
                {"raw_norm": key, "cited_as": set(), "affected_findings": set(), "contexts": []},
            )
            bucket["cited_as"].add(raw.strip())
            bucket["affected_findings"].add(str(fid))
            if problem:
                bucket["contexts"].append(problem[:200])
    # unbucket sets → lists
    for v in norms.values():
        v["cited_as"] = sorted(v["cited_as"])
        v["affected_findings"] = sorted(v["affected_findings"])
        # уникализируем contexts с сохранением порядка
        seen, uniq = set(), []
        for c in v["contexts"]:
            if c not in seen:
                uniq.append(c)
                seen.add(c)
        v["contexts"] = uniq[:5]
    return norms


def _collect_from_list_file(path: Path) -> dict[str, dict]:
    text = path.read_text(encoding="utf-8").strip()
    codes: list[str] = []
    if text.startswith("["):
        try:
            data = json.loads(text)
            if isinstance(data, list):
                codes = [str(x).strip() for x in data if str(x).strip()]
            elif isinstance(data, dict) and "norms" in data:
                codes = [str(x).strip() for x in data["norms"] if str(x).strip()]
        except json.JSONDecodeError:
            codes = []
    if not codes:
        codes = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    return {c: {"raw_norm": c, "cited_as": [c], "affected_findings": [], "contexts": []} for c in codes}


def _collect_from_codes(csv: str) -> dict[str, dict]:
    codes = [c.strip() for c in csv.split(",") if c.strip()]
    return {c: {"raw_norm": c, "cited_as": [c], "affected_findings": [], "contexts": []} for c in codes}


# ---------- основная логика ----------


def _suggested_action(family: str | None, supported: bool) -> str:
    if not supported:
        return "review_family_support"
    if is_vault_hosted_family(family):
        return "add_document_to_vault"
    return "add_manual_override"


def classify(norms: dict[str, dict]) -> dict[str, Any]:
    resolved: list[dict] = []
    unresolved: list[dict] = []
    family_stats: dict[str, dict[str, int]] = {}

    for raw_norm, info in sorted(norms.items()):
        status = get_norm_status(raw_norm)
        family = status.get("detected_family") or detect_family(raw_norm)
        fam_key = family or "unknown"
        bucket = family_stats.setdefault(fam_key, {"total": 0, "resolved": 0, "unresolved": 0})
        bucket["total"] += 1

        base = {
            "raw_norm": raw_norm,
            "normalized_norm": status.get("normalized_query"),
            "detected_family": family,
            "supported_family": bool(family and is_supported_family(family)),
            "resolution_reason": status.get("resolution_reason"),
            "affected_findings": info.get("affected_findings", []),
            "contexts": info.get("contexts", []),
            "cited_as": info.get("cited_as", []),
        }

        if status.get("found") and status.get("authoritative"):
            bucket["resolved"] += 1
            resolved.append({**base, "matched_code": status["matched_code"], "status": status["status"]})
        else:
            bucket["unresolved"] += 1
            unresolved.append(
                {
                    **base,
                    "suggested_action": _suggested_action(family, base["supported_family"]),
                }
            )

    return {
        "resolved": resolved,
        "unresolved": unresolved,
        "family_stats": family_stats,
    }


def write_outputs(source: str, input_path: str | None, norms: dict[str, dict], result: dict) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    report = {
        "meta": {
            "generated_at": now,
            "source": source,
            "input_path": input_path,
            "total_discovered": len(norms),
            "total_resolved": len(result["resolved"]),
            "total_unresolved": len(result["unresolved"]),
            "family_stats": result["family_stats"],
        },
        "resolved": result["resolved"],
        "unresolved": result["unresolved"],
    }
    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    queue = {
        "meta": {
            "generated_at": now,
            "source": source,
            "input_path": input_path,
            "total": len(result["unresolved"]),
            "actions": _count_actions(result["unresolved"]),
        },
        "items": result["unresolved"],
    }
    OUT_QUEUE_JSON.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        "# missing_norms_queue",
        "",
        f"_generated: {now}_",
        f"_source: {source}_" + (f" ({input_path})" if input_path else ""),
        "",
        f"Всего неразрешённых норм: **{len(result['unresolved'])}**",
        "",
    ]
    if queue["meta"]["actions"]:
        md_lines.append("## Действия")
        for action, cnt in queue["meta"]["actions"].items():
            md_lines.append(f"- **{action}**: {cnt}")
        md_lines.append("")

    if result["unresolved"]:
        md_lines.append("## Норма → suggested_action")
        md_lines.append("")
        for item in result["unresolved"]:
            fids = ", ".join(item["affected_findings"]) or "—"
            md_lines.append(
                f"### {item['raw_norm']}\n"
                f"- detected_family: `{item['detected_family']}`\n"
                f"- supported_family: `{item['supported_family']}`\n"
                f"- resolution_reason: `{item['resolution_reason']}`\n"
                f"- suggested_action: **{item['suggested_action']}**\n"
                f"- affected_findings: {fids}\n"
            )
    OUT_QUEUE_MD.write_text("\n".join(md_lines), encoding="utf-8")


def _count_actions(unresolved: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in unresolved:
        a = item.get("suggested_action", "review_family_support")
        out[a] = out.get(a, 0) + 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--findings", type=Path, help="путь к 03_findings.json")
    g.add_argument("--list", type=Path, dest="list_path", help="JSON или txt со списком норм")
    g.add_argument("--codes", type=str, help="inline список через запятую")
    args = ap.parse_args()

    if args.findings:
        if not args.findings.exists():
            print(f"ERROR: файл не найден: {args.findings}", file=sys.stderr)
            return 1
        norms = _collect_from_findings(args.findings)
        source, input_path = "findings", str(args.findings)
    elif args.list_path:
        if not args.list_path.exists():
            print(f"ERROR: файл не найден: {args.list_path}", file=sys.stderr)
            return 1
        norms = _collect_from_list_file(args.list_path)
        source, input_path = "list", str(args.list_path)
    else:
        norms = _collect_from_codes(args.codes)
        source, input_path = "codes", None

    if not norms:
        print("ERROR: не удалось извлечь ни одной нормы из входа", file=sys.stderr)
        return 1

    result = classify(norms)
    write_outputs(source, input_path, norms, result)

    print(f"Обработано норм: {len(norms)}", file=sys.stderr)
    print(f"  resolved:   {len(result['resolved'])}", file=sys.stderr)
    print(f"  unresolved: {len(result['unresolved'])}", file=sys.stderr)
    actions = _count_actions(result["unresolved"])
    for a, c in actions.items():
        print(f"    {a}: {c}", file=sys.stderr)
    print(f"Записано: {OUT_REPORT.name}, {OUT_QUEUE_JSON.name}, {OUT_QUEUE_MD.name}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
