"""Coverage report для Norms authoritative index.

Сравнивает **status_index.json** с реальными входными нормами:
    --findings <path>    03_findings.json-совместимый файл
    --list <path>         JSON/txt с нормативными ссылками
    --queue <path>        missing_norms_queue.json (из intake_missing_norms.py)
    --codes c1,c2,...     inline-список
без аргументов — показать только статистику самого status_index.json.

НЕ использует данные из первого проекта (norms_db.json/WebSearch).

Запуск:
    python3 tools/coverage_report.py
    python3 tools/coverage_report.py --findings /path/to/03_findings.json
    python3 tools/coverage_report.py --queue tools/missing_norms_queue.json
    python3 tools/coverage_report.py --json                 # машинный вывод
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from norms_api import (  # noqa: E402
    detect_family,
    effective_status,
    get_norm_status,
    is_supported_family,
    is_vault_hosted_family,
    load_status_index,
)

REPORT_FAMILIES = ["ГОСТ Р", "ГОСТ", "СП", "СНиП", "ВСН", "МДС", "РД", "ПУЭ", "ФЗ", "ПП РФ", "СО", "other"]

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


def collect_input_norms(args: argparse.Namespace) -> list[str]:
    codes: set[str] = set()
    if args.findings:
        data = json.loads(Path(args.findings).read_text(encoding="utf-8"))
        for f in data.get("findings") or data.get("items") or []:
            parts = [f.get("norm") or "", f.get("finding") or f.get("problem") or "",
                     f.get("recommendation") or f.get("solution") or ""]
            if parts[0].strip():
                codes.add(re.sub(r"\s+", " ", parts[0]).strip())
            for piece in _extract_from_text("\n".join(parts)):
                codes.add(re.sub(r"\s+", " ", piece).strip())
    if args.list_path:
        text = Path(args.list_path).read_text(encoding="utf-8").strip()
        try:
            j = json.loads(text)
            if isinstance(j, list):
                codes.update(str(x).strip() for x in j if str(x).strip())
            elif isinstance(j, dict) and "norms" in j:
                codes.update(str(x).strip() for x in j["norms"] if str(x).strip())
        except json.JSONDecodeError:
            codes.update(line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#"))
    if args.queue:
        q = json.loads(Path(args.queue).read_text(encoding="utf-8"))
        for it in q.get("items", []):
            raw = it.get("raw_norm")
            if raw:
                codes.add(raw.strip())
    if args.codes:
        codes.update(c.strip() for c in args.codes.split(",") if c.strip())
    return sorted(codes)


def _family_of(code: str) -> str:
    return detect_family(code) or "other"


def index_snapshot() -> dict[str, Any]:
    """Agregate со стороны status_index.json (без учёта внешнего входа)."""
    idx = load_status_index(force_reload=True)
    entries = idx.get("norms", [])
    by_family: dict[str, dict[str, int]] = {}
    for e in entries:
        fam = e.get("type") or "other"
        if fam not in REPORT_FAMILIES:
            fam = "other"
        bucket = by_family.setdefault(
            fam,
            {"total": 0, "active": 0, "outdated_edition": 0, "replaced": 0, "cancelled": 0,
             "unknown": 0, "has_text": 0, "override_only": 0},
        )
        bucket["total"] += 1
        eff = effective_status(e.get("doc_status"), e.get("edition_status"))
        bucket[eff] = bucket.get(eff, 0) + 1
        if e.get("has_text"):
            bucket["has_text"] += 1
        if e.get("source") == "override_only":
            bucket["override_only"] += 1
    return {
        "meta": idx.get("meta", {}),
        "by_family": by_family,
    }


def input_coverage(input_codes: list[str]) -> dict[str, Any]:
    """Покрытие внешних норм authoritative-индексом."""
    if not input_codes:
        return {}
    by_family: dict[str, dict[str, int]] = {}
    covered: list[dict] = []
    missing_supported: list[dict] = []
    missing_unsupported: list[dict] = []

    for code in input_codes:
        st = get_norm_status(code)
        fam = st.get("detected_family") or _family_of(code)
        if fam not in REPORT_FAMILIES:
            fam = "other"
        bucket = by_family.setdefault(
            fam,
            {"total": 0, "covered": 0, "missing_supported": 0, "unsupported": 0,
             "active": 0, "outdated_edition": 0, "replaced": 0, "cancelled": 0, "unknown": 0},
        )
        bucket["total"] += 1
        if st.get("found") and st.get("authoritative"):
            bucket["covered"] += 1
            bucket[st["status"]] = bucket.get(st["status"], 0) + 1
            covered.append({"raw": code, "matched_code": st["matched_code"], "status": st["status"]})
        else:
            supported = bool(fam and is_supported_family(fam))
            if supported:
                bucket["missing_supported"] += 1
                missing_supported.append(
                    {"raw": code, "family": fam, "resolution_reason": st["resolution_reason"]}
                )
            else:
                bucket["unsupported"] += 1
                missing_unsupported.append(
                    {"raw": code, "family": fam, "resolution_reason": st["resolution_reason"]}
                )

    return {
        "total_input": len(input_codes),
        "covered": len(covered),
        "missing_supported": len(missing_supported),
        "unsupported_family": len(missing_unsupported),
        "by_family": by_family,
        "samples": {
            "covered": covered[:10],
            "missing_supported": missing_supported[:10],
            "unsupported_family": missing_unsupported[:10],
        },
    }


def format_human(index_snap: dict, coverage: dict | None) -> str:
    out: list[str] = []
    meta = index_snap.get("meta", {})
    out.append("=" * 72)
    out.append("  Norms authoritative index")
    out.append("=" * 72)
    out.append(f"  indexed_at: {meta.get('indexed_at', '?')}")
    out.append(f"  total:      {meta.get('total', 0)}")
    out.append(f"  doc_status: {meta.get('totals_by_doc_status', {})}")
    out.append(f"  effective:  {meta.get('totals_by_effective_status', {})}")
    out.append(f"  override_only: {len(meta.get('override_only', []))}")
    out.append("")
    out.append("  По семейству (из index):")
    by_fam = index_snap.get("by_family", {})
    for fam in REPORT_FAMILIES:
        if fam not in by_fam:
            out.append(f"    {fam:8s} — нет в базе")
            continue
        b = by_fam[fam]
        out.append(
            f"    {fam:8s} total={b['total']:<4d} "
            f"active={b.get('active', 0):<4d} "
            f"outdated={b.get('outdated_edition', 0):<3d} "
            f"replaced={b.get('replaced', 0):<3d} "
            f"cancelled={b.get('cancelled', 0):<3d} "
            f"has_text={b.get('has_text', 0):<4d} "
            f"override_only={b.get('override_only', 0)}"
        )

    if coverage is None:
        out.append("")
        out.append("  (входной список не задан — coverage пропущен)")
        return "\n".join(out)

    out.append("")
    out.append("=" * 72)
    out.append("  Coverage входного списка")
    out.append("=" * 72)
    out.append(f"  total_input:         {coverage['total_input']}")
    out.append(f"  covered (authoritative): {coverage['covered']}")
    out.append(f"  missing (supported):  {coverage['missing_supported']}")
    out.append(f"  unsupported family:   {coverage['unsupported_family']}")
    out.append("")
    out.append("  По семейству (из входа):")
    for fam in REPORT_FAMILIES:
        b = coverage["by_family"].get(fam)
        if not b:
            continue
        supp = "yes" if is_supported_family(fam) else "NO"
        hosted = "vault" if is_vault_hosted_family(fam) else "override-only"
        out.append(
            f"    {fam:8s} total={b['total']:<3d} "
            f"covered={b['covered']:<3d} "
            f"missing_supported={b['missing_supported']:<3d} "
            f"unsupported={b['unsupported']:<3d} "
            f"[family_supported={supp}, hosted_as={hosted}]"
        )

    if coverage["samples"]["missing_supported"]:
        out.append("")
        out.append("  Примеры missing_supported (первые 10):")
        for s in coverage["samples"]["missing_supported"]:
            out.append(f"    - {s['raw']}  (family={s['family']}, reason={s['resolution_reason']})")
    if coverage["samples"]["unsupported_family"]:
        out.append("")
        out.append("  Примеры unsupported_family (первые 10):")
        for s in coverage["samples"]["unsupported_family"]:
            out.append(f"    - {s['raw']}  (family={s['family']}, reason={s['resolution_reason']})")

    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--findings", type=Path)
    ap.add_argument("--list", dest="list_path", type=Path)
    ap.add_argument("--queue", type=Path)
    ap.add_argument("--codes", type=str)
    ap.add_argument("--json", action="store_true", help="Машинный JSON-вывод")
    args = ap.parse_args()

    index_snap = index_snapshot()
    input_codes = collect_input_norms(args)
    coverage = input_coverage(input_codes) if input_codes else None

    if args.json:
        print(
            json.dumps(
                {"index": index_snap, "coverage": coverage},
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(format_human(index_snap, coverage))

    return 0


if __name__ == "__main__":
    sys.exit(main())
