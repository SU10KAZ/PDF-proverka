"""Сводный отчёт по классификации норм в проектах.

Проходит по проектным `_output/03_findings.json` (или указанному пути) и
для каждого файла:

  1. Извлекает все цитаты норм из findings.
  2. Прогоняет их через `norms.external_provider.resolve_norm_status`.
  3. Считает распределение по 4-м классам: authoritative, known_unverified,
     missing, unsupported.
  4. Печатает в stdout и сохраняет JSON в `reports/`.

Цели:
  - Проверить, что после фикса verify-логики массового попадания известных
    норм в `missing` нет.
  - Подтвердить, что `known_unverified` отделён от `missing` и `authoritative`.
  - Отчёт можно использовать как regression-baseline.

Использование:
    python3 scripts/norms_classification_summary.py
        [--root PATH]
        [--output-name NAME]
        [--findings PATTERN]     # glob, по умолчанию: **/03_findings.json
        [--print-lists]           # печатать списки кодов

Пути по умолчанию: <repo>/projects/**/03_findings.json
Отчёты пишутся в <repo>/reports/norms_classification_<timestamp>.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from norms import _core  # noqa: E402
from norms.external_provider import (  # noqa: E402
    diagnostics,
    resolve_norm_status,
    _reset_cache,
)

DEFAULT_FINDINGS_GLOB = "projects/**/03_findings.json"
REPORTS_DIR = _REPO_ROOT / "reports"


def _extract_norms_from_findings_file(path: Path) -> dict[str, list[str]]:
    """Достать все цитаты норм из 03_findings.json. {norm_raw: [finding_ids]}."""
    try:
        norms_data = _core.extract_norms_from_findings(path)
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, list[str]] = {}
    for norm_raw, info in (norms_data.get("norms") or {}).items():
        out[norm_raw] = list(info.get("affected_findings") or [])
    return out


def _classify_one(norm_raw: str) -> dict:
    r = resolve_norm_status(norm_raw)
    return {
        "norm": norm_raw,
        "matched_code": r.get("matched_code"),
        "classification": r.get("classification"),
        "status": r.get("status"),
        "source": r.get("source"),
        "resolution_reason": r.get("resolution_reason"),
        "detected_family": r.get("detected_family"),
        "authoritative": bool(r.get("authoritative")),
    }


def summarize_findings_file(path: Path) -> dict:
    norms = _extract_norms_from_findings_file(path)
    classified: list[dict] = []
    counts = Counter()
    source_counts = Counter()
    lists: defaultdict[str, list[str]] = defaultdict(list)

    for norm_raw in sorted(norms):
        c = _classify_one(norm_raw)
        classified.append(c)
        counts[c["classification"]] += 1
        source_counts[c.get("source") or "?"] += 1
        lists[c["classification"]].append(norm_raw)

    return {
        "path": str(path),
        "total_norms": len(norms),
        "counts": dict(counts),
        "by_source": dict(source_counts),
        "classified": classified,
        "lists": dict(lists),
    }


def run_summary(
    root: Path,
    pattern: str,
    output_name: str,
    print_lists: bool,
) -> dict:
    findings_files = sorted(root.glob(pattern))
    grand_counts = Counter()
    grand_sources = Counter()
    per_file: list[dict] = []
    aggregate_lists: defaultdict[str, set] = defaultdict(set)

    for fp in findings_files:
        rep = summarize_findings_file(fp)
        per_file.append(rep)
        for k, v in rep["counts"].items():
            grand_counts[k] += v
        for k, v in rep["by_source"].items():
            grand_sources[k] += v
        for cls, codes in rep["lists"].items():
            aggregate_lists[cls].update(codes)

    diag = diagnostics()

    summary = {
        "generated_at": datetime.now().isoformat(),
        "root": str(root),
        "pattern": pattern,
        "files_scanned": len(findings_files),
        "diagnostics": diag,
        "grand_totals": dict(grand_counts),
        "by_source": dict(grand_sources),
        "aggregate_lists": {k: sorted(v) for k, v in aggregate_lists.items()},
        "per_file": per_file,
    }

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"{output_name}.json"
    out_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # ── вывод в консоль ──────────────────────────────────────────────────
    print("== Norms classification summary ==")
    print(f"Mode: {diag.get('mode')}")
    print(
        f"status_index: {diag['status_index']['total_norms']} норм; "
        f"vault files: {diag['vault']['files_count']}; "
        f"known_unverified: {diag['known_unverified']['total']}"
    )
    print(f"Files scanned: {len(findings_files)}")
    print(f"Grand totals: {dict(grand_counts)}")
    print(f"By source: {dict(grand_sources)}")
    if print_lists:
        for cls in ("authoritative", "known_unverified", "missing", "unsupported"):
            codes = sorted(aggregate_lists[cls])
            if codes:
                print(f"\n— {cls} ({len(codes)}):")
                for c in codes:
                    print(f"   {c}")
    print(f"\nReport saved → {out_path}")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=_REPO_ROOT)
    ap.add_argument("--findings", default=DEFAULT_FINDINGS_GLOB,
                    help="Glob от --root (по умолчанию projects/**/03_findings.json)")
    ap.add_argument(
        "--output-name",
        default=f"norms_classification_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )
    ap.add_argument("--print-lists", action="store_true")
    args = ap.parse_args()

    _reset_cache()
    run_summary(
        root=args.root,
        pattern=args.findings,
        output_name=args.output_name,
        print_lists=args.print_lists,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
