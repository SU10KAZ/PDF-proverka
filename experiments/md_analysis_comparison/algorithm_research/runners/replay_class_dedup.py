"""H6 / Stage 6 replay — apply class-level dedup retroactively to parent
stand's cached `multi_agent.json` outputs and write to
`algorithm_research/results/replay_class_dedup__baseline/<case_id>.json`.

This script is **purely Python**. No LLM calls. It tests:
  H6: "class-level dedup alone drops FP by ≥ 80 over baseline multi_agent
       if applied retroactively."
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runners._common import EXP_ROOT, RESULTS_DIR, make_run_result
from runners.class_dedup import collapse_to_canonical, fuzzy_dedup, Finding


PARENT_RESULTS = EXP_ROOT / "results"
OUT_DIR = RESULTS_DIR / "replay_class_dedup__baseline"
OUT_DIR_FUZZY = RESULTS_DIR / "replay_fuzzy_dedup__baseline"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR_FUZZY.mkdir(parents=True, exist_ok=True)
    cases = sorted([d for d in PARENT_RESULTS.iterdir() if (d / "multi_agent.json").exists()])
    for case_dir in cases:
        case_id = case_dir.name
        src = case_dir / "multi_agent.json"
        data = json.loads(src.read_text(encoding="utf-8"))
        findings = data.get("findings") or []
        collapsed, report = collapse_to_canonical(findings)
        fuzzy, fuzzy_report = fuzzy_dedup(findings, sim_threshold=0.65)

        from runners._common import coerce_finding
        f_objs: list[Finding] = []
        for i, raw in enumerate(collapsed, start=1):
            try:
                f_objs.append(coerce_finding(raw, i, source_agent=raw.get("source_agent", "")))
            except Exception:
                continue

        out = make_run_result(
            method="replay_class_dedup__baseline",
            case_id=case_id,
            discipline=data.get("discipline", ""),
            findings=f_objs,
            duration=float(data.get("duration_sec") or 0.0),
            meta={
                "source_method": "multi_agent (parent stand)",
                "original_count": len(findings),
                "collapsed_count": len(collapsed),
                "dedup_drops": report.same_class_drops,
                "clusters_with_dupes": sum(1 for v in report.same_class_drops_by_key.values() if v),
            },
            errors=[],
        )
        out_path = OUT_DIR / f"{case_id}.json"
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

        # Fuzzy dedup variant
        f_fuzzy = []
        for i, raw in enumerate(fuzzy, start=1):
            try:
                f_fuzzy.append(coerce_finding(raw, i, source_agent=raw.get("source_agent", "")))
            except Exception:
                continue
        out_fuzzy = make_run_result(
            method="replay_fuzzy_dedup__baseline",
            case_id=case_id,
            discipline=data.get("discipline", ""),
            findings=f_fuzzy,
            duration=float(data.get("duration_sec") or 0.0),
            meta={
                "source_method": "multi_agent (parent stand)",
                "original_count": len(findings),
                "fuzzy_collapsed_count": len(fuzzy),
                "fuzzy_dedup_drops": fuzzy_report.same_class_drops,
                "fuzzy_threshold": 0.65,
            },
            errors=[],
        )
        (OUT_DIR_FUZZY / f"{case_id}.json").write_text(
            json.dumps(out_fuzzy, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        print(f"{case_id}: orig={len(findings)} class-collapsed={len(collapsed)} fuzzy-collapsed={len(fuzzy)}")
    print(f"\nReplay results saved under {OUT_DIR} (class) and {OUT_DIR_FUZZY} (fuzzy)")


if __name__ == "__main__":
    main()
