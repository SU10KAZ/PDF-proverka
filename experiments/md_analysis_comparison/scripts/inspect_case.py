"""Side-by-side qualitative inspection of one case.

Loads ground_truth.json, current.json, multi_agent.json — prints which
GT findings each method caught, which were missed, and what extra (non-GT)
findings each emitted. Useful for eyeballing quality beyond the strict
metric.
"""
from __future__ import annotations

import argparse
import json
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402
from runners.unified_output_schema import load_run_result  # noqa: E402
from scripts.compare_results import _match_finding_to_gt, GroundTruthFinding, DEFAULT_MATCH_THRESHOLD  # noqa: E402


def inspect(case_id: str):
    case = cfg.DATASETS_DIR / case_id
    gt_data = json.loads((case / "ground_truth.json").read_text(encoding="utf-8"))
    gt_items = [
        GroundTruthFinding(
            id=g.get("id", f"GT-{i:02d}"),
            severity=g.get("severity", ""),
            description=g.get("description", ""),
            must_match_substring=g.get("must_match_substring", ""),
            is_critical=bool(g.get("is_critical") or g.get("severity") == "КРИТИЧЕСКОЕ"),
            is_trap=bool(g.get("is_trap", False)),
        )
        for i, g in enumerate(gt_data.get("expected_findings", []), start=1)
    ]
    real_gt = [g for g in gt_items if not g.is_trap]
    traps = [g for g in gt_items if g.is_trap]

    print(f"\n==== {case_id} ====")
    print(f"GT findings (real): {len(real_gt)}, traps: {len(traps)}")

    for method, fname in [("current_method", "current.json"), ("multi_agent", "multi_agent.json")]:
        path = cfg.RESULTS_DIR / case_id / fname
        if not path.exists():
            print(f"\n  [{method}] NOT RUN")
            continue
        rr = load_run_result(path)
        print(f"\n  ── {method} ({len(rr.findings)} findings, {rr.duration_sec:.0f}s) ──")
        matched_ids: set[str] = set()
        for gt in real_gt:
            best = (0.0, None)
            for f in rr.findings:
                if f.id in matched_ids:
                    continue
                s = _match_finding_to_gt(f, gt)
                if s > best[0]:
                    best = (s, f)
            sim, f = best
            if f and sim >= DEFAULT_MATCH_THRESHOLD:
                matched_ids.add(f.id)
                print(f"    ✓ {gt.id} ({gt.severity[:5]}) → {f.id} sim={sim:.2f}: {f.problem[:80]}")
            else:
                tag = "✗ MISSED-CRIT" if gt.is_critical else "✗ missed"
                print(f"    {tag} {gt.id} ({gt.severity[:5]}): {gt.description[:80]}")
        trapped = []
        for f in rr.findings:
            for t in traps:
                if _match_finding_to_gt(f, t) >= DEFAULT_MATCH_THRESHOLD:
                    trapped.append((f, t))
        if trapped:
            print(f"    Traps triggered:")
            for f, t in trapped:
                print(f"      ⚠ {t.id} caught by {f.id} ({f.severity[:5]}): {f.problem[:70]}")
        extras = [f for f in rr.findings if f.id not in matched_ids and f not in [tt[0] for tt in trapped]]
        if extras:
            print(f"    Extras beyond GT ({len(extras)} findings, may be legit or noise):")
            for f in extras[:5]:
                print(f"      ? {f.id} ({f.severity[:5]}): {f.problem[:80]}")
            if len(extras) > 5:
                print(f"      ... and {len(extras) - 5} more")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("case_id")
    args = ap.parse_args()
    inspect(args.case_id)


if __name__ == "__main__":
    main()
