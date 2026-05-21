"""Noise audit — sample N findings labelled FP and categorise them.

Categories:
  - real_fp        — speculative / no evidence / wrong scope.
  - beyond_gt      — substantive, evidence-backed engineering observation
                     not in GT but useful.
  - dup_same_class — duplicate of another finding by class.
  - severity_inflate — substance OK but КРИТ should be lower.

Produces a CSV-like file `algorithm_research/results/_noise_audit.json`.

This script is a STRUCTURED INSPECTION SCAFFOLD — not an LLM-driven
classifier. It expects a human reviewer to fill in the category labels.
"""
from __future__ import annotations

import argparse
import json
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def collect_unmatched_findings(run_path: Path, gt_path: Path, threshold: float = 0.45) -> list[dict]:
    """Return findings that don't match any GT — candidates for FP review."""
    data = json.loads(run_path.read_text(encoding="utf-8"))
    gt = json.loads(gt_path.read_text(encoding="utf-8"))
    real_gt = [g for g in gt.get("expected_findings", []) if not g.get("is_trap")]
    out = []
    for f in data.get("findings", []):
        best = 0.0
        for g in real_gt:
            sim = SequenceMatcher(None,
                                  (f.get("problem", "") + " " + f.get("description", "")).lower(),
                                  g.get("description", "").lower(),
                                  autojunk=False).ratio()
            best = max(best, sim)
        if best < threshold:
            out.append({
                "id": f.get("id"),
                "severity": f.get("severity"),
                "category": f.get("category"),
                "problem_class": f.get("problem_class"),
                "affected_system": f.get("affected_system"),
                "problem": f.get("problem"),
                "evidence_quote": (f.get("evidence_quote") or "")[:200],
                "best_sim_to_gt": round(best, 2),
                "label": "TODO",
            })
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", required=True)
    ap.add_argument("--gt", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    items = collect_unmatched_findings(Path(args.run), Path(args.gt))
    out = Path(args.out) if args.out else Path(args.run).with_suffix(".noise.json")
    out.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(items)} unmatched findings for inspection -> {out}")


if __name__ == "__main__":
    main()
