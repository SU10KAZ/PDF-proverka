"""Phase 0 dedup runner — applies class_dedup and fuzzy_dedup retroactively
to A0_baseline_current__baseline outputs and saves three variants:

  - A0_phase0_classdedup__baseline    — collapse_to_canonical only
  - A0_phase0_fuzzydedup__baseline    — fuzzy_dedup (sim 0.7) only
  - A0_phase0_combined__baseline      — class then fuzzy (production-like)

No LLM calls. Pure Python post-processing.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
RESEARCH = HERE.parents[1]
sys.path.insert(0, str(RESEARCH))

from runners._common import RESULTS_DIR, make_run_result, coerce_finding  # noqa: E402
from runners.class_dedup import (  # noqa: E402
    collapse_to_canonical, fuzzy_dedup, Finding,
)

SRC = RESULTS_DIR / "A0_baseline_current__baseline"
OUT_CLASS = RESULTS_DIR / "A0_phase0_classdedup__baseline"
OUT_FUZZY = RESULTS_DIR / "A0_phase0_fuzzydedup__baseline"
OUT_BOTH = RESULTS_DIR / "A0_phase0_combined__baseline"


def _to_findings(rows: list[dict]) -> list[Finding]:
    out: list[Finding] = []
    for i, r in enumerate(rows, start=1):
        try:
            out.append(coerce_finding(r, i, source_agent=r.get("source_agent", "")))
        except Exception:
            continue
    return out


def process(case_id: str, src: Path, skip_existing: bool) -> dict:
    data = json.loads(src.read_text(encoding="utf-8"))
    findings = data.get("findings") or []
    duration = float(data.get("duration_sec") or 0.0)
    discipline = data.get("discipline", "")

    # 1) class dedup
    class_out, class_rep = collapse_to_canonical(findings)
    # 2) fuzzy dedup
    fuzzy_out, fuzzy_rep = fuzzy_dedup(findings, sim_threshold=0.7)
    # 3) combined: class first, then fuzzy
    combo_step1, _ = collapse_to_canonical(findings)
    combo_out, combo_rep = fuzzy_dedup(combo_step1, sim_threshold=0.7)

    res = {}
    for out_dir, name, items, rep in [
        (OUT_CLASS, "A0_phase0_classdedup__baseline", class_out, class_rep),
        (OUT_FUZZY, "A0_phase0_fuzzydedup__baseline", fuzzy_out, fuzzy_rep),
        (OUT_BOTH, "A0_phase0_combined__baseline",  combo_out, combo_rep),
    ]:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{case_id}.json"
        if skip_existing and out_path.exists():
            res[name] = {"used_cache": True, "n": len(items)}
            continue
        f_objs = _to_findings(items)
        bundle = make_run_result(
            method=name,
            case_id=case_id,
            discipline=discipline,
            findings=f_objs,
            duration=duration,
            meta={
                "source_method": "A0_baseline_current__baseline",
                "original_count": len(findings),
                "post_dedup_count": len(items),
                "dedup_report": rep.__dict__,
            },
            errors=[],
        )
        out_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
        res[name] = {"used_cache": False, "n": len(items),
                     "drops": rep.same_class_drops}
    res["original"] = len(findings)
    return res


def main():
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--case", help="Single case")
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    sources = sorted(SRC.glob("*.json")) if not args.case else [SRC / f"{args.case}.json"]
    summary = {}
    for src in sources:
        if not src.exists():
            print(f"  skip {src.name}: no A0 source", file=sys.stderr)
            continue
        case_id = src.stem
        r = process(case_id, src, args.skip_existing)
        summary[case_id] = r
        print(f"  {case_id:35s} orig={r['original']:>3}"
              f" -> class={r['A0_phase0_classdedup__baseline']['n']:>3}"
              f"  fuzzy={r['A0_phase0_fuzzydedup__baseline']['n']:>3}"
              f"  combined={r['A0_phase0_combined__baseline']['n']:>3}")
    return summary


if __name__ == "__main__":
    main()
