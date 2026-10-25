#!/usr/bin/env python3
"""Offline comparison of Mapper plans. Real evidence requires the frozen DEV guard.

No provider construction, credentials, model calls, or production writes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.project_change_v3 import contracts
from backend.app.services.project_change_v3.mapper_planning import MapperPlanner
from backend.app.services.project_change_v3.source_prep import prepare_comparison_sources
from backend.app.services.project_change_v3.transport import TransportIntegrityError
from experiments.project_change_272 import policy


def synthetic_structure():
    return [{"side": side, "physical_page": number, "blocks": [
        {"block_id": f"{side}_{number}", "modality": "TEXT", "md_block_type": "TEXT",
         "text_source": "STRUCTURED_MD", "bbox": [0, 0, 1, 1], "tables": [], "graphic_crop_ref": "",
         "structured_md": f"{side} {number}: " + "Синтетический текст. " * 900}]
    } for side, count in (("OLD", 8), ("NEW", 24)) for number in range(1, count + 1)]


def run(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    datasets = []
    if args.synthetic:
        datasets.append(("synthetic", synthetic_structure(), {"synthetic": True}))
    else:
        kwargs = {"root": Path(args.corpus_root)} if args.corpus_root else {}
        for pair in policy.admitted_pairs("DEV", indices=args.dev_pair, **kwargs):
            if any(pair["embargo_pages"].values()):
                raise PermissionError(f"pair {pair['index']} has embargoed pages; full-corpus planning refused")
            paths = {side: {"pdf": Path(pair[side]["artifacts"]["pdf"]["path"]),
                            "blocks": Path(pair[side]["artifacts"]["blocks"]["path"]),
                            "markdown": Path(pair[side]["artifacts"]["work_md"]["path"])}
                     for side in ("old", "new")}
            prepared = prepare_comparison_sources(pair_id=pair["pair_key"], old_paths=paths["old"],
                                                  new_paths=paths["new"], object_id="272",
                                                  work_dir=output / f"pair_{pair['index']}" / "sources")
            datasets.append((pair["pair_key"], prepared["structure"],
                             {"index": pair["index"], "partition": "DEV", "manifest": prepared["manifest"]}))
    reports = []
    for pair_id, structure, admission in datasets:
        for profile_key in args.profile or ["astra", "opus55"]:
            planner = MapperPlanner(pair_id, structure, contracts.MODEL_PROFILES[profile_key])
            selection = planner.plan()
            selection.report["final_packets"] = planner.verify(selection.portions)
            reports.append({"pair_id": pair_id, "admission": admission, **selection.report})
    report = {"status": "planned", "model_calls": 0, "quality_validation": "not_evaluated", "reports": reports}
    (output / "planning.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    lines = ["ProjectChange: локальное планирование, 0 вызовов моделей.",
             "Токены оценочные. Качество сопоставления этим отчётом не проверяется."]
    for r in reports:
        lines.append(f"\n{r['pair_id']} / {r['profile']['key']}: выбран {r['selected']}")
        for row in r["candidates"]:
            if row["status"] == "feasible":
                lines.append(f"  {row['name']}: {row['calls']} вызовов, {row['estimated_input_tokens']:,} входных токенов")
            else:
                lines.append(f"  {row['name']}: {row['status']} {row.get('error', '')}")
    summary = "\n".join(lines) + "\n"
    (output / "planning.txt").write_text(summary)
    print(summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--synthetic", action="store_true")
    sources.add_argument("--dev-pair", type=int, nargs="+")
    parser.add_argument("--corpus-root", help="Directory with the unchanged frozen split and inventory")
    parser.add_argument("--profile", action="append", choices=["astra", "opus55"])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        run(args)
    except (OSError, ValueError, TransportIntegrityError) as exc:
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        refusal = {"status": "refused", "model_calls": 0, "error": str(exc)}
        (output / "planning_refusal.json").write_text(json.dumps(refusal, ensure_ascii=False, indent=2) + "\n")
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
