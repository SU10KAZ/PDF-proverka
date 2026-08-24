"""Run the gap-fill probes against the real multimodal model.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvb_run \
        [--probes P01 P02] [--conditions A_OPEN C_CLOSED] [--workers 6] [--tag main]

Per-call records land in `artifacts/vvb_runs/<job_id>.json`; the roll-up in
`artifacts/vvb_results_<tag>.json`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vvb_gapfill as G

PROBES_JSON = G.ARTIFACTS / "vvb_probes.json"


def load_probes() -> list[dict]:
    return json.loads(PROBES_JSON.read_text(encoding="utf-8"))["probes"]


def build_jobs(probes, conditions, repeat: int = 1) -> list[dict]:
    jobs = []
    for probe in probes:
        for condition in conditions:
            for run in range(repeat):
                suffix = "" if repeat == 1 else f"_r{run + 1}"
                jobs.append({
                    "job_id": f"{probe['probe_id']}_{condition}{suffix}",
                    "probe_id": probe["probe_id"],
                    "condition": condition,
                    "crop_png": probe["crop_png"],
                    "prompt": G.build_prompt(probe, condition),
                })
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probes", nargs="*", default=None)
    parser.add_argument("--conditions", nargs="*", default=["A_OPEN", "B_OPEN_UNK", "C_CLOSED"])
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--tag", default="main")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    probes = load_probes()
    index = {p["probe_id"]: p for p in probes}
    if args.probes:
        probes = [index[pid] for pid in args.probes]
    jobs = build_jobs(probes, args.conditions, repeat=args.repeat)
    print(f"{len(jobs)} calls, workers={args.workers}", flush=True)
    records = G.run_batch(jobs, G.RUNS, workers=args.workers, timeout=args.timeout)

    rows = []
    for record in records:
        probe = index[record["probe_id"]]
        scored = G.classify(probe, record.get("answer"))
        rows.append({
            "job_id": record["job_id"],
            "probe_id": record["probe_id"],
            "family": probe["family"],
            "answerable": probe["answerable"],
            "block": probe["block"],
            "condition": record["condition"],
            "ok": record.get("ok"),
            "truth": probe["truth"],
            **scored,
            "usage_payload_attributable": record.get("usage_payload_attributable"),
            "usage_raw": record.get("usage_raw"),
            "wall_seconds": record.get("wall_seconds"),
        })
    rows.sort(key=lambda row: (row["probe_id"], row["condition"]))
    out = G.ARTIFACTS / f"vvb_results_{args.tag}.json"
    out.write_text(json.dumps({"schema": "vvb-results-v1", "rows": rows}, ensure_ascii=False,
                              indent=2) + "\n", encoding="utf-8")
    print(f"-> {out.relative_to(G.ROOT)}")
    for row in rows:
        print(f"  {row['job_id']:22s} {row['outcome']:20s} conf={str(row['confidence']):6s} "
              f"{str(row['raw'])[:48]!r}")


if __name__ == "__main__":
    main()
