"""VVG — is the verification LABEL itself reproducible?

Re-runs the identical call (same crop, same fact sheet, same prompt) on a subset of the
fresh blocks and compares the status.  Any gate can only be as good as the stability of
the thing it is asked to predict.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_repeat
"""
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
RUNS = ART / "vvg_runs"
REP = ART / "vvg_runs_rep2"

SUBSET = [
    "fmc_eom_room_schedule_values__left",   # VERIFIED
    "fmc_kj_spec_table_reflow__left",       # VERIFIED
    "fmc_ar_hatch_sections__left",          # PARTIAL
    "fmc_ov_block_split_widened__left",     # PARTIAL
    "fmc_km_broken_text_swap__left",        # PARTIAL
    "fmc_tx_raster_scan__left",             # FAILED
    "fmc_vk_spec_positions__left",          # FAILED
    "fmc_eom_cable_table_values__left",     # FAILED
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    REP.mkdir(parents=True, exist_ok=True)
    jobs = []
    for name in SUBSET:
        out = REP / f"{name}.json"
        if out.exists():
            continue
        first = json.loads((RUNS / f"{name}.json").read_text(encoding="utf-8"))
        jobs.append((name, first, out))

    def run(job):
        name, first, out = job
        rec = vv.verify(first["crop_png"], first["fact_sheet"], timeout=420, retries=1)
        rec["block"] = name
        rec["family"] = "fresh_control"
        rec["mutation"] = "clean"
        rec["repeat_of_status"] = first["status"]
        out.write_text(json.dumps(rec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        same = rec.get("status") == first["status"]
        print(f"{name}: run1={first['status']} run2={rec.get('status')} "
              f"{'SAME' if same else 'DIFFERENT'} payload={rec.get('usage_payload_attributable')}",
              flush=True)
        return rec

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(run, jobs))
    print("done")


if __name__ == "__main__":
    main()
