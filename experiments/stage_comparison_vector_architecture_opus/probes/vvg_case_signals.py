"""VVG — gate signals for every case in the ARM-1 manifest (artifacts/vv_cases.json).

Each case is materialized (description AFTER the mutation, exactly what the fact sheet
describes) and the zero-model-call gate signals are computed on it.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_case_signals
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv
from experiments.stage_comparison_vector_architecture_opus.probes import vvg_signals as sg

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"


def main() -> None:
    manifest = json.loads((ART / "vv_cases.json").read_text(encoding="utf-8"))
    rows = []
    for case in manifest["cases"]:
        started = time.time()
        description, sheet, gt = vv.materialize_case(case, manifest)
        row = {
            "case_id": case["case_id"],
            "block": case["block"],
            "pair_id": case["pair_id"],
            "side": case["side"],
            "mutation": case["mutation"],
            "family": case["family"],
            "synthetic": case["synthetic"],
            "disclose_limits": case["disclose_limits"],
            "corrupted": bool(gt.get("corrupted")),
            "expected_status": case.get("expected_status"),
            "acceptable_status": case.get("acceptable_status"),
            "changed_claims": case.get("changed_claims", []),
            "strength": case.get("strength"),
            "crop_png": case["crop_png"],
            "sheet_characters": sheet["characters"],
        }
        row.update(sg.compute_signals(description))
        row["signal_seconds"] = round(time.time() - started, 2)
        rows.append(row)
        print(f"{case['case_id']} {case['block']:<32} {case['mutation']:<17} "
              f"seg={row['segments']:>6} txt={row['text_items']:>4} "
              f"read={row['readable_text_ratio']:.3f} cap={row['retained_fraction_min']:.3f} "
              f"instab={row['group_instability']:.3f} unbound={row['unbound_text_ratio']:.3f} "
              f"({row['signal_seconds']}s)", flush=True)
    out = ART / "vvg_case_signals.json"
    out.write_text(json.dumps({"rows": rows}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {out} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
