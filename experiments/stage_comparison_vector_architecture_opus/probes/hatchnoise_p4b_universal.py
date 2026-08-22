"""P4b — is there a SINGLE threshold setting that is safe on every sheet at once?

Reads the grids written by hatchnoise_p4_transfer and searches for settings whose
worst-case foreground loss over all ground-truth blocks stays under a bound.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p4b_universal
"""
from __future__ import annotations

import json
import statistics

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C

SRC = C.ART / "hatchnoise_p4_transfer.json"
OUT = C.ART / "hatchnoise_p4_universal.json"


def main() -> None:
    data = json.loads(SRC.read_text(encoding="utf-8"))
    blocks = data["blocks"]
    names = list(blocks)
    keyed = {
        name: {
            (g["p1_min_support"], g["p2_motif_count"], g["p3_luminance"], g["p4_min_length"]): g
            for g in blocks[name]["grid"]
        }
        for name in names
    }
    rows = []
    for key in keyed[names[0]]:
        fg = [keyed[n][key]["foreground_eaten_frac"] for n in names]
        bg = [keyed[n][key]["background_removed_frac"] for n in names]
        rows.append({"setting": list(key), "max_fg_eaten": max(fg), "mean_bg_removed": statistics.mean(bg),
                     "per_block_bg_removed": bg, "per_block_fg_eaten": fg})

    analysis = []
    for bound in (0.01, 0.02, 0.05, 0.10):
        safe = sorted([r for r in rows if r["max_fg_eaten"] <= bound], key=lambda r: -r["mean_bg_removed"])
        analysis.append({
            "max_fg_eaten_allowed": bound,
            "n_settings": len(safe),
            "n_settings_total": len(rows),
            "best": safe[0] if safe else None,
        })

    oracle = {}
    for name in names:
        candidates = [g for g in blocks[name]["grid"] if g["foreground_eaten_frac"] <= 0.01]
        candidates.sort(key=lambda g: -g["background_removed_frac"])
        oracle[name] = candidates[0]["background_removed_frac"] if candidates else 0.0

    payload = {
        "probe": "hatchnoise_p4b_universal",
        "blocks": names,
        "universal_safe_analysis": analysis,
        "per_sheet_oracle_bg_removed": oracle,
        "per_sheet_oracle_mean": round(statistics.mean(oracle.values()), 4),
        "per_sheet_oracle_zero_blocks": sum(1 for v in oracle.values() if v == 0.0),
    }
    C.write_json(OUT, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=1)[:2500])


if __name__ == "__main__":
    main()
