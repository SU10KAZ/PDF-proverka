#!/usr/bin/env python3
"""Probe HYBRID-4: how much of Track A's vector prompt is provably non-informative.

Three provably-wasted classes, measured in o200k tokens:
  W1 undecodable text  — spans the extractor's own layer_quality rule calls control-char
                          garbage, plus every diff entry derived from them;
  W2 opaque identifiers — sha256 signatures and pattern_xxxxxxxx ids the model cannot
                          reason about (no cross-side meaning; see pattern-stability probe);
  W3 method telemetry   — tolerance_experiment rows, caveats repeated per pair, primitive
                          counts the Track A report itself calls packaging noise.

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p4_wasted_tokens
"""
from __future__ import annotations

import json
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import (
    hybrid_p1_prompt_composition as p1,
)

OUT = Path(__file__).resolve().parents[1] / "artifacts"


def suspicious(s: str) -> int:
    return sum(1 for c in s if ord(c) < 32 and not c.isspace())


def garbage(s: str) -> bool:
    return suspicious(s) >= 1


def main() -> None:
    payloads = p1.build_payloads()
    rows = {}
    tot = {"W1_undecodable": 0, "W2_opaque_ids": 0, "W3_method_telemetry": 0, "payload": 0}
    for pl in payloads:
        pid = pl["pair_id"]
        w1 = 0
        for side in ("left_level_3", "right_level_3"):
            bad = [t for t in pl[side]["texts"] if garbage(t[0])]
            w1 += p1.tok(bad)
        d = pl["deterministic_diff"]
        bad_added = [s for s in d["text"]["added"] if garbage(s)]
        bad_removed = [s for s in d["text"]["removed"] if garbage(s)]
        bad_vc = [v for v in d["text"]["value_changes"] if garbage(v.get("left", "")) or garbage(v.get("right", ""))]
        w1 += p1.tok(bad_added) + p1.tok(bad_removed) + p1.tok(bad_vc)

        w2 = 0
        for side in ("left_level_3", "right_level_3"):
            sig = pl[side]["signatures"]
            w2 += p1.tok({k: v for k, v in sig.items() if isinstance(v, str)})
            w2 += p1.tok(pl[side]["patterns"])
        w2 += p1.tok(d["repeated_patterns"])

        w3 = (
            p1.tok(d["geometry"]["tolerance_experiment"])
            + p1.tok(d["caveats"])
            + p1.tok([x for x in d["differences"] if x.startswith("Число примитивов")])
            + p1.tok([pl[s]["hatch_candidates"] for s in ("left_level_3", "right_level_3")])
        )
        total = p1.tok(pl)
        rows[pid] = {
            "payload_tokens": total,
            "W1_undecodable": w1,
            "W2_opaque_ids": w2,
            "W3_method_telemetry": w3,
            "wasted_total": w1 + w2 + w3,
            "wasted_pct": round(100.0 * (w1 + w2 + w3) / total, 1),
            "n_value_changes": len(d["text"]["value_changes"]),
            "n_value_changes_on_garbage": len(bad_vc),
            "left_layer": d["text"]["left_layer_quality"]["status"],
            "right_layer": d["text"]["right_layer_quality"]["status"],
        }
        tot["W1_undecodable"] += w1
        tot["W2_opaque_ids"] += w2
        tot["W3_method_telemetry"] += w3
        tot["payload"] += total
    tot["wasted_total"] = tot["W1_undecodable"] + tot["W2_opaque_ids"] + tot["W3_method_telemetry"]
    tot["wasted_pct"] = round(100.0 * tot["wasted_total"] / tot["payload"], 1)
    res = {"per_pair": rows, "totals": tot}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_wasted_tokens.json").write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
