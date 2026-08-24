"""VVG — measured cost of one vision verification, and the three policies extrapolated.

Sources: every verification record on disk (ARM 1's `vvd_runs/`, ARM 3's `vvg_runs/`,
and the two smoke calls in `vv_verify/`).  Gate cost comes from `vvg_gate_cost.json`
(measured signal computation over 34 real descriptions).

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_cost
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"


def records() -> list[dict]:
    out = []
    for pattern in ("vvd_runs/**/*.json", "vvg_runs/*.json", "vv_verify/*.json"):
        for path in sorted(ART.glob(pattern)):
            try:
                rec = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not rec.get("status"):
                continue
            usage = rec.get("usage_raw") or {}
            out.append({
                "source": path.parent.name,
                "id": rec.get("case_id") or rec.get("block") or path.stem,
                "status": rec["status"],
                "payload_tokens": rec.get("usage_payload_attributable"),
                "input": usage.get("input_tokens"),
                "cache_creation": usage.get("cache_creation_input_tokens"),
                "cache_read": usage.get("cache_read_input_tokens"),
                "output": usage.get("output_tokens"),
                "raw_total": sum(int(usage.get(k) or 0) for k in
                                 ("input_tokens", "cache_creation_input_tokens",
                                  "cache_read_input_tokens", "output_tokens")),
                "wall_seconds": rec.get("wall_seconds"),
                "duration_ms": rec.get("duration_ms"),
                "crop_bytes": rec.get("crop_bytes"),
                "sheet_chars": rec.get("fact_sheet_characters"),
            })
    return out


def stats(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return {"n": len(values), "mean": round(statistics.mean(values), 1),
            "median": round(statistics.median(values), 1),
            "min": round(min(values), 1), "max": round(max(values), 1),
            "p90": round(sorted(values)[int(0.9 * (len(values) - 1))], 1)}


def main() -> None:
    recs = records()
    gate = json.loads((ART / "vvg_gate_cost.json").read_text(encoding="utf-8"))["rows"]
    gate_s = [r["signals_s"] + r["load_s"] for r in gate]

    summary = {
        "verification_records": len(recs),
        "payload_tokens": stats([r["payload_tokens"] for r in recs]),
        "raw_reported_tokens": stats([r["raw_total"] for r in recs]),
        "cache_read_tokens": stats([r["cache_read"] for r in recs]),
        "output_tokens": stats([r["output"] for r in recs]),
        "wall_seconds": stats([r["wall_seconds"] for r in recs]),
        "gate_seconds_per_block": stats(gate_s),
        "gate_tokens_per_block": 0,
    }

    mean_tok = summary["payload_tokens"]["mean"]
    mean_raw = summary["raw_reported_tokens"]["mean"]
    mean_s = summary["wall_seconds"]["mean"]
    gate_mean = summary["gate_seconds_per_block"]["mean"]

    policies = {}
    for n_blocks in (30, 45, 60):
        row = {}
        row["a_verify_everything"] = {
            "vision_calls": n_blocks,
            "payload_tokens": round(mean_tok * n_blocks),
            "raw_reported_tokens": round(mean_raw * n_blocks),
            "serial_seconds": round(mean_s * n_blocks),
            "seconds_at_concurrency_6": round(mean_s * n_blocks / 6),
        }
        row["c_never_verify"] = {"vision_calls": 0, "payload_tokens": 0,
                                "raw_reported_tokens": 0, "serial_seconds": 0,
                                "seconds_at_concurrency_6": 0}
        row["b_gated"] = {}
        for frac in (0.15, 0.25, 0.33, 0.5):
            calls = round(n_blocks * frac)
            row["b_gated"][f"{int(frac*100)}%"] = {
                "vision_calls": calls,
                "payload_tokens": round(mean_tok * calls),
                "raw_reported_tokens": round(mean_raw * calls),
                "gate_seconds": round(gate_mean * n_blocks, 2),
                "serial_seconds": round(mean_s * calls + gate_mean * n_blocks),
                "seconds_at_concurrency_6": round(mean_s * calls / 6 + gate_mean * n_blocks),
            }
        policies[str(n_blocks)] = row

    out = {"summary": summary, "policies": policies, "records": recs}
    (ART / "vvg_cost.json").write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n",
                                       encoding="utf-8")
    print(json.dumps({"summary": summary, "policies": policies}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
