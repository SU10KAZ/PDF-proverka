"""Roll the gap-fill results up into the rate tables ARM 4 reports.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvb_analyze [--tag main]
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vvb_gapfill as G

ORDER = ["A_OPEN", "B_OPEN_UNK", "C_CLOSED", "D_READFLAG"]


def load(tag: str) -> list[dict]:
    path = G.ARTIFACTS / f"vvb_results_{tag}.json"
    return json.loads(path.read_text(encoding="utf-8"))["rows"]


def summarise(rows: list[dict]) -> dict:
    probes = json.loads((G.ARTIFACTS / "vvb_probes.json").read_text(encoding="utf-8"))["probes"]
    index = {p["probe_id"]: p for p in probes}
    by_condition = collections.defaultdict(list)
    for row in rows:
        by_condition[row["condition"]].append(row)

    out: dict = {"schema": "vvb-summary-v1", "n_rows": len(rows), "conditions": {},
                 "by_family": {}, "per_probe": {}, "cost": {}}

    for condition in ORDER:
        block = by_condition.get(condition, [])
        unanswerable = [r for r in block if not r["answerable"]]
        answerable = [r for r in block if r["answerable"]]
        invented = [r for r in unanswerable if r["outcome"] == "INVENTED"]
        refused = [r for r in unanswerable if r["outcome"] == "REFUSED"]
        correct = [r for r in answerable if r["outcome"] == "CORRECT"]
        wrong = [r for r in answerable if r["outcome"] == "WRONG_ON_LEGIBLE"]
        over_refused = [r for r in answerable if r["outcome"] == "REFUSED_ON_LEGIBLE"]
        out["conditions"][condition] = {
            "unanswerable_n": len(unanswerable),
            "invented": len(invented),
            "refused": len(refused),
            "no_answer": len([r for r in unanswerable if r["outcome"] == "NO_ANSWER"]),
            "invention_rate": round(len(invented) / max(1, len(unanswerable)), 4),
            "invented_with_high_confidence": len([r for r in invented if r["confidence"] == "high"]),
            "invented_matching_hidden_truth": len([r for r in invented if r["matches_hidden_truth"]]),
            "invented_but_prose_admits_not_read": len([r for r in invented if r["admits_not_read"]]),
            "invented_with_read_flag_true": len(
                [r for r in invented if r.get("read_from_picture") is True]),
            "invented_with_read_flag_false": len(
                [r for r in invented if r.get("read_from_picture") is False]),
            "correct_with_read_flag_true": len(
                [r for r in correct if r.get("read_from_picture") is True]),
            "answerable_n": len(answerable),
            "correct": len(correct),
            "wrong_on_legible": len(wrong),
            "refused_on_legible": len(over_refused),
            "fill_accuracy": round(len(correct) / max(1, len(answerable)), 4),
            "correct_but_not_byte_identical": len(
                [r for r in correct if not r.get("exact_match_vector_string")]),
            "invented_ids": sorted(r["probe_id"] for r in invented),
            "wrong_ids": sorted(r["probe_id"] for r in wrong),
            "over_refused_ids": sorted(r["probe_id"] for r in over_refused),
        }

    families = sorted({r["family"] for r in rows})
    for family in families:
        row_family = {}
        for condition in ORDER:
            block = [r for r in by_condition.get(condition, []) if r["family"] == family]
            if not block:
                continue
            if family == "answerable":
                row_family[condition] = {
                    "n": len(block),
                    "correct": len([r for r in block if r["outcome"] == "CORRECT"]),
                    "wrong": len([r for r in block if r["outcome"] == "WRONG_ON_LEGIBLE"]),
                    "refused": len([r for r in block if r["outcome"] == "REFUSED_ON_LEGIBLE"]),
                }
            else:
                invented = [r for r in block if r["outcome"] == "INVENTED"]
                row_family[condition] = {
                    "n": len(block), "invented": len(invented),
                    "rate": round(len(invented) / len(block), 3),
                }
        out["by_family"][family] = row_family

    for probe_id in sorted(index):
        probe = index[probe_id]
        entry = {"family": probe["family"], "block": probe["block"],
                 "truth": probe["truth"], "answerable": probe["answerable"]}
        for condition in ORDER:
            match = [r for r in by_condition.get(condition, []) if r["probe_id"] == probe_id]
            if match:
                row = match[0]
                entry[condition] = {"outcome": row["outcome"], "raw": row["raw"],
                                    "confidence": row["confidence"],
                                    "admits_not_read": row["admits_not_read"],
                                    "read_from_picture": row.get("read_from_picture"),
                                    "evidence": row["evidence"]}
        out["per_probe"][probe_id] = entry

    payload = [r["usage_payload_attributable"] for r in rows if r.get("usage_payload_attributable")]
    walls = [r["wall_seconds"] for r in rows if r.get("wall_seconds")]
    raw_totals = []
    for row in rows:
        usage = row.get("usage_raw") or {}
        if usage:
            raw_totals.append(sum(int(usage.get(k, 0)) for k in
                                  ("input_tokens", "cache_creation_input_tokens",
                                   "cache_read_input_tokens", "output_tokens")))
    out["cost"] = {
        "calls": len(rows),
        "payload_attributable_tokens_total": sum(payload),
        "payload_attributable_tokens_mean": round(sum(payload) / max(1, len(payload)), 1),
        "payload_attributable_tokens_min": min(payload) if payload else None,
        "payload_attributable_tokens_max": max(payload) if payload else None,
        "raw_reported_tokens_total": sum(raw_totals),
        "wall_seconds_mean": round(sum(walls) / max(1, len(walls)), 1),
        "wall_seconds_max": max(walls) if walls else None,
        "note": ("payload-attributable = input + cache_creation + output; cache_read is dominated by "
                 "the Claude Code system prompt (~50k/call) and is not attributable to our payload"),
    }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="main")
    args = parser.parse_args()
    rows = load(args.tag)
    summary = summarise(rows)
    out = G.ARTIFACTS / f"vvb_summary_{args.tag}.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"{'condition':12s} {'inv/16':>8s} {'rate':>6s} {'hiconf':>7s} {'prose-admits':>13s} "
          f"{'correct/8':>10s} {'wrong':>6s} {'over-refused':>13s}")
    for condition in ORDER:
        data = summary["conditions"].get(condition)
        if not data:
            continue
        print(f"{condition:12s} {data['invented']:3d}/{data['unanswerable_n']:<4d} "
              f"{data['invention_rate']:6.3f} {data['invented_with_high_confidence']:7d} "
              f"{data['invented_but_prose_admits_not_read']:13d} "
              f"{data['correct']:5d}/{data['answerable_n']:<4d} {data['wrong_on_legible']:6d} "
              f"{data['refused_on_legible']:13d}")
    print()
    for family, block in summary["by_family"].items():
        cells = "  ".join(
            f"{c}={block[c].get('rate', block[c].get('correct'))}" for c in ORDER if c in block)
        print(f"  {family:14s} {cells}")
    print()
    print(json.dumps(summary["cost"], ensure_ascii=False, indent=2))
    print(f"-> {out.relative_to(G.ROOT)}")


if __name__ == "__main__":
    main()
