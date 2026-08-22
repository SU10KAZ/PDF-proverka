#!/usr/bin/env python3
"""Probe HYBRID-1: exact token composition of Track A's vector AI prompt.

Rebuilds the payload with Track A's own `_vector_prompt` builder, then measures
o200k_base tokens for every sub-tree so we can say WHERE the 70,631-token call
went. Run from repo root with a python that has tiktoken installed:

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p1_prompt_composition
"""
from __future__ import annotations

import json
from pathlib import Path

import tiktoken

from experiments.stage_comparison_vector_blocks import run_ai_experiment as rae

ENC = tiktoken.get_encoding("o200k_base")
OUT = Path(__file__).resolve().parents[1] / "artifacts"
SEP = (",", ":")


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=SEP)


def tok(obj) -> int:
    return len(ENC.encode(dumps(obj) if not isinstance(obj, str) else obj))


def build_payloads() -> list[dict]:
    """Re-run Track A's builder but capture the per-pair dicts, not the string."""
    payloads = []
    A = rae.ARTIFACT_DIR
    for pair_id in rae.PAIR_IDS:
        left = rae._load(A / "descriptions" / pair_id / "left" / "vector_block.json")
        right = rae._load(A / "descriptions" / pair_id / "right" / "vector_block.json")
        comparison = rae._load(A / "comparisons" / pair_id / "comparison.json")
        payloads.append(
            {
                "pair_id": pair_id,
                "left_level_3": left["size_metrics"]["compact_payload"],
                "right_level_3": right["size_metrics"]["compact_payload"],
                "deterministic_diff": {
                    "status": comparison["status"],
                    "geometry": {
                        "similarity": comparison["geometry"]["similarity"],
                        "selected_tolerance": comparison["geometry"]["selected_tolerance"],
                        "left_coverage": comparison["geometry"]["left_coverage"],
                        "right_coverage": comparison["geometry"]["right_coverage"],
                        "encoding_rewrite_suspected": comparison["geometry"]["encoding_rewrite_suspected"],
                        "tolerance_experiment": [
                            {k: run[k] for k in ("tolerance", "similarity", "left_coverage",
                                                 "right_coverage", "left_used", "right_used", "capped")}
                            for run in comparison["geometry"]["tolerance_experiment"]
                        ],
                    },
                    "text": {
                        k: comparison["text"][k]
                        for k in ("similarity", "character_stream_similarity", "effective_similarity",
                                  "reliable", "left_layer_quality", "right_layer_quality",
                                  "removed", "added", "value_changes", "truncated")
                    },
                    "topology": comparison["topology"],
                    "repeated_patterns": comparison["repeated_patterns"],
                    "differences": comparison["differences"],
                    "caveats": comparison["caveats"],
                },
            }
        )
    return payloads


BUCKETS = {
    # bucket -> list of (path within payload)
    "L3.texts (positioned text list, capped 100)": [("left_level_3", "texts"), ("right_level_3", "texts")],
    "L3.signatures (sha256 hashes + degree hist)": [("left_level_3", "signatures"), ("right_level_3", "signatures")],
    "L3.topology": [("left_level_3", "topology"), ("right_level_3", "topology")],
    "L3.summary": [("left_level_3", "summary"), ("right_level_3", "summary")],
    "L3.patterns (repeated-element fingerprints)": [("left_level_3", "patterns"), ("right_level_3", "patterns")],
    "L3.hatch_candidates": [("left_level_3", "hatch_candidates"), ("right_level_3", "hatch_candidates")],
    "L3.quality": [("left_level_3", "quality"), ("right_level_3", "quality")],
    "diff.text.added/removed (unmatched id lists)": [("deterministic_diff", "text", "added"),
                                                     ("deterministic_diff", "text", "removed")],
    "diff.text.value_changes": [("deterministic_diff", "text", "value_changes")],
    "diff.text.* scalars + layer_quality": [("deterministic_diff", "text", "similarity"),
                                            ("deterministic_diff", "text", "character_stream_similarity"),
                                            ("deterministic_diff", "text", "effective_similarity"),
                                            ("deterministic_diff", "text", "reliable"),
                                            ("deterministic_diff", "text", "truncated"),
                                            ("deterministic_diff", "text", "left_layer_quality"),
                                            ("deterministic_diff", "text", "right_layer_quality")],
    "diff.geometry.tolerance_experiment (table)": [("deterministic_diff", "geometry", "tolerance_experiment")],
    "diff.geometry.* scalars": [("deterministic_diff", "geometry", "similarity"),
                                ("deterministic_diff", "geometry", "selected_tolerance"),
                                ("deterministic_diff", "geometry", "left_coverage"),
                                ("deterministic_diff", "geometry", "right_coverage"),
                                ("deterministic_diff", "geometry", "encoding_rewrite_suspected")],
    "diff.topology": [("deterministic_diff", "topology")],
    "diff.repeated_patterns (pattern-id diagnostics)": [("deterministic_diff", "repeated_patterns")],
    "diff.differences (human-readable lines)": [("deterministic_diff", "differences")],
    "diff.caveats (boilerplate, repeated 5x)": [("deterministic_diff", "caveats")],
    "diff.status": [("deterministic_diff", "status")],
}


def dig(obj, path):
    for key in path:
        obj = obj[key]
    return obj


def main() -> None:
    payloads = build_payloads()
    prompt = rae._vector_prompt()
    on_disk = (rae.AI_DIR / "vector_prompt.txt").read_text(encoding="utf-8")

    result = {
        "prompt_rebuild_matches_artifact": prompt == on_disk,
        "prompt_characters": len(prompt),
        "prompt_bytes_utf8": len(prompt.encode("utf-8")),
        "prompt_tokens_o200k": len(ENC.encode(prompt)),
        "reported_total_tokens_vector_arm": 70631,
        "reported_total_tokens_vision_arm": 38069,
        "instructions_tokens": tok(rae._base_prompt())
        + tok("Используй только приведённые Level 3 vector descriptions и deterministic diff; картинок нет.")
        + tok("Числа координат и подписи можно повторять только как evidence из входа."),
        "per_pair": {},
        "buckets": {},
    }

    bucket_tot = {name: 0 for name in BUCKETS}
    for payload in payloads:
        pid = payload["pair_id"]
        pair_tokens = tok(payload)
        per_bucket = {}
        for name, paths in BUCKETS.items():
            n = 0
            for path in paths:
                try:
                    n += tok(dig(payload, path))
                except KeyError:
                    pass
            per_bucket[name] = n
            bucket_tot[name] += n
        result["per_pair"][pid] = {
            "tokens_total": pair_tokens,
            "chars": len(dumps(payload)),
            "buckets": per_bucket,
            "accounted": sum(per_bucket.values()),
            "text_items_left": len(payload["left_level_3"]["texts"]),
            "text_items_right": len(payload["right_level_3"]["texts"]),
            "added_items": len(payload["deterministic_diff"]["text"]["added"]),
            "removed_items": len(payload["deterministic_diff"]["text"]["removed"]),
        }
    total_pairs = sum(v["tokens_total"] for v in result["per_pair"].values())
    result["pairs_tokens_total"] = total_pairs
    for name, n in sorted(bucket_tot.items(), key=lambda kv: -kv[1]):
        result["buckets"][name] = {
            "tokens": n,
            "pct_of_pair_payload": round(100.0 * n / total_pairs, 2),
        }
    result["buckets_accounted_tokens"] = sum(bucket_tot.values())
    result["buckets_unaccounted_tokens"] = total_pairs - sum(bucket_tot.values())

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_prompt_composition.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({k: v for k, v in result.items() if k != "per_pair"}, ensure_ascii=False, indent=2))
    for pid, v in result["per_pair"].items():
        print(pid, v["tokens_total"], "chars", v["chars"], "texts", v["text_items_left"], v["text_items_right"])


if __name__ == "__main__":
    main()
