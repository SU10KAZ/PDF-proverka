"""falsify_ probe, attack A (text layer): a value PERMUTATION is invisible to v0.1.

comparator._text_diff scores the text layer as
    effective_similarity = max(multiset_f1, character_stream_similarity)
and compare_descriptions uses only that number. A multiset is invariant under any
permutation of the values, so any revision that MOVES a value from one device to
another - «номинал 250 А теперь стоит на другом вводе», a swapped riser number,
a swapped room label - leaves multiset_f1 at exactly 1.0. Because the code takes
the MAX of the two similarities, the character-stream signal that would have
caught it is discarded.

This probe takes REAL Track A block descriptions, applies the minimal realistic
perturbation (swap the strings of two same-category spans, everything else
untouched) and reports the resulting v0.1 numbers and status.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_text_permutation
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from experiments.stage_comparison_vector_blocks import comparator, extractor

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
ART = Path(__file__).resolve().parents[1] / "artifacts"


def main() -> None:
    rows = []
    for left_file in sorted(TRACK_A.glob("*/left/vector_block.json")):
        pair = left_file.parent.parent.name
        base = json.loads(left_file.read_text(encoding="utf-8"))
        texts = base["texts"]
        # pick two spans of the same category with different values, far apart
        pick = None
        for i, a in enumerate(texts):
            for b in texts[i + 1 :]:
                if a["category"] != b["category"] or a["text"] == b["text"]:
                    continue
                if abs(a["y_norm"] - b["y_norm"]) < 0.15:
                    continue
                pick = (a, b)
                break
            if pick:
                break
        if not pick:
            rows.append({"pair": pair, "swappable_pair_found": False})
            continue
        a, b = pick
        mutated = copy.deepcopy(base)
        ia = texts.index(a)
        ib = texts.index(b)
        mutated["texts"][ia]["text"], mutated["texts"][ib]["text"] = b["text"], a["text"]
        # keep the description internally consistent: recompute every derived layer
        prims = mutated["geometry"]["primitives"]
        mutated["primitive_summary"] = extractor._summary(prims, mutated["texts"], mutated["topology"])
        mutated["structural_signature"] = extractor._signatures(prims, mutated["texts"], mutated["topology"])
        mutated["size_metrics"] = extractor._size_metrics(mutated)
        diff = comparator._text_diff(base["texts"], mutated["texts"])
        cmp_ = comparator.compare_descriptions(base, mutated)
        rows.append(
            {
                "pair": pair,
                "swappable_pair_found": True,
                "swapped": [a["text"], b["text"]],
                "category": a["category"],
                "y_norm": [a["y_norm"], b["y_norm"]],
                "multiset_similarity": diff["similarity"],
                "character_stream_similarity": diff["character_stream_similarity"],
                "effective_similarity_used_by_status": diff["effective_similarity"],
                "value_changes_reported": len(diff["value_changes"]),
                "added": diff["added"],
                "removed": diff["removed"],
                "status": cmp_["status"],
                "text_layer_quality_left": diff["left_layer_quality"]["status"],
                "text_reliable": diff["reliable"],
                "level_1_signature_equal": cmp_["exact_vector_signature_equal"],
                "level_3_signature_equal": cmp_["structural_signature_equal"],
                "differences": cmp_["differences"],
            }
        )
    payload = {
        "note": "Controlled minimal perturbation of REAL v0.1 descriptions: two "
        "same-category text spans exchange their strings; geometry untouched.",
        "rows": rows,
    }
    (ART / "falsify_text_permutation.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print("%-24s %-10s %8s %8s %8s %-18s %s" % ("pair", "cat", "multiset", "stream", "used", "status", "swapped"))
    for r in rows:
        if not r["swappable_pair_found"]:
            print("%-24s (no swappable pair)" % r["pair"])
            continue
        print(
            "%-24s %-10s %8.4f %8.4f %8.4f %-18s %s"
            % (r["pair"], r["category"], r["multiset_similarity"],
               r["character_stream_similarity"], r["effective_similarity_used_by_status"],
               r["status"], r["swapped"])
        )
    print("wrote", ART / "falsify_text_permutation.json")


if __name__ == "__main__":
    main()
