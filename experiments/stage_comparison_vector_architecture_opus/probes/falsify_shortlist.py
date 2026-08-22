"""falsify_ probe, step 2: turn the corpus scan into A/B falsification candidates.

Reads artifacts/falsify_corpus_scan.json and prints/writes shortlists:

  A_localized : text multiset identical between versions, page geometry almost
                identical (geom_jaccard >= 0.97) but NOT identical -> a real,
                localized, graphics-only revision.  These are the cases where
                v0.1's dilution problem bites.

  B_repackaged: text multiset identical, page geometry strongly different
                (geom_jaccard <= 0.85) while the number of drawing primitives
                stays in the same ballpark -> candidate re-export / repackaging.

  B_outlined  : text words collapse (>=80% fewer) while geometry explodes
                -> text converted to outlines.

  B_reflow    : page size changed -> scale/format change.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_shortlist
"""
from __future__ import annotations

import json
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"


def main() -> None:
    data = json.loads((ART / "falsify_corpus_scan.json").read_text(encoding="utf-8"))
    a_local, b_repack, b_outline, b_size, all_pages = [], [], [], [], []
    for res in data["results"]:
        if "pages" not in res:
            continue
        for p in res["pages"]:
            row = dict(p)
            row["doc"] = res["doc"]
            row["pair"] = res["pair"]
            all_pages.append(row)
            if not row["size_same"]:
                b_size.append(row)
                continue
            wl, wr = row["n_words"]
            if wl >= 50 and wr <= wl * 0.2 and row["n_segs"][1] > row["n_segs"][0] * 1.3:
                b_outline.append(row)
                continue
            if wl >= 50 and wl <= wr * 0.2 and row["n_segs"][0] > row["n_segs"][1] * 1.3:
                b_outline.append(row)
                continue
            if not row["text_same"]:
                continue
            j = row["geom_jaccard"]
            if 0.90 <= j < 0.99999 and min(row["n_segs"]) > 500:
                a_local.append(row)
            elif j <= 0.85 and min(row["n_segs"]) > 500:
                b_repack.append(row)
    a_local.sort(key=lambda r: -r["geom_jaccard"])
    b_repack.sort(key=lambda r: r["geom_jaccard"])
    out = {
        "scanned_pages": len(all_pages),
        "docs": data["scanned_docs"],
        "counts": {
            "A_localized_graphics_only": len(a_local),
            "B_repackaged": len(b_repack),
            "B_text_outlined": len(b_outline),
            "B_page_size_changed": len(b_size),
        },
        "A_localized": a_local[:60],
        "B_repackaged": b_repack[:60],
        "B_text_outlined": b_outline[:40],
        "B_page_size_changed": b_size[:40],
    }
    (ART / "falsify_shortlist.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(json.dumps(out["counts"], indent=1))
    print("\nA_localized (text identical, geometry almost but not quite identical)")
    for r in a_local[:20]:
        print(
            "  %-58s %s p%-3d jac=%.5f segs=%s paths=%s"
            % (r["doc"][-58:], r["pair"], r["page_index"], r["geom_jaccard"], r["n_segs"], r["paths"])
        )
    print("\nB_repackaged (text identical, geometry strongly different)")
    for r in b_repack[:20]:
        print(
            "  %-58s %s p%-3d jac=%.5f segs=%s paths=%s ops=%s"
            % (r["doc"][-58:], r["pair"], r["page_index"], r["geom_jaccard"], r["n_segs"], r["paths"],
               [(o["l"], o["c"], o["re"]) for o in r["ops"]])
        )
    print("\nB_text_outlined")
    for r in b_outline[:20]:
        print("  %-58s %s p%-3d words=%s segs=%s" % (r["doc"][-58:], r["pair"], r["page_index"], r["n_words"], r["n_segs"]))
    print("wrote", ART / "falsify_shortlist.json")


if __name__ == "__main__":
    main()
