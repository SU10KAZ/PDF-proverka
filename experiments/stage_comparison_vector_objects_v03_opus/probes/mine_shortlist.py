"""mine · step 5 — deterministic candidate generation per benchmark class.

Reads artifacts/mine_align2.jsonl (equal-scale registration).  Emits, per class, the
top candidates by an explicit rule, deduplicated -> artifacts/mine_shortlist.jsonl
(+ mine_shortlist_stats.json with how many pairs each rule could find at all).

No label here is final: every shortlisted pair is rendered and confirmed by eye
before it enters the benchmark.
"""
import json, sys, hashlib
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"
PER_CLASS = 14


def pid(r):
    h = hashlib.sha1(f"{r['doc_id']}|{r['ver_a']}|{r['ver_b']}|{r['block_a']}|{r['block_b']}".encode()).hexdigest()[:8]
    return f"{r['discipline']}-{h}"


def comps_split(a2):
    """(components dominated by A-only, by B-only) among the big ones."""
    A = [c for c in a2["top_components"] if c["px"] >= 40 and c["a_px"] > 3 * c["b_px"]]
    B = [c for c in a2["top_components"] if c["px"] >= 40 and c["b_px"] > 3 * c["a_px"]]
    return A, B


def centre(c):
    x0, y0, x1, y1 = c["bbox_px"]
    return ((x0 + x1) / 2, (y0 + y1) / 2)


def main():
    rows = [json.loads(l) for l in open(ART / "mine_align2.jsonl", encoding="utf-8")]
    rows = [r for r in rows if "align2" in r and not r.get("same_pdf")]
    for r in rows:
        r["pair_id"] = pid(r)
    rules = {}

    def rule(name, pred, key, take=PER_CLASS, spread=True):
        cand = [r for r in rows if pred(r)]
        cand.sort(key=key)
        picked, seen_doc = [], {}
        for r in cand:
            if spread:
                d = r["doc_id"]
                if seen_doc.get(d, 0) >= 2:
                    continue
                seen_doc[d] = seen_doc.get(d, 0) + 1
            picked.append(r)
            if len(picked) >= take:
                break
        rules[name] = {"n_candidates": len(cand), "n_picked": len(picked),
                       "picked": [r["pair_id"] for r in picked]}
        for r in picked:
            r.setdefault("cand_classes", []).append(name)
        return picked

    A2 = lambda r: r["align2"]

    rule("unchanged_control",
         lambda r: A2(r)["diff_frac_block"] < 0.0005 and A2(r)["n_components_big"] == 0,
         key=lambda r: (A2(r)["diff_frac_block"], -A2(r)["ink_a"]))
    rule("small_local_change",
         lambda r: 0.0002 <= A2(r)["diff_frac_block"] <= 0.02 and 1 <= A2(r)["n_components_big"] <= 4,
         key=lambda r: (-A2(r)["top1_frac_of_diff"], A2(r)["diff_frac_block"]))
    rule("object_added",
         lambda r: A2(r)["only_b"] > 6 * (A2(r)["only_a"] + 1) and A2(r)["n_components_big"] >= 1
                   and A2(r)["diff_frac_block"] < 0.08,
         key=lambda r: (-A2(r)["top1_frac_of_diff"],))
    rule("object_removed",
         lambda r: A2(r)["only_a"] > 6 * (A2(r)["only_b"] + 1) and A2(r)["n_components_big"] >= 1
                   and A2(r)["diff_frac_block"] < 0.08,
         key=lambda r: (-A2(r)["top1_frac_of_diff"],))

    def moved(r):
        a2 = A2(r)
        if not (0.0002 <= a2["diff_frac_block"] <= 0.05):
            return False
        A, B = comps_split(a2)
        if not A or not B:
            return False
        a, b = A[0], B[0]
        if min(a["px"], b["px"]) < 60:
            return False
        if max(a["px"], b["px"]) / max(1, min(a["px"], b["px"])) > 2.5:
            return False
        ca, cb = centre(a), centre(b)
        d = ((ca[0] - cb[0]) ** 2 + (ca[1] - cb[1]) ** 2) ** 0.5
        return 5 < d < 0.5 * max(A2(r)["canvas"])
    rule("object_moved", moved, key=lambda r: (-A2(r)["top1_frac_of_diff"],))

    rule("block_moved",
         lambda r: A2(r)["shift_frac_block"] > 0.03 and A2(r)["diff_frac_block"] < 0.01,
         key=lambda r: (-A2(r)["shift_frac_block"],))
    rule("rotated_page",
         lambda r: (r["rot_a"] in (90, 270) or r["rot_b"] in (90, 270)),
         key=lambda r: (-A2(r)["diff_frac_block"],))
    rule("rotated_page_quiet",
         lambda r: (r["rot_a"] in (90, 270) or r["rot_b"] in (90, 270))
                   and A2(r)["diff_frac_block"] < 0.01,
         key=lambda r: (A2(r)["diff_frac_block"],), take=6)
    rule("bbox_shape_change",
         lambda r: r["shape_a"] != r["shape_b"],
         key=lambda r: (-A2(r)["diff_frac_block"],), take=8)
    rule("stamp_block",
         lambda r: r["cat_a"] == "stamp" and r["cat_b"] == "stamp"
                   and 0.0001 < A2(r)["diff_frac_block"] < 0.05,
         key=lambda r: (A2(r)["diff_frac_block"],), take=12)
    rule("heavy_change",
         lambda r: A2(r)["diff_frac_block"] > 0.10,
         key=lambda r: (-A2(r)["diff_frac_block"],), take=10)
    rule("dense_ink",
         lambda r: r["ink_density_a"] > 0.18 and A2(r)["diff_frac_block"] > 0.0005,
         key=lambda r: (-r["ink_density_a"],), take=12)
    rule("sparse_ink",
         lambda r: r["ink_density_a"] < 0.02 and A2(r)["diff_frac_block"] > 0.0002,
         key=lambda r: (r["ink_density_a"],), take=8)
    rule("no_ocr_text",
         lambda r: r["ocr_len_a"] < 5 and r["ocr_len_b"] < 5,
         key=lambda r: (-A2(r)["diff_frac_block"],), take=10)
    rule("many_components",
         lambda r: A2(r)["n_components_big"] >= 8 and A2(r)["diff_frac_block"] < 0.03,
         key=lambda r: (-A2(r)["n_components_big"],), take=8)

    picked = [r for r in rows if r.get("cand_classes")]
    with open(ART / "mine_shortlist.jsonl", "w", encoding="utf-8") as fh:
        for r in picked:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    stats = {"schema_version": "mine_shortlist/1", "research_only": True,
             "n_pairs_total": len(rows), "n_shortlisted": len(picked),
             "per_class": rules,
             "note": "n_candidates = how many pairs in the whole screened corpus satisfy the rule"}
    (ART / "mine_shortlist_stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=1),
                                                   encoding="utf-8")
    print(json.dumps({k: (v["n_candidates"], v["n_picked"]) for k, v in rules.items()},
                     ensure_ascii=False, indent=1))
    print("shortlisted", len(picked))


if __name__ == "__main__":
    main()
