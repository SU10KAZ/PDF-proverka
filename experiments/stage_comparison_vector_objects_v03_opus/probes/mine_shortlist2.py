"""mine · step 5b — candidate generation, second pass.

First pass lesson (checked by eye on SS-8ee57c88, SS-392b7bd3, SS-b689be61,
VK-f355ee3a): the top "added / removed object" candidates in real revision pairs are
almost all a SHEET FRAME LINE that one version's prepared bbox includes and the
other's does not, and half of all matched pairs are title blocks (stamp).  Both are
filtered here:

  * category_code == "stamp" is excluded from the object classes;
  * a residual component is INTERIOR only if its bbox stays away from the block
    border by max(6 px, 2 % of the long side).

Writes artifacts/mine_shortlist2.jsonl and artifacts/mine_shortlist2_stats.json.
"""
import json, hashlib
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"


def load():
    rows = {}
    for name in ("mine_extract.jsonl", "mine_extract2.jsonl", "mine_align2.jsonl"):
        p = ART / name
        if not p.exists():
            continue
        for l in open(p, encoding="utf-8"):
            r = json.loads(l)
            if "align2" not in r or r.get("same_pdf"):
                continue
            if "pair_id" not in r:
                h = hashlib.sha1(f"{r['doc_id']}|{r['ver_a']}|{r['ver_b']}|{r['block_a']}|{r['block_b']}".encode()).hexdigest()[:8]
                r["pair_id"] = f"{r['discipline']}-{h}"
            old = rows.get(r["pair_id"])
            if old and "EA" in old and "EA" not in r:
                continue
            rows[r["pair_id"]] = r
    return list(rows.values())


def interior(r):
    a2 = r["align2"]
    s = a2["scale_px_per_pt"]
    w = r["wh_pt_a"][0] * s; h = r["wh_pt_a"][1] * s
    mx = max(6.0, 0.02 * max(w, h))
    out = []
    for c in a2["top_components"]:
        x0, y0, x1, y1 = c["bbox_px"]
        if x0 < mx or y0 < mx or x1 > w - mx or y1 > h - mx:
            continue
        out.append(c)
    return out, (w, h)


def main():
    rows = load()
    for r in rows:
        ic, (w, h) = interior(r)
        r["interior_components"] = ic
        r["block_px_wh"] = [round(w, 1), round(h, 1)]
        r["int_px"] = sum(c["px"] for c in ic)
        r["int_a_px"] = sum(c["a_px"] for c in ic)
        r["int_b_px"] = sum(c["b_px"] for c in ic)
        r["int_frac_block"] = round(r["int_px"] / max(1.0, w * h), 6)
        r["int_n_big"] = sum(1 for c in ic if c["px"] >= 60)
        r["is_stamp"] = (r["cat_a"] == "stamp" or r["cat_b"] == "stamp")
        e = r.get("EA"); f = r.get("EB")
        r["seg_max"] = max(e["segments"], f["segments"]) if e and f else None
        r["txt_lines_max"] = max(e["n_text_lines"], f["n_text_lines"]) if e and f else None
        r["has_raster"] = bool(e and f and (e["n_images"] or f["n_images"]))
    rules = {}

    def rule(name, pred, key, take=10, per_doc=2):
        cand = [r for r in rows if pred(r)]
        cand.sort(key=key)
        picked, seen = [], {}
        for r in cand:
            if seen.get(r["doc_id"], 0) >= per_doc:
                continue
            seen[r["doc_id"]] = seen.get(r["doc_id"], 0) + 1
            picked.append(r)
            if len(picked) >= take:
                break
        rules[name] = {"n_candidates": len(cand), "n_picked": len(picked),
                       "picked": [r["pair_id"] for r in picked]}
        for r in picked:
            r.setdefault("cand2", []).append(name)
        return picked

    notstamp = lambda r: not r["is_stamp"]
    A2 = lambda r: r["align2"]

    rule("obj_added_interior",
         lambda r: notstamp(r) and r["int_n_big"] >= 1 and r["int_b_px"] > 6 * (r["int_a_px"] + 1)
                   and r["int_frac_block"] < 0.05,
         key=lambda r: -r["int_px"], take=12)
    rule("obj_removed_interior",
         lambda r: notstamp(r) and r["int_n_big"] >= 1 and r["int_a_px"] > 6 * (r["int_b_px"] + 1)
                   and r["int_frac_block"] < 0.05,
         key=lambda r: -r["int_px"], take=12)
    rule("small_local_interior",
         lambda r: notstamp(r) and 1 <= r["int_n_big"] <= 3 and 0 < r["int_frac_block"] < 0.02,
         key=lambda r: -r["int_px"], take=14)
    rule("obj_moved_interior",
         lambda r: notstamp(r) and r["int_n_big"] >= 2 and r["int_frac_block"] < 0.03
                   and min(r["int_a_px"], r["int_b_px"]) > 0.25 * max(r["int_a_px"], r["int_b_px"]),
         key=lambda r: -min(r["int_a_px"], r["int_b_px"]), take=12)
    rule("quiet_true",
         lambda r: A2(r)["diff_frac_block"] < 0.0002 and r["int_n_big"] == 0,
         key=lambda r: (-(r.get("seg_max") or 0),), take=12)
    rule("packaging",
         lambda r: r.get("EA") and A2(r)["diff_frac_block"] < 0.002
                   and r.get("seg_ratio", 1) < 0.9,
         key=lambda r: r.get("seg_ratio", 1), take=10)
    rule("dense_block",
         lambda r: (r.get("seg_max") or 0) > 5000 and r["int_n_big"] >= 1
                   and r["int_frac_block"] < 0.05,
         key=lambda r: -(r.get("seg_max") or 0), take=12)
    rule("no_labels",
         lambda r: r.get("txt_lines_max") == 0,
         key=lambda r: -A2(r)["diff_frac_block"], take=10)
    rule("few_labels",
         lambda r: r.get("txt_lines_max") is not None and 0 < r["txt_lines_max"] <= 3,
         key=lambda r: -A2(r)["diff_frac_block"], take=8)
    rule("raster_graphics",
         lambda r: r.get("has_raster"),
         key=lambda r: -A2(r)["diff_frac_block"], take=10)
    rule("rotated_interior",
         lambda r: (r["rot_a"] in (90, 270) or r["rot_b"] in (90, 270)) and r["int_n_big"] >= 1
                   and r["int_frac_block"] < 0.05,
         key=lambda r: -r["int_px"], take=10)
    rule("rotated_quiet",
         lambda r: (r["rot_a"] in (90, 270) or r["rot_b"] in (90, 270))
                   and A2(r)["diff_frac_block"] < 0.0002,
         key=lambda r: -(r.get("seg_max") or 0), take=8)
    rule("block_shifted",
         lambda r: (A2(r)["shift_pt"][0] ** 2 + A2(r)["shift_pt"][1] ** 2) ** 0.5 > 20
                   and A2(r)["diff_frac_block"] < 0.005,
         key=lambda r: -((A2(r)["shift_pt"][0] ** 2 + A2(r)["shift_pt"][1] ** 2) ** 0.5), take=10)
    rule("stamp_text_only",
         lambda r: r["is_stamp"] and r["int_n_big"] == 0
                   and 0.00005 < A2(r)["diff_frac_block"] < 0.01,
         key=lambda r: -A2(r)["diff_frac_block"], take=10)
    rule("many_interior",
         lambda r: notstamp(r) and r["int_n_big"] >= 6 and r["int_frac_block"] < 0.05,
         key=lambda r: -r["int_n_big"], take=10)

    picked = [r for r in rows if r.get("cand2")]
    with open(ART / "mine_shortlist2.jsonl", "w", encoding="utf-8") as fh:
        for r in picked:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    (ART / "mine_shortlist2_stats.json").write_text(json.dumps(
        {"schema_version": "mine_shortlist2/1", "research_only": True,
         "n_pairs_pool": len(rows), "n_with_extract": sum(1 for r in rows if r.get("EA")),
         "n_shortlisted": len(picked), "per_class": rules}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps({k: (v["n_candidates"], v["n_picked"]) for k, v in rules.items()},
                     ensure_ascii=False))
    print("pool", len(rows), "shortlisted", len(picked))


if __name__ == "__main__":
    main()
