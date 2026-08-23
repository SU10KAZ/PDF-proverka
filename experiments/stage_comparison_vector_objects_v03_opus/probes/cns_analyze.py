# -*- coding: utf-8 -*-
"""CNS-4 — aggregate the census: taxonomy, per-class stats, vector eligibility."""
from __future__ import annotations
import json, os, sys
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes.cns_rules import classify, RULES_DOC, CLASSES

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
FEAT = ART / (sys.argv[1] if len(sys.argv) > 1 else "cns_features.jsonl")


def pct(a, qs=(10, 25, 50, 75, 90, 99)):
    a = np.asarray(a, dtype=float)
    if a.size == 0:
        return {f"p{q}": None for q in qs}
    return {f"p{q}": round(float(np.percentile(a, q)), 4) for q in qs}


def main():
    rows = []
    with open(FEAT, encoding="utf-8") as fh:
        for line in fh:
            rows.append(json.loads(line))
    errs = [r for r in rows if "error" in r]
    ok = [r for r in rows if "error" not in r]

    # page position of every block (from the foundation census)
    pos = {}
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            x1, y1, x2, y2 = b["coords_px"]
            pw, ph = b["page_px"]
            if pw and ph:
                pos[(b["doc_id"], b["version"], b["block_id"])] = (
                    round((x1 + x2) / 2 / pw, 4), round((y1 + y2) / 2 / ph, 4))

    ptl_path = ART / "cns_page_text_lines.json"
    ptl = json.loads(ptl_path.read_text(encoding="utf-8")) if ptl_path.exists() else {}
    n_missing_ptl = 0
    for r in ok:
        if r.get("n_text", 1) == 0:
            k = f"{r['pdf']}|{r['page_index']}"
            if k in ptl:
                r["page_text_lines"] = ptl[k]
            else:
                n_missing_ptl += 1
                r["page_text_lines"] = 10 ** 6   # unknown -> NOT counted as curved_text
    for r in ok:
        c, rid = classify(r)
        r["_class"] = c
        r["_rule"] = rid
        p = pos.get((r["doc_id"], r["version"], r["block_id"]))
        r["_cx"], r["_cy"] = p if p else (None, None)

    n = len(ok)
    by_class = defaultdict(list)
    for r in ok:
        by_class[r["_class"]].append(r)

    # ---------------- taxonomy ------------------------------------------------
    tax = {
        "n_result_json_docs": len({r["pdf"] for r in ok}),
        "n_blocks_measured": n,
        "n_blocks_error": len(errs),
        "n_zero_text_blocks_without_page_text_measurement": n_missing_ptl,
        "error_kinds": Counter(e["error"].split(":")[0] for e in errs).most_common(10),
        "rules": RULES_DOC,
        "class_share": {c: round(len(v) / n, 5) for c, v in sorted(by_class.items(), key=lambda kv: -len(kv[1]))},
        "class_count": {c: len(v) for c, v in sorted(by_class.items(), key=lambda kv: -len(kv[1]))},
        "rule_count": dict(Counter(r["_rule"] for r in ok).most_common()),
        "by_discipline": {},
        "stamp_source": {
            "category_code_stamp": sum(1 for r in ok if r.get("category_code") == "stamp"),
            "geom_fallback": sum(1 for r in ok if r["_rule"] == "R5_stamp_geom"),
            "category_code_present": sum(1 for r in ok if r.get("category_code")),
        },
    }
    for disc in sorted({r["discipline"] for r in ok}):
        sub = [r for r in ok if r["discipline"] == disc]
        cc = Counter(r["_class"] for r in sub)
        tax["by_discipline"][disc] = {"n": len(sub),
                                      "share": {k: round(v / len(sub), 4) for k, v in cc.most_common()}}

    # ---------------- duplicates ---------------------------------------------
    groups = defaultdict(list)
    for r in ok:
        if r.get("geom_sha") and r.get("n_seg", 0) >= 50:
            groups[r["geom_sha"]].append(r)
    dup_blocks = {id(r): False for r in ok}
    dup_scope = Counter()
    dup_group_sizes = []
    for g, v in groups.items():
        if len(v) < 2:
            continue
        dup_group_sizes.append(len(v))
        docs = {(x["doc_id"]) for x in v}
        vers = {(x["doc_id"], x["version"]) for x in v}
        scope = ("cross_document" if len(docs) > 1
                 else ("cross_version" if len(vers) > 1 else "within_version"))
        for x in v:
            x["_dup"] = True
            x["_dup_scope"] = scope
            dup_scope[scope] += 1
    n_dupable = sum(1 for r in ok if r.get("geom_sha") and r.get("n_seg", 0) >= 50)
    dup_stats = {
        "definition": "identical isotropically-normalised segment set (sha1 of all rounded segments), blocks with n_seg>=50 only",
        "n_considered": n_dupable,
        "n_in_duplicate_group": sum(1 for r in ok if r.get("_dup")),
        "share_of_considered": round(sum(1 for r in ok if r.get("_dup")) / max(1, n_dupable), 5),
        "share_of_corpus": round(sum(1 for r in ok if r.get("_dup")) / n, 5),
        "n_groups": len(dup_group_sizes),
        "group_size": pct(dup_group_sizes) if dup_group_sizes else {},
        "max_group": max(dup_group_sizes) if dup_group_sizes else 0,
        "by_scope": dict(dup_scope),
    }

    # ---------------- per-class stats ----------------------------------------
    stats = {}
    for c, v in sorted(by_class.items(), key=lambda kv: -len(kv[1])):
        seg = [r["n_seg"] for r in v]
        txt = [r["n_text"] for r in v]
        stats[c] = {
            "n": len(v), "share": round(len(v) / n, 5),
            "n_seg": {"median": float(np.median(seg)), **pct(seg)},
            "n_text": {"median": float(np.median(txt)), **pct(txt)},
            "share_no_text_at_all": round(sum(1 for r in v if r["n_text"] == 0) / len(v), 5),
            "share_no_uniq_designation_near_geometry": round(
                sum(1 for r in v if r.get("n_uniq_desig_near_geom", 0) == 0) / len(v), 5),
            "share_no_designation_at_all": round(
                sum(1 for r in v if r.get("n_uniq_desig", 0) == 0) / len(v), 5),
            "uniq_desig_near_geom": pct([r.get("n_uniq_desig_near_geom", 0) for r in v]),
            "size_pt": {"W": pct([r["W_pt"] for r in v]), "H": pct([r["H_pt"] for r in v]),
                        "area_pt2": pct([r["area_pt2"] for r in v])},
            "page_area_frac": pct([r["page_area_frac"] for r in v]),
            "aspect": pct([r["aspect"] for r in v]),
            "page_position": {"cx": pct([r["_cx"] for r in v if r["_cx"] is not None]),
                              "cy": pct([r["_cy"] for r in v if r["_cy"] is not None])},
            "duplicate_share": round(sum(1 for r in v if r.get("_dup")) / len(v), 5),
            "raster_coverage": pct([r["raster_coverage"] for r in v]),
            "len_share_on_rulings": pct([r["len_share_on_rulings"] for r in v]),
            "text_area_share": pct([r["text_area_share"] for r in v]),
            "invisible_share_segments": pct([r["invisible_share"] for r in v]),
            "top_disciplines": Counter(r["discipline"] for r in v).most_common(5),
        }
    class_stats = {"per_class": stats, "duplicates": dup_stats,
                   "corpus": {
                       "n": n,
                       "share_no_text_at_all": round(sum(1 for r in ok if r["n_text"] == 0) / n, 5),
                       "share_no_uniq_desig_near_geom": round(
                           sum(1 for r in ok if r.get("n_uniq_desig_near_geom", 0) == 0) / n, 5),
                       "n_seg": pct([r["n_seg"] for r in ok]),
                       "n_text": pct([r["n_text"] for r in ok]),
                       "page_area_frac": pct([r["page_area_frac"] for r in ok]),
                       "area_pt2": pct([r["area_pt2"] for r in ok]),
                   }}

    # ---------------- eligibility --------------------------------------------
    def elig(r):
        if r["_class"] == "empty":
            return "no_geometry_empty"
        if r["_class"] == "raster":
            return "vision_only_raster"
        if r["n_seg"] == 0:
            return "vision_only_no_vector"
        if r["raster_coverage"] >= 0.50:
            return "vision_only_raster_dominant"
        if r["n_seg"] < 20:
            return "too_thin_lt20_segments"
        if r["n_seg"] > 200000:
            return "too_heavy_gt200k_segments"
        return "eligible"

    for r in ok:
        r["_elig"] = elig(r)
    ec = Counter(r["_elig"] for r in ok)
    eligible = [r for r in ok if r["_elig"] == "eligible"]
    eligibility = {
        "definition": {
            "eligible": "vector layer present, 20 <= n_seg <= 200000, largest raster < 50 % of the block, not empty",
            "vision_only_raster": "class raster (raster_only, or raster >=50 % with <=200 segments)",
            "too_thin_lt20_segments": "fewer than 20 inked segments — no object structure to build",
        },
        "counts": dict(ec.most_common()),
        "shares": {k: round(v / n, 5) for k, v in ec.most_common()},
        "eligible_share": round(len(eligible) / n, 5),
        "vision_required_share": round(sum(v for k, v in ec.items() if k.startswith("vision_") or k == "no_geometry_empty") / n, 5),
        "degraded_but_eligible": {
            "curved_text_share_of_eligible": round(sum(1 for r in eligible if r["_class"] == "curved_text") / max(1, len(eligible)), 5),
            "no_text_layer_share_of_eligible": round(sum(1 for r in eligible if r["n_text"] == 0) / max(1, len(eligible)), 5),
            "broken_text_share_of_eligible": round(sum(1 for r in eligible if r.get("broken_text")) / max(1, len(eligible)), 5),
            "raster_patch_share_of_eligible": round(sum(1 for r in eligible if r["raster_coverage"] >= 0.15) / max(1, len(eligible)), 5),
            "no_uniq_desig_near_geom_share_of_eligible": round(sum(1 for r in eligible if r.get("n_uniq_desig_near_geom", 0) == 0) / max(1, len(eligible)), 5),
        },
        "eligible_geometry_and_text": round(sum(1 for r in eligible if r["n_text"] > 0 and not r.get("broken_text")) / n, 5),
        "excluding_stamps": {},
        "by_discipline": {},
        "by_class": {c: {"n": len(v),
                         "eligible": round(sum(1 for r in v if r["_elig"] == "eligible") / len(v), 4)}
                     for c, v in by_class.items()},
        "segments_of_eligible": pct([r["n_seg"] for r in eligible]),
        "corpus_context": {
            "n_graphic_blocks_total_in_667_result_json": 52057,
            "n_blocks_with_pdf_present": 43261,
            "share_of_all_graphic_blocks_measured": round(n / 52057, 5),
            "note": "8 796 blocks (16.9 %) live in versions whose document.pdf is absent — they cannot be read at all",
        },
    }
    nonstamp = [r for r in ok if r["_class"] != "stamp"]
    eligibility["excluding_stamps"] = {
        "n": len(nonstamp),
        "share_of_corpus": round(len(nonstamp) / n, 5),
        "eligible": round(sum(1 for r in nonstamp if r["_elig"] == "eligible") / max(1, len(nonstamp)), 5),
        "counts": dict(Counter(r["_elig"] for r in nonstamp).most_common()),
    }
    for disc in sorted({r["discipline"] for r in ok}):
        sub = [r for r in ok if r["discipline"] == disc]
        eligibility["by_discipline"][disc] = {
            "n": len(sub),
            "eligible": round(sum(1 for r in sub if r["_elig"] == "eligible") / len(sub), 4),
            "vision_only": round(sum(1 for r in sub if r["_elig"].startswith("vision_")) / len(sub), 4),
        }

    (ART / "cns_block_taxonomy.json").write_text(json.dumps(tax, ensure_ascii=False, indent=1), encoding="utf-8")
    (ART / "cns_class_stats.json").write_text(json.dumps(class_stats, ensure_ascii=False, indent=1), encoding="utf-8")
    (ART / "cns_vector_eligibility.json").write_text(json.dumps(eligibility, ensure_ascii=False, indent=1), encoding="utf-8")

    # compact per-block class table for downstream probes
    with open(ART / "cns_block_classes.jsonl", "w", encoding="utf-8") as out:
        for r in ok:
            out.write(json.dumps({"block_id": r["block_id"], "doc_id": r["doc_id"], "version": r["version"],
                                  "discipline": r["discipline"], "page_number": r["page_number"],
                                  "cls": r["_class"], "rule": r["_rule"], "elig": r["_elig"],
                                  "n_seg": r["n_seg"], "n_text": r["n_text"],
                                  "dup": bool(r.get("_dup")), "dup_scope": r.get("_dup_scope"),
                                  "geom_sha": r.get("geom_sha", "")}, ensure_ascii=False) + "\n")
    print(json.dumps(tax["class_share"], ensure_ascii=False, indent=1))
    print(json.dumps(eligibility["shares"], ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
