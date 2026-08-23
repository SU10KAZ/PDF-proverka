# -*- coding: utf-8 -*-
"""Assemble artifacts/ctr_contract_v03.json: schema + per-field justification + examples."""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C

ART = C.ART
EX = ART / "ctr_examples"

F = lambda name, typ, why, ev: {"field": name, "type": typ, "why": why, "breaks_without_it": ev}

SCHEMA = {
    "provenance": [
        F("pdf_sha256", "str", "identity of the source file",
          "mine M2: 7 of 122 version pairs are the SAME file (equal sha256) — such a pair destroyed the v0.1 benchmark; ctr_ablation_pair.json: 0 of 30 benchmark pairs are self-pairs only because this was checked"),
        F("pdf", "str(path)", "locator of the geometry",
          "fnd F1a: render_block reproduces the production crop byte-for-byte (70/70 sha256) only from pdf+coords_px+page_px; without the locator nothing in this track is reproducible"),
        F("result_json", "str(path)", "locator of the prepared block record", "same as pdf"),
        F("block_id", "str", "the prepared unit", "upstream contract key"),
        F("page_number", "int(1-based)", "page as written in result.json", "fnd F0"),
        F("page_index", "int(0-based)", "ALWAYS page_number-1",
          "fnd F0: 55.4 % of blocks carry a conflicting page_index; ctr_ablation_ex.json: reading by the raw field returns 0.68 / 0.45 / 0.016 of the segments — a different part of the sheet"),
        F("page_index_field", "int|null", "raw upstream value, kept for the audit", "fnd F0"),
        F("page_index_conflict", "bool", "raw value != page_number-1", "fnd F0 (55.4 %)"),
        F("coords_px", "[x1,y1,x2,y2]", "block rectangle in the px system of result.json",
          "fnd F4: coords_px == crop_px on 9 461 blocks, 0 mismatches — this is the frame the human saw"),
        F("page_px", "[w,h]", "px page size the coords refer to", "fnd F4 / frame reconstruction"),
        F("rotation", "0|90|270", "/Rotate of the page",
          "fnd F1b: naive path reads another part of the sheet (segments on ink 0.131 / 0.153 vs 1.000); ctr_ablation_ex.json: Jaccard of the segment sets correct-vs-naive = 0.000 on both rotated examples (272 vs 115 and 8 367 vs 1 483 segments)"),
        F("rotation_source", "'result_json'|'pdf'", "result.json misses rotation on 14 568 blocks", "BRIEF §1"),
        F("shape_type", "'rectangle'|'polygon'", "the clip is a rectangle, the block may not be",
          "fnd F4': 12.57 % of blocks are polygons"),
        F("polygon_pt", "[[x,y],...]|null", "the block polygon in DISPLAY points (the space objects live in)",
          "ctr_ablation_ex.json: on the single-line-diagram block 56.2 % of the ink inside the rectangle lies OUTSIDE the declared polygon (398 of 1 736 objects, 22.9 %) — without the ring that foreign ink cannot be excluded and is published as ADDED/REMOVED"),
        F("polygon_area_share", "float|null", "area(polygon)/area(bbox)",
          "fnd F4': median 0.863, <0.9 for 64.3 %; ctr_ablation_ex.json: 2.0 / 5.9 / 9.6 % of the ink inside the rectangle lies OUTSIDE the polygon — i.e. belongs to a neighbouring block and would be published as ADDED"),
        F("extractor", "{module, drop_invisible, curve_steps, params_sha}", "two payloads are comparable only if produced the same way",
          "fnd_GATEFIX: one predicate changed 33.7 % of the stroke length (median) and 81.9 % at p90; fnd F2\": the ink filter drops 1.36 % of segments, 16.75 % of them sit on real dark stroke"),
    ],
    "frame": [
        F("clip_display_pt", "[x0,y0,x1,y1]", "the block region in DISPLAY space — objects, polygon and every bbox live here",
          "fnd F1a: this is the region the production crop renders, i.e. the picture the human saw; on a rot=270 block the object bboxes (788..1229 pt) lie completely outside clip_page_pt (534..851 pt), so a payload carrying only the page-space rect is unusable"),
        F("clip_page_pt", "[x0,y0,x1,y1]", "the same region in the page's OWN (unrotated) space",
          "fnd F1b: this is the only rectangle that get_drawings/get_text may be given; the naive substitution reads another part of the sheet (segments on ink 0.131 / 0.153 vs 1.000)"),
        F("size_pt", "[w,h]", "physical size of the block",
          "ctr_ablation_pair.json: '0.5 % of the block' means 0.87 pt on one benchmark block and 19.77 pt on another — a 22.6x spread; a fraction is not a tolerance"),
        F("px_per_pt", "[sx,sy]", "px->pt of the crop the human saw",
          "vis S4: the Vision window is priced in pixels (median 695.6 tokens, 22 of 30 full blocks hit the 3 051 ceiling); a ledger record can only be turned into a window with this factor"),
    ],
    "scale": [
        F("S", "float(pt)", "characteristic scale of the block, in points",
          "grp G5': the source of S is the strongest 'parameter' of the whole layer (x1.60 objects); ctr_ablation_ex.json: replacing text-S by geom-S multiplies the object count by 5.46 / 5.93 / 1.82 / 1.71 on real blocks"),
        F("S_source", "'text'|'geom'|'none'", "which branch produced S",
          "neg N-4: 5.3 % of blocks have <5 text lines; on 0.79 % of real pairs the SOURCE of S differs between sides; ctr_ablation_pair.json: 3 of 30 benchmark pairs differ in source"),
        F("s_text, s_geom", "float(pt)", "both candidates travel so the pair can agree",
          "grp G2-2: identical geometry (10 972 = 10 972 segments) gave 1 635 objects against 200 because each side chose its own S"),
        F("n_text_lines", "int", "why the text branch was or was not taken", "grp G5' / neg N-3"),
        F("S_shared", "float|null", "S = max(S_a, S_b), filled by the pair context",
          "grp G2-2b: shared S moves median 1:1 churn 0.937 -> 0.988, 4 pairs better, 0 worse; ctr_ablation_pair.json: EOM-7fef43a3 object counts 1635/200 -> 200/200, SS-76640e11 412/311 -> 313/311"),
    ],
    "quality": [
        F("block_class", "stamp|table|drawing|legend_notes|raster|vector_raster_mix|curved_text|empty",
          "routing, not description",
          "ctr_route_census.json: 35.0 % of the 43 261 corpus blocks must go to the table pipeline; neg N-11: routing them out costs 0 real findings on the benchmark"),
        F("n_seg", "int", "cost and churn band",
          "grp G2-3: median 1:1 churn falls to 0.507 above 15 000 segments and 0.384 above 50 000; loc L16: one comparison is 26 minutes at the corpus maximum"),
        F("density_band", "sparse|light|medium|dense|very_dense|extreme", "derived band used by the router", "grp G2-3"),
        F("has_vector, raster_only, raster_coverage", "bool/float", "vector comparison is impossible without vector",
          "cns CNS-5: 3.0 % of blocks are Vision-only; hyb H-13: AR-a32b30a6 (no vector on either side) was answered only by the Vision route"),
        F("no_text, n_curves, page_text_lines", "int/bool", "the three ingredients of 'text is drawn as curves'",
          "cns CNS-4: 'no text & >=20 curves' alone has precision 3.9 % (872 blocks -> 33 real); the disambiguator is the TEXT LAYER OF THE WHOLE PAGE (34 blocks). neg N-13/N-14: on a real curves block a one-line text edit gives a false graphic change in 0.977 [CF] / 0.893 [REAL] and even an oracle letter filter leaves 57.6 % — the only cure is the route"),
        F("text_in_curves", "bool", "the derived route flag", "neg N-14"),
        F("broken_text, garbled_ratio", "bool/float", "when the label may not be trusted",
          "mine M10: pixel-identical block, text_jaccard 0.123 (broken font encodings); lbl LBL-11: 5E4 -> 5ES is a font defect, not a project change"),
        F("invisible_share", "float", "how much of the paint the ink filter removed",
          "fnd F2: 1.36 % overall but >20 % on 2.4 % of blocks; F2\": 16.75 % of removed segments sit on real dark stroke — the number must be visible to whoever reads the description"),
        F("ink_outside_polygon_share", "float", "how much of the ink in the rectangle is not in the block",
          "ctr_ablation_ex.json: 0.0199 / 0.0120 / 0.5624 on the three polygon examples — the last one is a whole pair of load tables that belong to the table pipeline"),
        F("border_share", "float", "share of segments cut by the block border",
          "mine M5: on 36.5 % of real pairs with a residual, ALL large difference components touch the block border"),
        F("frame_clamped", "bool", "the block reached outside the page", "fnd F4 (0 cases in the corpus, kept as a guard)"),
        F("route", "TABLE_PIPELINE|VISION_ONLY[:reason]|VECTOR|VECTOR:tile", "the decision itself",
          "ctr_route_census.json: without it 45.0 % of corpus blocks (19 479) reach the generic vector comparator that must not receive them"),
    ],
    "objects[]": [
        F("oid", "str", "cache key INSIDE one extraction, never an identity across versions",
          "grp G8: oid survives repacking (1.000) but only 0.391 of ids survive 0.25 pt coordinate rounding"),
        F("cls", "symbol|linear|area|composite|stray", "the element gate",
          "fam F8: cls == symbol takes the false 'count changed' rows from 45 to 6 on 54 quiet real pairs; neg N-13: 82 of 82 surviving false rows on a curves block are class symbol"),
        F("bbox", "[x0,y0,x1,y1] pt", "position in the common physical frame — the actual identity",
          "grp G7: top-1 matching by descriptor alone 0.700, by position alone 1.000; lbl LBL-4: geometry -> geometry+position moves top-1 0.390 -> 0.805; ctr_ablation_ex.json: 49.6-91.8 % of objects have a descriptor twin closer than 0.05 inside the SAME block"),
        F("ink_pt", "float", "stroke length of the object, the unit the ledger is keyed on",
          "neg N-1: 0 false rows on 700 text-only counterfactuals when the ledger is keyed on unpaired ink; mov MOV-15: object bookkeeping claims changes on 37.7 % of pairs where the ink is silent; loc L12: the publication threshold is a length in points (60 pt -> 0.42 recall at 0.0 false)"),
        F("border", "bool", "does the object contain a segment cut by the block border",
          "mov MOV-9: false 'object moved' 0.2434 -> 0.0053 with this provenance flag; loc L6: without border attribution 8 of 14 quiet pairs raise a false alarm"),
        F("desc", "float[25]", "shape descriptor: families and the element gate",
          "fam F2-a: families survive repacking with ARI 1.000 on 247/247 blocks — only because the descriptor is decomposition-insensitive; fam F6\": dir_concentration (desc[2:8]) separates a symbol from table ruling with precision 1.000 / recall 1.000; ctr_desc_quantisation.json: nothing cheaper works — rounding to 2 decimals saves 4 % of the payload and costs ARI 0.973"),
        F("label", "str|null", "text anchor ONLY; never evidence of change",
          "ctr_label_address.json: the share of counterfactual changes that have at least one usable address falls 0.8607 -> 0.8148 without LABEL_ANCHOR (31 of 675 changes lose their only address); lbl LBL-8: the same field as VERDICT gives 78.7 % false GRAPHIC_CHANGE on pure renames"),
        F("fam", "int|null", "family index of this object",
          "fam F4-a: a real cardinality change of a repeating family is caught in 0.981 (deletion) / 0.854 (duplication)"),
    ],
    "families[]": [
        F("fid, n, ink_pt", "int/int/float", "the right to say '12 -> 14'",
          "fam F4-b: only 30.9 % of changes are expressible as a cardinality change — the field states WHEN the layer may speak; fam F3: 45 false rows on 54 quiet pairs, 10 at n>=3, so n must travel"),
    ],
    "relations": [
        F("policy", "const 'derived on demand, never stored'", "storage would dominate the payload",
          "rel R-13: median 204 edges per block, p90 2 720, max 18 752, while building them costs 0.033 s (median)"),
        F("types", "[ADJACENT, CONNECTED_TO, ALIGNED, LABEL_ANCHOR]", "the whitelist",
          "ctr_label_address.json: these four give 0.8607 of usable addresses, all nine give 0.8711 (+0.0104); rel R-9: LEADER_TO is 0.667 false; rel R-7: REPEATED_WITH is unique in 0.000; rel R-2: 76.9 % of INSIDE/CONTAINS exist only because of a broken circle detector"),
        F("guard", "const 'arc_min_pts>=6'", "without it a rectangle becomes a circle",
          "rel R-1/R-3: 4 253 of 13 042 'closed arcs' are built from <=4 segments; false INSIDE 0.667 -> 0.000 with the guard"),
    ],
    "change_record (what a consumer emits; listed because the border flag lives here)": [
        F("bbox_pt", "[x0,y0,x1,y1]", "where the unpaired ink is", "loc L8: two records, both inside the changed row, 0 spurious"),
        F("ink_pt", "float", "stroke length of the unpaired ink — the publication threshold",
          "loc L12: T = 2 pt -> recall 0.71 at 0.36 false records per quiet pair; T = 60 pt -> 0.42 at 0.00"),
        F("at_boundary", "bool", "the record touches the crop border (derived from object.border and common_frame_pt)",
          "mine M5: on 36.5 % of real pairs with a residual ALL large difference components touch the border; loc L6: without the flag 8 of 14 quiet pairs raise a false alarm, with the hard rule 0 of 14 (recall 13/14 -> 10/14), with the soft 20 pt band 13/14 at 4/14; mov MOV-9: 0.2434 -> 0.0053"),
        F("named_objects", "[oid]", "objects the record is attributed to — naming, not finding",
          "loc L18: an object-to-object ledger gives median 62 false records at 0.25 pt rounding, the ink ledger 0"),
        F("evidence", "const 'unpaired_ink'", "the key of the ledger",
          "neg N-1/N-8: 0 false rows on 700 text-only and 104 table-only counterfactuals"),
    ],
    "pair_context": [
        F("same_pdf", "bool", "refuse to compare a file with itself", "mine M2 (7 of 122)"),
        F("S_shared", "float", "S = max(S_a, S_b) for BOTH sides", "grp G2-2b, ctr_ablation_pair.json"),
        F("common_frame_pt", "[x0,y0,x1,y1]|null", "the intersection of the two frames, in points",
          "mine M5: 36.5 % of real residuals are border artefacts; mov MOV-2: aligning by the crop bbox leaves residual 0.9973 against 0.05775 by object anchors (34 of 34 pairs)"),
        F("transform", "{t, s, rot}", "the global transform recovered by registration",
          "mov MOV-1: recovered exactly (median error 0.0 pt, 112/112 rotations); MOV-10: |t| > 1 pt on 13.8 % of benchmark pairs"),
        F("comparable_share", "float|null", "share of ink inside the common frame",
          "mov MOV-12: min(A,B) < 0.95 on 35.3 % of pairs and 4 of 5 misses are there; neg N-7: the guard lost_share >= 0.90 -> NOT_COMPARABLE takes false alarms 0.071 -> 0.000 and recall 0.867 -> 0.929"),
    ],
}

EXCLUDED = [
    {"field": "primitive.raw (per-segment points)",
     "killed_by": "ctr_ablation_ex.json payload_variants: storing raw segments multiplies the payload x3.9 - x16.5 (up to 3.78 MB on one dense block). It is not needed, because the geometry is re-derivable byte-for-byte from provenance (fnd F1a: 70/70 identical sha256; cf CF2: 357/357 byte-identical repeats)."},
    {"field": "primitive.style (colour, line width)",
     "killed_by": "grp G4-a: style as a merge condition moves churn by median 0.000 (mean +0.005..+0.008), while changing the object count on 94 of 194 blocks — it costs volume and buys nothing"},
    {"field": "CAD layer name (OCG)",
     "killed_by": "grp G4-b: declared on 28.8 % of documents, reaches the drawing operators on 24.7 % of blocks, and there the object almost never mixes layers (median purity 1.000) — no decision changes"},
    {"field": "object.diag",
     "killed_by": "ctr_ablation_ex.json derivable: max |diag - hypot(bbox)| = 0.0014 pt over all objects of 6 blocks — pure duplication of bbox"},
    {"field": "object.arc_share",
     "killed_by": "ctr_ablation_ex.json derivable: max |arc_share - desc[24]| = 5e-05 — it IS desc[24]"},
    {"field": "object.n_seg / n_prim",
     "killed_by": "no measurement of the track keys on a per-object segment count; cost is measured per block (grp G6, loc L16), churn per ink (mov MOV-15)"},
    {"field": "object.cycle / object.dashed",
     "killed_by": "fam F8: the best element gate is cls == symbol (6 false rows, recall 0.431); the cycle-based gate is worse on recall (3 rows, 0.377). Judgement call, numbers on both sides — excluded until a measurement needs it"},
    {"field": "normalised coordinates x/w, y/h",
     "killed_by": "fnd F3: anisotropic normalisation turns a 45-degree line by up to 24.47 degrees; ctr_ablation_pair.json: max 20.86 degrees on quiet real pairs; mine M4: x4.8 residual inflation"},
    {"field": "object counts as evidence of change",
     "killed_by": "mov MOV-15: 23 of 61 real pairs get object-level 'changes' while the ink residual is exactly 0; neg N-2: up to 8 023 false rows on a text-only edit; loc L18: an object-to-object ledger gives median 62 false records at 0.25 pt rounding against median 0 for the ink ledger"},
    {"field": "text content beyond the label",
     "killed_by": "lbl LBL-8: a rename QF1->QF2 with byte-identical geometry becomes a false GRAPHIC_CHANGE in 78.7 % of blocks when the text is allowed into the verdict"},
    {"field": "stamp_data.stage",
     "killed_by": "pd PD-2: precision of the value 'P' is 0.133 (2 of 15 verified by eye)"},
    {"field": "sheet_name as a matching anchor",
     "killed_by": "pd PD-11: 10 sheet-name matches gave 0 real drawing matches"},
    {"field": "an exact hash of the motif",
     "killed_by": "v0.2 OBJ-11 (given): an exact hash transfers for 4-8 classes out of ~50"},
]


def main():
    sizes = json.load(open(ART / "ctr_payload_sizes.json", encoding="utf-8"))["cases"]
    contract = {
        "contract": C.CONTRACT,
        "unit": "one ALREADY PREPARED graphic block (block_type == 'image' of result.json)",
        "rule_of_inclusion": "a field is in the contract only if a measurement of this track breaks without it; the measurement is quoted per field",
        "coordinates": "PDF points. objects/polygon/bboxes live in DISPLAY space (frame.clip_display_pt), extraction input is frame.clip_page_pt. Normalisation by the block bbox is forbidden.",
        "contract_constants": {"units": "pdf_pt",
                               "normalization": "none (measured: mine M4 own-bbox normalisation inflates the residual x4.8; ctr_ablation_pair.json - the per-axis fit turns a 45-degree line by median 1.42, max 20.86 degrees on 14 quiet real pairs; block widths differ >2 % on 20 of 30 pairs)",
                               "page_index_rule": "page_number-1",
                               "S_rule_for_a_pair": "S = max(S_a, S_b)",
                               "object_id_scope": "cache key inside ONE extraction, never an identity across versions",
                               "relations": "derived on demand, never stored"},
        "sections": SCHEMA,
        "excluded_fields": EXCLUDED,
        "examples": [{"case": r["case"], "file": f"ctr_examples/{r['case']}.json",
                      "block_id": r["block_id"], "discipline": r["discipline"],
                      "class": r["census_cls"], "n_seg": r["n_seg"], "n_obj": r["n_obj"],
                      "rotation": r["rotation"], "shape_type": r["shape_type"],
                      "route": r["route"], "bytes": r["bytes"], "tokens": r["tokens"],
                      "head_only_bytes": r["bytes_head_only"],
                      "head_only_tokens": r["tokens_head_only"]} for r in sizes],
        "example_inline": json.load(open(EX / "rotated_270.json", encoding="utf-8")),
    }
    p = ART / "ctr_contract_v03.json"
    p.write_text(json.dumps(contract, ensure_ascii=False, indent=1), encoding="utf-8")
    print("written", p, C.nbytes(contract), "bytes")


if __name__ == "__main__":
    main()
