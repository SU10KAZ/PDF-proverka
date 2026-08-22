#!/usr/bin/env python3
"""VV — build the verification case manifest ``artifacts/vv_cases.json``.

    cd /home/coder/projects/PDF-proverka
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_build_cases

Every number written into the manifest is computed here from the real Track A
descriptions; nothing is estimated.  Cases are reproducible from
``(block, mutation, seed, disclose_limits)`` via ``vv_harness.materialize_case``.
"""
from __future__ import annotations

import collections
import copy
import datetime as dt
import json
import random
import struct
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

SEED = 20260823

# 20 clean controls: every Track A block.  6 of them also carry the full
# mutation battery.  4 real (non-synthetic) defect cases close the set.
MUTATION_BLOCKS = [
    ("ss_simple_node", "left"),           # sparsest: 2 paths / 45 segments, readable
    ("ss_scheme_text_changed", "left"),   # scheme, 100 texts, 407 components, readable
    ("ss_table_graphic", "left"),         # table + node, 1609 segments, readable
    ("eom_singleline_changed", "left"),   # single-line, 75 % micro-segments, readable
    ("ar_wall_sections", "left"),         # dense, storage-capped, readable
    ("vk_nodes", "left"),                 # dense, storage-capped, half the text broken
]
MUTATIONS = [k for k in vv.MUTATION_KINDS if k != "clean"]

# capped_geometry models a SILENT cap, so its sheet must not confess one.
SILENT = {"capped_geometry"}

EXPECTED = {
    "clean": ("VERIFIED", ["VERIFIED"]),
    "deleted_object": ("FAILED", ["FAILED", "PARTIAL"]),
    "wrong_count": ("FAILED", ["FAILED", "PARTIAL"]),
    "missing_labels": ("PARTIAL", ["PARTIAL", "FAILED"]),
    "wrong_topology": ("FAILED", ["FAILED", "PARTIAL"]),
    "broken_text": ("FAILED", ["FAILED", "PARTIAL"]),
    "capped_geometry": ("FAILED", ["FAILED", "PARTIAL"]),
}


def png_size(path: Path) -> tuple[int, int]:
    head = path.open("rb").read(33)
    width, height = struct.unpack(">II", head[16:24])
    return width, height


def rel(path: Path) -> str:
    return str(Path(path).resolve().relative_to(vv.ROOT))


def block_profile(description: dict[str, Any]) -> dict[str, Any]:
    summary = description["primitive_summary"]
    topology = description["topology"]
    extraction = description["geometry"]["extraction"]
    view = vv._text_view(description)
    segments = summary["total_segment_count"]
    tiny = sum(
        1
        for primitive in description["geometry"]["primitives"]
        for start, end in primitive["normalized"]["segments"]
        if vv.ex._distance(start, end) < 0.001
    )
    return {
        "primitives": summary["primitive_count"],
        "segments": segments,
        "segments_shorter_than_0.001_norm": tiny,
        "tiny_segment_share": round(tiny / max(1, segments), 4),
        "texts": view["n"],
        "texts_garbled": view["broken"],
        "garbled_share": round(view["broken"] / max(1, view["n"]), 4),
        "connected_components": topology["connected_components"],
        "branch_points": topology["branch_points"],
        "repeated_families": len(description["repeated_elements"]),
        "largest_repeated_family": description["repeated_elements"][0]["count"]
        if description["repeated_elements"] else 0,
        "vector_quality": description["vector_quality"],
        "storage_capped": bool(extraction["storage_capped"]),
        "primitives_uncapped": extraction["primitives_uncapped"],
        "density_class": "sparse" if segments < 1000 else ("medium" if segments < 10000 else "dense"),
        "text_class": "broken" if view["broken"] else "readable",
    }


def build() -> dict[str, Any]:
    pairs = sorted(p.name for p in vv.DESCRIPTIONS.iterdir() if p.is_dir())
    blocks: dict[str, Any] = {}
    cache: dict[str, dict[str, Any]] = {}
    for pair in pairs:
        for side in ("left", "right"):
            path = vv.description_path(pair, side)
            if not path.exists():
                continue
            description = vv.load_description(path)
            cache[f"{pair}:{side}"] = description
            crop = vv.crop_for(pair, side)
            width, height = png_size(crop)
            blocks[f"{pair}:{side}"] = {
                "pair_id": pair,
                "side": side,
                "block_id": description["block_id"],
                "description_path": rel(path),
                "crop_png": rel(crop),
                "crop_pixels": [width, height],
                "crop_bytes": crop.stat().st_size,
                "crop_render": "PyMuPDF page.get_pixmap, zoom 1.35, clip = block bbox (Track A recipe)",
                "pdf": description["source"]["pdf"].replace(str(vv.ROOT) + "/", ""),
                "page_index": description["page_index"],
                "bbox_norm_on_page": description["bbox_norm_on_page"],
                "profile": block_profile(description),
            }

    cases: list[dict[str, Any]] = []
    counter = 0

    def add(block_key: str, mutation: str, *, family: str, synthetic: bool,
            disclose: bool, seed: int, ground_truth: dict[str, Any],
            sheet: dict[str, Any], base_sheet: dict[str, Any],
            expected: tuple[str, list[str]], must_name: list[str],
            notes: str, extra: dict[str, Any] | None = None) -> None:
        nonlocal counter
        counter += 1
        delta = vv.sheet_delta(base_sheet, sheet)
        case = {
            "case_id": f"vv{counter:03d}",
            "block": block_key,
            "pair_id": blocks[block_key]["pair_id"],
            "side": blocks[block_key]["side"],
            "crop_png": blocks[block_key]["crop_png"],
            "mutation": mutation,
            "family": family,
            "synthetic": synthetic,
            "seed": seed,
            "disclose_limits": disclose,
            "fact_sheet": {"text": sheet["text"], "characters": sheet["characters"],
                           "claims": sheet["claims"]},
            "ground_truth": ground_truth,
            "changed_claims": [d["claim_id"] for d in delta],
            "claim_delta": delta,
            "detectable_in_fact_sheet": bool(delta) if family != "control" else None,
            "expected_status": expected[0],
            "acceptable_status": expected[1],
            "must_name": must_name,
            "notes": notes,
        }
        if extra:
            case.update(extra)
        cases.append(case)

    # ---- 20 clean controls -------------------------------------------------
    for key in sorted(blocks):
        description = cache[key]
        sheet = vv.fact_sheet(description, disclose_limits=True)
        add(key, "clean", family="control", synthetic=True, disclose=True, seed=SEED,
            ground_truth={"kind": "clean", "corrupted": False,
                          "detail": {"note": "description used unchanged"}},
            sheet=sheet, base_sheet=sheet, expected=EXPECTED["clean"], must_name=[],
            notes="False-alarm control: anything other than VERIFIED is a false alarm.")

    # ---- 36 mutations ------------------------------------------------------
    for pair, side in MUTATION_BLOCKS:
        key = f"{pair}:{side}"
        description = cache[key]
        for mutation in MUTATIONS:
            disclose = mutation not in SILENT
            base_sheet = vv.fact_sheet(description, disclose_limits=disclose)
            seed = SEED + counter
            mutated, ground_truth = vv.mutate(description, mutation, random.Random(seed))
            sheet = vv.fact_sheet(mutated, disclose_limits=disclose)
            detail = ground_truth["detail"]
            must_name: list[str] = []
            strength = "normal"
            if mutation == "deleted_object":
                must_name = [detail["where"]] + detail["removed_text_sample"][:4]
                if detail["segments_removed"] < 5 and detail["texts_removed"] < 3:
                    strength = "weak"
            elif mutation == "wrong_count":
                must_name = [detail["target"]["field"], str(detail["true_count"])]
            elif mutation == "missing_labels":
                must_name = [d["text"] for d in detail["dropped"]]
            elif mutation == "wrong_topology":
                must_name = ["C7", "C8"]
            elif mutation == "broken_text":
                must_name = ["C2"] + [s["before"] for s in detail["sample"][:3]]
            elif mutation == "capped_geometry":
                must_name = ["C9"]
                if detail["segment_fraction_kept"] > 0.5:
                    strength = "weak"
            add(key, mutation, family="mutation", synthetic=True, disclose=disclose,
                seed=seed, ground_truth=ground_truth, sheet=sheet, base_sheet=base_sheet,
                expected=EXPECTED[mutation], must_name=must_name,
                notes={
                    "deleted_object": "A spatially contiguous, visible element was removed and every "
                                      "derived count recomputed; the sheet is internally consistent.",
                    "wrong_count": "Only the stated count was changed; the geometry still shows the true one.",
                    "missing_labels": "Three of the sheet's own 'largest lettering' strings were dropped.",
                    "wrong_topology": "connected_components / branch_points moved substantially; geometry untouched.",
                    "broken_text": "Readable spans replaced by control-character garbage (mimics O8).",
                    "capped_geometry": "Longest-first truncation with NO disclosure in the sheet (mimics O11).",
                }[mutation],
                extra={"strength": strength})

    # ---- 4 real, non-synthetic defect cases --------------------------------
    real_specs = [
        {
            "key": "vk_nodes:left",
            "name": "real_vk_nodes_capped_and_broken_text",
            "expected": ("FAILED", ["FAILED", "PARTIAL"]),
            "must_name": ["C9", "C2", "C3"],
            "notes": "REAL, not synthetic. The description is genuinely truncated and half its text "
                     "carries no letters, and the fact sheet does NOT disclose either. Track A's "
                     "comparator sees 8.5 % of this block's geometry (finding O11).",
        },
        {
            "key": "ss_table_graphic:left",
            "name": "real_ss_table_graphic_crop_cuts_first_row",
            "expected": ("PARTIAL", ["PARTIAL", "FAILED"]),
            "must_name": ["table", "cut", "top", "C11"],
            "notes": "REAL, not synthetic. The block bbox cuts the first row of the specification "
                     "table; Track A's Vision run turned that into the false claim 'position 1 was "
                     "added'. A verifier should report the clipped content, not the drawing.",
        },
        {
            "key": "eom_singleline_changed:left",
            "name": "real_eom_left_microsegment_explosion",
            "expected": ("PARTIAL", ["PARTIAL", "FAILED", "VERIFIED"]),
            "must_name": ["C9"],
            "notes": "REAL, not synthetic. 75 % of this side's segments are shorter than 0.001 of the "
                     "block because v001 explodes dashed linework (finding O12); the paired side has "
                     "27 %. The stated segment count is an exporter property, not a drawing property.",
        },
        {
            "key": "ss_plan_dense:left",
            "name": "real_ss_plan_dense_downstream_blindness",
            "expected": ("VERIFIED", ["VERIFIED"]),
            "must_name": [],
            "notes": "REAL, not synthetic, and a SCOPE control: the fact sheet is accurate, but the "
                     "comparator downstream only looks at its 12 000 longest segments (finding O11). "
                     "Verifying the description cannot catch a downstream cap — VERIFIED is correct "
                     "here, and that is the point of the case.",
        },
    ]
    for spec in real_specs:
        key = spec["key"]
        description = cache[key]
        profile = blocks[key]["profile"]
        sheet = vv.fact_sheet(description, disclose_limits=False)
        base_sheet = vv.fact_sheet(description, disclose_limits=True)
        measured = {
            "segments": profile["segments"],
            "texts": profile["texts"],
            "texts_garbled": profile["texts_garbled"],
            "garbled_share": profile["garbled_share"],
            "tiny_segment_share": profile["tiny_segment_share"],
            "storage_capped": profile["storage_capped"],
            "primitives_kept": profile["primitives"],
            "primitives_uncapped": profile["primitives_uncapped"],
            "kept_share_of_paths": round(profile["primitives"] / max(1, profile["primitives_uncapped"]), 4),
            "boundary_sides_touched": vv._boundary_touch(description),
            "comparator_segment_cap": vv.COMPARATOR_SEGMENT_CAP,
            "share_of_segments_the_comparator_would_see": round(
                min(1.0, vv.COMPARATOR_SEGMENT_CAP / max(1, profile["segments"])), 4),
        }
        measured["_notes"] = {
            "share_of_segments_the_comparator_would_see":
                "cap / segments RETAINED by this description. On a storage-capped block the "
                "extractor already dropped paths first, so the share of the BLOCK is smaller — "
                "finding O11 measures 8.5 % for vk_nodes with an uncapped independent extractor.",
            "tiny_segment_share":
                "share of segments shorter than 0.001 of the block, measured on THIS v0.1 "
                "description's anisotropically normalised coordinates. Finding O12 reports 75.3 % "
                "for eom left from an independent isotropic extractor; the two numbers are "
                "different measurements of the same defect, not a contradiction.",
        }
        add(key, "clean", family="real_defect", synthetic=False, disclose=False, seed=SEED,
            ground_truth={"kind": spec["name"], "corrupted": False, "real": True,
                          "detail": {"measured": measured,
                                     "provenance": "measured here from the Track A description; "
                                                   "cross-referenced to orchestrator findings O8/O10/O11/O12"}},
            sheet=sheet, base_sheet=base_sheet, expected=spec["expected"],
            must_name=spec["must_name"], notes=spec["notes"],
            extra={"real_case_name": spec["name"]})

    families = collections.Counter(c["family"] for c in cases)
    mutations = collections.Counter(c["mutation"] for c in cases)
    manifest = {
        "schema_version": "vv-cases-v1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "generator": "experiments/stage_comparison_vector_architecture_opus/probes/vv_build_cases.py",
        "harness": "experiments/stage_comparison_vector_architecture_opus/probes/vv_harness.py",
        "research_only": True,
        "reproduce": [
            "cd /home/coder/projects/PDF-proverka",
            "python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_harness selftest",
            "python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_build_cases",
            "python -m experiments.stage_comparison_vector_architecture_opus.probes.vv_smoke",
        ],
        "conventions": {
            "fact_sheet": "<=1200 characters, one numbered claim per line, each checkable from the crop alone",
            "disclose_limits": "when false the sheet omits the extractor's cap/self-rating claims; "
                               "this models a pipeline that truncates silently",
            "changed_claims": "claim ids whose text the mutation actually moved; an empty list means "
                              "the corruption is invisible in the sheet and cannot be detected from it",
            "must_name": "strings/claim-ids a correct verifier should surface; used for scoring recall",
            "strength": "'weak' marks a mutation the block could not carry at full force",
        },
        "summary": {
            "cases": len(cases),
            "by_family": dict(families),
            "by_mutation": dict(mutations),
            "clean_share": round(families["control"] / len(cases), 4),
            "blocks": len(blocks),
            "mutation_blocks": [f"{p}:{s}" for p, s in MUTATION_BLOCKS],
        },
        "blocks": blocks,
        "cases": cases,
    }
    return manifest


def main() -> None:
    manifest = build()
    vv.CASES_JSON.parent.mkdir(parents=True, exist_ok=True)
    vv.CASES_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                             encoding="utf-8")
    print(json.dumps(manifest["summary"], ensure_ascii=False, indent=2))
    print("written:", vv.CASES_JSON)
    undetectable = [c["case_id"] for c in manifest["cases"]
                    if c["family"] == "mutation" and not c["changed_claims"]]
    weak = [c["case_id"] for c in manifest["cases"] if c.get("strength") == "weak"]
    print("mutations invisible in their own fact sheet:", undetectable or "none")
    print("weak mutations:", weak or "none")


if __name__ == "__main__":
    main()
