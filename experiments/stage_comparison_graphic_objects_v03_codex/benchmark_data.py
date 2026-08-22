"""Real prepared-block benchmark and independently fixed human graphic GT."""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_blocks_v02_codex.benchmark_data import benchmark_manifest as v02_manifest


EXCLUDED = {
    "eom_singleline_changed": "No upstream 02_work/blocks.json exists for either selected EOM version; its v0.2 manual bbox is forbidden by the v0.3 input contract.",
}
GRAPHIC_POSITIVES = {
    "vk_plan": [
        {"type": "ADDED_OBJECT", "region": "plumbing connection points across plan", "fact": "RIGHT adds numerous small red ring markers at plumbing nodes."},
    ],
    "vk_nodes": [
        {"type": "ADDED_OBJECT", "region": "lower orange drainage fragment", "fact": "RIGHT adds a short horizontal branch/fitting and its graphical dimension line."},
    ],
    "ov_plan_floor07": [
        {"type": "REMOVED_OBJECT", "region": "several pink equipment zones", "fact": "Internal graphical equipment contours/symbols present on the left are absent on the right."},
    ],
}
UNSURE = {
    "ss_crop_mismatch_page07": "Prepared blocks cover different semantic extents; a block matcher or remapping would be needed, and is out of scope.",
}
TEXT_ONLY = {"ss_scheme_text_changed", "vk_axono_page17"}
TABLE_ONLY = {"ss_table_page19", "ov_equipment_table"}


def _reference(side: dict[str, Any], pair_id: str) -> dict[str, Any]:
    # Deliberately omit bbox_norm: resolver must read the authoritative row.
    return {
        "blocks_json": str(Path(side["pdf"]).with_name("blocks.json")),
        "block_id": side["block_id"],
        "block_group_id": pair_id,
    }


def benchmark_pairs() -> list[dict[str, Any]]:
    result = []
    for prior in v02_manifest()["pairs"]:
        pair_id = prior["pair_id"]
        if pair_id in EXCLUDED:
            continue
        test_focus = []
        if pair_id in TEXT_ONLY:
            test_focus.append("TEXT_ONLY_CHANGE")
        if pair_id in TABLE_ONLY:
            test_focus.append("TABLE_ONLY_OR_TABLE_BLOCK")
        if "dense" in prior.get("type", "") or "large" in prior.get("type", ""):
            test_focus.append("DENSE_BLOCK")
        if "crop" in prior.get("type", "") or pair_id in UNSURE:
            test_focus.append("CROP_EDGE_CASE")
        if pair_id == "ov_plan_floor07":
            test_focus.extend(["LOCAL_REMOVAL", "GRAPHIC_CHANGE"])
        expected = "GRAPHIC_CHANGE" if pair_id in GRAPHIC_POSITIVES else ("UNSURE" if pair_id in UNSURE else "NO_GRAPHIC_CHANGE")
        result.append({
            "pair_id": pair_id,
            "discipline": prior["discipline"],
            "source_kind": prior.get("type"),
            "comparison_unit": "ALREADY_PREPARED_GRAPHIC_BLOCK",
            "selection": "Existing manually paired block IDs from baseline commits 1619fc3f/5e334546; coordinates are not copied and are resolved from blocks.json.",
            "scope": {
                "block_group_id": pair_id,
                "left_blocks": [_reference(prior["left"], pair_id)],
                "right_blocks": [_reference(prior["right"], pair_id)],
                "pairing_source": "fixed benchmark pair; automatic block and 1-to-N matching are out of scope",
            },
            "test_focus": test_focus or ["REAL_UNCHANGED_CONTROL"],
            "ground_truth": {
                "expected_graphic_verdict": expected,
                "important_graphic_events": copy.deepcopy(GRAPHIC_POSITIVES.get(pair_id, [])),
                "graphic_scope_note": UNSURE.get(pair_id) or ("Text differences are handled by the separate text pipeline and are not graphic GT." if pair_id in TEXT_ONLY else "No manually confirmed graphical-object change."),
            },
        })
    assert len(result) == 38
    assert len({row["pair_id"] for row in result}) == 38
    return result


def benchmark_manifest() -> dict[str, Any]:
    return {
        "schema_version": "prepared-graphic-block-benchmark-v0.3-codex",
        "research_only": True,
        "baseline_commits": ["1619fc3f", "5e334546"],
        "comparison_unit": "ALREADY_PREPARED_GRAPHIC_BLOCK",
        "selection_method": "38 real pairs whose block IDs resolve in existing blocks.json. No bbox is present in this manifest and no block matcher runs.",
        "corpus_limit": "Three manually confirmed graphical-change positives exist among eligible prepared pairs after independent raster adjudication; controlled falsifiers supplement mechanism coverage but are reported separately.",
        "excluded": EXCLUDED,
        "pairs": benchmark_pairs(),
    }


def ground_truth_artifact() -> dict[str, Any]:
    return {
        "schema_version": "graphic-object-ground-truth-v0.3-codex",
        "judge": "Manual side-by-side raster review retained from the baseline research and reclassified to graphic-only scope. VK plan/nodes were independently raster-adjudicated again after a conflict and visible graphical additions were confirmed; model statements are not ground truth.",
        "label_policy": {"GRAPHIC_CHANGE": "manually confirmed graphical-object event", "NO_GRAPHIC_CHANGE": "no confirmed graphical event; text/table facts excluded", "PARTIAL": "some but not all graphic facts observable", "UNSURE": "prepared scope prevents a defensible graphic judgment"},
        "pairs": [{"pair_id": row["pair_id"], **copy.deepcopy(row["ground_truth"])} for row in benchmark_pairs()],
    }


__all__ = ["benchmark_manifest", "ground_truth_artifact", "benchmark_pairs", "TEXT_ONLY", "TABLE_ONLY"]
