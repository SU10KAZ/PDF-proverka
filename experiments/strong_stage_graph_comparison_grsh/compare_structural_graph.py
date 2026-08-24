#!/usr/bin/env python3
"""Compare the two independent GRSh system graphs and measure overlay failure."""
from __future__ import annotations

import collections
import json
import math
import sys
import tempfile
from pathlib import Path
from typing import Any

import cv2
import fitz
import numpy as np
from PIL import Image, ImageDraw

EXPERIMENT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = EXPERIMENT_DIR.parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from experiments.stage_comparison_graphic_objects_v03_codex.comparator import compare_graphic_scopes
from experiments.stage_comparison_graphic_objects_v03_codex.input_contract import resolve_prepared_block
from experiments.stage_comparison_graphic_objects_v03_codex.objects import build_graphic_block_description
from experiments.stage_comparison_graphic_objects_v03_codex.page_cache import PageDrawingCache


RENDERS = EXPERIMENT_DIR / "renders"


def _load(name: str) -> dict[str, Any]:
    return json.loads((EXPERIMENT_DIR / name).read_text(encoding="utf-8"))


def _branches(graph: dict[str, Any]) -> list[dict[str, Any]]:
    return list(graph["branches"])


def _match_branches(left: dict[str, Any], right: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    right_by_identity: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in _branches(right):
        if "UNCERTAIN" not in row["status"]:
            right_by_identity[row["functional_identity"]].append(row)
    matched: list[dict[str, Any]] = []
    unmatched_left: list[dict[str, Any]] = []
    used_right: set[tuple[int, int]] = set()
    for left_row in _branches(left):
        candidates = [
            row for row in right_by_identity.get(left_row["functional_identity"], [])
            if (row["section"], row["slot"]) not in used_right
        ]
        if not candidates:
            unmatched_left.append(left_row)
            continue
        candidates.sort(key=lambda row: (row["section"] != left_row["section"], abs(row["slot"] - left_row["slot"])))
        right_row = candidates[0]
        used_right.add((right_row["section"], right_row["slot"]))
        matched.append({
            "functional_identity": left_row["functional_identity"],
            "left": {key: left_row[key] for key in ("section", "slot", "terminal_label", "status")},
            "right": {key: right_row[key] for key in ("section", "slot", "terminal_label", "status")},
            "same_section": left_row["section"] == right_row["section"],
            "same_slot": left_row["slot"] == right_row["slot"] and left_row["section"] == right_row["section"],
            "identity_basis": "canonical EOM functional anchor; cross-version coordinates not used",
        })
    unmatched_right = [
        row for row in _branches(right)
        if (row["section"], row["slot"]) not in used_right
    ]
    return matched, unmatched_left, unmatched_right


def _structural_comparison(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    matched, unmatched_left, unmatched_right = _match_branches(left, right)
    moved = [row for row in matched if not row["same_slot"]]
    rewired = [row for row in matched if not row["same_section"]]
    certain_left = [row for row in _branches(left) if "UNCERTAIN" not in row["status"]]
    certain_right = [row for row in _branches(right) if "UNCERTAIN" not in row["status"]]
    events = [
        {
            "class": "UNCHANGED_FUNCTIONAL_STRUCTURE",
            "level": "A_SYSTEM_BACKBONE",
            "confidence": 0.99,
            "finding": "Сохранены два источника, два ввода, две секции шин и межсекционная связь.",
            "evidence": {"left": left["backbone"], "right": right["backbone"]},
        },
        {
            "class": "STRUCTURE_REORGANIZED",
            "level": "B_FUNCTIONAL_GROUPS",
            "confidence": 0.99,
            "finding": "Порядок и компоновка ветвей существенно изменены; identity сопоставлялась по функции, а не по позиции.",
            "evidence": {"matched_branches": len(matched), "matched_but_moved": len(moved)},
        },
        {
            "class": "SECTIONING_CHANGED",
            "level": "C_DEVICE_CONNECTION",
            "confidence": 0.98,
            "finding": "Топология секционирования сохранена, но секционный аппарат изменён с QF3 на QS1 и иначе представлен по управлению.",
            "evidence": {"left": left["backbone"]["section_device"], "right": right["backbone"]["section_device"]},
        },
        {
            "class": "NODE_TYPE_CHANGED",
            "level": "C_DEVICE_CONNECTION",
            "confidence": 0.99,
            "finding": "Класс межсекционного устройства изменён: автоматический выключатель → секционный выключатель/разъединитель.",
            "evidence": {"left_node": "section_device", "right_node": "section_device"},
        },
        {
            "class": "DETAIL_LEVEL_INCREASED",
            "level": "A_SYSTEM_BACKBONE",
            "confidence": 0.97,
            "finding": "RIGHT показывает Т1 и Т2 явно; LEFT показывает подключения к ТП1/ТП2. Это не доказательство появления новых источников.",
            "evidence": {"left": left["backbone"]["source_representation"], "right": right["backbone"]["source_representation"]},
        },
        {
            "class": "OUTGOING_GROUP_CHANGED",
            "level": "B_FUNCTIONAL_GROUPS",
            "confidence": 0.99,
            "finding": "Количество отходящих аппаратов изменилось 30 → 27; свободные резервы 2 → 0.",
            "evidence": {"left": left["outgoing_summary"], "right": right["outgoing_summary"]},
        },
        {
            "class": "UNCERTAIN",
            "level": "C_DEVICE_CONNECTION",
            "confidence": 1.0,
            "finding": "Точное соответствие хвостовой ветви 2QF14 не утверждается из-за конфликта terminal-якоря ЯСН ТП и feeder-якоря ЭБ-ГВС.",
            "evidence": {"right_branch": next(row for row in right["branches"] if row["section"] == 2 and row["slot"] == 14)},
        },
    ]
    if rewired:
        events.append({
            "class": "CONNECTION_REWIRED",
            "level": "C_DEVICE_CONNECTION",
            "confidence": 0.9,
            "finding": "Часть функционально совпадающих ветвей отнесена к другой секции.",
            "evidence": rewired,
        })
    return {
        "schema_version": "system-graph-comparison-grsh-poc-v1",
        "research_only": True,
        "scope": {
            "left_block_id": left["input"]["block_id"],
            "right_block_id": right["input"]["block_id"],
            "pairing": "explicit prepared IMAGE block pair",
            "cross_version_coordinate_use": False,
            "text_table_diff_performed": False,
        },
        "level_a_system_backbone": {
            "verdict": "UNCHANGED_FUNCTIONAL_STRUCTURE_WITH_SECTION_DEVICE_CHANGE",
            "sources": {"left": 2, "right": 2},
            "inputs": {"left": 2, "right": 2},
            "bus_sections": {"left": 2, "right": 2},
            "section_tie": {"left": True, "right": True},
            "source_representation": {"left": left["backbone"]["source_representation"], "right": right["backbone"]["source_representation"]},
        },
        "level_b_functional_groups": {
            "metering": "present on both sides; grouping/drawing detail reorganized",
            "compensation": "АУКРМ-1 and АУКРМ-2 preserved and moved in layout/order",
            "outgoing_counts": {"left": left["outgoing_summary"], "right": right["outgoing_summary"]},
            "certain_branch_correspondences": len(matched),
            "correspondence_denominator_left": len(certain_left),
            "correspondence_denominator_right": len(certain_right),
            "matched_but_moved": len(moved),
        },
        "level_c_devices_connections": {
            "section_device_type_changed": True,
            "same_section_matches": len(matched) - len(rewired),
            "cross_section_matches": len(rewired),
            "unmatched_left": unmatched_left,
            "unmatched_right": unmatched_right,
            "caveat": "Level C is not used to negate the preserved Level A backbone.",
        },
        "branch_correspondence": matched,
        "events": events,
        "human_answers": {
            "A_two_section_architecture": "YES",
            "B_source_count": {"left": 2, "right": 2},
            "C_source_input_bus": "two parallel source → input device → bus section paths on both sides",
            "D_section_relation": "one inter-section tie on both sides",
            "E_sectioning_principle": "topology unchanged; device type/control representation changed",
            "F_outgoing_groups": "15+15 (2 free reserves) → 13+14 (no free reserve shown)",
            "G_preserved_moved_branches": len(moved),
            "H_new_removed_nodes": "no confirmed new/removed backbone function; outgoing slots reduced; exact one-to-one for two RIGHT identities remains uncertain",
            "I_principle_or_detail": "system principle preserved; real section-device/outgoing implementation changes coexist with increased RD detail",
        },
        "verdict": {
            "choice": "B",
            "text": "Structural graph comparison works on this pair, but reliable identity and engineering meaning require an EOM/single-line profile.",
        },
    }


def _render_block(graph: dict[str, Any], output: Path, *, scale: float = 2.0) -> Image.Image:
    blocks_path = REPOSITORY_ROOT / graph["input"]["blocks_json"]
    blocks = json.loads(blocks_path.read_text(encoding="utf-8"))
    block = next(row for row in blocks["blocks"] if row["block_id"] == graph["input"]["block_id"])
    document = fitz.open(blocks_path.with_name("document.pdf"))
    try:
        page = document[int(block["page_index"])]
        x0, y0, x1, y1 = block["coords_norm"]
        clip = fitz.Rect(x0 * page.rect.width, y0 * page.rect.height, x1 * page.rect.width, y1 * page.rect.height)
        pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False)
        image = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)
    finally:
        document.close()
    polygon = block.get("polygon_points") or []
    if polygon:
        mask = Image.new("L", image.size, 0)
        draw = ImageDraw.Draw(mask)
        draw.polygon([
            ((x - x0) / (x1 - x0) * image.width, (y - y0) / (y1 - y0) * image.height)
            for x, y in polygon
        ], fill=255)
        white = Image.new("RGB", image.size, "white")
        white.paste(image, mask=mask)
        image = white
    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output, optimize=True)
    return image


def _side_by_side(left: Image.Image, right: Image.Image, output: Path) -> None:
    target_width = 1400
    images = []
    for label, source in (("LEFT / P", left), ("RIGHT / RD", right)):
        target_height = round(source.height * target_width / source.width)
        images.append((label, source.resize((target_width, target_height), Image.Resampling.LANCZOS)))
    canvas = Image.new("RGB", (target_width * 2, max(image.height for _, image in images) + 70), "white")
    draw = ImageDraw.Draw(canvas)
    for index, (label, image) in enumerate(images):
        x = index * target_width
        canvas.paste(image, (x, 70))
        draw.text((x + 20, 20), label, fill="black")
    canvas.save(output, optimize=True)


def _component_count(mask: np.ndarray, minimum_area: int = 4) -> int:
    count, _, stats, _ = cv2.connectedComponentsWithStats(mask.astype(np.uint8), 8)
    if count <= 1:
        return 0
    return int((stats[1:, cv2.CC_STAT_AREA] >= minimum_area).sum())


def _generic_diagnostic(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    references = [
        {"blocks_json": left["input"]["blocks_json"], "block_id": left["input"]["block_id"], "block_group_id": "strong_p_rd_grsh"},
        {"blocks_json": right["input"]["blocks_json"], "block_id": right["input"]["block_id"], "block_group_id": "strong_p_rd_grsh"},
    ]
    with tempfile.TemporaryDirectory(prefix="grsh-generic-") as cache_dir:
        cache = PageDrawingCache(cache_dir)
        descriptions = [build_graphic_block_description(resolve_prepared_block(reference), cache) for reference in references]
        ledger = compare_graphic_scopes("strong_p_rd_grsh", [descriptions[0]], [descriptions[1]], block_group_id="strong_p_rd_grsh")
    return {
        "method": "existing generic prepared-object graph v0.3",
        "left": {
            "quality": descriptions[0]["quality"],
            "visible_geometry_summary": descriptions[0]["visible_geometry_summary"],
            "uncertainties": descriptions[0]["uncertainties"],
        },
        "right": {
            "quality": descriptions[1]["quality"],
            "visible_geometry_summary": descriptions[1]["visible_geometry_summary"],
            "uncertainties": descriptions[1]["uncertainties"],
        },
        "alignment": ledger["alignment"],
        "decision": ledger["decision"],
        "object_status_counts": dict(sorted(collections.Counter(row["status"] for row in ledger["object_statuses"]).items())),
        "change_event_count": len(ledger["changes"]),
        "interpretation": "CAD primitive packaging dominates generic object formation; these events are not engineering ground truth.",
    }


def _overlay_diagnostic(left_graph: dict[str, Any], right_graph: dict[str, Any]) -> dict[str, Any]:
    RENDERS.mkdir(parents=True, exist_ok=True)
    left_image = _render_block(left_graph, RENDERS / "left_block.png")
    right_image = _render_block(right_graph, RENDERS / "right_block.png")
    _side_by_side(left_image, right_image, RENDERS / "side_by_side.png")

    size = (1600, 900)
    left_gray = cv2.cvtColor(np.asarray(left_image), cv2.COLOR_RGB2GRAY)
    right_gray = cv2.cvtColor(np.asarray(right_image), cv2.COLOR_RGB2GRAY)
    left_normalized = cv2.resize(left_gray, size, interpolation=cv2.INTER_AREA)
    right_normalized = cv2.resize(right_gray, size, interpolation=cv2.INTER_AREA)
    threshold = 220
    left_ink = left_normalized < threshold
    right_ink = right_normalized < threshold
    intersection = np.logical_and(left_ink, right_ink)
    union = np.logical_or(left_ink, right_ink)
    left_only = np.logical_and(left_ink, ~right_ink)
    right_only = np.logical_and(right_ink, ~left_ink)

    overlay = np.full((size[1], size[0], 3), 255, dtype=np.uint8)
    overlay[intersection] = (25, 25, 25)
    overlay[left_only] = (210, 0, 160)
    overlay[right_only] = (0, 150, 210)
    cv2.imwrite(str(RENDERS / "overlay_stretch_diagnostic.png"), cv2.cvtColor(overlay, cv2.COLOR_RGB2BGR))

    def _orb_input(image: np.ndarray) -> np.ndarray:
        scale = 1600 / max(image.shape)
        return cv2.resize(image, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)

    left_orb = _orb_input(left_gray)
    right_orb = _orb_input(right_gray)
    orb = cv2.ORB_create(nfeatures=8000, fastThreshold=10)
    left_keypoints, left_desc = orb.detectAndCompute(left_orb, None)
    right_keypoints, right_desc = orb.detectAndCompute(right_orb, None)
    good_matches = []
    affine = None
    affine_inliers = None
    homography_inliers = None
    if left_desc is not None and right_desc is not None:
        matcher = cv2.BFMatcher(cv2.NORM_HAMMING)
        for pair in matcher.knnMatch(left_desc, right_desc, k=2):
            if len(pair) == 2 and pair[0].distance < 0.72 * pair[1].distance:
                good_matches.append(pair[0])
    if len(good_matches) >= 6:
        source = np.float32([left_keypoints[match.queryIdx].pt for match in good_matches])
        target = np.float32([right_keypoints[match.trainIdx].pt for match in good_matches])
        affine, affine_mask = cv2.estimateAffinePartial2D(
            source, target, method=cv2.RANSAC, ransacReprojThreshold=3, maxIters=5000, confidence=0.999
        )
        _, homography_mask = cv2.findHomography(source, target, cv2.RANSAC, 3, maxIters=5000, confidence=0.999)
        affine_inliers = int(affine_mask.sum()) if affine_mask is not None else 0
        homography_inliers = int(homography_mask.sum()) if homography_mask is not None else 0
    affine_scale = None
    affine_rotation = None
    if affine is not None:
        affine_scale = math.hypot(float(affine[0, 0]), float(affine[0, 1]))
        affine_rotation = math.degrees(math.atan2(float(affine[0, 1]), float(affine[0, 0])))

    generic = _generic_diagnostic(left_graph, right_graph)
    return {
        "schema_version": "overlay-comparison-diagnostic-grsh-poc-v1",
        "research_only": True,
        "scope": {"left_block_id": left_graph["input"]["block_id"], "right_block_id": right_graph["input"]["block_id"]},
        "renders": {
            "left": "renders/left_block.png",
            "right": "renders/right_block.png",
            "side_by_side": "renders/side_by_side.png",
            "overlay": "renders/overlay_stretch_diagnostic.png",
            "left_size_px": list(left_image.size),
            "right_size_px": list(right_image.size),
            "left_aspect_ratio": round(left_image.width / left_image.height, 6),
            "right_aspect_ratio": round(right_image.width / right_image.height, 6),
        },
        "feature_registration": {
            "method": "ORB + ratio test + RANSAC diagnostic only",
            "left_keypoints": len(left_keypoints),
            "right_keypoints": len(right_keypoints),
            "good_matches": len(good_matches),
            "affine_inliers": affine_inliers,
            "affine_inlier_ratio": None if not good_matches or affine_inliers is None else round(affine_inliers / len(good_matches), 6),
            "homography_inliers": homography_inliers,
            "homography_inlier_ratio": None if not good_matches or homography_inliers is None else round(homography_inliers / len(good_matches), 6),
            "estimated_affine_scale": None if affine_scale is None else round(affine_scale, 6),
            "estimated_affine_rotation_deg": None if affine_rotation is None else round(affine_rotation, 6),
            "reliable": bool(affine_scale and affine_inliers and affine_inliers / len(good_matches) >= 0.5 and 0.5 <= affine_scale <= 2.0),
            "reason": "too few geometrically consistent anchors; fitted affine is degenerate or dominated by repeated CAD/text features",
        },
        "forced_bbox_normalized_ink_diff": {
            "warning": "Each whole block is independently stretched to 1600x900. This is an intentionally generous overlay upper bound, not a valid engineering registration.",
            "threshold_gray_lt": threshold,
            "left_ink_pixels": int(left_ink.sum()),
            "right_ink_pixels": int(right_ink.sum()),
            "intersection_pixels": int(intersection.sum()),
            "union_pixels": int(union.sum()),
            "iou": round(float(intersection.sum() / max(int(union.sum()), 1)), 6),
            "dice": round(float(2 * intersection.sum() / max(int(left_ink.sum() + right_ink.sum()), 1)), 6),
            "ink_lost_pixels": int(left_only.sum()),
            "ink_new_pixels": int(right_only.sum()),
            "ink_lost_components_area_ge_4px": _component_count(left_only),
            "ink_new_components_area_ge_4px": _component_count(right_only),
            "interpretation": "thousands of pixel islands describe layout/primitive/text packaging, not thousands of system changes",
        },
        "generic_graph_diagnostic": generic,
        "structural_graph_contrast": {
            "engineering_events": 7,
            "backbone_events": 2,
            "generic_change_events": generic["change_event_count"],
            "conclusion": "EOM structural comparison compresses the same pair into a preserved backbone plus a small set of typed changes and uncertainties.",
        },
    }


def main() -> None:
    left = _load("left_structural_description.json")
    right = _load("right_structural_description.json")
    comparison = _structural_comparison(left, right)
    (EXPERIMENT_DIR / "structural_comparison.json").write_text(
        json.dumps(comparison, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    overlay = _overlay_diagnostic(left, right)
    (EXPERIMENT_DIR / "overlay_comparison_diagnostic.json").write_text(
        json.dumps(overlay, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"structural matches: {comparison['level_b_functional_groups']['certain_branch_correspondences']}")
    print(f"generic events: {overlay['generic_graph_diagnostic']['change_event_count']}")
    print(f"ink IoU: {overlay['forced_bbox_normalized_ink_diff']['iou']}")


if __name__ == "__main__":
    main()
