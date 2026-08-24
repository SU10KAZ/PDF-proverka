"""Deterministic translation + uniform-scale registration for Mode 1."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Sequence

import cv2
import numpy as np

from .extraction import ink_length, rasterize
from .policy import GraphicMode1Policy


@dataclass(frozen=True)
class SimilarityTransform:
    scale: float = 1.0
    rotation_deg: float = 0.0
    tx: float = 0.0
    ty: float = 0.0

    def apply_points(self, points: np.ndarray) -> np.ndarray:
        if len(points) == 0:
            return points.copy()
        cosine = math.cos(math.radians(self.rotation_deg)) * self.scale
        sine = math.sin(math.radians(self.rotation_deg)) * self.scale
        result = np.empty_like(points, dtype=np.float64)
        result[:, 0] = cosine * points[:, 0] - sine * points[:, 1] + self.tx
        result[:, 1] = sine * points[:, 0] + cosine * points[:, 1] + self.ty
        return result

    def apply_segments(self, segments: np.ndarray) -> np.ndarray:
        if len(segments) == 0:
            return segments.copy()
        starts = self.apply_points(segments[:, :2].astype(np.float64))
        ends = self.apply_points(segments[:, 2:].astype(np.float64))
        return np.concatenate([starts, ends], axis=1).astype(np.float32)

    def apply_fill_groups(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        output = []
        for group in groups:
            output.append({
                "polys": [self.apply_points(polygon).astype(np.float32) for polygon in group.get("polys") or []],
                "even_odd": bool(group.get("even_odd")),
                "clips": [
                    [self.apply_points(polygon).astype(np.float32) for polygon in clip_group]
                    for clip_group in group.get("clips") or []
                ] or None,
            })
        return output

    def inverse(self) -> "SimilarityTransform":
        if abs(self.scale) < 1e-12:
            raise ValueError("non_invertible_transform")
        inverse_scale = 1.0 / self.scale
        angle = -self.rotation_deg
        cosine = math.cos(math.radians(angle)) * inverse_scale
        sine = math.sin(math.radians(angle)) * inverse_scale
        return SimilarityTransform(
            scale=inverse_scale,
            rotation_deg=angle,
            tx=-(cosine * self.tx - sine * self.ty),
            ty=-(sine * self.tx + cosine * self.ty),
        )

    def public_dict(self) -> dict[str, float]:
        return {key: round(float(value), 6) for key, value in asdict(self).items()}


def transform_polygon(
    polygon: Sequence[Sequence[float]] | None,
    transform: SimilarityTransform,
) -> list[list[float]] | None:
    if not polygon:
        return None
    result = transform.apply_points(np.asarray(polygon, np.float64))
    return result.tolist()


def transform_bbox(bbox: Sequence[float], transform: SimilarityTransform) -> list[float]:
    corners = np.asarray([
        [bbox[0], bbox[1]], [bbox[2], bbox[1]],
        [bbox[2], bbox[3]], [bbox[0], bbox[3]],
    ], np.float64)
    result = transform.apply_points(corners)
    return [
        float(result[:, 0].min()), float(result[:, 1].min()),
        float(result[:, 0].max()), float(result[:, 1].max()),
    ]


def merge_collinear_chains(
    segments: np.ndarray, path_ids: np.ndarray, angle_tolerance_deg: float = 1.0,
) -> np.ndarray:
    """Remove CAD packaging differences before descriptor matching."""
    if len(segments) == 0:
        return segments.copy()
    output = []
    index = 0
    while index < len(segments):
        x0, y0, x1, y1 = map(float, segments[index])
        path_id = int(path_ids[index])
        angle = math.degrees(math.atan2(y1 - y0, x1 - x0))
        next_index = index + 1
        while next_index < len(segments) and int(path_ids[next_index]) == path_id:
            ax, ay, bx, by = map(float, segments[next_index])
            if abs(ax - x1) > 1e-4 or abs(ay - y1) > 1e-4:
                break
            next_angle = math.degrees(math.atan2(by - ay, bx - ax))
            delta = abs((next_angle - angle + 180.0) % 360.0 - 180.0)
            if delta > angle_tolerance_deg:
                break
            x1, y1 = bx, by
            next_index += 1
        output.append((x0, y0, x1, y1))
        index = next_index
    return np.asarray(output, np.float32).reshape(-1, 4)


def _descriptors(
    segments: np.ndarray, min_length: float, length_quantum: float = 0.25,
    angle_quantum: float = 1.0,
) -> tuple[dict[tuple[int, int], list[int]], np.ndarray]:
    if len(segments) == 0:
        return {}, np.zeros((0, 2), np.float32)
    dx = segments[:, 2] - segments[:, 0]
    dy = segments[:, 3] - segments[:, 1]
    lengths = np.hypot(dx, dy)
    indices = np.nonzero(lengths >= min_length)[0]
    if len(indices) == 0:
        return {}, np.zeros((0, 2), np.float32)
    angles = np.degrees(np.arctan2(dy[indices], dx[indices])) % 180.0
    midpoints = np.stack([
        (segments[indices, 0] + segments[indices, 2]) / 2.0,
        (segments[indices, 1] + segments[indices, 3]) / 2.0,
    ], axis=1).astype(np.float32)
    length_keys = np.round(lengths[indices] / length_quantum).astype(np.int64)
    angle_bins = max(1, int(round(180.0 / angle_quantum)))
    angle_keys = np.round(angles / angle_quantum).astype(np.int64) % angle_bins
    groups: dict[tuple[int, int], list[int]] = {}
    for local_index in range(len(indices)):
        groups.setdefault((int(length_keys[local_index]), int(angle_keys[local_index])), []).append(local_index)
    return groups, midpoints


def _translation_vote(
    left_segments: np.ndarray,
    right_segments: np.ndarray,
    min_length: float,
    *,
    bin_pt: float = 2.0,
    max_per_descriptor: int = 6,
    max_votes: int = 400_000,
) -> tuple[tuple[float, float] | None, int]:
    left_groups, left_midpoints = _descriptors(left_segments, min_length)
    right_groups, right_midpoints = _descriptors(right_segments, min_length)
    common = sorted(
        set(left_groups) & set(right_groups),
        key=lambda key: len(left_groups[key]) * len(right_groups[key]),
    )
    votes = []
    for key in common:
        for left_index in left_groups[key][:max_per_descriptor]:
            for right_index in right_groups[key][:max_per_descriptor]:
                votes.append(right_midpoints[right_index] - left_midpoints[left_index])
        if len(votes) >= max_votes:
            break
    if not votes:
        return None, 0
    values = np.asarray(votes, np.float64)
    bins = np.round(values / bin_pt).astype(np.int64)
    unique, counts = np.unique(bins, axis=0, return_counts=True)
    best = unique[int(np.argmax(counts))]
    selected = (np.abs(bins[:, 0] - best[0]) <= 1) & (np.abs(bins[:, 1] - best[1]) <= 1)
    median = np.median(values[selected], axis=0)
    return (float(median[0]), float(median[1])), int(selected.sum())


def _correspondences(
    left_segments: np.ndarray,
    right_segments: np.ndarray,
    transform: SimilarityTransform,
    min_length: float,
    tolerance: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Greedy nearest same-descriptor pairs, with no SciPy dependency.

    A tolerance-sized spatial grid keeps repeated CAD primitives linear in
    practice.  Looking in the neighbouring 3x3 cells is exhaustive for every
    candidate that can be within ``tolerance``.
    """
    left_groups, left_midpoints = _descriptors(left_segments, min_length)
    right_groups, right_midpoints = _descriptors(right_segments, min_length)
    transformed = transform.apply_points(left_midpoints.astype(np.float64))
    sources = []
    targets = []
    for key in sorted(set(left_groups) & set(right_groups)):
        grid: dict[tuple[int, int], set[int]] = {}
        for right_index in right_groups[key]:
            point = right_midpoints[right_index]
            cell = (
                math.floor(float(point[0]) / tolerance),
                math.floor(float(point[1]) / tolerance),
            )
            grid.setdefault(cell, set()).add(right_index)
        for left_index in left_groups[key]:
            if not grid:
                break
            point = transformed[left_index]
            cell_x = math.floor(float(point[0]) / tolerance)
            cell_y = math.floor(float(point[1]) / tolerance)
            candidate_indices = np.asarray(sorted(
                candidate
                for offset_y in (-1, 0, 1)
                for offset_x in (-1, 0, 1)
                for candidate in grid.get((cell_x + offset_x, cell_y + offset_y), ())
            ), dtype=np.int64)
            if len(candidate_indices) == 0:
                continue
            delta = right_midpoints[candidate_indices] - transformed[left_index]
            distances = np.hypot(delta[:, 0], delta[:, 1])
            best_local = min(
                range(len(candidate_indices)),
                key=lambda index: (float(distances[index]), int(candidate_indices[index])),
            )
            if float(distances[best_local]) <= tolerance:
                right_index = int(candidate_indices[best_local])
                right_point = right_midpoints[right_index]
                right_cell = (
                    math.floor(float(right_point[0]) / tolerance),
                    math.floor(float(right_point[1]) / tolerance),
                )
                grid[right_cell].remove(right_index)
                if not grid[right_cell]:
                    del grid[right_cell]
                sources.append(left_midpoints[left_index])
                targets.append(right_midpoints[right_index])
    return (
        np.asarray(sources, np.float32).reshape(-1, 2),
        np.asarray(targets, np.float32).reshape(-1, 2),
    )


def _fit_similarity(
    sources: np.ndarray,
    targets: np.ndarray,
    *,
    allow_rotation: bool,
) -> SimilarityTransform | None:
    if len(sources) < 3:
        return None
    source_center = sources.mean(axis=0)
    target_center = targets.mean(axis=0)
    left = sources - source_center
    right = targets - target_center
    variance = float((left**2).sum())
    if variance < 1e-9:
        return None
    if allow_rotation:
        cosine_term = float((left[:, 0] * right[:, 0] + left[:, 1] * right[:, 1]).sum())
        sine_term = float((left[:, 0] * right[:, 1] - left[:, 1] * right[:, 0]).sum())
        rotation = math.degrees(math.atan2(sine_term, cosine_term))
        scale = math.hypot(cosine_term, sine_term) / variance
    else:
        rotation = 0.0
        scale = float((left * right).sum() / variance)
    if not 0.2 < scale < 5.0:
        return None
    cosine = math.cos(math.radians(rotation)) * scale
    sine = math.sin(math.radians(rotation)) * scale
    tx = target_center[0] - (cosine * source_center[0] - sine * source_center[1])
    ty = target_center[1] - (sine * source_center[0] + cosine * source_center[1])
    return SimilarityTransform(scale, rotation, float(tx), float(ty))


def _coverage(
    left_segments: np.ndarray,
    right_segments: np.ndarray,
    transform: SimilarityTransform,
    frame: Sequence[float],
    policy: GraphicMode1Policy,
    *,
    left_fills: list[dict[str, Any]],
    right_fills: list[dict[str, Any]],
    left_polygon: Sequence[Sequence[float]] | None,
    right_polygon: Sequence[Sequence[float]] | None,
    right_raster: tuple[np.ndarray, np.ndarray] | None = None,
) -> dict[str, float | int]:
    left_mask = rasterize(
        transform.apply_segments(left_segments), None, frame, policy.cell_pt,
        fills=transform.apply_fill_groups(left_fills),
        clip_polygon=transform_polygon(left_polygon, transform),
    )
    radius = max(1, int(round(policy.tolerance_pt / policy.cell_pt)))
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * radius + 1, 2 * radius + 1))
    if right_raster is None:
        right_mask = rasterize(
            right_segments, None, frame, policy.cell_pt,
            fills=right_fills, clip_polygon=right_polygon,
        )
        right_dilated = cv2.dilate(right_mask, kernel)
    else:
        right_mask, right_dilated = right_raster
    left_dilated = cv2.dilate(left_mask, kernel)
    left_cells, right_cells = int(left_mask.sum()), int(right_mask.sum())
    left_matched = int((left_mask & right_dilated).sum())
    right_matched = int((right_mask & left_dilated).sum())
    return {
        "left_ink_cells": left_cells,
        "right_ink_cells": right_cells,
        "left_matched": left_matched,
        "right_matched": right_matched,
        "left_cov": left_matched / left_cells if left_cells else 0.0,
        "right_cov": right_matched / right_cells if right_cells else 0.0,
        "sym_cov": (left_matched + right_matched) / (left_cells + right_cells) if left_cells + right_cells else 0.0,
    }


def _residual(
    sources: np.ndarray, targets: np.ndarray, transform: SimilarityTransform,
) -> dict[str, float | int | None]:
    if len(sources) == 0:
        return {"n": 0, "median_pt": None, "p90_pt": None}
    transformed = transform.apply_points(sources.astype(np.float64))
    distances = np.hypot(transformed[:, 0] - targets[:, 0], transformed[:, 1] - targets[:, 1])
    return {
        "n": int(len(distances)),
        "median_pt": round(float(np.median(distances)), 4),
        "p90_pt": round(float(np.percentile(distances, 90)), 4),
    }


def register(
    left: dict[str, Any],
    right: dict[str, Any],
    policy: GraphicMode1Policy,
) -> tuple[dict[str, Any], SimilarityTransform, np.ndarray, np.ndarray]:
    """Estimate LEFT -> RIGHT; free affine and non-uniform scale are absent."""
    left_segments = merge_collinear_chains(left["segments"], left["path_ids"])
    right_segments = merge_collinear_chains(right["segments"], right["path_ids"])
    left_bbox = left["bbox_visual_pt"]
    right_bbox = right["bbox_visual_pt"]
    left_width, left_height = left_bbox[2] - left_bbox[0], left_bbox[3] - left_bbox[1]
    right_width, right_height = right_bbox[2] - right_bbox[0], right_bbox[3] - right_bbox[1]
    frame = tuple(map(float, right_bbox))
    min_length = max(1.5, 0.004 * math.hypot(right_width, right_height))
    left_center = ((left_bbox[0] + left_bbox[2]) / 2, (left_bbox[1] + left_bbox[3]) / 2)
    right_center = ((right_bbox[0] + right_bbox[2]) / 2, (right_bbox[1] + right_bbox[3]) / 2)
    anchor = SimilarityTransform(1.0, 0.0, right_bbox[0] - left_bbox[0], right_bbox[1] - left_bbox[1])
    scale = min(right_width / left_width, right_height / left_height) if left_width > 0 and left_height > 0 else 1.0
    bbox_fit = SimilarityTransform(
        scale, 0.0,
        right_center[0] - scale * left_center[0],
        right_center[1] - scale * left_center[1],
    )
    # Same page coordinates are a legitimate and especially important
    # hypothesis when prepared crop borders drift.  Without it a repeated CAD
    # grid can vote itself one bay aside and manufacture border changes.
    hypotheses: list[tuple[str, SimilarityTransform]] = [
        ("page_identity", SimilarityTransform()),
        ("bbox_anchor", anchor),
        ("bbox_fit", bbox_fit),
    ]
    vote, vote_count = _translation_vote(anchor.apply_segments(left_segments), right_segments, min_length)
    if vote is not None:
        hypotheses.append(("translation_vote", SimilarityTransform(1.0, 0.0, anchor.tx + vote[0], anchor.ty + vote[1])))
    if abs(scale - 1.0) > 0.02:
        scaled_vote, _ = _translation_vote(bbox_fit.apply_segments(left_segments), right_segments, min_length * scale)
        if scaled_vote is not None:
            hypotheses.append(("scaled_translation_vote", SimilarityTransform(scale, 0.0, bbox_fit.tx + scaled_vote[0], bbox_fit.ty + scaled_vote[1])))

    try:
        left_mask = rasterize(anchor.apply_segments(left_segments), None, frame, policy.cell_pt).astype(np.float32)
        right_mask = rasterize(right_segments, None, frame, policy.cell_pt).astype(np.float32)
        if left_mask.sum() > 20 and right_mask.sum() > 20:
            window = cv2.createHanningWindow((left_mask.shape[1], left_mask.shape[0]), cv2.CV_32F)
            (dx, dy), _response = cv2.phaseCorrelate(left_mask * window, right_mask * window)
            hypotheses.append(("phase_correlation", SimilarityTransform(1.0, 0.0, anchor.tx + dx * policy.cell_pt, anchor.ty + dy * policy.cell_pt)))
    except cv2.error:
        pass

    trace = []
    candidates: list[tuple[str, SimilarityTransform, dict[str, Any]]] = []
    radius = max(1, int(round(policy.tolerance_pt / policy.cell_pt)))
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * radius + 1, 2 * radius + 1))
    right_mask = rasterize(
        right_segments, None, frame, policy.cell_pt,
        fills=right["fills"], clip_polygon=right.get("polygon_visual_pt"),
    )
    right_raster = (right_mask, cv2.dilate(right_mask, kernel))
    seen_transforms: set[tuple[float, float, int, int]] = set()

    def transform_key(value: SimilarityTransform) -> tuple[float, float, int, int]:
        # Translations below one hundredth of a raster cell are raster-identical
        # for candidate scoring; scale/rotation retain substantially finer
        # precision because their effect grows with distance from the origin.
        translation_quantum = max(1e-6, policy.cell_pt / 100.0)
        return (
            round(value.scale, 8),
            round(value.rotation_deg, 6),
            round(value.tx / translation_quantum),
            round(value.ty / translation_quantum),
        )

    def measure_candidate(value: SimilarityTransform) -> dict[str, Any] | None:
        key = transform_key(value)
        if key in seen_transforms:
            return None
        seen_transforms.add(key)
        return _coverage(
            left_segments, right_segments, value, frame, policy,
            left_fills=left["fills"], right_fills=right["fills"],
            left_polygon=left.get("polygon_visual_pt"), right_polygon=right.get("polygon_visual_pt"),
            right_raster=right_raster,
        )

    for name, hypothesis in hypotheses:
        coverage = measure_candidate(hypothesis)
        if coverage is None:
            continue
        candidates.append((name, hypothesis, coverage))
        sources, targets = _correspondences(
            left_segments, right_segments, hypothesis, min_length,
            policy.tolerance_pt * 2.5,
        )
        no_rotation = _fit_similarity(sources, targets, allow_rotation=False)
        refinements = [("uniform_lsq", no_rotation)]
        limited_rotation = _fit_similarity(sources, targets, allow_rotation=True)
        refinements.append(("limited_rotation_lsq", limited_rotation))
        for suffix, refined in refinements:
            if refined is None:
                continue
            if abs(refined.scale - hypothesis.scale) > policy.max_scale_delta_from_hypothesis:
                continue
            if abs(refined.rotation_deg) > policy.max_rotation_deg:
                continue
            refined_coverage = measure_candidate(refined)
            if refined_coverage is None:
                continue
            if suffix == "limited_rotation_lsq" and abs(refined.rotation_deg) > 1e-6:
                if refined_coverage["sym_cov"] < coverage["sym_cov"] + policy.rotation_min_coverage_gain:
                    continue
            candidates.append((f"{name}+{suffix}", refined, refined_coverage))

    best_name, best_transform, best_coverage = max(
        candidates,
        key=lambda item: (float(item[2]["sym_cov"]), -abs(item[1].rotation_deg), item[0]),
    )
    for name, transform, coverage in candidates:
        trace.append({
            "hypothesis": name,
            "transform": transform.public_dict(),
            "symmetric_coverage": round(float(coverage["sym_cov"]), 5),
        })
    sources, targets = _correspondences(
        left_segments, right_segments, best_transform, min_length,
        policy.tolerance_pt * 2.5,
    )
    residual = _residual(sources, targets, best_transform)
    left_anchor_count = len(_descriptors(left_segments, min_length)[1])
    right_anchor_count = len(_descriptors(right_segments, min_length)[1])
    anchor_coverage = len(sources) / max(1, min(left_anchor_count, right_anchor_count))
    success = float(best_coverage["sym_cov"]) >= policy.registration_success_floor
    failure_reason = None
    if min(len(left_segments), len(right_segments)) < policy.min_registration_primitives:
        success = False
        failure_reason = "TOO_FEW_PRIMITIVES"
    elif not success:
        failure_reason = "LOW_MATCHED_INK"
    elif residual["median_pt"] is not None and float(residual["median_pt"]) > policy.tolerance_pt * 2:
        success = False
        failure_reason = "HIGH_RESIDUAL"
    confidence = min(1.0, float(best_coverage["sym_cov"]))
    if residual["median_pt"] is not None:
        confidence *= max(0.0, 1.0 - float(residual["median_pt"]) / (policy.tolerance_pt * 3.0))
    public_coverage = {
        key: round(float(value), 5) if isinstance(value, float) else int(value)
        for key, value in best_coverage.items()
    }
    result = {
        "success": bool(success),
        "failure_reason": failure_reason,
        "method": best_name,
        "transform": best_transform.public_dict(),
        "frame_visual_pt": [round(value, 3) for value in frame],
        "coverage": public_coverage,
        "residual": residual,
        "confidence": round(float(confidence), 4),
        "anchors": {
            "left_candidates": left_anchor_count,
            "right_candidates": right_anchor_count,
            "matched": len(sources),
            "coverage": round(anchor_coverage, 4),
            "min_length_pt": round(min_length, 3),
            "translation_votes": vote_count,
        },
        "left_segments_merged": int(len(left_segments)),
        "right_segments_merged": int(len(right_segments)),
        "left_ink_pt": round(ink_length(left_segments), 2),
        "right_ink_pt": round(ink_length(right_segments), 2),
        "hypotheses": trace,
        "transform_family": "translation+uniform_scale+limited_rigid_rotation",
    }
    return result, best_transform, left_segments, right_segments


__all__ = [
    "SimilarityTransform",
    "merge_collinear_chains",
    "register",
    "transform_bbox",
    "transform_polygon",
]
