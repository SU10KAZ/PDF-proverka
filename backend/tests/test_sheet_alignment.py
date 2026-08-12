from __future__ import annotations

import math
from pathlib import Path

import fitz
import numpy as np

from backend.app.services.stage_comparison.sheet_alignment import (
    align_pdf_pages,
    estimate_similarity_transform,
    transform_bbox,
    transform_points,
)


def _similarity(*, scale=1.0, angle=0.0, tx=0.0, ty=0.0):
    radians = math.radians(angle)
    return np.asarray([
        [scale * math.cos(radians), -scale * math.sin(radians), tx],
        [scale * math.sin(radians), scale * math.cos(radians), ty],
        [0.0, 0.0, 1.0],
    ])


def _pdf(path: Path, matrix: np.ndarray, *, changed_stamp=False, reverse=False, labels=True):
    """Сохранить V3: его элементы получаются inverse(V3→V2) transform."""
    inverse = np.linalg.inv(matrix)
    doc = fitz.open(); page = doc.new_page(width=300, height=200)
    commands = []
    if labels:
        for point, text in [((40, 40), "AlphaPlan"), ((210, 38), "BetaPlan"), ((70, 115), "GammaPlan"), ((175, 140), "DeltaPlan"), ((115, 75), "EpsilonPlan")]:
            x, y = transform_points(inverse, [point])[0]
            commands.append(lambda x=x, y=y, text=text: page.insert_text((x, y), text, fontsize=8))
    for start, end in [((25, 170), (270, 170)), ((35, 55), (250, 120)), ((45, 150), (260, 60))]:
        transformed = transform_points(inverse, [start, end])
        commands.append(lambda transformed=transformed: page.draw_line(tuple(transformed[0]), tuple(transformed[1]), width=.7))
    # Нижний правый штамп intentionally changes and must not dominate anchors.
    stamp = "REV-B" if changed_stamp else "REV-A"
    x, y = transform_points(inverse, [(255, 185)])[0]
    commands.append(lambda: page.insert_text((x, y), stamp, fontsize=7))
    for command in reversed(commands) if reverse else commands:
        command()
    doc.save(path); doc.close()


def _aligned_pair(tmp_path: Path, matrix: np.ndarray, **kwargs):
    left, right = tmp_path / "left.pdf", tmp_path / "right.pdf"
    _pdf(left, np.eye(3), **kwargs)
    _pdf(right, matrix, **kwargs)
    return left, right


def _assert_matrix(result, expected: np.ndarray, tolerance=.015):
    assert result["status"] in {"aligned", "weak_alignment"}, result
    actual = np.asarray(result["transform"]["matrix"], dtype=float)
    probes = np.asarray([[30, 25], [150, 90], [270, 175]], dtype=float)
    assert np.max(np.linalg.norm(transform_points(actual, probes) - transform_points(expected, probes), axis=1)) < tolerance * 300


def test_similarity_translation_is_recovered_deterministically():
    source = np.asarray([[1, 1], [2, 4], [7, 3], [8, 9]], dtype=float)
    expected = _similarity(tx=12, ty=-7)
    first, first_inliers = estimate_similarity_transform(source, transform_points(expected, source))
    second, second_inliers = estimate_similarity_transform(source, transform_points(expected, source))
    assert np.allclose(first, expected)
    assert np.allclose(first, second)
    assert first_inliers.tolist() == second_inliers.tolist() == [True] * 4


def test_similarity_scale_rotation_and_translation_are_recovered():
    source = np.asarray([[1, 1], [2, 4], [7, 3], [8, 9]], dtype=float)
    expected = _similarity(scale=1.015, angle=1.2, tx=3, ty=-2)
    actual, _ = estimate_similarity_transform(source, transform_points(expected, source))
    assert np.allclose(actual, expected, atol=1e-5)


def test_pdf_translation_and_bbox_mapping(tmp_path: Path):
    expected = _similarity(tx=8, ty=-4)
    left, right = _aligned_pair(tmp_path, expected)
    result = align_pdf_pages(left, right, 1, 1, diagnostics_dir=tmp_path / "images")
    _assert_matrix(result, expected)
    assert transform_bbox(result["transform"]["matrix"], [10, 20, 30, 40]) == [18.0, 16.0, 38.0, 36.0]
    assert Path(result["diagnostics"]["overlay"]).is_file()
    assert Path(result["diagnostics"]["color_overlay"]).is_file()


def test_pdf_scale_rotation_and_stamp_change_do_not_break_alignment(tmp_path: Path):
    expected = _similarity(scale=1.01, angle=.8, tx=2, ty=-1)
    left, right = _aligned_pair(tmp_path, expected, changed_stamp=True)
    result = align_pdf_pages(left, right, 1, 1)
    _assert_matrix(result, expected, tolerance=.025)
    assert result["quality"]["blank_area_excluded"] is True


def test_different_pdf_object_order_is_not_a_geometric_problem(tmp_path: Path):
    left, right = _aligned_pair(tmp_path, np.eye(3), reverse=False)
    # Пересоздаём правый PDF с обратным порядком команд: source objects differ.
    _pdf(right, np.eye(3), reverse=True)
    result = align_pdf_pages(left, right, 1, 1)
    _assert_matrix(result, np.eye(3))


def test_insufficient_anchors_fails_instead_of_overfitting(tmp_path: Path):
    left, right = tmp_path / "left.pdf", tmp_path / "right.pdf"
    _pdf(left, np.eye(3), labels=False)
    _pdf(right, _similarity(tx=35, ty=20), labels=False)
    result = align_pdf_pages(left, right, 1, 1)
    assert result["status"] in {"weak_alignment", "failed"}
    assert result["status"] != "aligned"


def test_clearly_different_pages_are_not_forced_to_aligned(tmp_path: Path):
    left, right = tmp_path / "left.pdf", tmp_path / "right.pdf"
    _pdf(left, np.eye(3), labels=False)
    doc = fitz.open(); page = doc.new_page(width=300, height=200); page.insert_text((20, 30), "completely unrelated content", fontsize=12); doc.save(right); doc.close()
    result = align_pdf_pages(left, right, 1, 1)
    assert result["status"] != "aligned"
