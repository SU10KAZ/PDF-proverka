from __future__ import annotations

import shutil
from pathlib import Path

import fitz

from backend.app.services.stage_comparison.sheet_identity import evaluate_page_identity, evaluate_sheet_identity


def _prepared(*, blocks=True):
    return {"blocks": []} if blocks else None


def _pdf(path: Path, *, text="A 100", line=False, line_x=20, image=None, reverse=False):
    document = fitz.open()
    page = document.new_page(width=200, height=100)
    actions = []
    if text is not None:
        actions.append(lambda: page.insert_text((20, 30), text))
    if line:
        actions.append(lambda: page.draw_line((line_x, 60), (180, 60), width=1))
    if image:
        actions.append(lambda: page.insert_image(fitz.Rect(20, 35, 40, 55), filename=str(image)))
    for action in reversed(actions) if reverse else actions:
        action()
    document.save(path)
    document.close()


def _image(path: Path, rgb: bytes):
    pixmap = fitz.Pixmap(fitz.csRGB, 2, 2, rgb * 4, False)
    pixmap.save(path)


_DEFAULT = object()


def _compare(left: Path, right: Path, *, prepared_left=_DEFAULT, prepared_right=_DEFAULT):
    return evaluate_page_identity(
        left, right, 1, 1,
        left_prepared=_prepared() if prepared_left is _DEFAULT else prepared_left,
        right_prepared=_prepared() if prepared_right is _DEFAULT else prepared_right,
    )


def test_fully_identical_page_is_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Fixed drawing", line=True)
    shutil.copyfile(left, right)
    result = _compare(left, right)
    assert result["status"] == "identical"
    assert result["signals"]["render"]["status"] == "equal"


def test_same_content_with_different_pdf_object_order_is_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Fixed drawing", line=True, reverse=False)
    _pdf(right, text="Fixed drawing", line=True, reverse=True)
    result = _compare(left, right)
    assert result["status"] == "identical"
    assert result["signals"]["vector"]["status"] == "equal"


def test_changed_text_cannot_be_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Value 100")
    _pdf(right, text="Value 101")
    result = _compare(left, right)
    assert result["status"] == "needs_comparison"
    assert result["signals"]["text"]["status"] == "different"


def test_added_line_cannot_be_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Plan", line=False)
    _pdf(right, text="Plan", line=True)
    result = _compare(left, right)
    assert result["status"] == "needs_comparison"
    assert result["signals"]["vector"]["status"] == "different"


def test_changed_object_coordinate_cannot_be_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Plan", line=True, line_x=20)
    _pdf(right, text="Plan", line=True, line_x=22)
    result = _compare(left, right)
    assert result["status"] == "needs_comparison"
    assert result["signals"]["vector"]["status"] == "different"


def test_changed_image_cannot_be_identical(tmp_path: Path):
    red, blue = tmp_path / "red.png", tmp_path / "blue.png"
    _image(red, b"\xff\x00\x00"); _image(blue, b"\x00\x00\xff")
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text=None, image=red)
    _pdf(right, text=None, image=blue)
    result = _compare(left, right)
    assert result["status"] == "needs_comparison"
    assert result["signals"]["images"]["status"] == "different"


def test_scan_page_can_be_identical_by_image_and_render(tmp_path: Path):
    image = tmp_path / "scan.png"; _image(image, b"\x44\x55\x66")
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text=None, image=image); shutil.copyfile(left, right)
    result = _compare(left, right)
    assert result["status"] == "identical"
    assert result["signals"]["text"]["status"] == "unavailable"


def test_mixed_page_can_be_identical(tmp_path: Path):
    image = tmp_path / "mixed.png"; _image(image, b"\x44\x55\x66")
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Mixed", line=True, image=image); shutil.copyfile(left, right)
    assert _compare(left, right)["status"] == "identical"


def test_missing_prepared_blocks_is_uncertain_not_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Fixed", line=True); shutil.copyfile(left, right)
    result = _compare(left, right, prepared_left=None, prepared_right={"blocks": []})
    assert result["status"] == "uncertain"


def test_conflicting_prepared_blocks_are_never_false_identical(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left, text="Fixed", line=True); shutil.copyfile(left, right)
    result = _compare(left, right, prepared_left={"blocks": []}, prepared_right={"blocks": [{"type": "text", "normalized_bbox": [0, 0, 1, 1]}]})
    assert result["status"] == "needs_comparison"
    assert result["signals"]["blocks"]["status"] == "different"


def test_only_matched_and_manual_pairs_are_checked_deterministically(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left); shutil.copyfile(left, right)
    doc = {"document": {"code": "AR"}, "pages": [{"pdf_page": 1, "blocks": []}]}
    matching = {"matches": [{"left_page": 1, "right_page": 1, "status": "matched"}]}
    first = evaluate_sheet_identity(doc, doc, left_pdf=left, right_pdf=right, sheet_matching=matching, alignment_items=[{"mode": "uncertain", "left_page": 1, "right_page": 1}])
    assert first == evaluate_sheet_identity(doc, doc, left_pdf=left, right_pdf=right, sheet_matching=matching, alignment_items=[])
    assert first["summary"] == {"identical": 1, "needs_comparison": 0, "uncertain": 0}


def test_manual_confirmed_pair_is_checked_but_uncertain_pair_is_not(tmp_path: Path):
    left, right = tmp_path / "l.pdf", tmp_path / "r.pdf"
    _pdf(left); shutil.copyfile(left, right)
    doc = {"document": {"code": "AR"}, "pages": [{"pdf_page": 1, "blocks": []}]}
    result = evaluate_sheet_identity(
        doc, doc, left_pdf=left, right_pdf=right, sheet_matching={"matches": []},
        alignment_items=[{"mode": "uncertain", "left_page": 1, "right_page": 1}, {"mode": "manual", "left_page": 1, "right_page": 1}],
    )
    assert len(result["items"]) == 1
    assert result["items"][0]["source"] == "manual_alignment"
