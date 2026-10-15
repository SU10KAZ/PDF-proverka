import cv2
import numpy as np
from backend.app.services.version_comparison import tiles


def result():
    return {'canvas': {'dpi': 72, 'origin_pt': [0, 0]}, 'text': {'changes': [
        {'kind': 'changed', 'a_bbox': [1, 1, 4, 3], 'b_in_a_bbox': [2, 1, 5, 3]},
        {'kind': 'moved', 'a_bbox': [6, 1, 7, 3], 'b_in_a_bbox': [7, 1, 8, 3]}]}}


def decode(png):
    return cv2.cvtColor(cv2.imdecode(np.frombuffer(png, np.uint8), cv2.IMREAD_COLOR), cv2.COLOR_BGR2RGB)


def test_swipe_tints_complete_changed_text_and_preserves_unchanged_ink():
    rgb = np.full((8, 8, 3), 255, np.uint8)
    rgb[1, 1:4] = 0
    rgb[1, 6] = [35, 160, 70]
    png = tiles._encode(rgb)
    for color in ('red', 'blue'):
        out = decode(tiles.tint_text_tile(png, result(), 'a', color, 0, 0, 0))
        assert np.all(out[1, 1:4] == tiles.TEXT_COLORS[color])
        assert np.array_equal(out[1, 6], rgb[1, 6])
        assert np.all(out[2, 1:4] == 255)
    assert tiles.tint_text_tile(png, result(), 'a', 'original', 0, 0, 0) == png


def test_overlay_lower_blue_upper_red_original_colors_elsewhere(tmp_path, monkeypatch):
    monkeypatch.setattr(tiles, 'overlay_tile', lambda *args: None)
    a = np.full((8, 8, 3), 255, np.uint8)
    b = a.copy()
    a[1, 1:4] = 0
    b[1, 2:5] = 0
    a[5, 5] = b[5, 5] = [35, 160, 70]
    out = decode(tiles.source_overlay_tile(tmp_path, result(), tiles._encode(a), tiles._encode(b),
                                         0, 0, 0, ('a_only', 'b_only')))
    assert tuple(out[1, 1]) == tiles.TEXT_COLORS['blue']
    assert np.all(out[1, 2:5] == tiles.TEXT_COLORS['red'])
    assert np.array_equal(out[5, 5], a[5, 5])


def test_text_boxes_respect_origin_level_and_tile_boundary():
    r = {'canvas': {'dpi': 144, 'origin_pt': [10, 20]}, 'text': {'changes': [
        {'kind': 'changed', 'a_bbox': [520, 21, 525, 23]}]}}
    area = tiles._text_area(r, 'a', 1, 1, 0, (8, 8))
    assert area[1:3, :3].all()
    assert area.sum() == 6


def test_dates_moving_between_table_rows_are_colored_on_both_sheets():
    # Same values still exist, but each physical row now contains a different date.
    boxes = [[10, y, 43, y + 10] for y in (10, 25, 40)]
    r = {'canvas': {'dpi': 72, 'origin_pt': [0, 0]}, 'text': {'changes': [
        {'kind': 'removed', 'before': '29.01.26', 'after': '', 'a_bbox': boxes[0]},
        {'kind': 'moved', 'before': '04.03.26', 'after': '04.03.26',
         'a_bbox': boxes[1], 'b_in_a_bbox': boxes[0]},
        {'kind': 'moved', 'before': '03.04.26', 'after': '03.04.26',
         'a_bbox': boxes[2], 'b_in_a_bbox': boxes[1]},
        {'kind': 'added', 'before': '', 'after': '09.06.26', 'b_in_a_bbox': boxes[2]},
    ]}}
    for side in ('a', 'b'):
        area = tiles._text_area(r, side, 0, 0, 0, (64, 64))
        for x0, y0, x1, y1 in boxes:
            assert area[y0:y1, x0:x1].all(), (side, y0)
        assert area.sum() == 3 * 33 * 10
