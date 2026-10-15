import fitz
import numpy as np
from backend.app.services.version_comparison import block_alignment as ba, tiles
from test_version_comparison_text_colors import decode


def sample():
    return {'canvas': {'dpi': 72, 'origin_pt': [0, 0], 'width_px': 700, 'height_px': 100},
            'transform': {'matrix': [[1, 0, 0], [0, 1, 0]]},
            'regions': [{'id': 'b1', 'class': 'block_moved', 'share': .99,
                         'bbox_a_pt': [480, 10, 500, 30], 'bbox_b_in_a_pt': [530, 10, 550, 30],
                         'shift_pt': [50, 0], 'bbox_px': [480, 10, 550, 30]}],
            'text': {'changes': [{'b_in_a_bbox': [532, 12, 540, 20]}]}}


def test_translation_crosses_tile_boundary_without_changing_other_elements(tmp_path):
    path = tmp_path / 'b.pdf'
    doc = fitz.open(); page = doc.new_page(width=700, height=100)
    page.draw_rect(fitz.Rect(532, 12, 548, 28), fill=(0, 0, 0))
    page.draw_rect(fitz.Rect(580, 50, 590, 60), fill=(0, 1, 0))
    doc.save(path); doc.close()
    r = sample()
    left = decode(ba.render_b(path, 1, r, 0, 0, 0))
    right = decode(ba.render_b(path, 1, r, 0, 1, 0))
    original = decode(tiles.page_tile(path, 1, r['canvas'], r['transform']['matrix'], 0, 1, 0))
    assert left[20, 490].max() == 0
    assert right[20, 540-512].min() == 255
    assert np.array_equal(right[45:65, 65:85], original[45:65, 65:85])
    assert original[20, 540-512].max() == 0


def test_display_coordinates_do_not_modify_saved_result():
    r = sample(); displayed = ba.display_result(r)
    assert displayed['text']['changes'][0]['b_in_a_bbox'] == [482, 12, 490, 20]
    assert r['text']['changes'][0]['b_in_a_bbox'] == [532, 12, 540, 20]
    assert displayed['regions'][0]['bbox_px'] == [480, 10, 500, 30]
    assert displayed['aligned_blocks'] == 1


def test_ambiguous_or_overlapping_blocks_stay_in_place():
    r = sample(); r['regions'][0]['share'] = .8
    assert ba.moves(r) == []
    r = sample(); r['regions'].append(dict(r['regions'][0], id='b2'))
    assert ba.moves(r) == []
