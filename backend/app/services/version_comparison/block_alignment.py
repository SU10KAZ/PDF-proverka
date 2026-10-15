"""Optional display-only translation of confidently matched blocks in B."""
from copy import deepcopy
import math

import numpy as np


def overlap(a, b):
    return min(a[2], b[2]) > max(a[0], b[0]) and min(a[3], b[3]) > max(a[1], b[1])


def contains(a, b):
    return b and a[0] <= b[0] and a[1] <= b[1] and a[2] >= b[2] and a[3] >= b[3]


def moves(result):
    candidates = [r for r in result.get('regions', []) if r.get('class') == 'block_moved'
                  and r.get('share', 0) >= .9 and r.get('bbox_a_pt') and r.get('bbox_b_in_a_pt')
                  and isinstance(r.get('shift_pt'), list)]
    # Never apply competing translations to the same area.
    return [r for r in candidates if not any(
        overlap(a, b) for other in candidates if other is not r
        for a in (r['bbox_a_pt'], r['bbox_b_in_a_pt'])
        for b in (other['bbox_a_pt'], other['bbox_b_in_a_pt']))]


def shifted(box, move):
    dx, dy = move['shift_pt']
    return [box[0] - dx, box[1] - dy, box[2] - dx, box[3] - dy]


def display_result(result):
    out = deepcopy(result)
    selected = moves(result)
    out['aligned_blocks'] = len(selected)
    for change in out.get('text', {}).get('changes', []):
        for move in selected:
            if contains(move['bbox_b_in_a_pt'], change.get('b_in_a_bbox')):
                change['b_in_a_bbox'] = shifted(change['b_in_a_bbox'], move)
                break
    canvas = result['canvas']
    scale = canvas['dpi'] / 72
    ox, oy = canvas['origin_pt']
    for region in out.get('regions', []):
        for move in selected:
            box = region.get('bbox_b_in_a_pt')
            if contains(move['bbox_b_in_a_pt'], box):
                region['bbox_b_in_a_pt'] = shifted(box, move)
                a = region.get('bbox_a_pt') or region['bbox_b_in_a_pt']
                b = region['bbox_b_in_a_pt']
                union = [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]
                region['bbox_px'] = [round((v - (ox if i % 2 == 0 else oy)) * scale) for i, v in enumerate(union)]
                break
    return out


def area(result, boxes, level, tx, ty, shape):
    mask = np.zeros(shape, bool)
    scale = result['canvas']['dpi'] / 72 / 2 ** level
    ox, oy = result['canvas']['origin_pt']
    for box in boxes:
        x0, y0 = math.floor((box[0]-ox)*scale)-tx*512, math.floor((box[1]-oy)*scale)-ty*512
        x1, y1 = math.ceil((box[2]-ox)*scale)-tx*512, math.ceil((box[3]-oy)*scale)-ty*512
        if x1 > 0 and y1 > 0 and x0 < shape[1] and y0 < shape[0]:
            mask[max(0,y0):min(shape[0],y1),max(0,x0):min(shape[1],x1)] = True
    return mask


def render_b(pdf, page, result, level, tx, ty):
    from . import tiles
    import cv2
    def decode(png):
        return cv2.cvtColor(cv2.imdecode(np.frombuffer(png, np.uint8), cv2.IMREAD_COLOR), cv2.COLOR_BGR2RGB)
    matrix = result['transform']['matrix']
    png = tiles.page_tile(pdf, page, result['canvas'], matrix, level, tx, ty)
    selected = moves(result)
    if png is None or not selected:
        return png
    out = decode(png)
    # Clear sources before pasting destinations, including overlapping old/new positions.
    out[area(result, [m['bbox_b_in_a_pt'] for m in selected], level, tx, ty, out.shape[:2])] = 255
    for move in selected:
        destination = shifted(move['bbox_b_in_a_pt'], move)
        mask = area(result, [destination], level, tx, ty, out.shape[:2])
        if not mask.any():
            continue
        translated = deepcopy(matrix)
        translated[0][2] -= move['shift_pt'][0]
        translated[1][2] -= move['shift_pt'][1]
        patch = decode(tiles.page_tile(pdf, page, result['canvas'], translated, level, tx, ty))
        # White in the translated patch must not erase other page elements.
        out[mask] = np.minimum(out[mask], patch[mask])
    return tiles._encode(out)
