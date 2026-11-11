"""Render only declared graphics from user-provided sources; no OCR or model call."""
import hashlib
import json
import math
from pathlib import Path
import re
import fitz
from PIL import Image

HERE = Path(__file__).parent
ROOT = Path('/tmp/auditmanager_user_km_214_v002_20261111/source_only')


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    intake_path = HERE / 'user_KM_214_v002_intake.json'
    intake = json.loads(intake_path.read_text())
    for name, entry in intake['files'].items():
        assert sha(ROOT / name) == entry['sha256']
    meta = json.loads((ROOT / '02_work/blocks.json').read_text())
    assert meta['coordinate_space'] == 'normalized_page_top_left'
    headers = re.findall(r'^### BLOCK #\d+ \[([^\]]+)\]: (\S+)',
                         (ROOT / '02_work/document.md').read_text(), re.M)
    kinds = {bid: kind for kind, bid in headers}
    assert len(kinds) == len(headers)
    graphics = [b for b in meta['blocks'] if b['block_type'] in ('image', 'graphic')]
    assert {b['block_id'] for b in graphics} == {bid for bid, kind in kinds.items() if kind in ('IMAGE', 'GRAPHIC')}
    out = ROOT / '03_analysis/runs/candidate/blocks_stage02_100'
    out.mkdir(parents=True, exist_ok=False)
    rows = []
    with fitz.open(ROOT / '02_work/document.pdf') as pdf:
        for b in graphics:
            assert b['shape_type'] == 'rectangle'
            x0, y0, x1, y1 = b['coords_norm']
            assert all(math.isfinite(v) for v in (x0, y0, x1, y1))
            assert 0 <= x0 < x1 <= 1 and 0 <= y0 < y1 <= 1
            page = pdf[b['page_index']]
            side = max((x1-x0)*page.rect.width, (y1-y0)*page.rect.height)
            scale = max(100/72, 801/side)
            pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            image = Image.frombytes('RGB', (pix.width, pix.height), pix.samples)
            image = image.crop(tuple(round(v*(pix.width if i % 2 == 0 else pix.height))
                                     for i, v in enumerate((x0, y0, x1, y1))))
            assert max(image.size) >= 800 and min(image.size) > 0
            name = 'block_' + b['block_id'] + '.png'; path = out / name
            image.save(path)
            rows.append({'block_id': b['block_id'], 'page': b['page_index']+1,
                         'file': name, 'block_type': 'image', 'sha256': sha(path),
                         'render_size': list(image.size)})
    index = {'profile': 'stage02_100', 'dpi': 100, 'min_long_side': 800,
             'compact': False, 'skip_small': False, 'total_blocks': len(rows), 'blocks': rows}
    (out / 'index.json').write_text(json.dumps(index, ensure_ascii=False, indent=2)+'\n')
    for name, entry in intake['files'].items():
        assert sha(ROOT / name) == entry['sha256']
    report = {'status': 'nine_graphics_rendered_source_bytes_unchanged',
        'source_intake_sha256': sha(intake_path), 'root': str(ROOT),
        'index_path': str(out/'index.json'), 'index_sha256': sha(out/'index.json'),
        'graphic_blocks': len(rows), 'text_blocks_not_rendered': sum(k=='TEXT' for k in kinds.values()),
        'stamp_blocks_not_rendered': sum(b['block_type']=='stamp' for b in meta['blocks']),
        'rendition': 'Local PDF visual orientation and normalized source rectangles; 100dpi with minimum long side 800, one-pixel margin before rounding.',
        'source_semantics_reviewed': False, 'network_calls': 0, 'model_calls': 0,
        'remaining': ['Minimal runner metadata with local source paths', 'Source-only document graph and block context', 'Frozen separate calibration protocol'],
        'independent_new_object_claimed': False, 'production_acceptance': False}
    (HERE / 'user_KM_214_graphics_receipt.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'graphics': len(rows), 'text_blocks_preserved': report['text_blocks_not_rendered'], 'model_calls': 0}))


if __name__ == '__main__':
    main()
