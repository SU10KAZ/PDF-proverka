"""Local GRAPHIC-only recovery; never extracts or replaces authoritative TEXT."""
import argparse
import hashlib
import json
from pathlib import Path

import fitz


def render(pdf_path, blocks_path, block_id, expected_pdf_sha256, output_dir,
           columns=4, scale=2.0, overlap=0.025):
    pdf_path, blocks_path, output_dir = map(Path, (pdf_path, blocks_path, output_dir))
    digest = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    if digest != expected_pdf_sha256:
        raise ValueError('Source PDF version changed')
    data = json.loads(blocks_path.read_text())
    if data.get('coordinate_space') != 'normalized_page_top_left':
        raise ValueError('Unsupported coordinate space')
    matches = [b for b in data['blocks'] if b['block_id'] == block_id]
    if len(matches) != 1 or matches[0]['block_type'] != 'image':
        raise ValueError('Exactly one GRAPHIC block required')
    block = matches[0]
    n = block['coords_norm']
    if (len(n) != 4 or not all(0 <= x <= 1 for x in n)
            or n[0] >= n[2] or n[1] >= n[3]):
        raise ValueError('Invalid block geometry')
    if not 1 <= columns <= 16 or not 0 < scale <= 4 or not 0 <= overlap <= 0.1:
        raise ValueError('Invalid rendering budget')
    output_dir.mkdir(parents=True, exist_ok=False)
    receipts = []
    with fitz.open(pdf_path) as doc:
        page = doc[block['page_index']]
        rect = fitz.Rect(n[0]*page.rect.width, n[1]*page.rect.height,
                         n[2]*page.rect.width, n[3]*page.rect.height)
        for i in range(columns):
            clip = fitz.Rect(max(rect.x0, rect.x0+rect.width*(i/columns-overlap)),
                             rect.y0,
                             min(rect.x1, rect.x0+rect.width*((i+1)/columns+overlap)),
                             rect.y1)
            if clip.width*clip.height*scale**2 > 20_000_000:
                raise ValueError('Tile exceeds pixel budget')
            pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False)
            path = output_dir / f'detail_tile_{i}.png'
            pix.save(path)
            receipts.append({'block_id': block_id, 'page_index': block['page_index'],
                'pdf_rotation': page.rotation, 'clip_rotated_page_points': list(clip),
                'path': str(path), 'width': pix.width, 'height': pix.height,
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest = {'pdf_path': str(pdf_path), 'pdf_sha256': digest,
        'blocks_path': str(blocks_path),
        'blocks_sha256': hashlib.sha256(blocks_path.read_bytes()).hexdigest(),
        'purpose': 'GRAPHIC-only higher resolution tiles; no TEXT re-extraction',
        'columns': columns, 'scale': scale, 'overlap': overlap, 'tiles': receipts,
        'model_call_performed': False, 'semantic_verified': False}
    (output_dir/'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ('pdf-path', 'blocks-path', 'block-id', 'expected-pdf-sha256', 'output-dir'):
        parser.add_argument('--'+arg, required=True)
    parser.add_argument('--columns', type=int, default=4)
    parser.add_argument('--scale', type=float, default=2.0)
    parser.add_argument('--overlap', type=float, default=0.025)
    result = render(**vars(parser.parse_args()))
    print(json.dumps({'tiles': len(result['tiles']), 'semantic_verified': False}))
