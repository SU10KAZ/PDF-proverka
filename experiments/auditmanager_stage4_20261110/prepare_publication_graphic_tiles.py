"""Replace an oversized linked GRAPHIC with its complete hash-bound tile grid."""
import argparse
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import fingerprint
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet


def verify_tiles(manifest):
    for prefix in ('pdf', 'blocks'):
        if sha(Path(manifest[f'{prefix}_path'])) != manifest[f'{prefix}_sha256']:
            raise ValueError(f'{prefix} source changed')
    tiles = manifest['tiles']
    rows, columns = manifest['rows'], manifest['columns']
    if len(tiles) != rows * columns:
        raise ValueError('Incomplete tile grid')
    ids = {t['block_id'] for t in tiles}
    if len(ids) != 1 or len({t['page_index'] for t in tiles}) != 1:
        raise ValueError('Mixed graphic sources')
    grid = {(t['grid_row'], t['grid_column']): t for t in tiles}
    if len(grid) != len(tiles):
        raise ValueError('Duplicate tile position')
    x0, y0, x1, y1 = manifest['original_crop_points']
    previous_y = y0
    for row in range(rows):
        previous_x = x0
        boxes = []
        for column in range(columns):
            t = grid[(row, column)]
            if sha(Path(t['path'])) != t['sha256']:
                raise ValueError('Tile changed')
            a, b, c, d = t['clip_rotated_page_points']
            if not x0 <= a < c <= x1 or not y0 <= b < d <= y1 or a > previous_x:
                raise ValueError('Invalid tile geometry or horizontal gap')
            previous_x = max(previous_x, c)
            boxes.append((b, d))
        if previous_x < x1 or len(set(boxes)) != 1 or boxes[0][0] > previous_y:
            raise ValueError('Incomplete row or vertical gap')
        previous_y = max(previous_y, boxes[0][1])
    if previous_y < y1:
        raise ValueError('Incomplete bottom row')
    return next(iter(ids)), [grid[(r, c)] for r in range(rows) for c in range(columns)]


def prepare(root, manifest_path, output):
    manifest = json.loads(manifest_path.read_text())
    ref, tiles = verify_tiles(manifest)
    original = json.loads((root / 'preparation.json').read_text())
    output.mkdir(parents=True, exist_ok=False)
    requests = []
    for row in original['requests']:
        if not any(x['ref'] == ref for x in row.get('unavailable_linked_sources', [])):
            continue
        request = verify_packet(row)
        envelope = json.loads(request['messages'][-1]['content'])
        packet = envelope['packet']
        bindings = [{'ref': Path(path).stem.removeprefix('block_'), 'path': path}
                    for path in request['image_hashes']]
        for t in tiles:
            alias = f"{ref}:tile:{t['grid_row']}:{t['grid_column']}"
            packet['attached_graphic_refs'].append(alias)
            request['image_hashes'][t['path']] = t['sha256']
            bindings.append({'ref': alias, 'path': t['path'], 'source_block_id': ref,
                             'page_index': t['page_index'], 'clip_rotated_page_points': t['clip_rotated_page_points']})
        envelope['packet_sha256'] = fingerprint(packet)
        envelope['image_bindings_in_attachment_order'] = bindings
        envelope['graphic_scope'] = 'Complete overlapping tile grid of one GRAPHIC block. Cite exact tile refs, not the unattached original block. Overlap is not a second object. MD TEXT remains authoritative.'
        envelope['unavailable_linked_sources'] = [x for x in envelope['unavailable_linked_sources'] if x['ref'] != ref]
        request['stage'] += '_tiles'
        request['messages'][-1]['content'] = json.dumps(envelope, ensure_ascii=False)
        path = output / f"{request['stage']}.json"
        write(path, request)
        size = sum(Path(p).stat().st_size for p in request['image_hashes'])
        requests.append({**row, 'path': str(path), 'sha256': sha(path),
                         'images': len(request['image_hashes']), 'image_bytes': size,
                         'unavailable_linked_sources': envelope['unavailable_linked_sources'],
                         'transport_size_ready': not envelope['unavailable_linked_sources'] and size <= 16000000
                         and sum(len(m['content']) for m in request['messages']) < 1000000})
    record = {**original, 'requests': requests, 'dispatched': False,
              'selection': original['selection'] + ' Oversized linked graphic replaced by ALL tiles, no reviewer-based region selection.',
              'tile_manifest': {'path': str(manifest_path), 'sha256': sha(manifest_path)},
              'parent_preparation_sha256': sha(root / 'preparation.json')}
    write(output / 'preparation.json', record)
    return {'prepared': len(requests), 'ready': sum(r['transport_size_ready'] for r in requests),
            'packets': [{k:r[k] for k in ('finding_id','images','image_bytes')} for r in requests],
            'preparation_path': str(output / 'preparation.json'),
            'preparation_sha256': sha(output / 'preparation.json')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('root', 'manifest', 'output'):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.root.resolve(), args.manifest.resolve(), args.output.resolve()), ensure_ascii=False))
