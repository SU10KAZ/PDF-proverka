"""Replace oversized GRAPHIC attachments with a sealed full-extent tile set."""
import argparse
import json
from pathlib import Path
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet


def prepare(preparation_path, manifest_path, destination):
    manifest = json.loads(manifest_path.read_text())
    for kind in ('pdf', 'blocks'):
        if sha(Path(manifest[kind + '_path'])) != manifest[kind + '_sha256']:
            raise ValueError('Graphic source version changed')
    tiles = manifest['tiles']
    if len(tiles) != manifest['columns'] * manifest['rows'] or len({(t['grid_row'], t['grid_column']) for t in tiles}) != len(tiles):
        raise ValueError('Incomplete full-extent grid')
    block_ids = {t['block_id'] for t in tiles}
    if len(block_ids) != 1:
        raise ValueError('Expected one graphic block')
    block_id = next(iter(block_ids))
    for tile in tiles:
        if sha(Path(tile['path'])) != tile['sha256']:
            raise ValueError('Tile changed')
    destination.mkdir(exist_ok=False)
    (destination / 'requests').mkdir()
    rows = []
    for row in json.loads(preparation_path.read_text())['requests']:
        if row['transport_size_ready']:
            continue
        path = Path(row['path'])
        if sha(path) != row['sha256']:
            raise ValueError('Held request changed')
        request = json.loads(path.read_text())
        packet = json.loads(request['messages'][-1]['content'])
        ids = packet['attached_image_block_ids_in_order']
        if len(ids) != len(request['image_hashes']) or ids.count(block_id) != 1:
            raise ValueError('Ambiguous source attachment order')
        images, new_ids = {}, []
        for bid, (name, digest) in zip(ids, request['image_hashes'].items()):
            if sha(Path(name)) != digest:
                raise ValueError('Original attachment changed')
            items = [(t['path'], t['sha256']) for t in tiles] if bid == block_id else [(name, digest)]
            for file, file_sha in items:
                images[file] = file_sha
                new_ids.append(bid)
        packet['attached_image_block_ids_in_order'] = new_ids
        packet['graphic_detail_manifest'] = manifest
        packet['graphic_detail_instructions'] = ('The oversized full-plan image is replaced by ALL tiles of its full bounding rectangle, '
            'in grid order; overlapping regions depict the same objects. Reconstruct connections across adjacent tiles. '
            'Do not count overlaps as extra objects. Cite tile index and source block for graphic observations. '
            'This is a GRAPHIC evidence route, never a correction to authoritative TEXT. If still unreadable, retain needs_data.')
        request.update(stage=request['stage'] + '_fulltiles', image_hashes=images)
        request['output_filename'] = request['output_filename'].replace('.json', '_fulltiles.json')
        request['messages'][-1]['content'] = json.dumps(packet, ensure_ascii=False)
        target = destination / 'requests' / (request['stage'] + '.json')
        write(target, request)
        chars = sum(len(m['content']) for m in request['messages'])
        size = sum(Path(p).stat().st_size for p in images)
        result = {'path': str(target), 'sha256': sha(target), 'message_characters': chars,
            'images': len(images), 'image_bytes': size, 'image_budget': 16_000_000,
            'transport_size_ready': chars < 1_000_000 and size <= 16_000_000,
            'replaces_held_request': row, 'graphic_manifest_sha256': sha(manifest_path)}
        verify_packet(result)
        rows.append(result)
    write(destination / 'preparation.json', {'history_supplied': False, 'production_acceptance': False,
        'graphic_manifest': str(manifest_path), 'requests': rows})
    return [{k: row[k] for k in ('path', 'images', 'image_bytes', 'message_characters')} for row in rows]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('preparation', type=Path)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.preparation, args.manifest, args.destination), ensure_ascii=False))
