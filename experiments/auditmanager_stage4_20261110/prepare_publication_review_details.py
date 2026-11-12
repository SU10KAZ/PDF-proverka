"""Package an entire verified GRAPHIC grid for the source-review executor."""
import argparse
import json
from pathlib import Path
from .prepare_publication_graphic_tiles import verify_tiles
from .run_primary_text_runtime_probe import sha, write


def prepare(manifest_path, md_path, destination):
    manifest = json.loads(manifest_path.read_text())
    block_id, tiles = verify_tiles(manifest)
    destination.mkdir(parents=True, exist_ok=False)
    images = []
    for index, tile in enumerate(tiles):
        target = destination / f'detail_{index}.png'
        target.write_bytes(Path(tile['path']).read_bytes())
        assert sha(target) == tile['sha256']
        images.append({'block_id': block_id, 'file': target.name, 'sha256': sha(target),
                       'page_index': tile['page_index'],
                       'clip_rotated_page_points': tile['clip_rotated_page_points'],
                       'source_pdf_sha256': manifest['pdf_sha256']})
    result = {'source_md_sha256': sha(md_path), 'images': images,
              'source_tile_manifest': str(manifest_path), 'source_tile_manifest_sha256': sha(manifest_path),
              'complete_grid_geometry_verified': True, 'semantic_verified': False}
    write(destination / 'graphic_detail_manifest.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('md', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.manifest, args.md, args.destination), ensure_ascii=False))
