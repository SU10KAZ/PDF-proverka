"""Publish executor-created detail views for subsequent independent source review."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile


def publish_graphic_details(binding, manifest):
    root=Path(binding['graphic_source']['output_dir']).resolve()
    if manifest['pdf_sha256'] != binding['graphic_source']['pdf_sha256']:
        raise ValueError('detail_catalogue_pdf_version_changed')
    if manifest['blocks_sha256'] != binding['graphic_source']['blocks_sha256']:
        raise ValueError('detail_catalogue_blocks_version_changed')
    rows=[]
    for tile in manifest['tiles']:
        path=Path(tile['path']).resolve()
        if not path.is_relative_to(root) or hashlib.sha256(path.read_bytes()).hexdigest()!=tile['sha256']:
            raise ValueError('detail_catalogue_image_changed')
        rows.append({k:tile[k] for k in ('block_id','sha256','page_index','clip_rotated_page_points')})
        rows[-1].update(file=str(path.relative_to(root)),source_pdf_sha256=manifest['pdf_sha256'])
    root.mkdir(parents=True,exist_ok=True)
    index=root/'graphic_detail_manifest.json'
    with (root/'.catalogue.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        current=json.loads(index.read_text()) if index.exists() else {'source_md_sha256':binding['md_sha256'],'images':[]}
        if current['source_md_sha256']!=binding['md_sha256']:
            raise ValueError('detail_catalogue_md_version_changed')
        # Latest complete grid for a block; previous files and request manifests
        # stay immutable. Never mix tiles from several grids as extra objects.
        updated={row['block_id'] for row in rows}
        current['images']=[row for row in current['images'] if row['block_id'] not in updated]+rows
        with tempfile.NamedTemporaryFile('w',dir=root,prefix='.catalogue_',delete=False) as output:
            name=output.name
            json.dump(current,output,ensure_ascii=False,indent=2)
        os.replace(name,index)
    return str(index)
