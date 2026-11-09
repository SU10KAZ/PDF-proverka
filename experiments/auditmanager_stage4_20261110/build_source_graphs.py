"""Build experimental graphs from unchanged authoritative MD and source geometry."""
import hashlib
import json
import re
from pathlib import Path

from backend.app.pipeline.stages.prepare.graph_builder import build_block_page_index, build_local_text_links


def build(root):
    md_path = root / '02_work/document.md'
    md = md_path.read_text()
    geometry = json.loads((root / '02_work/blocks.json').read_text())
    meta = {b['block_id']: b for b in geometry['blocks']}
    output = root / '03_analysis/runs/candidate'
    crops = json.loads((output / 'blocks_stage02_100/index.json').read_text())
    images = {b['block_id']: b for b in crops['blocks']}
    headers = list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I))
    page_headers = list(re.finditer(r'^##[ \t]+(?:Page|СТРАНИЦА)[ \t]+(\d+)\b[^\n]*$', md, re.M | re.I))
    pages, seen = {}, set()
    for i, header in enumerate(headers):
        bid, kind = header.group(2), header.group(1).upper()
        if bid in seen or bid not in meta:
            raise ValueError('Duplicate or unknown MD block: ' + bid)
        seen.add(bid)
        preceding = [p for p in page_headers if p.start() < header.start()]
        page = int(preceding[-1].group(1)) if preceding else None
        if page != meta[bid]['page_index'] + 1:
            raise ValueError('MD/geometry page mismatch: ' + bid)
        end = headers[i+1].start() if i+1 < len(headers) else len(md)
        end = min([end] + [p.start() for p in page_headers if header.start() < p.start() < end])
        content = md[header.end():end]
        pg = pages.setdefault(page, {'page': page, 'page_index': page-1,
            'source_file': 'document.md', 'sheet_no_raw': None, 'sheet_no_normalized': None,
            'sheet_name': None, 'sheet_confidence': 'missing', 'text_blocks': [], 'image_blocks': []})
        common = {'id': bid, 'page': page, 'source_file': 'document.md',
                  'source': 'authoritative_md', 'coords_norm': meta[bid]['coords_norm'],
                  'coords_px': [], 'md_start_line': md.count('\n',0,header.start())+1}
        if kind in ('TEXT','TABLE'):
            pg['text_blocks'].append({**common, 'text': content})
        elif kind in ('IMAGE','GRAPHIC'):
            if bid not in images:
                raise ValueError('Missing graphic crop: ' + bid)
            pg['image_blocks'].append({**common, 'type': None, 'ocr_raw': content,
                'ocr_text_normalized': content, 'file': images[bid]['file'],
                'graphic_description_is_verified': False})
        else:
            raise ValueError('Unsupported MD kind: ' + kind)
    geometry_only = [meta[bid] for bid in sorted(set(meta)-seen)]
    if any(block['block_type'] != 'stamp' for block in geometry_only):
        raise ValueError('Non-stamp source geometry missing from MD')
    if set(images) != {b['id'] for pg in pages.values() for b in pg['image_blocks']}:
        raise ValueError('MD and crop graphic inventories differ')
    for pg in pages.values():
        pg['local_text_links'] = build_local_text_links(pg)
    graph = {'version': 2, 'source': 'authoritative_md_with_source_geometry',
        'source_md_sha256': hashlib.sha256(md_path.read_bytes()).hexdigest(),
        'total_pages': len(pages), 'pages': list(pages.values()),
        'total_text_blocks': sum(len(p['text_blocks']) for p in pages.values()),
        'total_image_blocks': sum(len(p['image_blocks']) for p in pages.values()),
        'geometry_only_stamp_ids': [b['block_id'] for b in geometry_only],
        'boundary': 'No PDF text extraction or old audit graph. Sheet metadata not yet parsed; graphic descriptions are source context, not verified graphic evidence.'}
    if set(build_block_page_index(graph)) != seen:
        raise ValueError('Runtime block index lost source IDs')
    path = output / 'document_graph.json'
    if path.exists():
        raise FileExistsError(path)
    path.write_text(json.dumps(graph,ensure_ascii=False,indent=2)+'\n')
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'pages': graph['total_pages'], 'text_blocks': graph['total_text_blocks'],
            'image_blocks': graph['total_image_blocks'], 'sheet_metadata_complete': False}


if __name__ == '__main__':
    directory = Path(__file__).resolve().parent
    package = json.loads((directory/'six_project_input_package.json').read_text())
    results = []
    for project in package['projects']:
        result = {'discipline': project['discipline'], **build(Path(project['root']))}
        results.append(result)
        print(result['discipline'],result['pages'],result['text_blocks'],result['image_blocks'])
    (directory/'six_project_source_graphs.json').write_text(json.dumps(
        {'graphs': results, 'new_model_calls': 0, 'runtime_acceptance': False},ensure_ascii=False,indent=2)+'\n')
