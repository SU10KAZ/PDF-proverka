"""Validate crop identity against source geometry and emit minimal candidate indexes."""
import hashlib
import json
from pathlib import Path


def check(project):
    files = project['inputs']
    def read(relative):
        entry = files[relative]
        raw = Path(entry['path']).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('Changed input: ' + relative)
        return json.loads(raw)
    metadata = read('02_work/blocks.json')
    prefix = '03_analysis/runs/control/blocks_stage02_100/'
    original = read(prefix + 'index.json')
    blocks = {b['block_id']: b for b in metadata['blocks']}
    if len(blocks) != len(metadata['blocks']):
        raise ValueError('Duplicate source block ID')
    clean, seen, errors = [], set(), []
    for block in original['blocks']:
        bid = block['block_id']
        source = blocks.get(bid)
        if bid in seen or source is None:
            errors.append({'block_id': bid, 'reason': 'duplicate_or_unknown_identity'})
            continue
        seen.add(bid)
        name = block['file']
        if Path(name).name != name or not name.endswith('.png'):
            errors.append({'block_id': bid, 'reason': 'nonlocal_image_path'})
            continue
        entry = files.get(prefix + name)
        if entry is None or hashlib.sha256(Path(entry['path']).read_bytes()).hexdigest() != block['sha256']:
            errors.append({'block_id': bid, 'reason': 'missing_or_changed_crop'})
            continue
        if block['page'] != source['page_index'] + 1:
            errors.append({'block_id': bid, 'reason': 'page_identity_mismatch'})
            continue
        if source['block_type'].lower() not in ('image', 'graphic'):
            errors.append({'block_id': bid, 'reason': 'text_raster_not_allowed'})
            continue
        clean.append({k: block[k] for k in ('block_id','page','file','block_type','sha256','render_size')})
    if len(clean) != project['graphics_declared']:
        errors.append({'reason': 'graphic_count_mismatch'})
    policy_keys=('profile','dpi','min_long_side','compact','skip_small')
    if any(k not in original for k in policy_keys):
        raise ValueError('Original crop policy missing; cannot infer rendition settings')
    policy={k:original[k] for k in policy_keys}
    return {'discipline': project['discipline'], 'status': 'passed' if not errors else 'failed',
            'errors': errors, 'source_geometry_blocks': len(blocks),
            'candidate_index': {**policy, 'total_blocks': len(clean), 'blocks': clean},
            'omitted_index_fields': ['ocr_label', 'ocr_text_len', 'source_result_json', 'rendition_note'],
            'boundary': 'Identity/page/hash checks only. Minimal index excludes free-form summaries and historical paths. Source MD is unchanged; filesystem isolation and graph rebuild are still required.'}


if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / 'six_project_comparison_preflight.json').read_text())
    results = [check(project) for project in manifest['projects']]
    (root / 'six_project_index_checks.json').write_text(
        json.dumps({'projects': results, 'new_model_calls': 0}, ensure_ascii=False, indent=2) + '\n')
    for result in results:
        print(result['discipline'], result['status'], result['candidate_index']['total_blocks'], result['errors'][:3])
    if any(result['status'] != 'passed' for result in results):
        raise SystemExit(1)
