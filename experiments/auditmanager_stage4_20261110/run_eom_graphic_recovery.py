"""Recover a model-reported unreadable EOM graphic with same-source tiles."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .render_graphic_detail import render
from .run_km_graphic_recovery import INSTRUCTIONS
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry

ROOT = Path('/tmp/auditmanager_eom_graphic_recovery_20261110')
GRID = False
BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work')
PREVIOUS = Path('/tmp/auditmanager_eom_balance_inventory_control_20261110_split/EOM/execution/object_inventory_002/result.json')


def prepare():
    prior = json.loads(PREVIOUS.read_text())
    assert not prior['is_error']
    blocks = json.loads((BASE / 'blocks.json').read_text())
    index = {b['block_id']: b for b in blocks['blocks']}
    selected = [r['block_id'] for r in prior['json_data']['object_inventory_coverage']
                if r['status'] == 'needs_data' and index[r['block_id']]['block_type'] == 'image']
    assert len(selected) == 1
    bid = selected[0]
    pdf_hash = hashlib.sha256((BASE / 'document.pdf').read_bytes()).hexdigest()
    folder = ROOT / bid
    if (folder / 'manifest.json').exists():
        tiles = json.loads((folder / 'manifest.json').read_text())
        assert tiles['pdf_sha256'] == pdf_hash
        assert tiles['blocks_sha256'] == hashlib.sha256((BASE / 'blocks.json').read_bytes()).hexdigest()
    else:
        tiles = render(BASE / 'document.pdf', BASE / 'blocks.json', bid, pdf_hash, folder, columns=8 if GRID else 4, rows=3 if GRID else 1, scale=3)
    for tile in tiles['tiles']:
        assert hashlib.sha256(Path(tile['path']).read_bytes()).hexdigest() == tile['sha256']
    metadata = [{k: t[k] for k in ('block_id', 'page_index', 'clip_rotated_page_points', 'width', 'height')}
                for t in tiles['tiles']]
    request = {'messages': [{'role': 'system', 'content': INSTRUCTIONS + ('''
Electrical focus: extract every individually distinguishable branch, including reserves.
For each record read its literal circuit/cable mark, upstream assembly/panel, protective
apparatus designation, consumer, active/reserve role and load if visible. Never infer
marks from sequential numbering. Keep unreadable fields explicit. Preserve connections
across adjacent tiles using their coordinates; repeated overlapping views are not new
branches. These images are a row-major grid (8 columns, 3 rows), not a single strip.
The task is branch identity extraction, not an electrical calculation or final defect.
''' if GRID else '')},
        {'role': 'user', 'content': json.dumps({'source_blocks': selected, 'images_in_order': metadata,
         'scope': ('Visible GRAPHIC only. Overlapping coordinate-addressed tiles of one source block.'
                   if GRID else 'Visible GRAPHIC only. Four overlapping left-to-right tiles of one source block.'),
         **({'grid_columns': 8, 'grid_rows': 3} if GRID else {})})}],
        'image_paths': [t['path'] for t in tiles['tiles']]}
    manifest = {'request_sha256': evidence_digest(request), 'source_tiles': tiles,
        'previous_result_sha256': hashlib.sha256(PREVIOUS.read_bytes()).hexdigest(),
        'selection': 'All model-reported unreadable GRAPHIC blocks in completed extraction packet002.',
        'history_supplied': False, 'expected_marks_supplied': False,
        'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high', 'automatic_retries': 0,
        'message_chars': sum(len(m['content']) for m in request['messages']),
        'image_bytes': sum(Path(p).stat().st_size for p in request['image_paths'])}
    for name, data in [('request.json', request), ('manifest.json', manifest)]:
        path = ROOT / name
        if path.exists():
            assert json.loads(path.read_text()) == data
        else:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    return request, manifest


async def execute():
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    request, manifest = prepare()
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra') == 'gpt-6-astra'
    output = ROOT / 'result.json'
    if output.exists():
        result = json.loads(output.read_text())
    else:
        with (ROOT / 'attempt.json').open('x') as f:
            json.dump({'status': 'started', 'request_sha256': manifest['request_sha256']}, f)
        print('START', manifest['message_chars'], 'chars', len(request['image_paths']), 'images', flush=True)
        result = asdict(await run_codex_json_messages(request['messages'], model='codex/gpt-6-astra',
            reasoning_effort='high', timeout=900, stage='graphic_inventory_recovery',
            project_id='EOM-graphic-recovery', image_paths=request['image_paths'], allowed_tools=''))
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        (ROOT / 'attempt.json').write_text(json.dumps({'status': 'finished', 'is_error': result['is_error']}))
    assert not result['is_error'] and isinstance(result['json_data'], dict)
    prepare()  # verify frozen inputs again before admission
    bid = manifest['source_tiles']['tiles'][0]['block_id']
    coverage = reconcile_object_registry(result['json_data'], {bid})
    (ROOT / 'coverage.json').write_text(json.dumps(coverage, ensure_ascii=False, indent=2) + '\n')
    print('COMPLETE', len(coverage['objects']), 'objects', coverage['unresolved_source_blocks'], 'unresolved blocks', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--grid', action='store_true')
    args = parser.parse_args()
    if args.grid:
        GRID = True
        ROOT = ROOT.with_name(ROOT.name + '_grid')
    if args.execute:
        asyncio.run(execute())
    else:
        _, manifest = prepare()
        print(json.dumps({k: v for k, v in manifest.items() if k != 'source_tiles'}, ensure_ascii=False))
