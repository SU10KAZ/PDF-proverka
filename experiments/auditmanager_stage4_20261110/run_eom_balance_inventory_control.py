"""Known-source extraction control using an unchanged generic inventory packet.

Selection is informed by the known control; no expected marks or engineer
answers are added to the model request. This is not blind project discovery.
"""
import argparse
import asyncio
import hashlib
import json
import re
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from experiments.auditmanager_stage4_20261110 import run_whole_project_inventory as runner

ROOT = Path('/tmp/auditmanager_eom_balance_inventory_control_20261110')
SOURCE = Path('/tmp/auditmanager_whole_project_inventory_20261110/EOM')
STAGE = 'object_inventory_000'


def prepare(*, extraction_only=False):
    original = json.loads((SOURCE / 'manifest.json').read_text())
    index = next(i for i, row in enumerate(original['packets']) if row['stage'] == STAGE)
    row = original['packets'][index]
    request = json.loads((SOURCE / (STAGE + '.json')).read_text())
    assert evidence_digest(request) == original['request_digests'][index]
    selected_root = ROOT
    if extraction_only:
        from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes
        md = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work/document.md')
        assert hashlib.sha256(md.read_bytes()).hexdigest() == original['source_md_sha256']
        requests, _ = build_object_inventory_passes(md, Path(request['image_paths'][0]).parent)
        current = next(r for r in requests if r.stage == STAGE)
        assert current.messages[1:] == request['messages'][1:]
        assert [str(p) for p in current.image_paths] == request['image_paths']
        request = {**request, 'messages': current.messages}
        row = {**row, 'message_chars': sum(len(m['content']) for m in current.messages)}
        selected_root = ROOT.with_name(ROOT.name + '_extraction_only')
    manifest = {
        'discipline': 'EOM', 'packets': [row],
        'request_digests': [evidence_digest(request)],
        'source_md_sha256': original['source_md_sha256'],
        'blocks': len(row['block_ids']), 'history_supplied': False,
        'input_coverage_complete': False, 'semantic_inventory_complete': False,
        'source_manifest_sha256': evidence_digest(original),
        'scope': 'One unchanged packet selected for a known source control; not whole-project discovery.',
        'model': 'codex/gpt-6-astra', 'automatic_retries': 0,
        'extraction_only': extraction_only,
    }
    # Preserve the original frozen manifest byte-for-byte when replaying v1.
    if not extraction_only:
        manifest.pop('extraction_only')
    folder = selected_root / 'EOM'
    folder.mkdir(parents=True, exist_ok=True)
    for name, data in [('manifest.json', manifest), (STAGE + '.json', request)]:
        path = folder / name
        if path.exists():
            assert json.loads(path.read_text()) == data
        else:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    runner.ROOT = selected_root
    runner.preflight('EOM')
    print(json.dumps({'manifest_sha256': evidence_digest(manifest),
                     'message_chars': row['message_chars'], 'images': len(row['images']),
                     'blocks': len(row['block_ids'])}), flush=True)
    return manifest


def prepare_split():
    from backend.app.pipeline.stages.prepare.object_inventory_passes import build_object_inventory_passes
    original = json.loads((SOURCE / 'manifest.json').read_text())
    initial = json.loads((SOURCE / (STAGE + '.json')).read_text())
    payload = json.loads(initial['messages'][1]['content'])
    source_blocks = {b['block_id']: b for b in payload['source_blocks']}
    md_path = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work/document.md')
    assert hashlib.sha256(md_path.read_bytes()).hexdigest() == original['source_md_sha256']
    md = md_path.read_text()
    headers = list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I))
    assert [h.group(2) for h in headers[:len(source_blocks)]] == list(source_blocks)
    excerpt = md[:headers[len(source_blocks)].start()]
    selected_root = ROOT.with_name(ROOT.name + '_split')
    folder = selected_root / 'EOM'
    folder.mkdir(parents=True, exist_ok=True)
    excerpt_path = folder / 'source_excerpt.md'
    if excerpt_path.exists():
        assert excerpt_path.read_text() == excerpt
    else:
        excerpt_path.write_text(excerpt)
    requests, manifest = build_object_inventory_passes(
        excerpt_path, Path(initial['image_paths'][0]).parent, max_chars=12000, max_images=1)
    returned = {}
    image_paths = []
    digests = []
    for request in requests:
        body = json.loads(request.messages[1]['content'])
        for block in body['source_blocks']:
            assert block == source_blocks[block['block_id']]
            assert block['block_id'] not in returned
            returned[block['block_id']] = block
        image_paths.extend(str(p) for p in request.image_paths)
        data = {'messages': request.messages, 'image_paths': [str(p) for p in request.image_paths]}
        path = folder / (request.stage + '.json')
        if path.exists():
            assert json.loads(path.read_text()) == data
        else:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
        digests.append(evidence_digest(data))
    assert returned == source_blocks
    assert image_paths == initial['image_paths']
    manifest.update(discipline='EOM', request_digests=digests,
        parent_source_md_sha256=original['source_md_sha256'],
        parent_request_sha256=evidence_digest(initial), max_chars=12000, max_images=1,
        all_original_blocks_preserved=True, all_original_images_preserved=True,
        input_coverage_complete=False, extraction_only=True,
        scope='Same17-block known-source control split at existing block boundaries; not whole-project discovery.',
        model='codex/gpt-6-astra', automatic_retries=0)
    path = folder / 'manifest.json'
    if path.exists():
        assert json.loads(path.read_text()) == manifest
    else:
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    runner.ROOT = selected_root
    runner.preflight('EOM')
    print(json.dumps({'manifest_sha256': evidence_digest(manifest),
                     'packets': len(requests), 'blocks': len(returned), 'images': len(image_paths),
                     'message_chars': [r['message_chars'] for r in manifest['packets']]}), flush=True)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--extraction-only', action='store_true')
    parser.add_argument('--split', action='store_true')
    args = parser.parse_args()
    manifest = prepare_split() if args.split else prepare(extraction_only=args.extraction_only)
    if args.execute:
        asyncio.run(runner.execute('EOM', evidence_digest(manifest)))
