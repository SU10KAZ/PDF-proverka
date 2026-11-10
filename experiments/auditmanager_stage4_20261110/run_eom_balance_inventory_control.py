"""Known-source extraction control using an unchanged generic inventory packet.

Selection is informed by the known control; no expected marks or engineer
answers are added to the model request. This is not blind project discovery.
"""
import argparse
import asyncio
import hashlib
import json
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--extraction-only', action='store_true')
    args = parser.parse_args()
    manifest = prepare(extraction_only=args.extraction_only)
    if args.execute:
        asyncio.run(runner.execute('EOM', evidence_digest(manifest)))
