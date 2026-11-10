"""Known-source extraction control using an unchanged generic inventory packet.

Selection is informed by the known control; no expected marks or engineer
answers are added to the model request. This is not blind project discovery.
"""
import argparse
import asyncio
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from experiments.auditmanager_stage4_20261110 import run_whole_project_inventory as runner

ROOT = Path('/tmp/auditmanager_eom_balance_inventory_control_20261110')
SOURCE = Path('/tmp/auditmanager_whole_project_inventory_20261110/EOM')
STAGE = 'object_inventory_000'


def prepare():
    original = json.loads((SOURCE / 'manifest.json').read_text())
    index = next(i for i, row in enumerate(original['packets']) if row['stage'] == STAGE)
    row = original['packets'][index]
    request = json.loads((SOURCE / (STAGE + '.json')).read_text())
    assert evidence_digest(request) == original['request_digests'][index]
    manifest = {
        'discipline': 'EOM', 'packets': [row],
        'request_digests': [original['request_digests'][index]],
        'source_md_sha256': original['source_md_sha256'],
        'blocks': len(row['block_ids']), 'history_supplied': False,
        'input_coverage_complete': False, 'semantic_inventory_complete': False,
        'source_manifest_sha256': evidence_digest(original),
        'scope': 'One unchanged packet selected for a known source control; not whole-project discovery.',
        'model': 'codex/gpt-6-astra', 'automatic_retries': 0,
    }
    folder = ROOT / 'EOM'
    folder.mkdir(parents=True, exist_ok=True)
    for name, data in [('manifest.json', manifest), (STAGE + '.json', request)]:
        path = folder / name
        if path.exists():
            assert json.loads(path.read_text()) == data
        else:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    runner.ROOT = ROOT
    runner.preflight('EOM')
    print(json.dumps({'manifest_sha256': evidence_digest(manifest),
                     'message_chars': row['message_chars'], 'images': len(row['images']),
                     'blocks': len(row['block_ids'])}), flush=True)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    manifest = prepare()
    if args.execute:
        asyncio.run(runner.execute('EOM', evidence_digest(manifest)))
