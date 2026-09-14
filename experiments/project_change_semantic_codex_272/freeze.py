"""Freeze inference configuration after smoke; this does NOT unlock reserves."""
import argparse
from pathlib import Path
import subprocess

from experiments.project_change_272.inventory import REPO, read, sha, immutable, now
from experiments.project_change_semantic_272.packets import digest
from experiments.project_change_semantic_272.prompts import PROPOSE
from .provider import runtime_identity
from .run import BASE, code_identity, AUDIT
from .schema import PROPOSAL, VERIFICATION


def freeze(name, smoke_name):
    if any(not n.startswith('codex_') or '/' in n or '..' in n for n in [name, smoke_name]):
        raise ValueError('Separate Codex names required')
    smoke = BASE / 'runs' / smoke_name
    manifest = read(smoke / 'MANIFEST.json')
    receipt = read(smoke / 'RUN_RECEIPT.json')
    config = runtime_identity()
    if (not manifest['smoke'] or receipt['status'] != 'COMPLETE' or receipt['completed_packets'] != 2
            or manifest['code'] != code_identity() or manifest['config'] != config
            or manifest['proposal_prompt_sha256'] != digest(PROPOSE)
            or manifest['schema_hashes'] != dict(propose=digest(PROPOSAL), verify=digest(VERIFICATION))):
        raise ValueError('Smoke not complete or frozen configuration drift')
    # Resume must have been checked without additional invocation.
    resumed = read(smoke / 'RESUME_CHECK.json')
    if not resumed['passed'] or resumed['invocations_before'] != resumed['invocations_after']:
        raise ValueError('Successful no-network resume required')
    path = BASE / 'candidates' / name / 'INFERENCE_FREEZE.json'
    immutable(path, dict(at=now(), name=name, purpose='DEV_INFERENCE_ONLY_NOT_RESERVE_ACCESS',
        config=config, code=code_identity(), audit_sha256=sha(AUDIT),
        prompt_sha256=digest(PROPOSE), schema_hashes=manifest['schema_hashes'],
        smoke_manifest_sha256=sha(smoke / 'MANIFEST.json'), smoke_receipt_sha256=sha(smoke / 'RUN_RECEIPT.json'),
        repo_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
        final_holdout_opened=False, dev_quality_gate_passed=False))
    print(path)
    return path


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--name', required=True)
    p.add_argument('--smoke', required=True)
    args = p.parse_args()
    freeze(args.name, args.smoke)
