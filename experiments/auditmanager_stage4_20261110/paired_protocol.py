"""Reproducibility guard for the shared comparison harness, before backend import."""
import hashlib
import json
from pathlib import Path

SHARED_FILES=(
    'experiments/auditmanager_stage4_20261110/paired_protocol.py',
    'experiments/auditmanager_stage4_20261110/paired_runner.py',
    'experiments/classic_investigator/classic_control.py',
    'experiments/classic_investigator/investigate.py',
    'experiments/classic_investigator/subscription_retry.py',
)


def shared_manifest(repository):
    root=Path(repository).resolve()
    return {name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in SHARED_FILES}


def verify_shared_protocol(repository,source_root,work,variant):
    protocol=json.loads((Path(work)/'classic_control/protocol.json').read_text())
    shared=protocol.get('shared_execution')
    if not isinstance(shared,dict):raise ValueError('Missing frozen shared execution contract')
    if shared.get('variant')!=variant or shared.get('source_root')!=str(Path(source_root).resolve()):
        raise ValueError('Frozen comparison arm differs')
    if shared.get('source_files')!=shared_manifest(repository):
        raise ValueError('Shared harness changed or incomplete')
    # Candidate freeze and evaluator freeze are separate explicit prerequisites.
    # This guard proves only shared-code identity, never method acceptance.
    return shared
