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
    'experiments/classic_investigator/execution_receipts.py',
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


def verify_trial_binding(work, variant, discipline):
    """Verify preregistered run identity without loading evaluator answers.

    This is reproducibility, not semantic approval of a trial or its controls.
    Neither matching hashes nor a manifest status establishes quality acceptance.
    """
    work=Path(work).resolve()
    protocol=json.loads((work/'classic_control/protocol.json').read_text())

    def bound_file(binding):
        if not isinstance(binding,dict) or not isinstance(binding.get('path'),str):
            raise ValueError('Missing trial artifact binding')
        path=Path(binding['path'])
        if not path.is_absolute():raise ValueError('Trial artifact path must be absolute')
        actual=hashlib.sha256(path.read_bytes()).hexdigest()
        if actual!=binding.get('sha256'):raise ValueError('Trial artifact changed: '+str(path))
        return path

    trial_path=bound_file(protocol.get('comparison_trial'))
    trial=json.loads(trial_path.read_text())
    if trial.get('status')!='frozen_comparative_execution':
        raise ValueError('Comparative execution plan is not frozen')
    # Hash only: evaluator content is never supplied to the model/harness.
    bound_file(trial.get('evaluator'))
    bound_file(trial.get('acceptance_protocol'))
    runs=trial.get('runs')
    if not isinstance(runs,list) or not all(isinstance(r,dict) for r in runs):
        raise ValueError('Invalid trial run inventory')
    matches=[r for r in runs if r.get('work')==str(work)]
    if len(matches)!=1:raise ValueError('Missing or duplicate preregistered run')
    run=matches[0]
    if run.get('variant')!=variant or run.get('discipline')!=discipline:
        raise ValueError('Preregistered run identity differs')
    if type(run.get('repeat')) is not int or run['repeat'] not in (1,2,3):
        raise ValueError('Expected one of three preregistered repeats')
    actual=hashlib.sha256((work/'classic_control/input_manifest.json').read_bytes()).hexdigest()
    if run.get('input_manifest_sha256')!=actual or protocol.get('input_manifest_sha256')!=actual:
        raise ValueError('Preregistered input manifest differs')
    # Return no evaluator paths or answer content to the execution context.
    return {'variant':variant,'discipline':discipline,'repeat':run['repeat'],
            'trial_sha256':protocol['comparison_trial']['sha256']}
