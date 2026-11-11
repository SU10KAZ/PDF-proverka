"""Freeze the two verified fixes without changing earlier trial capsules."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

HERE = Path(__file__).parent
PARENT = HERE / 'candidate_catalog_snapshot_provenance.json'
OUTPUT = HERE / 'candidate_retrieval_v2_snapshot_provenance.json'
METHOD_CHANGES = {
    'backend/app/pipeline/stages/prepare/recovery_context.py',
    'backend/app/pipeline/stages/findings_verify/source_review_final_check.py',
}

SHARED_HARNESS_CHANGES = {
    'experiments/classic_investigator/classic_control.py',
    'experiments/classic_investigator/execution_receipts.py',
    'experiments/classic_investigator/subscription_retry.py',
}
EXPECTED = METHOD_CHANGES | SHARED_HARNESS_CHANGES


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    if OUTPUT.exists():
        raise ValueError('Preserve existing freeze; verify it instead of overwriting')
    parent = json.loads(PARENT.read_text())
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    previous = parent['provenance']['commit']
    changed = set(subprocess.check_output([
        'git', 'diff', '--name-only', previous, commit, '--',
        'backend', 'prompts', 'experiments/classic_investigator',
    ], text=True).splitlines())
    if changed != EXPECTED:
        raise ValueError(f'Unexpected source changes: {sorted(changed)}')
    archive = subprocess.check_output([
        'git', 'archive', commit, 'backend', 'prompts', 'experiments/classic_investigator',
    ])
    source = {}
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        for member in tar:
            if member.name in parent['source_files']:
                if not member.isfile():
                    raise ValueError('Nonregular source: ' + member.name)
                source[member.name] = tar.extractfile(member).read()
    if set(source) != set(parent['source_files']):
        raise ValueError('Missing source files')
    old = Path(parent['source_root'])
    actual_changes = []
    for name, data in source.items():
        if digest((old / name).read_bytes()) != parent['source_files'][name]:
            raise ValueError('Parent capsule changed: ' + name)
        if digest(data) != parent['source_files'][name]:
            actual_changes.append(name)
    if set(actual_changes) != EXPECTED:
        raise ValueError('Manifest change set differs')
    root = old.with_name('candidate_retrieval_v2_' + commit[:12])
    root.mkdir(exist_ok=False)
    for name, data in source.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        assert digest(target.read_bytes()) == digest(data)
    report = {
        'source_root': str(root),
        'source_files': {name: digest(data) for name, data in sorted(source.items())},
        'provenance': {
            'kind': 'committed_candidate_export', 'commit': commit,
            'parent_manifest': {'path': str(PARENT.resolve()), 'sha256': digest(PARENT.read_bytes())},
            'git_archive_sha256': digest(archive),
            'changes': sorted(actual_changes),
            'method_changes': sorted(METHOD_CHANGES),
            'shared_harness_changes': sorted(SHARED_HARNESS_CHANGES),
            'shared_harness_binding': 'The separately frozen executor must be identical across compared arms; bundled harness files are not an implicit executor selection.',
            'scope': 'Same runtime file set as catalog-complete candidate; exact document/sheet retrieval priority and metadata evidence prompt clarification.',
            'baseline_modified': False, 'model_calls': 0, 'method_acceptance': False,
            'trial_dispatch_authorized_by_this_manifest': False,
            'limits': 'Source freeze only. Arm eligibility, configuration and shared harness must be bound separately before dispatch. No evaluator changes.',
        },
    }
    with OUTPUT.open('x') as stream:
        stream.write(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'commit': commit, 'files': len(source), 'changes': sorted(actual_changes), 'model_calls': 0}))


if __name__ == '__main__':
    main()
