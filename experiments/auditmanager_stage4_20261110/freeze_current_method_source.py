"""Export committed runtime sources for a new trial; never resume old protocols."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[1]
OUTPUT = HERE/'current_candidate_snapshot_provenance.json'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def eligible(name):
    return (name == 'backend/__init__.py'
            or name.startswith('backend/app/') and name.endswith('.py')
            or name.startswith('backend/app/pipeline/stages/block_context/reference_catalog/')
            or name.startswith('prompts/')
            or name.startswith('experiments/classic_investigator/') and name.endswith('.py'))


def main():
    assert not OUTPUT.exists(), 'Existing source freeze is immutable'
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPOSITORY, text=True).strip()
    archive = subprocess.check_output(['git', 'archive', commit, 'backend', 'prompts',
                                       'experiments/classic_investigator'], cwd=REPOSITORY)
    sources = {}
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        for member in tar.getmembers():
            if not eligible(member.name) or member.isdir():
                continue
            assert member.isfile() and not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
            sources[member.name] = tar.extractfile(member).read()
    assert 'backend/__init__.py' in sources
    assert any('/reference_catalog/disciplines/' in name for name in sources)
    for name, data in sources.items():
        assert (REPOSITORY/name).read_bytes() == data, 'Uncommitted runtime change: '+name
    root = Path('/tmp/auditmanager_current_method_sources_20261112')/('candidate_'+commit[:12])
    root.mkdir(parents=True, exist_ok=False)
    for name, data in sources.items():
        target = root/name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        assert digest(target.read_bytes()) == digest(data)
    previous_path = HERE/'inventory_budget_candidate_catalog_snapshot_provenance.json'
    previous = json.loads(previous_path.read_text())
    hashes = {name: digest(data) for name, data in sorted(sources.items())}
    changed = sorted(n for n in hashes if previous['source_files'].get(n) != hashes[n])
    removed = sorted(set(previous['source_files'])-set(hashes))
    report = {'source_root': str(root), 'source_files': hashes,
              'provenance': {'kind': 'committed_candidate_export', 'commit': commit,
                'git_archive_sha256': digest(archive), 'working_source_bytes_verified': True,
                'previous_manifest': {'path': str(previous_path), 'sha256': digest(previous_path.read_bytes())},
                'added_or_changed': changed, 'removed': removed,
                'shared_harness': 'Freeze the shared executor separately; bundled harness is not an implicit selection.',
                'no_trial_dispatch': True, 'model_calls': 0, 'method_acceptance': False,
                'production_changed': False}}
    OUTPUT.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'source_root': str(root), 'commit': commit, 'files': len(hashes),
                      'added_or_changed': len(changed), 'removed': removed, 'model_calls': 0}))


if __name__ == '__main__':
    main()
