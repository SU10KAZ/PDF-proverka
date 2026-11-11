"""Verify unstarted experimental slots without touching live baseline outputs."""
import json
from pathlib import Path
from .materialize_trial_inputs import sha
from .paired_protocol import verify_shared_protocol

HERE = Path(__file__).parent
ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
ARMS = ('classic_current', 'inventory', 'source_review')


def main():
    layout = json.loads((ROOT / 'layout.json').read_text())
    seen = set()
    receipts = []
    runs = []
    for arm in ARMS:
        receipt = ROOT / (arm + '_input_copy_receipt.json')
        assert len(json.loads(receipt.read_text())['runs']) == 18
        receipts.append({'path': str(receipt), 'sha256': sha(receipt)})
    for row in layout['runs']:
        if row['variant'] not in ARMS:
            continue
        work = Path(row['work'])
        control = work / 'classic_control'
        assert not (control / 'protocol.json').exists()
        assert not (control / 'status').exists()
        path = control / 'input_manifest.json'
        manifest = json.loads(path.read_text())[0]
        version = Path(manifest['version_dir'])
        source = layout['sources_observed_now_not_yet_trial_frozen'][row['discipline']]
        expected = {n.replace('03_analysis/runs/candidate/', '03_analysis/runs/comparison/', 1): h
                    for n, h in source['files'].items()}
        assert manifest['files'] == expected
        assert not any(p.is_symlink() for p in version.rglob('*'))
        assert {str(p.relative_to(version)) for p in version.rglob('*') if p.is_file()} == set(expected)
        for name, digest in expected.items():
            target = version / name
            assert sha(target) == digest
            identity = (target.stat().st_dev, target.stat().st_ino)
            assert identity not in seen
            seen.add(identity)
            original = Path(source['root']) / name.replace('03_analysis/runs/comparison/', '03_analysis/runs/candidate/', 1)
            assert identity != (original.stat().st_dev, original.stat().st_ino)
        runs.append({'variant': row['variant'], 'discipline': row['discipline'], 'repeat': row['repeat'],
                     'input_manifest': str(path), 'sha256': sha(path)})
    assert len(runs) == 54
    trial = json.loads((ROOT / 'baseline_collection_trial.json').read_text())
    baseline = json.loads(Path(trial['source_snapshot']['path']).read_text())
    for row in trial['runs']:
        verify_shared_protocol(Path.cwd(), baseline['source_root'], row['work'], 'classic')
    result = {'status': '54_experimental_input_copies_verified_not_dispatched',
        'arms': list(ARMS), 'runs': runs, 'copy_receipts': receipts,
        'files_verified': len(seen), 'independent_file_inodes': True,
        'identical_source_bytes_across_repeats_and_arms': True,
        'active_baseline_shared_protocols_unchanged': 18,
        'combined_candidate_materialized': False,
        'model_calls': 0, 'trial_frozen': False, 'stage4_complete': False,
        'remaining': 'Bind experimental decisions, source snapshot, model budgets and dispatch order in a separate execution trial.'}
    (HERE / 'experimental_input_copies_v2_check.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'runs': len(runs), 'files_verified': len(seen), 'baseline_shared_protocols_verified': 18}))


if __name__ == '__main__':
    main()
