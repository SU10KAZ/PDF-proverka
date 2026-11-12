"""Prepare unchecked evaluator ledgers from completed, sealed trial outputs only."""
import argparse
import json
from pathlib import Path
from .observe_baseline_collection import process, read
from .score_atomic_review import prepare, sha


def build(root, destination):
    root, destination = root.resolve(), destination.resolve()
    if destination.is_relative_to(root) or root.is_relative_to(destination):
        raise ValueError('Evaluator workspace must be separate from audit workspace')
    trial_path = root/'experimental_collection_trial.json'
    trial = read(trial_path)
    evaluator = Path(trial['evaluator']['path'])
    assert sha(evaluator) == trial['evaluator']['sha256']
    collection = read(root/'experimental_collection_status.json')
    records = {r['work']: r for r in collection.get('runs', [])}
    entries, pending = [], []
    for run in trial['runs']:
        identity = {k:run[k] for k in ('discipline', 'variant', 'repeat')}
        record = records.get(run['work'], {})
        base = Path(run['work'])/'classic_control'
        seal_path = base/'status'/(run['discipline']+'.json')
        seal = read(seal_path)
        if (record.get('exit_code') != 0 or seal.get('status') != 'completed_unadjudicated'
                or process(record.get('pid'), 'paired_runner')['live'] is not False):
            pending.append(identity | {'reason': 'no_verified_terminal_completion'})
            continue
        assert sha(seal_path) == record['status_sha256']
        protocol_path = base/'protocol.json'
        protocol = read(protocol_path)
        assert protocol['comparison_trial']['sha256'] == sha(trial_path)
        manifest_path = base/'input_manifest.json'
        assert sha(manifest_path) == run['input_manifest_sha256']
        usage = seal['request_usage']
        assert usage['accounting_complete'] and not usage['issues']
        request_root = (base/'requests'/run['discipline']).resolve()
        for name, digest in usage['artifact_sha256'].items():
            path = (request_root/name).resolve()
            assert path.is_relative_to(request_root) and sha(path) == digest
        project, = read(manifest_path)
        final = Path(project['output_dir'])/'03_findings.json'
        assert final.resolve().is_relative_to(base.resolve())
        assert sha(final) == seal['final_sha256']
        snapshot = base/'snapshots'/run['discipline']/'findings_verify.json'
        assert read(snapshot) == read(final)
        destination.mkdir(parents=True, exist_ok=True)
        ledger_path = destination/(Path(run['work']).name+'_coverage_ledger.json')
        if ledger_path.exists():
            ledger = read(ledger_path)
            assert ledger['findings_sha256'] == sha(final)
            assert ledger['evaluator_sha256'] == sha(evaluator)
        else:
            ledger = prepare(evaluator, final, run['discipline'])
            with ledger_path.open('x') as stream:
                stream.write(json.dumps(ledger, ensure_ascii=False, indent=2)+'\n')
        entries.append(identity | {
            'seal_path': str(seal_path), 'seal_sha256': sha(seal_path),
            'final_path': str(final), 'final_sha256': sha(final),
            'ledger_path': str(ledger_path), 'ledger_sha256': sha(ledger_path),
            'targets': len(ledger['targets']),
            'unchecked_targets': sum(t['coverage'] == 'unchecked' for t in ledger['targets']),
            'request_artifacts_verified': len(usage['artifact_sha256']),
        })
    return {'status': 'evaluation_preparation_not_quality_acceptance',
        'trial_sha256': sha(trial_path), 'evaluator_sha256': sha(evaluator),
        'entries': entries, 'pending': pending, 'model_calls': 0,
        'production_acceptance': False,
        'limits': ['Only terminal, hash-verified outputs opened; existing reviews preserved.',
                   'Unchecked targets are not misses. Semantic adjudication remains required.',
                   'No evaluator or history written into independent audit inputs.',
                   'Novel findings, unsupported additions, retention and engineer workload need separate assessment.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    result = build(args.root, args.destination)
    args.destination.mkdir(parents=True, exist_ok=True)
    (args.destination/'preparation_status.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'prepared': len(result['entries']), 'pending': len(result['pending']), 'model_calls': 0}))
