"""Freeze evaluator-only quality queues after sealed runs and completed coverage."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from .score_atomic_review import score, sha
from .verify_experimental_seals import verify
from .verify_output_review_history import encode, initial_queue

HERE = Path(__file__).resolve().parent


def build(sealed, evaluator, ledger_path, destination):
    evaluator, ledger_path, destination = map(Path, (evaluator, ledger_path, destination))
    # The history replayer uses this exact evaluator release.
    if sha(evaluator) != sha(HERE / 'evaluator_release_errata_v2.json'):
        raise ValueError('Unsupported evaluator for history replay')
    final = Path(sealed['final_path'])
    if sha(final) != sealed['final_sha256']:
        raise ValueError('Sealed final changed')
    ledger = json.loads(ledger_path.read_text())
    for key in ('discipline', 'repeat', 'variant'):
        if ledger.get(key) != sealed[key]:
            raise ValueError('Coverage run identity mismatch: ' + key)
    summary = score(evaluator, final, ledger)
    if not ledger['entire_output_reviewed'] or not all(
        route['review_complete'] for route in summary['routes'].values()
    ):
        raise ValueError('Finish coverage review before freezing output links')
    targets = {t['target_id']: t for t in json.loads(evaluator.read_text())['targets']}
    findings = json.loads(final.read_text())['findings']
    if len({f['id'] for f in findings}) != len(findings):
        raise ValueError('Duplicate finding IDs')
    links = {f['id']: [] for f in findings}
    for target in ledger['targets']:
        for fid in target['finding_ids']:
            links[fid].append({'target_id': target['target_id'],
                              'route': targets[target['target_id']]['route'],
                              'coverage': target['coverage']})
    source = {k: sealed[k] for k in ('discipline', 'variant', 'repeat',
                                    'final_path', 'final_sha256', 'status_path', 'status_sha256')}
    source.update(ledger_path=str(ledger_path), ledger_sha256=sha(ledger_path))
    rows = [{'discipline': sealed['discipline'], 'repeat': sealed['repeat'],
             'finding_id': f['id'], 'finding': f, 'target_links': links[f['id']],
             'triage': 'mapped_requires_addition_review' if links[f['id']] else 'unmapped_not_yet_classified',
             'claim_reviews': [], 'unsupported_additions': 'unchecked',
             'novel_useful_content': 'unchecked', 'duplicate_burden': 'unchecked',
             'profile_and_section': 'unchecked', 'engineer_reuse': 'unchecked',
             'evidence': [], 'disposition': 'unchecked'} for f in findings]
    queue = {'status': 'prepared_output_review_not_quality_verdict', 'repeat': sealed['repeat'],
             'evaluator_sha256': sha(evaluator), 'sources': [source], 'rows': rows,
             'counts': dict(Counter(r['triage'] for r in rows)),
             'discipline_counts': {sealed['discipline']: len(rows)},
             'rules': ['Coverage does not approve additions, consequences or remedies.',
                       'Unmapped findings are not automatically false or novel.',
                       'Review compound claims, duplicates and report sections separately.',
                       'Reuse engineer decisions only for the same claim and source context.',
                       'Keep this evaluator queue outside independent model inputs.'],
             'model_calls': 0, 'production_acceptance': False}
    receipt = {k: v for k, v in queue.items() if k != 'rows'}
    receipt.update(queue_path=str(destination),
                   queue_sha256=hashlib.sha256(encode(queue)).hexdigest(), findings=len(rows))
    assert encode(initial_queue(receipt)) == encode(queue)
    return queue, receipt


def materialize(root, destination, run_key):
    root, destination = Path(root).resolve(), Path(destination).resolve()
    if destination == root or destination.is_relative_to(root):
        raise ValueError('Evaluator output must be outside discovery root')
    seals = verify(root)
    matches = [s for s in seals['verified_runs']
               if f"{s['discipline']}_r{s['repeat']}_{s['variant']}" == run_key]
    if len(matches) != 1:
        raise ValueError('Requested run is not uniquely sealed')
    sealed = matches[0]
    trial = json.loads((root / 'experimental_collection_trial.json').read_text())
    evaluator = Path(trial['evaluator']['path'])
    if sha(evaluator) != trial['evaluator']['sha256']:
        raise ValueError('Frozen evaluator changed')
    folder = destination / run_key
    queue_path = folder / 'output_review_queue.json'
    receipt_path = folder / 'output_review_initial_receipt.json'
    if queue_path.exists() or receipt_path.exists():
        raise FileExistsError('Preserve existing review queue and history')
    queue, receipt = build(sealed, evaluator, folder / 'coverage_ledger.json', queue_path)
    with queue_path.open('xb') as stream:
        stream.write(encode(queue))
    with receipt_path.open('xb') as stream:
        stream.write(encode(receipt))
    return {'queue': str(queue_path), 'receipt': str(receipt_path),
            'findings': len(queue['rows']), 'quality_acceptance': False, 'model_calls': 0}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--run', required=True)
    args = parser.parse_args()
    print(json.dumps(materialize(args.root, args.destination, args.run), ensure_ascii=False))
