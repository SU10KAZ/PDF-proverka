"""Create one immutable initial output-review queue for a completed coverage ledger.

Evaluator-only: no model requests, no runner inputs, no inferred engineering verdicts.
Separate discipline queues let completed reviews advance while other runs finish.
"""
import argparse
from collections import Counter
import json
from pathlib import Path

from .score_atomic_review import score, sha
from .verify_output_review_history import encode, initial_queue

HERE = Path(__file__).resolve().parent
TRIAL = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
DEST = Path('/tmp/auditmanager_comparison_evaluation_20261111')


def build(discipline, repeat, ledger_path):
    evaluator = HERE / 'evaluator_release_errata_v2.json'
    base = TRIAL / f'{discipline.lower()}_r{repeat}_classic/classic_control'
    seal = json.loads((base / f'status/{discipline}.json').read_text())
    if seal['status'] != 'completed_unadjudicated':
        raise ValueError('Run is not sealed')
    manifest = json.loads((base / 'input_manifest.json').read_text())[0]
    final = Path(manifest['output_dir']) / '03_findings.json'
    if sha(final) != seal['final_sha256']:
        raise ValueError('Final output changed after sealing')
    ledger_path = Path(ledger_path)
    review = json.loads(ledger_path.read_text())
    if review['discipline'] != discipline or review.get('repeat') != repeat:
        raise ValueError('Wrong discipline or repeat in coverage ledger')
    summary = score(evaluator, final, review)
    if not review['entire_output_reviewed'] or not all(
        route['review_complete'] for route in summary['routes'].values()
    ):
        raise ValueError('Coverage review must finish before freezing target links')
    truth = json.loads(evaluator.read_text())
    targets = {t['target_id']: t for t in truth['targets']}
    findings = json.loads(final.read_text())['findings']
    links = {f['id']: [] for f in findings}
    for target in review['targets']:
        for fid in target['finding_ids']:
            links[fid].append({'target_id': target['target_id'],
                              'route': targets[target['target_id']]['route'],
                              'coverage': target['coverage']})
    source = {'discipline': discipline, 'final_path': str(final),
              'final_sha256': sha(final), 'ledger_path': str(ledger_path),
              'ledger_sha256': sha(ledger_path)}
    rows = [{'discipline': discipline, 'repeat': repeat, 'finding_id': f['id'],
             'finding': f, 'target_links': links[f['id']],
             'triage': 'mapped_requires_addition_review' if links[f['id']] else 'unmapped_not_yet_classified',
             'claim_reviews': [], 'unsupported_additions': 'unchecked',
             'novel_useful_content': 'unchecked', 'duplicate_burden': 'unchecked',
             'profile_and_section': 'unchecked', 'engineer_reuse': 'unchecked',
             'evidence': [], 'disposition': 'unchecked'} for f in findings]
    queue = {'status': 'prepared_output_review_not_quality_verdict', 'repeat': repeat,
             'evaluator_sha256': sha(evaluator), 'sources': [source], 'rows': rows,
             'counts': dict(Counter(r['triage'] for r in rows)),
             'discipline_counts': {discipline: len(rows)},
             'rules': ['Match coverage does not approve additions, consequences or remedies.',
                       'Unmapped does not mean false, novel or useless.',
                       'Assess compound claims and duplicates separately.',
                       'Reuse engineer decisions only for the same claim and source context.',
                       'Do not expose evaluator queues to independent model runners.'],
             'model_calls': 0, 'production_acceptance': False}
    destination = DEST / f'baseline_r{repeat}/{discipline}_output_review_queue.json'
    receipt = {k: v for k, v in queue.items() if k != 'rows'}
    import hashlib
    receipt.update(queue_path=str(destination),
                   queue_sha256=hashlib.sha256(encode(queue)).hexdigest(), findings=len(rows))
    assert encode(initial_queue(receipt)) == encode(queue)
    return queue, receipt, destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--discipline', choices=['AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'], required=True)
    parser.add_argument('--repeat', type=int, required=True)
    parser.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args()
    queue, receipt, destination = build(args.discipline, args.repeat, args.ledger)
    receipt_path = HERE / f'{args.discipline}_r{args.repeat}_output_review_queue_receipt.json'
    if destination.exists() or receipt_path.exists():
        raise FileExistsError('Preserve existing queue/history; use the review receipt workflow')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('xb') as stream:
        stream.write(encode(queue))
    with receipt_path.open('xb') as stream:
        stream.write(encode(receipt))
    print(json.dumps({'discipline': args.discipline, 'findings': len(queue['rows']),
                      'queue': str(destination), 'quality_acceptance': False}))


if __name__ == '__main__':
    main()
