"""Create evaluator-only output review queue, without treating matches as validation."""
import hashlib
import json
from collections import Counter
from pathlib import Path

ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
HERE = Path(__file__).parent

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    evaluator = HERE / 'evaluator_release_errata_v2.json'
    truth = json.loads(evaluator.read_text())
    targets = {t['target_id']: t for t in truth['targets']}
    rows, sources = [], []
    for discipline in ('AK', 'ITP', 'AR', 'KM'):
        work = ROOT / (discipline.lower() + '_r1_classic') / 'classic_control'
        state = json.loads((work / 'status' / (discipline + '.json')).read_text())
        assert state['status'] == 'completed_unadjudicated'
        manifest = json.loads((work / 'input_manifest.json').read_text())[0]
        final = Path(manifest['output_dir']) / '03_findings.json'
        ledger = HERE / (discipline + '_r1_completed_coverage_ledger.json')
        review = json.loads(ledger.read_text())
        assert sha(final) == state['final_sha256'] == review['findings_sha256']
        assert review['evaluator_sha256'] == sha(evaluator)
        findings = json.loads(final.read_text())['findings']
        links = {f['id']: [] for f in findings}
        for target in review['targets']:
            for fid in target['finding_ids']:
                links[fid].append({'target_id': target['target_id'],
                                   'route': targets[target['target_id']]['route'],
                                   'coverage': target['coverage']})
        sources.append({'discipline': discipline, 'final_path': str(final),
                        'final_sha256': sha(final), 'ledger_path': str(ledger),
                        'ledger_sha256': sha(ledger)})
        for finding in findings:
            fid = finding['id']
            rows.append({'discipline': discipline, 'repeat': 1, 'finding_id': fid,
                         'finding': finding, 'target_links': links[fid],
                         'triage': 'mapped_requires_addition_review' if links[fid] else 'unmapped_not_yet_classified',
                         'claim_reviews': [], 'unsupported_additions': 'unchecked',
                         'novel_useful_content': 'unchecked', 'duplicate_burden': 'unchecked',
                         'profile_and_section': 'unchecked', 'engineer_reuse': 'unchecked',
                         'evidence': [], 'disposition': 'unchecked'})
    assert len({(r['discipline'], r['finding_id']) for r in rows}) == len(rows)
    result = {'status': 'prepared_output_review_not_quality_verdict',
              'evaluator_sha256': sha(evaluator), 'sources': sources, 'rows': rows,
              'counts': dict(Counter(r['triage'] for r in rows)),
              'rules': ['Split compound findings into factual claims before assessing additions.',
                        'No target link means unclassified, not false, novel or useless.',
                        'A full target match does not approve risk, remedy, severity or extra assertions.',
                        'Reuse historical engineer decisions only for the same atomic claim and source context.',
                        'Do not send this evaluator queue to independent discovery runs.',
                        'Do not count multiple target links as independent physical defects.'],
              'model_calls': 0, 'production_acceptance': False}
    destination = Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_r1/output_review_queue.json')
    with destination.open('x') as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    receipt = {k: v for k, v in result.items() if k != 'rows'}
    receipt.update(queue_path=str(destination), queue_sha256=sha(destination), findings=len(rows))
    HERE.joinpath('baseline_r1_output_review_queue_receipt.json').write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'findings': len(rows), 'counts': result['counts']}, ensure_ascii=False))

if __name__ == '__main__':
    main()
