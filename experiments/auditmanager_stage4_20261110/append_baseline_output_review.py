"""Append a sealed discipline without replacing existing claim assessments."""
import argparse
import json
from collections import Counter
from pathlib import Path

from .prepare_baseline_output_review import HERE, ROOT, sha


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('discipline', choices=('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'))
    args = parser.parse_args()
    discipline = args.discipline
    queue = Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_r1/output_review_queue.json')
    receipt_path = HERE / f'{discipline}_r1_output_queue_append.json'
    assert not receipt_path.exists(), 'Append already recorded; inspect before retrying'
    before = sha(queue)
    result = json.loads(queue.read_text())
    old_rows = list(result['rows'])
    assert not any(r['discipline'] == discipline for r in old_rows)
    assert not any(s['discipline'] == discipline for s in result['sources'])
    evaluator = HERE / 'evaluator_release_errata_v2.json'
    assert sha(evaluator) == result['evaluator_sha256']
    targets = {t['target_id']: t for t in json.loads(evaluator.read_text())['targets']}
    work = ROOT / f'{discipline.lower()}_r1_classic' / 'classic_control'
    state = json.loads((work / 'status' / f'{discipline}.json').read_text())
    assert state['status'] == 'completed_unadjudicated'
    manifest = json.loads((work / 'input_manifest.json').read_text())[0]
    final = Path(manifest['output_dir']) / '03_findings.json'
    ledger = HERE / f'{discipline}_r1_completed_coverage_ledger.json'
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
    source = {'discipline': discipline, 'final_path': str(final),
              'final_sha256': sha(final), 'ledger_path': str(ledger),
              'ledger_sha256': sha(ledger)}
    added = []
    for finding in findings:
        fid = finding['id']
        added.append({'discipline': discipline, 'repeat': 1, 'finding_id': fid,
                      'finding': finding, 'target_links': links[fid],
                      'triage': 'mapped_requires_addition_review' if links[fid] else 'unmapped_not_yet_classified',
                      'claim_reviews': [], 'unsupported_additions': 'unchecked',
                      'novel_useful_content': 'unchecked', 'duplicate_burden': 'unchecked',
                      'profile_and_section': 'unchecked', 'engineer_reuse': 'unchecked',
                      'evidence': [], 'disposition': 'unchecked'})
    result['sources'].append(source)
    result['rows'].extend(added)
    result['counts'] = dict(Counter(r['triage'] for r in result['rows']))
    assert result['rows'][:len(old_rows)] == old_rows
    assert len({(r['discipline'], r['finding_id']) for r in result['rows']}) == len(result['rows'])
    assert sha(queue) == before, 'Queue changed during preparation'
    pending = queue.with_suffix('.pending.json')
    with pending.open('x') as stream:
        stream.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    after = sha(pending)
    receipt = {'queue_path': str(queue), 'queue_before_sha256': before,
               'queue_after_sha256': after, 'source': source, 'added_rows': added,
               'preserved_rows': len(old_rows), 'total_rows': len(result['rows']),
               'model_calls': 0, 'quality_acceptance': False}
    with receipt_path.open('x') as stream:
        stream.write(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    pending.replace(queue)
    assert sha(queue) == after
    print(json.dumps({'added': len(added), 'preserved': len(old_rows),
                      'total': len(result['rows'])}))


if __name__ == '__main__':
    main()
