"""Replay evaluator-only review receipts; this does not validate engineering truth."""
import argparse
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).parent


def encode(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def read_bound(path, expected):
    raw = Path(path).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == expected, f'Source changed: {path}'
    return json.loads(raw)


def initial_queue(receipt):
    evaluator = read_bound(HERE / 'evaluator_release_errata_v2.json', receipt['evaluator_sha256'])
    targets = {t['target_id']: t for t in evaluator['targets']}
    rows = []
    for source in receipt['sources']:
        findings = read_bound(source['final_path'], source['final_sha256'])['findings']
        ledger = read_bound(source['ledger_path'], source['ledger_sha256'])
        assert ledger['findings_sha256'] == source['final_sha256']
        assert ledger['evaluator_sha256'] == receipt['evaluator_sha256']
        links = {f['id']: [] for f in findings}
        for target in ledger['targets']:
            for fid in target['finding_ids']:
                links[fid].append({'target_id': target['target_id'],
                                   'route': targets[target['target_id']]['route'],
                                   'coverage': target['coverage']})
        for finding in findings:
            fid = finding['id']
            rows.append({'discipline': source['discipline'], 'repeat': 1,
                         'finding_id': fid, 'finding': finding, 'target_links': links[fid],
                         'triage': 'mapped_requires_addition_review' if links[fid] else 'unmapped_not_yet_classified',
                         'claim_reviews': [], 'unsupported_additions': 'unchecked',
                         'novel_useful_content': 'unchecked', 'duplicate_burden': 'unchecked',
                         'profile_and_section': 'unchecked', 'engineer_reuse': 'unchecked',
                         'evidence': [], 'disposition': 'unchecked'})
    result = {'status': receipt['status'], 'evaluator_sha256': receipt['evaluator_sha256'],
              'sources': copy.deepcopy(receipt['sources']), 'rows': rows,
              'counts': dict(Counter(r['triage'] for r in rows)), 'rules': receipt['rules'],
              'model_calls': 0, 'production_acceptance': False}
    assert len(rows) == receipt['findings']
    assert digest(result) == receipt['queue_sha256'], 'Initial queue reconstruction differs'
    return result


def key(row):
    return row['discipline'], row['repeat'], row['finding_id']


def verify(queue_path=None):
    initial = json.loads((HERE / 'baseline_r1_output_review_queue_receipt.json').read_text())
    result = initial_queue(initial)
    transitions = {}
    for path in HERE.glob('*.json'):
        data = json.loads(path.read_text())
        if not isinstance(data, dict) or data.get('queue_path') != initial['queue_path']:
            continue
        before = data.get('queue_before_sha256')
        if before:
            assert before not in transitions, f'Branched review history: {path}'
            transitions[before] = (path, data)
    visited = []
    while digest(result) in transitions:
        path, event = transitions.pop(digest(result))
        if 'added_rows' in event:
            assert len(result['rows']) == event['preserved_rows']
            source = event['source']
            assert source['discipline'] not in [s['discipline'] for s in result['sources']]
            findings = read_bound(source['final_path'], source['final_sha256'])['findings']
            read_bound(source['ledger_path'], source['ledger_sha256'])
            assert [r['finding'] for r in event['added_rows']] == findings
            result['sources'].append(source)
            result['rows'].extend(event['added_rows'])
            result['counts'] = dict(Counter(r['triage'] for r in result['rows']))
            assert len(result['rows']) == event['total_rows']
        else:
            index = {key(row): i for i, row in enumerate(result['rows'])}
            for replacement in event['reviewed_rows']:
                i = index[key(replacement)]
                original = result['rows'][i]
                for field in ('finding', 'target_links', 'triage'):
                    assert replacement[field] == original[field], f'Frozen content changed: {path}, {field}'
                result['rows'][i] = replacement
        assert len({key(row) for row in result['rows']}) == len(result['rows'])
        assert digest(result) == event['queue_after_sha256'], f'Replay mismatch: {path}'
        visited.append({'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    assert not transitions, 'Disconnected or cyclic review history'
    actual = Path(queue_path or initial['queue_path']).read_bytes()
    assert actual == encode(result), 'Current queue differs from complete receipt replay'
    counts = {}
    for discipline in sorted({r['discipline'] for r in result['rows']}):
        rows = [r for r in result['rows'] if r['discipline'] == discipline]
        counts[discipline] = {'rows': len(rows),
                              'assessed': sum(r['disposition'] != 'unchecked' for r in rows),
                              'unchecked': sum(r['disposition'] == 'unchecked' for r in rows)}
    return {'status': 'exact_receipt_replay_verified', 'queue_sha256': digest(result),
            'transitions': visited, 'counts': counts, 'quality_acceptance': False,
            'limits': 'Verifies preservation and source binding, not factual truth, usefulness or resolved uncertainty.'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    report = verify(args.queue)
    if args.output:
        args.output.write_bytes(encode(report))
    print(json.dumps({k: v for k, v in report.items() if k != 'transitions'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
