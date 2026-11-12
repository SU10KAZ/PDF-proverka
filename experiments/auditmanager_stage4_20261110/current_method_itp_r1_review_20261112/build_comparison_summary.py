"""Summarize sealed, manually adjudicated evidence; never label it acceptance."""
import hashlib
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
EVALUATOR = HERE.parent / 'evaluator_release_current_methods_20261112.json'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_text())


def validate_references(value):
    if isinstance(value, dict):
        if 'path' in value and 'sha256' in value:
            path = Path(value['path'])
            if not path.is_absolute():
                path = REPO / path
            if digest(path) != value['sha256']:
                raise ValueError(f'Evidence changed: {path}')
            if 'pointer' in value and path.suffix == '.json' and value['pointer'].startswith('/'):
                node = load(path)
                for token in value['pointer'].split('/')[1:]:
                    token = token.replace('~1', '/').replace('~0', '~')
                    node = node[int(token)] if isinstance(node, list) else node[token]
        for item in value.values():
            validate_references(item)
    elif isinstance(value, list):
        for item in value:
            validate_references(item)


def build():
    names = ['itp_r1_classic_current_coverage_ledger.json',
             'claim_adjudication_index.json', 'loss_trace_index.json',
             'output_burden_and_routing_review.json',
             'question_adjudication_index.json',
             'valve_claim_adjudication.json',
             'tank_temperature_and_volume_claim_adjudication.json']
    inputs = {name: load(HERE / name) for name in names}
    for data in inputs.values():
        validate_references(data)
    evaluator = load(EVALUATOR)
    ledger = inputs[names[0]]
    if ledger['evaluator_sha256'] != digest(EVALUATOR):
        raise ValueError('Evaluator changed')
    truth = {t['target_id']: t for t in evaluator['targets']}
    targets = ledger['targets']
    if len({t['target_id'] for t in targets}) != len(targets):
        raise ValueError('Duplicate coverage target')
    counts = {route: Counter() for route in ('technical', 'question', 'negative')}
    for row in targets:
        counts[truth[row['target_id']]['route']][row['coverage']] += 1
    coverage = {}
    for route, count in counts.items():
        total = sum(count.values())
        coverage[route] = {'total': total, 'counts': dict(count)}
        if route != 'negative':
            coverage[route]['strict_full_coverage_percent'] = round(100 * count['full'] / total, 2)
            coverage[route]['full_or_partial_percent_not_recall'] = round(100 * (count['full'] + count['partial']) / total, 2)
    claims = inputs[names[1]]
    traces = inputs[names[2]]
    burden = inputs[names[3]]
    if len(claims['rows']) != len({r['finding_id'] for r in claims['rows']}):
        raise ValueError('Duplicate claim adjudication')
    expected_gaps = {t['target_id'] for t in targets
                     if truth[t['target_id']]['route'] != 'negative' and t['coverage'] != 'full'}
    if expected_gaps != {r['target_id'] for r in traces['rows']}:
        raise ValueError('Loss trace set differs from positive coverage gaps')
    return {
        'version': 1,
        'scope': 'ITP classic_current repeat 1 only; sealed historical regression, not independent transfer validation',
        'sources': [{'path': str((HERE / n).relative_to(REPO)), 'sha256': digest(HERE / n)} for n in names]
                   + [{'path': str(EVALUATOR.relative_to(REPO)), 'sha256': digest(EVALUATOR)}],
        'coverage': coverage,
        'claim_review': claims['counts'],
        'loss_trace': traces['counts'],
        'output_burden': burden['counts'],
        'additional_wrong_existing_reference_owners': [
            inputs[names[5]]['renumbering_reference_error']['owner'],
            inputs[names[6]]['renumbering_reference_error']['owner']],
        'normative_scope_pending': inputs[names[4]]['pending_normative_finding_ids'],
        'whole_output_precision': None,
        'engineer_review_time_seconds': None,
        'monetary_cost': None,
        'comparative_improvement': None,
        'stage4_complete': False,
        'production_acceptance': False,
        'limits': [
            'Full coverage is semantic coverage of a frozen reference atom, not proof that the entire emitted row is correct.',
            'Partial matches are reported separately and receive no full-recall credit.',
            'Absence of historical negative targets is not causal evidence that a new filter worked.',
            'Bounded adjudications include mixed, refuted and unresolved claims; 74 reviewed does not mean 74 valid.',
            '34 recorded loss traces do not establish exclusive causes or tested fixes.',
            'Eight duplicate pairs are a confirmed lower bound, not an exhaustive uniqueness assessment.',
            'Section disagreement is not technical falsehood. Questions remain a separate output category.',
            'No comparison to unfinished experimental arms, no measured engineer time or billed cost.',
        ],
    }


if __name__ == '__main__':
    output = HERE / 'comparison_summary.json'
    output.write_text(json.dumps(build(), ensure_ascii=False, indent=2) + '\n')
    print(output)
