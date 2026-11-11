"""Evaluator-only review ledger. No lexical matching and no automatic acceptance.

A reviewer must inspect all mandatory context before marking a full match.
Absence requires review of the entire sealed output; it never earns filter credit.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def project_targets(evaluator, discipline):
    selected = []
    for target in evaluator['targets']:
        disciplines = {m.group(1) for c in target['contexts'] for i in c['candidate_ids']
                       if (m := re.match(r'(?:B|H)-(AK|ITP|AR|KM|VK|EOM)-', i))}
        if len(disciplines) != 1:
            raise ValueError('Ambiguous target discipline: ' + target['target_id'])
        if discipline in disciplines:
            selected.append(target)
    return selected


def finding_ids(report):
    ids = [f['id'] for f in report['findings']]
    if len(ids) != len(set(ids)):
        raise ValueError('Duplicate finding IDs')
    return set(ids)


def prepare(evaluator_path, findings_path, discipline):
    evaluator, report = read(evaluator_path), read(findings_path)
    finding_ids(report)
    return {
        'schema_version': 1, 'discipline': discipline,
        'evaluator_sha256': sha(evaluator_path), 'findings_sha256': sha(findings_path),
        'reviewer': None, 'entire_output_reviewed': False,
        'targets': [{
            'target_id': t['target_id'], 'availability': 'unchecked',
            'coverage': 'unchecked', 'finding_ids': [], 'evidence': [],
            'reason': None, 'section_assessment': 'unchecked',
        } for t in project_targets(evaluator, discipline)],
        'limits': ['Full match requires all mandatory object, relationship and location context.',
                   'Score unsupported additions and novel findings separately.',
                   'Engineer confirmation is required; model suggestions alone are not final matches.'],
    }


def score(evaluator_path, findings_path, review):
    if review['evaluator_sha256'] != sha(evaluator_path) or review['findings_sha256'] != sha(findings_path):
        raise ValueError('Review is not bound to these evaluator/output bytes')
    evaluator, report = read(evaluator_path), read(findings_path)
    output_ids = finding_ids(report)
    targets = {t['target_id']: t for t in project_targets(evaluator, review['discipline'])}
    rows = review['targets']
    if len({r['target_id'] for r in rows}) != len(rows) or {r['target_id'] for r in rows} != set(targets):
        raise ValueError('Review must contain every canonical project target exactly once')
    dimensions = {}
    for hold in evaluator['holds']:
        for target_id in hold['target_ids']:
            dimensions.setdefault(target_id, set()).update(hold['held_dimensions'])
    routes = {route: Counter() for route in evaluator['truth_denominators']}
    section = Counter()
    for row in rows:
        target_id = row['target_id']
        route = targets[target_id]['route']
        availability, coverage = row['availability'], row['coverage']
        if availability not in ('available', 'unavailable', 'unchecked') or coverage not in ('full', 'partial', 'absent', 'unchecked'):
            raise ValueError('Unknown availability or coverage')
        links = row['finding_ids']
        if len(set(links)) != len(links) or not set(links) <= output_ids:
            raise ValueError('Invalid finding references')
        if coverage != 'unchecked':
            if not review['reviewer'] or not row['reason']:
                raise ValueError('Adjudicated coverage requires reviewer and reason')
            if availability != 'available':
                raise ValueError('Unavailable/unchecked evidence cannot yield a scored match or miss')
            if coverage in ('full', 'partial') and (not links or not row['evidence']):
                raise ValueError('Match requires output links and evidence')
            if coverage == 'absent' and (links or not review['entire_output_reviewed']):
                raise ValueError('Absence requires complete output review and no match links')
        elif links:
            raise ValueError('Suggested matches must be adjudicated separately, not entered as unchecked links')
        held = dimensions.get(target_id, set())
        if 'truth_scoring' in held:
            routes[route]['held'] += 1
        else:
            if target_id not in evaluator['truth_denominators'][route]:
                raise ValueError('Unexplained denominator exclusion')
            routes[route]['denominator'] += 1
            routes[route]['availability_' + availability] += 1
            routes[route][coverage] += 1
        assessment = row['section_assessment']
        if assessment not in ('correct', 'incorrect', 'unchecked'):
            raise ValueError('Unknown section assessment')
        if assessment != 'unchecked' and (coverage != 'full' or route == 'negative'):
            raise ValueError('Section accuracy requires fully matched positive target')
        if route != 'negative':
            if 'report_section_accuracy' in held:
                section['held'] += 1
            elif coverage == 'full':
                section[assessment] += 1
    metrics = {}
    for route, counts in routes.items():
        complete = counts['unchecked'] == 0 and counts['availability_available'] == counts['denominator']
        denominator = counts['denominator']
        metrics[route] = {
            'counts': dict(counts), 'review_complete': complete,
            'full_match_rate': counts['full'] / denominator if complete and denominator else None,
            'meaning': 'repeated false claim rate' if route == 'negative' else 'fully preserved target rate',
        }
    return {
        'status': 'review_summary_not_trial_acceptance', 'discipline': review['discipline'],
        'evaluator_sha256': review['evaluator_sha256'], 'findings_sha256': review['findings_sha256'],
        'routes': metrics, 'section_accuracy_counts': dict(section),
        'filter_effectiveness': None, 'production_acceptance': False,
        'unmeasured': ['Novel valid findings', 'Unsupported additions', 'Duplicate burden',
                       'Useful baseline retention', 'Engineer time', 'Run cost and duration',
                       'Candidate arrival and reasoned filter removal'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'score'])
    parser.add_argument('--evaluator', type=Path, required=True)
    parser.add_argument('--findings', type=Path, required=True)
    parser.add_argument('--discipline', choices=['AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'])
    parser.add_argument('--review', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        if not args.discipline:
            parser.error('prepare requires --discipline')
        result = prepare(args.evaluator, args.findings, args.discipline)
    else:
        if not args.review:
            parser.error('score requires --review')
        result = score(args.evaluator, args.findings, read(args.review))
    with args.output.open('x') as f:
        f.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
