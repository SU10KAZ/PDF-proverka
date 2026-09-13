"""Run three executable grouping alternatives against constructed DEV partitions."""
from collections import defaultdict
from .controls import cases
from .engine import APPROACHES, build_change, extract_differences, group_facts


def run():
    results, details = [], []
    for approach in APPROACHES:
        metrics = dict(approach=approach, cases=0, raw_differences=0, project_changes=0,
                       proven=0, review=0, false_project_changes=0, duplicate_project_changes=0,
                       over_grouped=0, under_grouped=0, exact_events=0, expected_events=0,
                       entity_matches=0)
        for case in cases():
            diff = extract_differences(case['pair'])
            truth = {}
            for f in diff['facts']:
                prop = 'presence' if f['property'] in {'added', 'removed'} else f['property']
                candidates = [(kind, props) for kind, props in case['expected'] if prop in props or
                              (prop == 'presence' and kind.endswith(f['property'].upper()))]
                if not candidates:
                    raise AssertionError('Control lacks independent expected partition: ' + case['name'])
                kind, props = candidates[0]
                truth[f['fact_id']] = (f['entity_key'], kind, tuple(sorted(props)))
            groups = group_facts(diff['facts'], approach)
            expected_facts = defaultdict(set)
            appearances = defaultdict(set)
            for fid, label in truth.items():
                expected_facts[label].add(fid)
            case_detail = dict(case=case['name'], approach=approach, groups=[])
            metrics['cases'] += 1
            metrics['expected_events'] += len(case['expected'])
            metrics['raw_differences'] += len(diff['facts'])
            metrics['entity_matches'] += len(diff['entity_matches'])
            metrics['project_changes'] += len(groups)
            for i, group in enumerate(groups):
                c = build_change(case['pair'], group)
                labels = {truth[f['fact_id']] for f in group}
                for label in labels:
                    appearances[label].add(i)
                over = len(labels) > 1
                wrong_type = len(labels) == 1 and c['change_type'] != next(iter(labels))[1]
                exact = len(labels) == 1 and not wrong_type and {f['fact_id'] for f in group} == expected_facts[next(iter(labels))]
                metrics['over_grouped'] += int(over)
                metrics['false_project_changes'] += int(over or wrong_type)
                metrics['exact_events'] += int(exact)
                metrics['proven' if c['status'] == 'PROVEN' else 'review'] += 1
                case_detail['groups'].append(dict(change_type=c['change_type'], exact=exact,
                    properties=[f['property'] for f in group], truth_partitions=[list(x) for x in sorted(labels)]))
            metrics['under_grouped'] += sum(len(v) > 1 for v in appearances.values())
            metrics['duplicate_project_changes'] += sum(max(0, len(v) - 1) for v in appearances.values())
            details.append(case_detail)
        metrics['exact_event_precision'] = metrics['exact_events'] / metrics['project_changes'] if metrics['project_changes'] else None
        metrics['parameter_compression_ratio'] = metrics['raw_differences'] / metrics['project_changes'] if metrics['project_changes'] else None
        results.append(metrics)
    return dict(basis='Constructed DEV event partitions, not human corpus truth; no holdout/EVAL',
                approaches=results, details=details)
