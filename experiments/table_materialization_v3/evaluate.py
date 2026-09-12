"""DEV-only rule calibration. No dependency on old or fresh evaluation truth."""
from collections import Counter
from pathlib import Path
import argparse

from experiments.semantic_foundation_v3.run import read
from experiments.semantic_foundation_v3.scorer import score
from .prepare import ROOT, immutable, load_result
from .model import RULES
from .run import TableResolver


def evaluate(directory, cases):
    results, predictions = {}, {}
    for version in sorted({c['document_version'] for c in cases}):
        result = load_result(directory, version)
        result['tables'] = read(Path(directory) / version / 'tables.json')
        results[version] = result
        resolver = TableResolver(result)
        for c in cases:
            if c['document_version'] == version:
                assert all(resolver.resolve(a) is not None for a in c['anchors'])
                predictions[c['case_id']] = resolver.predict(c)
    return predictions, results


def rule_audit(cases, results):
    policy = read(ROOT / 'reports/PROMOTION_POLICY.json')
    out = {}
    observed = {(r, d['rule_strata'].get(r, 'UNSPECIFIED')) for result in results.values()
                for d in result['tables']['boundaries'] for r in d['candidate_join'] + d['candidate_split']}
    for name, stratum in sorted(observed):
        direction = RULES[name]
        selected = []
        for c in cases:
            a, b = [x['line_id'] for x in c['anchors']]
            edges = [d for d in results[c['document_version']]['tables']['boundaries'] if a < d['right_anchor'] <= b]
            if any(name in d['candidate_join'] + d['candidate_split'] and d['rule_strata'].get(name) == stratum for d in edges):
                selected.append(c)
        correct = sum(c['answer'] == ('SAME' if direction == 'JOIN' else 'NEW') for c in selected)
        denominator = len(selected)
        documents = len({c['document_version'] for c in selected})
        precision = correct / denominator if denominator else None
        out[name + '@' + stratum] = {'direction': direction, 'denominator': denominator, 'correct': correct,
                     'precision': precision, 'documents': documents,
                     'strata': dict(Counter(c['stratum'] for c in selected)),
                     'case_ids': [c['case_id'] for c in selected],
                     'promoted': denominator >= policy['minimum_human_cases_per_rule'] and
                     documents >= policy['minimum_documents_per_rule'] and precision >= policy['minimum_precision']}
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run', type=Path, required=True)
    p.add_argument('--iteration', required=True)
    a = p.parse_args()
    cases = read(ROOT / 'TABLE_V3_DEV_TRUTH.json')['cases']
    pred, results = evaluate(a.run, cases)
    audit = rule_audit(cases, results)
    metrics = score(pred, {c['case_id']: c['answer'] for c in cases})
    out = ROOT / 'iterations' / a.iteration
    immutable(out / 'predictions.json', pred)
    immutable(out / 'score.json', metrics)
    immutable(out / 'rule_audit.json', audit)
    immutable(out / 'policy.json', {'promoted_rules': sorted(k for k, v in audit.items() if v['promoted'])})
    print(metrics)
    for k, v in audit.items():
        print(k, {x: v[x] for x in ('denominator', 'correct', 'precision', 'documents', 'promoted')})


if __name__ == '__main__':
    main()
