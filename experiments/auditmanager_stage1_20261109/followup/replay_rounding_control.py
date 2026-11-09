"""Reproduce mathematical countercontrols, not an AuditManager model run."""
import hashlib
import json
from decimal import Decimal as D, ROUND_HALF_UP
from pathlib import Path

HERE = Path(__file__).resolve().parent
STAGE = HERE.parent


def source(name):
    path = STAGE / 'decisions' / name
    obj = json.loads(path.read_text())
    md = Path(obj['source_md']['path'])
    assert hashlib.sha256(md.read_bytes()).hexdigest() == obj['source_md']['sha256']
    text = md.read_text()
    for e in obj['evidence']:
        assert text[e['start']:e['end']] == e['authoritative_TEXT']
    return obj, {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    negative, nref = source('B-EOM-F-003.json')
    positive, pref = source('B-EOM-F-005.json')
    assert negative['source_md'] == positive['source_md']
    rows = negative['arithmetic']['rows']
    selected = {f'C25{i}' for i in range(1, 12)}
    assert selected <= {r['line'] for r in rows}
    examples = {}
    for col, expected in [('Py', '94.1'), ('Pp', '89.5')]:
        published = [D(r[col].replace(',', '.')) for r in rows]
        synthetic = [D('0.0636') if r['line'] in selected else v
                     for r, v in zip(rows, published)]
        assert all(x.quantize(D('.1'), rounding=ROUND_HALF_UP) == v
                   for x, v in zip(synthetic, published))
        assert sum(synthetic).quantize(D('.1'), rounding=ROUND_HALF_UP) == D(expected)
        examples[col] = {'published_sum': str(sum(published)),
                         'synthetic_sum': str(sum(synthetic)), 'rounded_sum': expected}
    bounds = positive['rounding_bound_check']
    lower = D(bounds['P_interval_kW'][0]) / D(bounds['cos_interval'][1])
    upper_reported = D(bounds['reported_S_interval_kVA'][1])
    assert lower > upper_reported
    result = {
        'control_id': 'PAIR-EOM-ROUNDING-001',
        'negative_evidence': nref, 'positive_evidence': pref,
        'source_md': negative['source_md'],
        'rounding_counterexample': examples,
        'power_bound': {'minimum_possible_S_kVA': str(lower),
                        'maximum_reported_S_kVA': str(upper_reported),
                        'intervals_disjoint': True},
        'assumption': 'Nearest rounding at the displayed precision, not an engineering tolerance.',
        'limits': ['Synthetic loads are not actual project values.',
                   'F003 calculation correctness remains unknown.',
                   'F005 equipment adequacy is not established.',
                   'No model, filter, pipeline or production execution is tested.'],
        'mathematical_checks_passed': True,
        'auditmanager_test_executed': False,
    }
    (HERE / 'rounding_control_replay.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print('PASS: F003 rounding witness; F005 disjoint intervals; exact source bindings. Runtime untested.')


if __name__ == '__main__':
    main()
