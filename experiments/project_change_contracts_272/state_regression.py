"""Source-audit fixtures are data; the comparability engine knows no case IDs."""
from dataclasses import replace
from pathlib import Path
import json

from .states import EngineeringState, StateTransition
from .comparability import compare, materiality, evaluate_typed_claim

FIXTURES = Path(__file__).with_name('typed_state_cases.json')


def evaluate_fixture(case, packet=None):
    fixtures = json.loads(FIXTURES.read_text())['cases']
    data = next(r for r in fixtures if (r['pair_index'],r['case_id']) == (case['pair_index'],case['case_id']))
    t = StateTransition(old=EngineeringState(**data['old']), new=EngineeringState(**data['new']),
                        **{k:data[k] for k in ['identity_basis','mapping_basis','source_conflict','state_support']})
    comparison = compare(t)
    significance = materiality(t, comparison)
    passed = comparison['status'] == data['expected_comparability'] and significance['status'] == data['expected_materiality']
    variants = []
    for spec in data['variants']:
        changed = replace(t, old=replace(t.old,**spec.get('old_changes',{})),
                          new=replace(t.new,**spec.get('new_changes',{})))
        c, m = compare(changed), materiality(changed)
        ok = c['status'] == spec['expected_comparability'] and m['status'] == spec['expected_materiality']
        passed = passed and ok
        variants.append(dict(name=spec['name'], comparability=c, materiality=m, passed=ok))
    result = dict(typed_state=dict(old=t.old.to_dict(),new=t.new.to_dict()),
        applicable_conditions=comparison['conditions'], comparability=comparison,
        materiality_rule=significance, expected_semantic_disposition=data['expected_semantic_disposition'],
        typed_fixture_basis='SOURCE_AUDIT_INPUTS_NOT_NEW_MODEL_OUTPUT',
        f2_required=case['required_f2'], f2_regression_pass=passed, adversarial_variants=variants)
    if packet is not None:
        bound = replace(t, **{side:replace(getattr(t,side), evidence_ids=tuple(
            e['evidence_id'] for e in packet['evidence'] if e['side']==side.upper())) for side in ('old','new')})
        result['package_typed_diagnostic'] = evaluate_typed_claim(packet, bound)
    return result
