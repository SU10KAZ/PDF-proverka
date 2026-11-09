from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks,reconcile_requirement_checks
from copy import deepcopy
import pytest


def fixture():
    requirement={'id':'r','source_kind':'project_requirement','document':'TZ','revision':'1','requirement_text':'Only heating',
                 'scope':'heating','stage':'RD','execution':'specified coupling','exceptions':'none stated',
                 'source_evidence':[{'block_id':'b','quote':'Only heating'}]}
    check={'requirement_id':'r','object_id':'o','applicability':'applies','reason':'scope match',
           'scope_evidence':[{'block_id':'b','quote':'heating circuit'}]}
    return {'requirement_registry':[requirement],'object_registry':[{'id':'o'}],'requirement_applicability_checks':[check]}


def test_quotes_and_scope_are_required_but_not_semantic_proof():
    data=fixture();blocks={'b':'Only heating; heating circuit'}
    result=reconcile_requirement_checks(data,blocks)
    assert result['checks'][0]['status']=='reported_applies'
    assert not result['semantic_verified']
    data['requirement_registry'][0]['revision']='unknown'
    assert reconcile_requirement_checks(data,blocks)['checks'][0]['status']=='needs_data'


def test_wrong_quote_and_unverified_norm_cannot_close_applicability():
    data=fixture()
    assert reconcile_requirement_checks(data,{'b':'other text'})['unresolved']==1
    data['requirement_registry'][0]['source_kind']='normative_candidate'
    row=reconcile_requirement_checks(data,{'b':'Only heating; heating circuit'})['checks'][0]
    assert row['reason']=='norm_resolver_verification_required'


def test_graphic_summaries_and_duplicate_ids_are_not_authoritative_text():
    md='## Page 1\n### BLOCK #1 [TEXT]: b\nOnly heating\n### BLOCK #2 [GRAPHIC]: g\nHeating summary\n'
    blocks=authoritative_text_blocks(md)
    assert list(blocks)==['b']
    assert 'Heating summary' not in blocks['b']
    md+='### BLOCK #3 [TEXT]: b\nDifferent quote\n'
    assert authoritative_text_blocks(md)=={}


@pytest.mark.parametrize('second_verdict',['applies','does_not_apply','unknown'])
def test_repeated_object_requirement_pair_remains_unresolved(second_verdict):
    data=fixture()
    first=data['requirement_applicability_checks'][0]
    data['requirement_applicability_checks'].append({**first,'applicability':second_verdict})
    before=deepcopy(data)
    result=reconcile_requirement_checks(data,{'b':'Only heating; heating circuit'})
    assert result['unresolved']==2
    assert all(row['status']=='not_checked' for row in result['checks'])
    assert all(row['reason']=='duplicate_or_conflicting_applicability_checks' for row in result['checks'])
    assert [row['receipt']['applicability'] for row in result['checks']]==['applies',second_verdict]
    assert data==before


def test_same_requirement_for_distinct_objects_keeps_each_receipt():
    data=fixture()
    data['object_registry'].append({'id':'other'})
    first=data['requirement_applicability_checks'][0]
    data['requirement_applicability_checks'].append({**first,'object_id':'other','applicability':'does_not_apply'})
    result=reconcile_requirement_checks(data,{'b':'Only heating; heating circuit'})
    assert [row['status'] for row in result['checks']]==['reported_applies','reported_does_not_apply']
    assert result['unresolved']==0
    assert not result['semantic_verified']
