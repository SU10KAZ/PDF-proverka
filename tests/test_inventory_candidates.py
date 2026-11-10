from backend.app.pipeline.stages.text_analysis.inventory_candidates import inventory_text_candidates
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage


def test_local_ids_become_distinct_candidates_with_original_claim_and_sources():
    raw={'id':'F1','statement':'Level differs by 150 mm','source_refs':['plan','section'],'limitation':'Correct level unknown'}
    packets=[{'packet_id':name,'findings':[raw]} for name in ('inventory_000','inventory_006')]
    result=inventory_text_candidates(packets,{'plan','section'},reserved_ids=['T-010'])
    first,second=result['text_findings']
    assert [first['id'],second['id']]==['T-011','T-012']
    assert first['finding']==raw['statement'] and first['inventory_origin']['raw_finding']==raw
    assert first['related_block_ids']==['plan','section']
    assert not any(k in first for k in ('severity','solution','norm','report_section'))
    final=[{'source_finding_ids':[second['id']]}]
    text_candidate_lineage(final,result,{'plan','section'})
    receipt=final[0]['source_candidate_lineage'][0]
    assert receipt['source_claim']==raw['statement']
    assert receipt['status']=='requires_semantic_review'
    assert receipt['existing_block_ids']==['plan','section']


def test_invalid_claim_is_retained_pending_and_unresolved_source_remains_visible():
    raw={'id':'F1','statement':'Claim','source_refs':['missing']}
    result=inventory_text_candidates([{'packet_id':'p','findings':[raw,{'id':'bad'}]}],set())
    assert result['text_findings'][0]['unresolved_block_ids']==['missing']
    assert result['pending'][0]['raw_finding']=={'id':'bad'}
    assert not result['semantic_verified']


def test_description_only_inventory_claim_retains_uncertainty_and_does_not_invent_verdict():
    raw={'id':'p10_f01','description':'Difference 100 mm; common measurement basis unknown.',
        'status':'needs clarification','source_refs':['drawing','table']}
    result=inventory_text_candidates([{'packet_id':'p10','findings':[raw]}],{'drawing','table'})
    candidate=result['text_findings'][0]
    assert candidate['finding']==raw['description']
    assert candidate['inventory_origin']['claim_field']=='description'
    assert candidate['inventory_origin']['raw_finding']==raw
    assert candidate['requires_semantic_review']
    assert not result['pending']
    malformed={**raw,'statement':None}
    result=inventory_text_candidates([{'packet_id':'p10','findings':[malformed]}],{'drawing','table'})
    assert not result['text_findings'] and result['pending']
