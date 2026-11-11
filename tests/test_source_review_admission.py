from copy import deepcopy
import json
from openpyxl import Workbook
import pytest
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions,run_review_admission_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS


def case(action='suppress',verdict='refuted'):
    md='### BLOCK #1 [TEXT]: t\nPublished total 12; rows sum 12.\n'
    finding={'id':'F1','problem':'Wrong total','description':'Mismatch','solution':'Correct total',
        'source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t']}]}
    queue=build_source_review_queue([finding],md)
    response={'finding_id':'F1','finding_sha256':queue['packets'][0]['finding_sha256'],
        'source_md_sha256':queue['source_md_sha256'],'claims':[{'claim_id':'C1','claim':'Mismatch',
        'status':verdict,'reason':'Compare rows and total','evidence':[{'block_id':'t','quote':'rows sum 12'}]}]}
    if verdict=='needs_data':response['claims'][0]['missing_data']=['Object mapping']
    decision={'response_sha256':evidence_digest(response),'origin':'explicit_source_review','reviewer':'local_control',
        'reason':'Source comparison completed','action':action,'claim_reviews':[{'claim_id':'C1','role':'defect_basis',
        'verdict':verdict,'same_scope_verified':True,'evidence_sufficiency_verified':True,'reason':'Same table total checked'}]}
    return {'findings':[finding,{'id':'F2','problem':'Untouched useful finding'}]},[{'response':response,'queue':queue,'decision':decision}],md


def test_false_finding_archived_and_useful_finding_survives_excel():
    doc,entries,md=case();before=deepcopy(doc)
    out,receipts=apply_review_admissions(doc,entries,md)
    assert doc==before and receipts[0]['status']=='applied'
    assert out['findings']==[before['findings'][1]]
    assert out['suppressed_findings']==[before['findings'][0]]
    wb=Workbook();build_project_sheet(wb,{'project_id':'test','sheet_name':'test','findings_json':out})
    col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    ids=[row[col] for row in wb['test'].values]
    assert 'F2' in ids and 'F1' not in ids


@pytest.mark.parametrize('bad',['stale','scope','coverage','supported','duplicate'])
def test_unsafe_suppression_preserves_original(bad):
    doc,entries,md=case();decision=entries[0]['decision']
    if bad=='stale':decision['response_sha256']='old'
    if bad=='scope':decision['claim_reviews'][0]['same_scope_verified']=False
    if bad=='coverage':decision['claim_reviews']=[]
    if bad=='supported':decision['claim_reviews'][0]['verdict']='supported'
    if bad=='duplicate':entries.append(deepcopy(entries[0]))
    out,receipts=apply_review_admissions(doc,entries,md)
    assert out==doc and all(r['status']=='not_applied' for r in receipts)


def test_needs_data_stays_internal_without_engineer_action():
    doc,entries,md=case('defer','needs_data')
    out,receipts=apply_review_admissions(doc,entries,md)
    assert receipts[0]['status']=='applied'
    assert out['pending_source_findings']==[doc['findings'][0]]
    assert out['findings']==[doc['findings'][1]]


def test_supported_core_cannot_be_deferred():
    doc,entries,md=case('defer','supported')
    out,receipts=apply_review_admissions(doc,entries,md)
    assert out==doc and receipts[0]['status']=='not_applied'


def test_question_requires_exhausted_context_then_routes_separately():
    doc,entries,md=case('question','needs_data');d=entries[0]['decision']
    d.update(approved_fields={'problem':'Confirm object identity','description':'Two unresolved identities','solution':'Specify matching object'},final_wording_verified=True)
    out,_=apply_review_admissions(doc,entries,md);assert out==doc
    d.update(context_search_completed=True,engineer_action_required=True)
    out,receipts=apply_review_admissions(doc,entries,md)
    assert receipts[0]['status']=='applied' and out['findings'][0]['report_section']=='questions'


def test_phase_without_inbox_never_mutates_legacy_output(tmp_path):
    doc,_,md=case();path=tmp_path/'03_findings.json';path.write_text(json.dumps(doc));before=path.read_bytes()
    assert run_review_admission_phase(tmp_path,md)['skipped']
    assert path.read_bytes()==before


def test_phase_records_decisions_and_source_changes_prevent_application(tmp_path):
    doc,entries,md=case();path=tmp_path/'03_findings.json';path.write_text(json.dumps(doc))
    (tmp_path/'03_source_review_admissions.json').write_text(json.dumps(entries))
    assert run_review_admission_phase(tmp_path,md+'changed')['not_applied']==1
    assert json.loads(path.read_text())==doc
    assert run_review_admission_phase(tmp_path,md)['applied']==1
    assert len(json.loads(path.read_text())['source_review_history'])==1


def test_confirmed_minor_issue_requires_explicit_profile_decision():
    doc,entries,md=case('question','supported');d=entries[0]['decision']
    d.update(approved_fields={'problem':'Confirmed minor issue','description':'Fact preserved','solution':'Clarify on next revision'},final_wording_verified=True)
    out,_=apply_review_admissions(doc,entries,md);assert out==doc
    d.update(question_basis='minor_confirmed',profile_reason='Confirmed formatting issue without technical impact; separate user-selected section.')
    out,receipts=apply_review_admissions(doc,entries,md)
    assert receipts[0]['status']=='applied'
    assert out['findings'][0]['report_section']=='questions'
    assert out['source_review_history'][0]['decision']['claim_reviews'][0]['verdict']=='supported'


def test_retain_cannot_leave_unverified_context_in_original_wording():
    doc,entries,md=case('retain','supported');e=entries[0]
    e['response']['claims'].append({'claim_id':'C2','claim':'Original description says cause is X','status':'needs_data',
        'reason':'Cause missing','missing_data':['Calculation'],'evidence':[]})
    e['decision']['response_sha256']=evidence_digest(e['response'])
    e['decision']['claim_reviews'].append({'claim_id':'C2','role':'context','verdict':'needs_data','reason':'Cause is unverified'})
    out,receipts=apply_review_admissions(doc,entries,md)
    assert out==doc and receipts[0]['status']=='not_applied'


def display_case():
    doc,entries,md=case('revise','supported');f=doc['findings'][0];e=entries[0]
    f.update(risk='Equipment will fail',norm='Unverified standard',severity='ЭКСПЛУАТАЦИОННОЕ')
    e['queue']=build_source_review_queue([f],md)
    e['response']['finding_sha256']=e['queue']['packets'][0]['finding_sha256']
    e['decision'].update(response_sha256=evidence_digest(e['response']),
        approved_fields={'problem':'Conflicting labels','description':'Two source labels differ','solution':'Reconcile labels'},final_wording_verified=True)
    return doc,entries,md


def test_revision_cannot_silently_preserve_unreviewed_risk_and_norm():
    doc,entries,md=display_case()
    out,receipts=apply_review_admissions(doc,entries,md)
    assert out==doc and receipts[0]['reason']=='incomplete_display_field_review'


def test_explicit_display_revision_reaches_excel_and_preserves_original():
    doc,entries,md=display_case();f=doc['findings'][0]
    values={'risk':'Risk of ambiguous selection; physical failure not established','norm':'','severity':f['severity']}
    entries[0]['decision']['display_field_reviews']={k:{'original_value_sha256':evidence_digest(f[k]),
        'consistent_with_final_claim':True,'reason':'Only source label disagreement established','value':v} for k,v in values.items()}
    out,receipts=apply_review_admissions(doc,entries,md)
    assert receipts[0]['status']=='applied'
    assert out['source_review_history'][0]['original_finding']['risk']=='Equipment will fail'
    wb=Workbook();build_project_sheet(wb,{'project_id':'test','sheet_name':'test','findings_json':out})
    cells=[cell for row in wb['test'].values for cell in row]
    assert values['risk'] in cells and 'Equipment will fail' not in cells and 'Unverified standard' not in cells
    entries[0]['decision']['display_field_reviews']['risk']['original_value_sha256']='stale'
    unchanged,receipts=apply_review_admissions(doc,entries,md)
    assert unchanged==doc and receipts[0]['reason']=='stale_display_field_review'


def test_array_display_reviews_require_unique_original_hash_not_order():
    doc,entries,md=display_case();f=doc['findings'][0]
    rows=[{'original_value_sha256':evidence_digest(f[k]),'consistent_with_final_claim':True,
        'reason':'Explicitly reviewed field','value':f[k]} for k in ('norm','severity','risk')]
    entries[0]['decision']['display_field_reviews']=rows
    out,receipts=apply_review_admissions(doc,entries,md)
    assert receipts[0]['status']=='applied'
    assert out['findings'][0]['risk']==f['risk']
    entries[0]['decision']['display_field_reviews']=rows+[deepcopy(rows[0])]
    out,receipts=apply_review_admissions(doc,entries,md)
    assert out==doc and receipts[0]['reason']=='ambiguous_display_field_review'


def derived_case():
    doc,entries,md=case('retain','supported');e=entries[0]
    e['response']['claims'].append({'claim_id':'C2','claim':'Conditional consequence','status':'needs_data',
        'reason':'No direct event observed','missing_data':['Event record'],'evidence':[]})
    e['decision']['response_sha256']=evidence_digest(e['response'])
    e['decision']['claim_reviews'].append({'claim_id':'C2','role':'context','verdict':'supported',
        'same_scope_verified':True,'evidence_sufficiency_verified':True,
        'reason':'Conditional inference from the checked premise, not observed event','supporting_claim_ids':['C1']})
    return doc,entries,md


def test_explicit_derived_context_keeps_evidence_trace():
    doc,entries,md=derived_case();out,r=apply_review_admissions(doc,entries,md)
    assert r[0]['status']=='applied'
    assert out['source_review_history'][0]['decision']['claim_reviews'][1]['supporting_claim_ids']==['C1']


@pytest.mark.parametrize('bad',['self','missing','unresolved','defect','refutation'])
def test_derived_claim_cannot_replace_direct_defect_or_refutation_evidence(bad):
    doc,entries,md=derived_case();reviews=entries[0]['decision']['claim_reviews']
    if bad=='self':reviews[1]['supporting_claim_ids']=['C2']
    if bad=='missing':reviews[1]['supporting_claim_ids']=['C9']
    if bad=='unresolved':reviews[0]['verdict']='needs_data'
    if bad=='defect':reviews[1]['role']='defect_basis'
    if bad=='refutation':reviews[1]['verdict']='refuted'
    out,r=apply_review_admissions(doc,entries,md)
    assert out==doc and r[0]['status']=='not_applied'


def question_case(action='question'):
    doc, entries, md = case(action, 'supported')
    finding = doc['findings'][0]
    finding.update(report_section='questions', question_basis='actionable_source_ambiguity')
    queue = build_source_review_queue([finding], md)
    entries[0]['queue'] = queue
    entries[0]['response']['finding_sha256'] = queue['packets'][0]['finding_sha256']
    d = entries[0]['decision']
    d.update(response_sha256=evidence_digest(entries[0]['response']),
        question_basis='actionable_source_ambiguity', profile_reason='Clarify source applicability',
        context_search_completed=True, engineer_action_required=True,
        context_search_boundary='Published table and associated source notes reviewed; external documents unavailable.',
        decision_affected='Confirm which total applies to this object before using it.',
        approved_fields={'problem':'Clarify applicable total', 'description':'Published total is established; object applicability remains unclear.',
                         'solution':'Confirm applicability to the specified object.'}, final_wording_verified=True)
    return doc, entries, md


@pytest.mark.parametrize('action', ['retain', 'revise', 'question'])
def test_existing_question_does_not_become_technical_finding(action):
    doc, entries, md = question_case(action)
    out, receipts = apply_review_admissions(doc, entries, md)
    assert receipts[0]['status'] == 'applied'
    assert out['findings'][0]['report_section'] == 'questions'
    assert out['findings'][0]['source_evidence_review_status'] == 'reviewed_question'
    assert out['findings'][1] == doc['findings'][1]


@pytest.mark.parametrize('missing', ['context_search_completed', 'engineer_action_required',
    'context_search_boundary', 'decision_affected', 'profile_reason'])
def test_supported_ambiguity_question_requires_explicit_scope_and_action(missing):
    doc, entries, md = question_case()
    entries[0]['decision'].pop(missing)
    out, receipts = apply_review_admissions(doc, entries, md)
    assert out == doc and receipts[0]['status'] == 'not_applied'


def test_ambiguity_exception_cannot_demote_a_supported_technical_defect():
    doc, entries, md = question_case()
    doc['findings'][0]['report_section'] = 'technical_findings'
    queue = build_source_review_queue([doc['findings'][0]], md)
    entries[0]['queue'] = queue
    entries[0]['response']['finding_sha256'] = queue['packets'][0]['finding_sha256']
    entries[0]['decision']['response_sha256'] = evidence_digest(entries[0]['response'])
    out, receipts = apply_review_admissions(doc, entries, md)
    assert out == doc and receipts[0]['status'] == 'not_applied'


@pytest.mark.parametrize('boundary,accepted', [
    ({'scope': 'Local source package', 'actual_review': 'Plans and details compared',
      'limitations': 'Linked calculation not supplied', 'absence_proven': False}, True),
    ({}, False), ({'scope': 'Local package'}, False),
    ({'scope': 'Local package', 'actual_review': [], 'limitations': 'Unknown'}, False),
])
def test_structured_search_boundary_requires_scope_work_and_limits(boundary, accepted):
    doc, entries, md = question_case()
    entries[0]['decision']['context_search_boundary'] = boundary
    out, receipts = apply_review_admissions(doc, entries, md)
    assert (receipts[0]['status'] == 'applied') is accepted
    if not accepted:
        assert out == doc


@pytest.mark.parametrize('sources,review,accepted', [
    (['AR v1 drawings', 'KM v1 and v2 drawings'], 'Compared actual executions separately', True),
    ([], 'Compared drawings', False),
    ([{}], 'Compared drawings', False),
    (['AR v1'], '', False),
])
def test_search_boundary_source_list_requires_explicit_review(sources, review, accepted):
    doc, entries, md = question_case()
    entries[0]['decision']['context_search_boundary'] = {
        'sources': sources, 'actual_search_reviewed': review, 'limitations': 'Only supplied documents reviewed'}
    out, receipts = apply_review_admissions(doc, entries, md)
    assert (receipts[0]['status'] == 'applied') is accepted
    if not accepted:assert out == doc


@pytest.mark.parametrize('severity,accepted', [
    ('Требует устранения до закупки кабеля.', False),
    ('question', False), ('medium', False),
    ('ЭКСПЛУАТАЦИОННОЕ', True), ('CHECK_RELATED', True),
])
def test_review_severity_uses_report_vocabulary(severity, accepted):
    doc, entries, md = display_case()
    f = doc['findings'][0]
    entries[0]['decision']['display_field_reviews'] = {
        k: {'original_value_sha256': evidence_digest(f[k]),
            'consistent_with_final_claim': True, 'reason': 'Reviewed against final claim',
            'value': severity if k == 'severity' else f[k]}
        for k in ('risk', 'norm', 'severity')}
    out, receipts = apply_review_admissions(doc, entries, md)
    if accepted:
        assert receipts[0]['status'] == 'applied'
        assert out['findings'][0]['severity'] == severity
    else:
        assert out == doc
        assert receipts[0]['reason'] == 'invalid_report_severity'


def test_final_citation_rebinding_is_atomic_and_preserves_history():
    doc,entries,md=case('retain','supported')
    entries[0]['decision']['final_evidence_refs']=[{'claim_id':'C1','evidence_index':0}]
    original=deepcopy(doc)
    out,receipts=apply_review_admissions(doc,entries,md)
    assert receipts[0]['status']=='applied'
    assert out['findings'][0]['evidence']==[{'block_id':'t','quote':'rows sum 12'}]
    assert out['source_review_history'][0]['original_finding']==original['findings'][0]
    assert out['findings'][1]==original['findings'][1] and doc==original
    entries[0]['decision']['final_evidence_refs'][0]['evidence_index']=99
    unchanged,receipts=apply_review_admissions(doc,entries,md)
    assert unchanged==original and receipts[0]['status']=='not_applied'
