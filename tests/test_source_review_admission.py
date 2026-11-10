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
