"""Known AR omission and contract boundaries; selection is supplied, not model-tested."""
from copy import deepcopy
import json
from pathlib import Path
import pytest
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions

BASE = Path(__file__).resolve().parents[1]/'experiments/auditmanager_stage4_20261110'

@pytest.fixture
def case():
    entry = json.loads((BASE/'ar_rope_current_release_evidence/adjudication/candidate.json').read_text())['entry']
    md = json.loads((BASE/'ar_rope_bundle_source_evidence/bundle.json').read_text())['md']
    original = deepcopy(entry['queue']['packets'][0]['finding_snapshot'])
    entry['decision']['context_reference_obligations'] = [{
        'claim_id':'C8','evidence_index':0,'reference':'КМ8','role':'calculation_context',
        'reason':'The accepted question requests calculation context explicitly addressed by this source.'}]
    doc = {'findings':[original,{'id':'untouched','problem':'Independent useful finding'}]}
    return doc,entry,md


def test_real_omission_preserves_original_and_all_other_findings(case):
    doc,entry,md=case;before=deepcopy(doc)
    out,receipts=apply_review_admissions(doc,[entry],md)
    assert out==before==doc
    assert receipts==[{'finding_id':entry['response']['finding_id'],'status':'not_applied',
                      'reason':'context_reference_missing_from_final_wording'}]
    assert 'source_review_history' not in out and 'suppressed_findings' not in out


@pytest.mark.parametrize('reference',['КМ8','КМ 8'])
def test_retained_reference_does_not_require_claiming_calculation_is_verified(case,reference):
    doc,entry,md=case
    entry['decision']['approved_fields']['solution'] += f' Проверить применимый расчёт по существующей ссылке {reference}.'
    out,receipts=apply_review_admissions(doc,[entry],md)
    assert receipts[0]['status']=='applied'
    assert out['findings'][0]['report_section']=='questions'
    assert out['findings'][1]==doc['findings'][1]
    review=next(c for c in out['source_review_history'][0]['decision']['claim_reviews'] if c['claim_id']=='C8')
    assert review['verdict']=='needs_data'


@pytest.mark.parametrize('reference',['КМ80','КМ8.1','КМ8 . 1','КМ9.2'])
def test_another_document_cannot_replace_the_required_context_address(case,reference):
    doc,entry,md=case
    entry['decision']['approved_fields']['solution'] += f' Проверить {reference}.'
    out,receipts=apply_review_admissions(doc,[entry],md)
    assert out==doc and receipts[0]['reason']=='context_reference_missing_from_final_wording'


@pytest.mark.parametrize('bad',['unknown_claim','wrong_index','wrong_reference','wrong_role','duplicate'])
def test_bad_obligation_preserves_finding(case,bad):
    doc,entry,md=case;rows=entry['decision']['context_reference_obligations'];row=rows[0]
    if bad=='unknown_claim':row['claim_id']='unknown'
    if bad=='wrong_index':row['evidence_index']=999
    if bad=='wrong_reference':row['reference']='КМ99'
    if bad=='wrong_role':row['role']='confirmed_calculation'
    if bad=='duplicate':
        entry['decision']['approved_fields']['solution'] += ' Проверить КМ8.'
        rows.append(deepcopy(row))
    out,receipts=apply_review_admissions(doc,[entry],md)
    assert out==doc and receipts[0]['status']=='not_applied'


def test_legacy_decisions_do_not_invent_obligations(case):
    doc,entry,md=case;entry['decision'].pop('context_reference_obligations')
    _,receipts=apply_review_admissions(doc,[entry],md)
    assert receipts[0]['status']=='applied'


def test_final_release_stops_before_provider_when_reference_is_missing(case,tmp_path):
    import asyncio
    from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_check
    doc,entry,md=case;before=deepcopy(doc)
    async def forbidden(request,phase):
        pytest.fail('Missing required reference must be caught before model dispatch')
    with pytest.raises(ValueError,match='invalid_proposed_edit: context_reference_missing_from_final_wording'):
        asyncio.run(run_final_release_check(entry,doc,md,tmp_path/'release',invoke=forbidden,executor_id='test-no-dispatch'))
    assert doc==before
    assert not (tmp_path/'release'/'attempt.json').exists()


def test_admitted_context_reference_remains_visible_in_excel(case,tmp_path):
    from openpyxl import Workbook,load_workbook
    from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet
    doc,entry,md=case
    entry['decision']['approved_fields']['solution']+=' Проверить применимый расчёт по существующей ссылке КМ8.'
    out,receipts=apply_review_admissions(doc,[entry],md)
    assert receipts[0]['status']=='applied'
    wb=Workbook();build_project_sheet(wb,{'project_id':'AR','sheet_name':'AR','findings_json':out})
    path=tmp_path/'report.xlsx';wb.save(path);wb.close()
    wb=load_workbook(path,read_only=True)
    rows=list(wb['AR'].values)
    positions=[i for i,row in enumerate(rows) if entry['response']['finding_id'] in row]
    assert len(positions)==1
    heading=next(i for i,row in enumerate(rows) if 'Вопросы и уточнения' in row)
    assert positions[0]>heading
    assert entry['decision']['approved_fields']['solution'] in rows[positions[0]]
    assert any('untouched' in row for row in rows)
    wb.close()
