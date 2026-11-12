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
