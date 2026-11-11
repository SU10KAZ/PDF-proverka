"""Local profile adjudication, explicitly distinct from the blind model decision."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_eom_instance_operations import BASE, immutable
SOURCE=Path('/tmp/auditmanager_eom_f030_readjudication_20261111/candidate.json')
PRIOR=Path('/tmp/auditmanager_eom_f001_question_output_20261111/03_findings.json')
ROOT=Path('/tmp/auditmanager_eom_f030_question_output_20261111')
OUT=Path(__file__).parent

def main():
    ROOT.mkdir(exist_ok=True)
    from decimal import Decimal as D
    values = [D('23.9')/70, D('30.6')/80, D('39.4')/100, D('28.6')/80]
    assert all(v>D('0.1') and v>D('0.25') for v in values)
    candidate=json.loads(SOURCE.read_text());entry=deepcopy(candidate['entry'])
    model_decision=deepcopy(entry['decision'])
    entry['decision'].update(action='question',reviewer='local_explicit_profile_adjudicator',
        question_basis='minor_confirmed',
        profile_reason='Подтверждена ошибка числителей в расчётных таблицах. После пересчёта все четыре результата остаются выше указанного в таблицах порога 0,1 А; непригодность оборудования и потеря точности учёта не установлены. Требуется локальное исправление расчётного обоснования без доказанной необходимости изменения оборудования. Сохранить в отдельном разделе вопросов и уточнений по выбранному пользователем профилю.',
        model_decision_sha256=evidence_digest(model_decision),
        profile_decision_origin='Local evaluation after model result sealed; not autonomous model routing')
    before=json.loads(PRIOR.read_text());document=deepcopy(before)
    snapshot=next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']=='F-030')
    index=next(i for i,f in enumerate(document['findings']) if f['id']=='F-030')
    assert {k:v for k,v in snapshot.items() if k not in {'source_review_requests','source_evidence_review_status'}}==document['findings'][index]
    document['findings'][index]=snapshot
    after,receipts=apply_review_admissions(document,[entry],(BASE/'02_work/document.md').read_text())
    assert receipts==[{'finding_id':'F-030','status':'applied','action':'question'}],receipts
    assert len(after['findings'])==len(before['findings'])==51
    assert [f for f in after['findings'] if f['id']!='F-030']==[f for f in before['findings'] if f['id']!='F-030']
    item=after['findings'][index]
    assert item['report_section']=='questions'
    for key,value in model_decision['approved_fields'].items():assert item[key]==value
    immutable(ROOT/'03_findings.json',after);immutable(ROOT/'profile_entry.json',entry)
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    wb.save(ROOT/'report.xlsx');wb.close();wb=load_workbook(ROOT/'report.xlsx')
    rows=list(wb['EOM'].values);cols={key:i for i,(key,*_) in enumerate(PROJ_COLUMNS)}
    heading=next(i for i,r in enumerate(rows) if 'Вопросы и уточнения' in r)
    positions=[i for i,r in enumerate(rows) if r[cols['finding_id']]=='F-030']
    assert len(positions)==1 and positions[0]>heading
    row=rows[positions[0]]
    for key in ('problem','solution','risk','norm'):assert row[cols[key]]==item[key]
    assert row[cols['description']]==item['description']+'\nНорма: '+item['norm']
    wb.close()
    report={'status':'bounded_local_profile_control','receipts':receipts,'prior_other_findings_preserved':50,
        'final_findings':51,'question_exported_separately':True,'approved_model_wording_unchanged':True,
        'original_archived':True,'model_decision_sha256':evidence_digest(model_decision),
        'source_candidate_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'baseline_sha256':hashlib.sha256(PRIOR.read_bytes()).hexdigest(),
        'autonomous_model_profile_routing':False,'new_model_calls':0,'full_acceptance':False,
        'limitations':['Local profile decision is not proof of general automatic routing.','Actual meter accuracy, equipment suitability and normative threshold applicability remain unestablished.','No independent historical positive-negative pair.']}
    immutable(OUT/'EOM_F030_question_output_replay.json',report)
    immutable(OUT/'EOM_F030_profile_entry.json',entry)
    (OUT/'EOM_F030_question_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
