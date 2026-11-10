"""Actual recovered K-3 question alongside prior AR output through deterministic verification and Excel."""
import hashlib
import json
from pathlib import Path
import shutil
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS

BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AR/versions/v001')
RECOVERY=Path('/tmp/auditmanager_ar_roof_disposition_navigation_v2_20261111')
PRIOR=Path('/tmp/auditmanager_ar_question_output_20261110/03_findings.json')
OUT=Path('/tmp/auditmanager_ar_roof_handoff_v2_output_20261111')


def main():
    OUT.mkdir(exist_ok=True); (OUT/'02_work').mkdir(exist_ok=True)
    source=BASE/'03_analysis/runs/control'
    for name in ('01_blocks_analysis.json','02_text_analysis.json','document_graph.json'):
        shutil.copyfile(source/name,OUT/name)
    shutil.copyfile(BASE/'02_work/document.md',OUT/'02_work/document.md')
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
    baseline=json.loads(PRIOR.read_text())
    from .frozen_operation_request import load_frozen_operation
    from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
    _, request, _ = load_frozen_operation(RECOVERY, BASE/'02_work/document.md')
    raw = json.loads((RECOVERY/'result.json').read_text())
    assert not raw['is_error']
    payload = validate_object_operation_payload(request, raw['json_data'], context_search_exhausted=True)
    (OUT/'validated_with_handoff.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    assert len(payload['findings'])==1 and payload['findings'][0]['report_section']=='questions'
    original=combine_findings_with_targeted(baseline,[('AR_roof_terminal_disposition',payload)])
    question,=[f for f in original['findings'] if f['id'] not in {b['id'] for b in baseline['findings']}]
    assert question['report_section']=='questions'
    (OUT/'03_findings.json').write_text(json.dumps(original,ensure_ascii=False,indent=2))
    stats=_run_deterministic_phase(OUT,'AR-deferred-output-control')
    assert not any(stats.get(k) for k in ('critic_error','corrector_error','source_review_queue_error'))
    after=json.loads((OUT/'03_findings.json').read_text())
    current={f['id']:f for f in after['findings']}
    assert len(current)==len(original['findings'])
    for finding in original['findings']:
        for field in ('problem','finding','description','solution','recommendation','severity','report_section'):
            assert finding.get(field)==current[finding['id']].get(field),(finding['id'],field)
    wb=Workbook();build_project_sheet(wb,{'project_id':'AR','sheet_name':'AR','findings_json':after})
    report=OUT/'report.xlsx';wb.save(report);wb.close()
    saved=load_workbook(report);rows=list(saved['AR'].values)
    column=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    heading=next(i for i,row in enumerate(rows) if 'Вопросы и уточнения' in row)
    positions=[i for i,row in enumerate(rows) if row[column]==question['id']]
    assert len(positions)==1 and positions[0]>heading
    ids=[row[column] for row in rows if row[column] in current]
    assert len(ids)==len(current) and set(ids)==set(current)
    saved.close()
    result={'prior_output_findings_preserved':len(baseline['findings']),'classic_findings_preserved':22,'prior_recovered_questions_preserved':1,'final_rows':len(current),'recovered_question_id':question['id'],
        'question_core_fields_preserved':True,'question_exported_separately':True,
        'all_excel_ids_unique':True,'verifier_stats':stats,
        'source_recovery_sha256':hashlib.sha256((RECOVERY/'result.json').read_bytes()).hexdigest(),
        'handoff_payload_sha256':hashlib.sha256((OUT/'validated_with_handoff.json').read_bytes()).hexdigest(),
        'xlsx_path':str(report),'xlsx_sha256':hashlib.sha256(report.read_bytes()).hexdigest(),
        'new_model_calls':0,'semantic_acceptance':False,
        'boundary':'Real model-produced candidate through deterministic verifier and Excel. Does not test model absence verifier, engineer usefulness or complete pipeline acceptance.'}
    Path(__file__).with_name('AR_roof_handoff_v2_output_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    shutil.copyfile(report,Path(__file__).with_name('AR_roof_handoff_v2_output_control.xlsx'))
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
