"""Replay actual candidate adjudication; contract success is not semantic acceptance."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet
from experiments.auditmanager_stage4_20261110.replay_integrated_admission import BASE, HERE
from experiments.auditmanager_stage4_20261110.run_itp_semantic_adjudication import ROOT
from experiments.auditmanager_stage4_20261110.run_itp_claude_followup import ROOT as PREVIOUS

async def main():
    result=json.loads((ROOT/'result.json').read_text())
    assert not result['is_error']
    initial=Path('/tmp/auditmanager_itp_initial_context_astra_20261110')
    queue=json.loads((initial/'followup.json').read_text())['expanded_queue']
    finding=json.loads((initial/'finding.json').read_text())
    request=json.loads((PREVIOUS/'request.json').read_text())
    supplied={i['block_id']:i['sha256'] for i in request['image_bindings']}
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model']=supplied.get(receipt['block_id'])==receipt['sha256']
    response=json.loads((PREVIOUS/'result.json').read_text())['json_data']
    entry={'queue':queue,'response':response,'decision':result['json_data']}
    p=next(p for p in json.loads((BASE/'input_manifest.json').read_text()) if p['discipline']=='ITP')
    source=Path(p['output_dir']);out=Path(tempfile.mkdtemp(prefix='auditmanager_itp_adjudication_replay_'))
    (out/'02_work').mkdir();(out/'02_work/document.md').write_bytes((Path(p['version_dir'])/'02_work/document.md').read_bytes())
    for name in ('01_blocks_analysis.json','document_graph.json','02_text_analysis.json'):
        (out/name).write_bytes((source/name).read_bytes())
    document=json.loads((source/'03_findings.json').read_text());before=deepcopy(document)
    document['findings']=[finding if f['id']=='F-042' else f for f in document['findings']]
    (out/'03_findings.json').write_text(json.dumps(document,ensure_ascii=False))
    (out/'03_source_review_admissions.json').write_text(json.dumps([entry],ensure_ascii=False))
    ctx=SimpleNamespace(project_id='ITP',output_dir=out,project_info={},is_cancelled=None,log=AsyncMock(),update_pipeline_log=lambda *a,**kw:None)
    with patch('backend.app.pipeline.stages.findings_verify.runner._run_absence_phase',return_value={'candidates':0,'verified':False}):
        verified=await run_findings_verify(ctx)
    receipts=json.loads((out/'03_source_review_admission_results.json').read_text())
    assert verified.source_review_applied==1,receipts
    final=json.loads((out/'03_findings.json').read_text());current={f['id']:f for f in final['findings']}
    assert len(current)==53
    for f in before['findings']:
        if f['id']=='F-042':continue
        for k in ('problem','description','solution','severity'):assert current[f['id']].get(k)==f.get(k)
    assert all(current['F-042'][k]==v for k,v in entry['decision']['approved_fields'].items())
    wb=Workbook();build_project_sheet(wb,{'project_id':'ITP','sheet_name':'ITP','findings_json':final});wb.save(out/'report.xlsx');wb.close()
    wb=load_workbook(out/'report.xlsx');values=[v for row in wb['ITP'].values for v in row];wb.close()
    assert values.count('F-042')==1
    for value in entry['decision']['approved_fields'].values():assert any(isinstance(cell,str) and value in cell for cell in values)
    assessment={'status':'partial_pass','model':result['model'],'duration_ms':result['duration_ms'],
        'input_tokens':result['input_tokens'],'output_tokens':result['output_tokens'],'cost_source':result['cost_source'],
        'contract_admission_passed':True,'actual_verifier_and_excel_passed':True,'admission_receipts':receipts,
        'supported_annotation_conflicts_preserved':['K22 1000/1500 litres','K23 100/200 litres'],
        'C5_scope_error_corrected':True,'physical_defect_not_claimed':True,'other_findings_preserved':52,
        'semantic_acceptance':'Not complete: K23 calculation exists in source corpus but was omitted by bounded retrieval. Final recommendation still asks to confirm volume; must retrieve calculation before delegating this to engineer.',
        'remaining_output_issue':'Admission revises only problem/description/solution. Original consequence and normative candidate remain in Excel; full substantive-field consistency is not established.',
        'remaining_source_block':'blk_7e913f6c32cb4db7aa9d9b8688b6e979',
        'known_case_not_blind':True,'absence_model_phase':'Explicit no-op in replay',
        'production_changed':False,'automatic_model_dispatch_integrated':False,'output_dir':str(out)}
    for name in ('result','attempt','manifest'):
        (HERE/f'ITP_semantic_adjudication_{name}.json').write_bytes((ROOT/f'{name}.json').read_bytes())
    (HERE/'ITP_semantic_adjudication_assessment.json').write_text(json.dumps(assessment,ensure_ascii=False,indent=2)+'\n')
    readiness_path=HERE/'stage4_readiness.json';readiness=json.loads(readiness_path.read_text())
    readiness['itp_semantic_adjudication_live_control']=assessment
    readiness['integrated_source_review_admission']['automatic_semantic_decision_producer']='Live Claude decision completed; actual admission and Excel replay pass. Semantic acceptance partial because K23 retrieval remains incomplete.'
    readiness['stage4_next_gate_audit']['immediate_next_action']='Complete missing-context retrieval and final-wording checks, then close remaining H01-H07 controls before candidate freeze and full comparison.'
    readiness_path.write_text(json.dumps(readiness,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(assessment,ensure_ascii=False,indent=2))

if __name__=='__main__':asyncio.run(main())
