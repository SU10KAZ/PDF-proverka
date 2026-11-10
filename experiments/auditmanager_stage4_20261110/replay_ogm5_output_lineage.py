"""Actual OGM5 candidates through merge, verifier, review queue and saved Excel."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from openpyxl import Workbook, load_workbook

from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS


def main():
    root=Path('/tmp/auditmanager_km_ogm5_continuation_20261110')
    request=json.loads((root/'request.json').read_text())
    response=json.loads((root/'result.json').read_text())['json_data']
    validated=validate_object_operation_payload(SimpleNamespace(messages=request['messages']),response)
    assert len(validated['findings'])==2 and len(validated['operation_pending'])==1
    source=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/03_analysis/runs/control')
    original=json.loads((source/'03_findings.json').read_text())
    candidate=combine_findings_with_targeted(deepcopy(original),[('object_operations_ogm5_expanded',validated)])
    outputs=[]
    for data in (original,candidate):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            for name in ('01_blocks_analysis.json','document_graph.json'):
                (out/name).write_bytes((source/name).read_bytes())
            (out/'03_findings.json').write_text(json.dumps(data,ensure_ascii=False))
            stats=_run_deterministic_phase(out,'KM-OGM5-lineage')
            assert not stats['critic_error'] and not stats['corrector_error']
            outputs.append(json.loads((out/'03_findings.json').read_text()))
    before,after=outputs
    assert after['findings'][:len(before['findings'])]==before['findings']
    additions=after['findings'][len(before['findings']):]
    assert len(additions)==2
    for raw,final in zip(validated['findings'],additions,strict=True):
        for key in ('problem','description','solution','report_section','source_finding_ids','operation_candidate_lineage'):
            assert raw[key]==final[key],key
        assert final['source_evidence_review_status']=='pending'
    md=(source.parent.parent.parent/'02_work/document.md').read_text()
    queue=build_source_review_queue(additions,md)
    assert queue['pending_count']==2
    accounting=build_candidate_coverage({}, {}, after)
    operations=[row for row in accounting['candidates'] if row['origin']=='object_operation']
    assert len(operations)==3
    assert sum(r['status']=='linked_requires_semantic_review' for r in operations)==2
    assert sum(r['status']=='operation_pending_requires_review' for r in operations)==1
    dest=Path('/tmp/auditmanager_km_ogm5_output_lineage_20261110');dest.mkdir(exist_ok=True)
    wb=Workbook();build_project_sheet(wb,{'project_id':'KM-OGM5','sheet_name':'KM','findings_json':after})
    path=dest/'KM_candidate_questions.xlsx';wb.save(path);wb.close()
    wb=load_workbook(path);rows=list(wb['KM'].values);wb.close()
    col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    for f in additions:
        matching=[row for row in rows if row[col]==f['id']]
        assert len(matching)==1 and f['problem'] in matching[0]
    pending_problem=validated['operation_pending'][0]['finding']['problem']
    assert not any(pending_problem in row for row in rows)
    report={'baseline_findings_preserved':len(before['findings']),'final_findings':len(after['findings']),
        'new_question_candidates':len(additions),'source_reviews_pending':queue['pending_count'],
        'internal_context_candidate_preserved':True,'internal_context_candidate_exported':False,
        'candidate_accounting':operations,'source_review_queue':queue,
        'excel_path':str(path),'excel_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'new_model_calls':0,'model_semantic_and_absence_passes_executed':False,'production_accepted':False,
        'limits':['Raw model severity and non-verbatim quotes still need final display/wording review.',
            'This checks candidate transport and review scheduling, not production admission or six-project quality.']}
    Path(__file__).with_name('KM_OGM5_output_lineage_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps({k:report[k] for k in ('baseline_findings_preserved','final_findings','new_question_candidates','source_reviews_pending','internal_context_candidate_exported')}))


if __name__=='__main__':main()
