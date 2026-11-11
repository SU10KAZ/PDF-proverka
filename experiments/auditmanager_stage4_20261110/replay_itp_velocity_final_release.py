"""Replay the exact recorded final check through the runtime phase and Excel."""
import asyncio
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook,load_workbook
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_phase
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS
SOURCE=Path('/tmp/auditmanager_itp_pipe_velocity_shared_profile_release_20261111')
ROOT=Path('/tmp/auditmanager_itp_velocity_final_phase_replay_20261111')
CANDIDATE=Path('/tmp/auditmanager_itp_pipe_velocity_bounded_review_20261111/workflow/candidate.json')
MD=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003/02_work/document.md')

async def main():
    ROOT.mkdir(exist_ok=True)
    document=json.loads((SOURCE/'input_document.json').read_text())
    candidate=json.loads(CANDIDATE.read_text());fid=candidate['entry']['response']['finding_id']
    request=json.loads((SOURCE/'workflow/initial/request.json').read_text())
    result=json.loads((SOURCE/'workflow/initial/result.json').read_text());calls=[]
    async def replay(wire,phase):
        assert evidence_digest(wire)==evidence_digest(request),'Request changed'
        calls.append(phase);return result
    path=ROOT/'03_findings.json'
    if not path.exists():immutable(path,document)
    immutable(ROOT/'03_source_review_candidates.json',{'status':'finished','reports':[candidate],'not_attempted':0})
    options={'invoke':replay,'executor_id':'exact-recorded-ITP-final-control','max_findings':1,'complete_citations':True}
    first=await run_final_release_phase(ROOT,MD,**options)
    assert first['applied']+first['already_applied']==1 and not first['held'] and not first['not_completed']
    sealed=path.read_bytes();after=json.loads(sealed)
    assert [f for f in after['findings'] if f['id']!=fid]==[f for f in document['findings'] if f['id']!=fid]
    assert after['findings']==json.loads((SOURCE/'output_preview.json').read_text())['findings']
    second=await run_final_release_phase(ROOT,MD,**options)
    assert second['already_applied']==1 and path.read_bytes()==sealed
    wb=Workbook();build_project_sheet(wb,{'project_id':'ITP','sheet_name':'ITP','findings_json':after});wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');columns={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    rows={r[columns['finding_id']]:r for r in wb['ITP'].values if r[columns['finding_id']] in {f['id'] for f in after['findings']}}
    assert len(rows)==len(after['findings'])==54
    for finding in after['findings']:
        for field in ('problem','description','solution'):
            if finding.get(field):assert finding[field] in rows[finding['id']][columns[field]]
    wb.close()
    summary={'status':'runtime_final_release_and_excel_passed','baseline_rows':53,'output_rows':54,
       'all_baseline_findings_unchanged':True,'all_visible_claim_fields_checked':True,'rerun_idempotent':True,
       'new_model_calls':0,'recorded_calls_replayed':calls,'original_wall_control_recovered':False,
       'whole_audit_discovery':False,'full_stage4_acceptance':False,
       'output_sha256':hashlib.sha256(sealed).hexdigest(),'xlsx_sha256':hashlib.sha256((ROOT/'report.xlsx').read_bytes()).hexdigest()}
    (ROOT/'assessment.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n');print(json.dumps(summary))

if __name__=='__main__':asyncio.run(main())
