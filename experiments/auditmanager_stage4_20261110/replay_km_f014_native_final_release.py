"""Replay the exact recorded final check through the runtime phase and Excel."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook,load_workbook
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_phase
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS
SOURCE=Path('/tmp/auditmanager_km_f014_native_final_release_v2_20261111/native')
ROOT=Path('/tmp/auditmanager_km_f014_native_final_phase_replay_20261111')
CANDIDATE=Path('/tmp/auditmanager_km_f014_native_cross_plan_20261111/workflow/candidate.json')
MD=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/02_work/document.md')

async def main(source_root=SOURCE, output_root=ROOT, candidate_path=CANDIDATE):
    SOURCE, ROOT, CANDIDATE = source_root, output_root, candidate_path
    ROOT.mkdir(exist_ok=True)
    document=json.loads((SOURCE/'input_document.json').read_text())
    candidate=json.loads(CANDIDATE.read_text());fid=candidate['entry']['response']['finding_id']
    assert json.loads((SOURCE/'receipt.json').read_text())['status']=='applied'
    answers={};calls=[]
    for request_path in (SOURCE/'workflow').rglob('request.json'):
        result_path=request_path.with_name('result.json')
        if not result_path.is_file():continue
        wire=json.loads(request_path.read_text());result=json.loads(result_path.read_text())
        binding=json.loads(request_path.with_name('result_binding.json').read_text())
        assert binding=={'request':evidence_digest(wire),'result':evidence_digest(result)}
        answers[evidence_digest(wire)]=result
    assert 1<=len(answers)<=3
    async def replay(wire,phase):
        digest=evidence_digest(wire)
        assert digest in answers,'Request changed'
        calls.append({'phase':phase,'request_sha256':digest});return answers[digest]
    path=ROOT/'03_findings.json'
    if not path.exists():immutable(path,document)
    immutable(ROOT/'03_source_review_candidates.json',{'status':'finished','reports':[candidate],'not_attempted':0})
    options={'invoke':replay,'executor_id':'exact-recorded-KM-F014-final-control','max_findings':1,'complete_citations':True}
    first=await run_final_release_phase(ROOT,MD,**options)
    assert first['applied']+first['already_applied']==1 and not first['held'] and not first['not_completed']
    sealed=path.read_bytes();after=json.loads(sealed)
    assert [f for f in after['findings'] if f['id']!=fid]==[f for f in document['findings'] if f['id']!=fid]
    assert after['findings']==json.loads((SOURCE/'output_preview.json').read_text())['findings']
    second=await run_final_release_phase(ROOT,MD,**options)
    assert second['already_applied']==1 and path.read_bytes()==sealed
    wb=Workbook();build_project_sheet(wb,{'project_id':'KM','sheet_name':'KM','findings_json':after});wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');columns={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    rows={r[columns['finding_id']]:r for r in wb['KM'].values if r[columns['finding_id']] in {f['id'] for f in after['findings']}}
    assert len(rows)==len(after['findings'])==26
    assert len([r for r in wb['KM'].values if r[columns['finding_id']] in rows])==26
    for finding in after['findings']:
        for field in ('problem','description','solution','risk','norm','norm_quote','severity'):
            if finding.get(field):assert finding[field] in rows[finding['id']][columns[field]]
    wb.close()
    summary={'status':'runtime_final_release_and_excel_passed','baseline_rows':26,'output_rows':26,
       'other_25_classic_findings_unchanged':True,'all_visible_claim_fields_checked':True,'rerun_idempotent':True,
       'new_model_calls':0,'recorded_calls_replayed':calls,'known_F014_core_retained_with_revised_wording':True,'autonomous_context_retrieval_proven':False,
       'known_target_snapshot_adapter':True,'whole_audit_discovery':False,'full_stage4_acceptance':False,
       'output_sha256':hashlib.sha256(sealed).hexdigest(),'xlsx_sha256':hashlib.sha256((ROOT/'report.xlsx').read_bytes()).hexdigest()}
    (ROOT/'assessment.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n');print(json.dumps(summary))

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--source-root',type=Path,default=SOURCE)
    p.add_argument('--output-root',type=Path,default=ROOT)
    p.add_argument('--candidate',type=Path,default=CANDIDATE)
    args=p.parse_args()
    asyncio.run(main(args.source_root,args.output_root,args.candidate))
