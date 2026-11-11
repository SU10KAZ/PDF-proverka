"""Exact recorded model inputs through the general grouping phase and Excel."""
import asyncio
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_merge.group_workflow_phase import run_grouping_phase
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS

ROOT=Path('/tmp/auditmanager_ak_group_phase_replay_20261111')
ORIGINAL=Path('/tmp/auditmanager_ak_group_workflow_control_20261111')
PAIR=Path('/tmp/auditmanager_grouping_scope_pair_20261111/AK_00')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AK/versions/v001')

async def main():
    ROOT.mkdir(exist_ok=True);document=json.loads((ORIGINAL/'input.json').read_text())
    folders=[ORIGINAL/'workflow/group_proposal',PAIR]
    answers={evidence_digest(json.loads((p/'request.json').read_text())):json.loads((p/'result.json').read_text()) for p in folders}
    calls=[]
    async def replay(request,phase):
        digest=evidence_digest(request)
        if digest not in answers:raise ValueError('Request differs from real recorded input')
        calls.append({'phase':phase,'request_sha256':digest});return answers[digest]
    path=ROOT/'03_findings.json'
    if not path.exists():path.write_text(json.dumps(document,ensure_ascii=False,indent=2)+'\n')
    options=dict(invoke=replay,executor_id='exact-recorded-AK-group-control',max_groups=2)
    first=await run_grouping_phase(ROOT,BASE/'02_work/document.md',BASE/'03_analysis/runs/control/blocks_stage02_100',**options)
    assert first['applied']+first['already_applied']==1 and not first['needs_context']
    sealed=path.read_bytes();after=json.loads(sealed)
    assert after['findings']==json.loads((PAIR/'output_preview.json').read_text())['findings']
    grouped,=after['findings'];assert grouped['grouping_original_findings']==document['findings']
    second=await run_grouping_phase(ROOT,BASE/'02_work/document.md',BASE/'03_analysis/runs/control/blocks_stage02_100',**options)
    assert second['already_applied']==1 and path.read_bytes()==sealed
    wb=Workbook();build_project_sheet(wb,{'project_id':'AK','sheet_name':'AK','findings_json':after});wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');columns={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    row,=[r for r in wb['AK'].values if r[columns['finding_id']]==grouped['id']]
    for member in document['findings']:
        for field in ('problem','description','solution','risk','norm'):
            if member.get(field):assert str(member[field]) in row[columns['description']]
    wb.close()
    report={'status':'general_grouping_phase_and_excel_passed','input_candidates':5,'output_rows':1,
        'all_five_original_members_exactly_preserved':True,'member_claims_actions_risks_norms_visible':True,
        'new_model_calls':0,'exact_recorded_requests':list(answers),'calls_this_replay':calls,'rerun_idempotent':True,
        'inherited_member_issues_preserved_in_review':grouped['grouping_provenance']['decision']['source_review'].get('inherited_member_issues',[]),
        'whole_project_discovery':False,'full_stage4_acceptance':False,
        'boundary':'Five selected real model candidates, without normalized shared titles; no assertion that all member engineering facts or routing are independently certified.',
        'output_sha256':hashlib.sha256(sealed).hexdigest(),'xlsx_sha256':hashlib.sha256((ROOT/'report.xlsx').read_bytes()).hexdigest()}
    out=Path(__file__).parent/'AK_automatic_group_phase_replay.json'
    if not out.exists():out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='inherited_member_issues_preserved_in_review'},ensure_ascii=False))

if __name__=='__main__':asyncio.run(main())
