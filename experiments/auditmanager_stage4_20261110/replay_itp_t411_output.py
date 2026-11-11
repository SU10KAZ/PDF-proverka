"""Admit the actual T411 source-review candidate and check its final Excel row."""
import json
import hashlib
from pathlib import Path
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS
from .run_itp_pipe_operations import BASE,immutable
SOURCE=Path('/tmp/auditmanager_itp_t411_source_review_20261111')
BASELINE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/snapshots/ITP/findings_verify.json')
ROOT=Path('/tmp/auditmanager_itp_t411_output_20261111')
OUT=Path(__file__).parent


def main():
    ROOT.mkdir(exist_ok=True)
    candidate=json.loads((SOURCE/'workflow/candidate.json').read_text())
    finding=json.loads((SOURCE/'finding.json').read_text())
    revised,receipts=apply_review_admissions({'findings':[finding]},[candidate['entry']],(BASE/'02_work/document.md').read_text())
    assert len(receipts)==1 and receipts[0]['status']=='applied' and receipts[0]['action'] in ('retain','revise'),receipts
    item,=revised['findings']
    assert item['report_section']=='technical_findings'
    item['source_review_history']=revised['source_review_history']
    item['source_review_original_finding_id']=finding['id']
    before=json.loads(BASELINE.read_text());after=combine_findings_with_targeted(before,[('T411_source_review',revised)])
    assert after['findings'][:-1]==before['findings']
    assert len(before['findings'])==53 and len(after['findings'])==54
    new=after['findings'][-1]
    assert new['source_review_history']==revised['source_review_history']
    assert set(finding['source_finding_ids'])<=set(new['source_finding_ids'])
    immutable(ROOT/'03_findings.json',after)
    wb=Workbook();build_project_sheet(wb,{'project_id':'ITP','sheet_name':'ITP','findings_json':after});wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');rows=list(wb['ITP'].values);cols={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    selected=[row for row in rows if row[cols['finding_id']]==new['id']];assert len(selected)==1
    for key in ('problem','solution'):assert selected[0][cols[key]]==new[key]
    assert new['description'] in selected[0][cols['description']]
    assert all(value in selected[0][cols['description']] for value in ('2,73','5,89'))
    ids={f['id'] for f in after['findings']};exported=[row[cols['finding_id']] for row in rows if row[cols['finding_id']] in ids]
    assert len(exported)==len(set(exported))==54
    wb.close()
    report={'scope':'Known-source recovered T411 conflict through source admission, merge and Excel; not full-project discovery acceptance.',
        'source_candidate_sha256':hashlib.sha256((SOURCE/'workflow/candidate.json').read_bytes()).hexdigest(),
        'baseline_sha256':hashlib.sha256(BASELINE.read_bytes()).hexdigest(),'receipts':receipts,
        'prior_findings_preserved_exactly':53,'final_findings':54,'new_finding_id':new['id'],
        'related_baseline_findings_preserved':['F-011'],
        'local_duplicate_review':'F-011 addresses arithmetic/units in PZ circulation calculation; T411 addresses conflicting branch-flow labels between drawings. Distinct source propositions retained.',
        'historical_compound_control_closed':False,'full_acceptance':False,
        'limitations':['Pump insufficiency and full branch balance remain unresolved.','The 53-row baseline includes unreviewed findings; preservation is not truth certification.','Other prior ITP pilots are not all composed into this snapshot.']}
    immutable(OUT/'ITP_T411_output_replay.json',report)
    (OUT/'ITP_T411_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
