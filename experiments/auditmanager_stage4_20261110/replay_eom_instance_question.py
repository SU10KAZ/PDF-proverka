"""Apply sealed source review, preserve prior findings, and verify question export."""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
SOURCE=Path('/tmp/auditmanager_eom_instance_source_review_v2_20261111')
BASE=Path('/tmp/auditmanager_eom_model_grouped_output_20261111/03_findings.json')
MD=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/02_work/document.md')
ROOT=Path('/tmp/auditmanager_eom_instance_question_output_20261111')
OUT=Path(__file__).parent

def read(path):return json.loads(path.read_text())
def write(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

def main():
    ROOT.mkdir(exist_ok=True)
    candidate=read(SOURCE/'workflow/candidate.json');finding=read(SOURCE/'finding.json')
    revised,receipts=apply_review_admissions({'findings':[finding]},[candidate['entry']],MD.read_text())
    assert receipts==[{'finding_id':finding['id'],'status':'applied','action':'question'}],receipts
    item=revised['findings'][0]
    item['source_review_history']=revised['source_review_history']
    item['source_review_original_finding_id']=finding['id']
    before=read(BASE)
    after=combine_findings_with_targeted(before,[('source_review_instance_identity',revised)])
    assert after['findings'][:-1]==before['findings']
    assert len(after['findings'])==len(before['findings'])+1
    question=after['findings'][-1]
    assert question['report_section']=='questions' and question['source_evidence_review_status']=='reviewed_question'
    assert question['source_review_history']==revised['source_review_history']
    write(ROOT/'03_findings.json',after)
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');rows=list(wb['EOM'].values)
    col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    heading=next(i for i,row in enumerate(rows) if 'Вопросы и уточнения' in row)
    positions=[i for i,row in enumerate(rows) if row[col]==question['id']]
    assert len(positions)==1 and positions[0]>heading
    wb.close()
    report={'scope':'Known-source instance identity control through actual source admission, merge and Excel.',
        'source_candidate_sha256':hashlib.sha256((SOURCE/'workflow/candidate.json').read_bytes()).hexdigest(),
        'baseline_sha256':hashlib.sha256(BASE.read_bytes()).hexdigest(),'receipts':receipts,
        'prior_findings_preserved_exactly':len(before['findings']),'final_findings':len(after['findings']),
        'question_id':question['id'],'question_exported_separately':True,'new_model_calls':0,
        'limitations':['Two physical instances are not established; duplicate callouts and separately labelled units remain alternatives.',
            'No equipment shortage, electrical fault or replacement label is established.',
            'The original H03 positive control remains partial: a justified question is preserved, but physical instance identity is unresolved.',
            'One unrelated-floor graphic was explicitly omitted by the 16-image budget; all direct finding graphics and nine detail tiles were supplied.',
            'Prior grouping and source-review limitations of the 50-row baseline remain unchanged.'],
        'full_acceptance_proven':False}
    write(OUT/'EOM_instance_question_output_replay.json',report)
    (OUT/'EOM_instance_question_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
