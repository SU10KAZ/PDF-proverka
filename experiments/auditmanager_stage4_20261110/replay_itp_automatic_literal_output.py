"""Export the reviewed automatic T411 discovery against the original baseline.

A separate regression branch, not a replacement for the broader GVS pilot report.
"""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_itp_pipe_operations import BASE, immutable

ROOT=Path('/tmp/auditmanager_itp_automatic_t411_literal_workflow_20261111')
OUT=Path(__file__).parent
BASELINE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/snapshots/ITP/findings_verify.json')


def main():
    candidate=json.loads((ROOT/'workflow/candidate.json').read_text())
    finding=json.loads((ROOT/'finding.json').read_text())
    before=json.loads(BASELINE.read_text())
    assert candidate['status']=='candidate_ready'
    revised,receipts=apply_review_admissions({'findings':[finding]},[candidate['entry']],(BASE/'02_work/document.md').read_text())
    assert receipts[0]['status']=='applied',receipts
    row,=revised['findings']
    text='\n'.join(str(row.get(k,'')) for k in ('problem','description','solution'))
    assert all(v in text for v in ('Т411','5,89','2,73','3,16'))
    assert '2,78' not in text and '3,11' not in text
    assert row['report_section']=='technical_findings'
    row['source_review_history']=revised['source_review_history']
    row['independent_graphic_readings']=json.loads((ROOT/'workflow/independent_graphic_readings.json').read_text())
    # Local overlap check, not a generic semantic duplicate detector.
    assert not any('Т411' in str(f.get('problem',''))+str(f.get('description','')) for f in before['findings'])
    after=combine_findings_with_targeted(before,[('automatic_T411_source_review',revised)])
    assert len(before['findings'])==53 and len(after['findings'])==54
    assert after['findings'][:-1]==before['findings']
    output=ROOT/'output';output.mkdir(exist_ok=True)
    immutable(output/'03_findings.json',after)
    wb=Workbook();build_project_sheet(wb,{'project_id':'ITP','sheet_name':'ITP','findings_json':after})
    wb.save(output/'report.xlsx');wb.close()
    wb=load_workbook(output/'report.xlsx');rows=list(wb['ITP'].values)
    cols={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    ids={f['id'] for f in after['findings']}
    selected=[r for r in rows if r[cols['finding_id']] in ids]
    assert len(selected)==len({r[cols['finding_id']] for r in selected})==54
    final=after['findings'][-1];excel,=[r for r in selected if r[cols['finding_id']]==final['id']]
    assert final['description'] in excel[cols['description']]
    assert excel[cols['problem']]==final['problem'] and excel[cols['solution']]==final['solution']
    wb.close()
    report={'scope':'Automatic known-object candidate plus executor-selected literal crop, admission and actual Excel export. Not a complete blind project audit.',
        'baseline_sha256':hashlib.sha256(BASELINE.read_bytes()).hexdigest(),
        'source_candidate_sha256':hashlib.sha256((ROOT/'workflow/candidate.json').read_bytes()).hexdigest(),
        'original_rows_retained_exactly':53,'final_rows':54,'added_id':final['id'],
        'source_value_corrected':'2,78 -> 2,73','arithmetic_corrected':'3,11 -> 3,16',
        'useful_discrepancy_retained':True,'original_wrong_claim_archived':True,
        'autonomous_detail_selection_proven':False,'full_acceptance':False,
        'broader_pilot_composition':'Separate baseline branch. Do not append to the existing 55-row GVS report: its F054 already covers T411/T412. Broader rows are not removed or replaced.',
        'excel_path':str(output/'report.xlsx'),'receipts':receipts}
    immutable(OUT/'ITP_automatic_literal_output_replay.json',report)
    (OUT/'ITP_automatic_literal_control.xlsx').write_bytes((output/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':main()
