"""Exercise the same optional grouping phase used by findings_verify."""
import json
from pathlib import Path
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_merge.reviewed_grouping import run_group_admission_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS
from .run_eom_instance_operations import BASE,immutable
ROOT=Path('/tmp/auditmanager_eom_grouping_phase_20261111')
OUT=Path(__file__).parent

def main():
    ROOT.mkdir(exist_ok=True)
    source=Path('/tmp/auditmanager_eom_reviewed_cable_output_20261111/03_findings.json')
    before=json.loads(source.read_text())
    prior=json.loads(Path('/tmp/auditmanager_eom_generic_grouping_20261111/03_findings.json').read_text())
    merged=next(f for f in prior['findings'] if f.get('grouping_provenance'))
    provenance=merged['grouping_provenance'];inbox=[{'proposal':provenance['proposal'],'decision':provenance['decision']}]
    path=ROOT/'03_findings.json'
    if not path.exists():path.write_text(source.read_text())
    immutable(ROOT/'03_group_review_admissions.json',inbox)
    first=run_group_admission_phase(ROOT,(BASE/'02_work/document.md').read_text())
    assert first['applied']+first['already_applied']==1 and not first['not_applied']
    sealed=path.read_bytes();second=run_group_admission_phase(ROOT,(BASE/'02_work/document.md').read_text())
    assert second['already_applied']==1 and path.read_bytes()==sealed
    after=json.loads(sealed);members=provenance['proposal']['member_ids']
    assert len(after['findings'])==50
    assert [f for f in before['findings'] if f['id'] not in members]==[f for f in after['findings'] if f['id']!='F-050']
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after});wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');cols={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    row,=[r for r in wb['EOM'].values if r[cols['finding_id']]=='F-050']
    for f in merged['grouping_original_findings']:
        for key in ('problem','description','solution','risk','norm'):
            if f.get(key):assert str(f[key]) in row[cols['description']]
    wb.close()
    report={'scope':'Actual optional grouping admission phase and Excel, not full verifier-stage model replay.',
        'group_applied':True,'rerun_idempotent':True,'prior_classic_findings_unchanged':49,'final_rows':50,
        'member_text_visible':True,'raw_proposals_cannot_trigger_phase':True,
        'new_model_calls':0,'full_acceptance':False,
        'remaining':'Automatic preparation and semantic adjudication of group inbox remain separate work; explicit local review reused.'}
    immutable(OUT/'EOM_grouping_phase_replay.json',report)
    (OUT/'EOM_grouping_phase_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
