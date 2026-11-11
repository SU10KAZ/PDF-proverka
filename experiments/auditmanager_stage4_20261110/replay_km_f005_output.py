"""Known-control output adapter; no inference or benchmark-driven wording edits."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS

HERE=Path(__file__).parent
ROOT=Path('/tmp/auditmanager_km_f005_output_readjudication_20261111')
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    baseline_path=BASE/'snapshots/KM/findings_verify.json'
    baseline=json.loads(baseline_path.read_text())
    candidate=json.loads((ROOT/'candidate.json').read_text());entry=candidate['entry']
    prepared=entry['queue']['packets'][0]['finding_snapshot']
    md=(BASE/'inputs/KM/versions/v001/02_work/document.md').read_text()
    local,receipts=apply_review_admissions({'findings':[prepared]},[entry],md)
    assert receipts==[{'finding_id':'F-005','status':'applied','action':'revise'}]
    revised=local['findings'][0]
    original,=[f for f in baseline['findings'] if f['id']=='F-005']
    # The bounded source review used the exact visible original proposition.
    # Keep the complete classic metadata; never relabel a partial source snapshot
    # as if its fingerprint covered the full original finding.
    visible=('problem','description','solution','severity','norm','norm_quote','risk','sheet','page','evidence')
    assert all(original.get(k)==prepared.get(k) for k in visible)
    changed=deepcopy(original)
    keys=visible+('report_section','source_evidence_review_status','source_review_response_sha256','source_review_final_evidence_refs')
    for key in keys:
        if key in revised:changed[key]=deepcopy(revised[key])
    changed['source_review_output_adapter']={'scope':'Known-development exact-visible-proposition adapter; not autonomous full-report acceptance.',
        'source_candidate_sha256':sha(ROOT/'candidate.json'),'original_finding':deepcopy(original),
        'reviewed_snapshot':deepcopy(prepared),'visible_fields_verified':list(visible)}
    after=deepcopy(baseline);after['findings']=[changed if f['id']=='F-005' else f for f in baseline['findings']]
    after.setdefault('source_review_history',[]).extend(local['source_review_history'])
    untouched=[f for f in baseline['findings'] if f['id']!='F-005']
    assert len(after['findings'])==len(baseline['findings'])==26
    assert [f for f in after['findings'] if f['id']!='F-005']==untouched
    directory=ROOT/'full_output';directory.mkdir(exist_ok=True)
    (directory/'03_findings.json').write_text(json.dumps(after,ensure_ascii=False,indent=2)+'\n')
    wb=Workbook();build_project_sheet(wb,{'project_id':'KM','sheet_name':'KM','findings_json':after})
    path=directory/'report.xlsx';wb.save(path);wb.close()
    wb=load_workbook(path);rows=list(wb['KM'].values)
    column=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    ids={f['id'] for f in after['findings']};actual=[r[column] for r in rows if r[column] in ids]
    assert len(actual)==len(set(actual))==26
    row,=[r for r in rows if r[column]=='F-005']
    description_column=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='description')
    assert changed['description'] in row[description_column] and changed['sheet'] in row
    wb.close()
    archive=HERE/'km_f005_output_evidence';archive.mkdir(exist_ok=True)
    files=[]
    for source in sorted(ROOT.glob('*.json')):
        dest=archive/source.name;dest.write_bytes(source.read_bytes());files.append({'path':str(dest),'sha256':sha(dest)})
    for source in (directory/'03_findings.json',path):
        dest=archive/source.name;dest.write_bytes(source.read_bytes());files.append({'path':str(dest),'sha256':sha(dest)})
    report={'scope':'Known KM F005 whole-report output replay after source adjudication; not blind discovery.',
        'baseline_sha256':sha(baseline_path),'files':files,'original_rows':26,'final_rows':26,'other_findings_preserved_exactly':25,
        'final_pages':changed['page'],'final_sheet':changed['sheet'],'explicit_evidence_refs':len(changed['source_review_final_evidence_refs']),
        'anchor_totals_conditional':True,'plate_counts_and_conditional_mass_preserved':True,
        'S2_overlap_cross_referenced_to_F004':True,'new_findings_claimed':0,
        'profile_disagreement':'Model retains technical/economic routing on plate-balance scope; frozen target routes this as question. This replay does not resolve the evaluator disagreement.',
        'limitations':['Known-control adapter binds exact original visible fields, not the full original finding fingerprint.',
            'Other 25 findings unchanged means preservation, not validation of all their content.',
            'Literal mark identity mapping for some whole-project totals remains a model-supported interpretation.',
            'Full automatic report overlap integration and paired project comparison remain pending.'],
        'full_acceptance_proven':False,'production_changed':False}
    (archive/'assessment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('original_rows','final_rows','other_findings_preserved_exactly','final_pages','explicit_evidence_refs')},ensure_ascii=False))

if __name__=='__main__':main()
