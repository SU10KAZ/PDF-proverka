"""Replay completed model admissions through real Excel; preserve semantic caveats."""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_eom_cable_operations import BASE

HERE = Path(__file__).parent
ROOT = Path('/tmp/auditmanager_eom_reviewed_cable_output_20261111')
REVIEW = Path('/tmp/auditmanager_eom_cable_detail_source_workflow_20261111')


def main():
    before = json.loads(Path('/tmp/auditmanager_eom_cable_output_20261111/03_findings.json').read_text())
    entries = [json.loads((REVIEW/fid/'candidate.json').read_text())['entry'] for fid in ('F-050','F-051')]
    md = (BASE/'02_work/document.md').read_text()
    after, receipts = apply_review_admissions(before, entries, md)
    assert receipts == [{'finding_id':fid,'status':'applied','action':'revise'} for fid in ('F-050','F-051')]
    old = {f['id']: f for f in before['findings']}; new = {f['id']: f for f in after['findings']}
    assert len(new) == len(old) == 51
    retained = set(old) - {'F-050','F-051'}
    assert len(retained) == 49 and all(old[fid] == new[fid] for fid in retained)
    ROOT.mkdir(exist_ok=True)
    (ROOT/'03_findings.json').write_text(json.dumps(after,ensure_ascii=False,indent=2)+'\n')
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    output=ROOT/'report.xlsx';wb.save(output);wb.close()
    wb=load_workbook(output);rows=list(wb['EOM'].values)
    text='\n'.join(str(c) for row in rows for c in row if c is not None)
    index=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    ids=[row[index] for row in rows if row[index] in new]
    assert len(ids)==len(set(ids))==51
    for fid in ('F-050','F-051'):
        assert new[fid]['description'] in text
        assert new[fid]['report_section']=='technical_findings'
    for phrase in ('С2513','65 м','95 м','QF5.10','СЗ510'): assert phrase in text
    assert 'СЭ510' not in text
    wb.close()
    (HERE/'EOM_reviewed_cable_output_control.xlsx').write_bytes(output.read_bytes())
    report={'scope':'Known control: real source-review admission and Excel, using completed detailed-image Claude decisions without evaluator edits.',
            'receipts':receipts,'classic_findings_preserved_exactly':49,'output_findings':51,
            'technical_cores_retained':2,'new_model_calls':0,
            'source_candidate_sha256':{fid:hashlib.sha256((REVIEW/fid/'candidate.json').read_bytes()).hexdigest() for fid in ('F-050','F-051')},
            'xlsx_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
            'limitations':[
                'Structural application is not semantic approval of every assertion.',
                'F-050 reviewer omitted sheet 2.2 because graphic stamp was not supplied. New metadata code has only local tests, not a new model comparison.',
                'Prepared graphic stamp literally spells ЗОМ 2-1; retain this source spelling and do not silently normalize to ЭОМ or assert identical document codes.',
                'F-051 review asserts literal С3510=СЗ510; prior independent review leaves graphic 3/З ambiguous. Final displayed stable circuit address is preserved, but that provenance assertion is not independently accepted.',
                'Two cable addresses still form two rows of a shared specification issue. Autonomous grouping remains untested.',
                'Reviewer-added global absence claim was not part of the original scoped finding; unnecessary context searches remain a quality/cost issue.'
            ],'full_semantic_acceptance':False,'full_stage4_acceptance':False}
    (HERE/'EOM_reviewed_cable_output_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
