"""Compose reviewed supply and circulation conflicts in the actual Excel path."""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_itp_pipe_operations import BASE, immutable

SOURCE = Path(__file__).parent/'itp_gvs_supply_source_evidence'
BEFORE = Path(__file__).parent/'itp_gvs_circulation_source_evidence/03_findings.json'
ROOT = Path('/tmp/auditmanager_itp_gvs_combined_output_20261111')
OUT = Path(__file__).parent


def main():
    ROOT.mkdir(exist_ok=True)
    before = json.loads(BEFORE.read_text())
    finding = json.loads((SOURCE/'finding.json').read_text())
    candidate = json.loads((SOURCE/'workflow/candidate.json').read_text())
    assert candidate['status']=='candidate_ready'
    revised, receipts = apply_review_admissions({'findings':[finding]},[candidate['entry']],
                                                (BASE/'02_work/document.md').read_text())
    assert len(receipts)==1 and receipts[0]['status']=='applied',receipts
    item, = revised['findings']
    assert item['report_section']=='technical_findings'
    item['source_review_history']=revised['source_review_history']
    item['source_review_original_finding_id']=finding['id']
    after = combine_findings_with_targeted(before,[('GVS_supply_source_review',revised)])
    assert len(before['findings'])==54 and len(after['findings'])==55
    assert after['findings'][:-1]==before['findings']
    new=after['findings'][-1]
    assert new['id']=='F-055'
    assert all(t in new['description'] for t in ('Т311','7,71','8,71'))
    # Local reviewed scopes: F054 circulation, F055 supply; baseline has no T311 claim.
    assert not any('Т311' in str(f.get('problem',''))+str(f.get('description','')) for f in before['findings'])
    immutable(ROOT/'03_findings.json',after)
    wb=Workbook();build_project_sheet(wb,{'project_id':'ITP','sheet_name':'ITP','findings_json':after})
    wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');rows=list(wb['ITP'].values)
    cols={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    ids={f['id'] for f in after['findings']}
    selected=[r for r in rows if r[cols['finding_id']] in ids]
    assert len(selected)==len({r[cols['finding_id']] for r in selected})==55
    for finding_id in ('F-054','F-055'):
        f=next(f for f in after['findings'] if f['id']==finding_id)
        row=next(r for r in selected if r[cols['finding_id']]==finding_id)
        for key in ('problem','solution'):assert row[cols[key]]==f[key]
        assert f['description'] in row[cols['description']]
    wb.close()
    report={'scope':'Known-source composition of two independently reviewed GVS claims, not full-project acceptance.',
        'baseline_sha256':hashlib.sha256(BEFORE.read_bytes()).hexdigest(),
        'source_candidate_sha256':hashlib.sha256((SOURCE/'workflow/candidate.json').read_bytes()).hexdigest(),
        'receipts':receipts,'prior_findings_preserved_exactly':54,'original_classic_rows_preserved':53,
        'final_findings':55,'added_id':'F-055','circulation_id':'F-054',
        'duplicate_review':{'origin':'explicit_local_review','reason':'T311 supplies hot water; T411/T412 circulate it. Different branches and numerical propositions. Original 53-row descriptions contain no T311 conflict. F011 PZ arithmetic/units is distinct.','automatic_deduplication_proven':False},
        'full_acceptance':False,'historical_compound_control_closed':False,
        'limitations':['T312 supply balance and pipe thickness are not closed by T311.',
                      'Other ITP pilot corrections still require composition.',
                      'Original baseline retention is not certification of its truth.']}
    immutable(OUT/'ITP_GVS_combined_output_replay.json',report)
    immutable(OUT/'itp_gvs_supply_source_evidence/03_findings_combined.json',after)
    (OUT/'ITP_GVS_combined_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':main()
