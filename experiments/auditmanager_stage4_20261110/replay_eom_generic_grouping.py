"""Replay the previously source-reviewed cable group through generic admission."""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_merge.reviewed_grouping import apply_reviewed_group
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS
from .run_eom_instance_operations import BASE,immutable
ROOT=Path('/tmp/auditmanager_eom_generic_grouping_20261111')
OUT=Path(__file__).parent

def main():
    ROOT.mkdir(exist_ok=True)
    source=Path('/tmp/auditmanager_eom_reviewed_cable_output_20261111/03_findings.json')
    before=json.loads(source.read_text())
    result=json.loads(Path('/tmp/auditmanager_eom_semantic_grouping_control_20261111/result.json').read_text())
    proposal,=[g for g in result['json_data']['groups'] if set(g['member_ids'])=={'F-050','F-051'}]
    members=[f for f in before['findings'] if f['id'] in proposal['member_ids']]
    md=(BASE/'02_work/document.md').read_text()
    decision={'action':'group','reviewer':'local_explicit_source_review',
        'reason':'Reuse the bounded shared-specification review already recorded in EOM_model_grouped_output_replay.json. Keep separate physical addresses, conditional absence scope and all source wording; no sum into a shortage.',
        'same_issue_verified':True,'common_corrective_action_verified':True,'individual_scopes_preserved':True,
        'proposal_sha256':evidence_digest(proposal),'members_sha256':evidence_digest(members),
        'source_md_sha256':hashlib.sha256(md.encode()).hexdigest()}
    after,receipt=apply_reviewed_group(before,proposal,decision,md)
    assert receipt['status']=='applied',receipt
    unchanged=[f for f in before['findings'] if f['id'] not in proposal['member_ids']]
    assert [f for f in after['findings'] if f['id']!=receipt['surviving_id']]==unchanged
    assert len(after['findings'])==50 and len(unchanged)==49
    immutable(ROOT/'03_findings.json',after);immutable(ROOT/'decision.json',decision)
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    wb.save(ROOT/'report.xlsx');wb.close();wb=load_workbook(ROOT/'report.xlsx')
    rows=list(wb['EOM'].values);columns={k:i for i,(k,*_) in enumerate(PROJ_COLUMNS)}
    row,=[r for r in rows if r[columns['finding_id']]==receipt['surviving_id']]
    for f in members:
        for field in ('problem','description','solution','risk','norm'):
            if f.get(field):assert str(f[field]) in row[columns['description']]
    wb.close()
    report={'receipt':receipt,'classic_findings_preserved_exactly':49,'final_rows':50,
        'all_member_claims_solutions_risks_norms_visible_in_excel':True,
        'generic_admission_used':True,'source_review_still_explicit_local':True,
        'new_model_calls':0,'full_acceptance':False,
        'remaining':['Automatic production dispatch and semantic review not wired.','No independent historical negative grouping pair yet.','Cable source identifier and completeness limitations remain.']}
    immutable(OUT/'EOM_generic_grouping_replay.json',report)
    (OUT/'EOM_generic_grouping_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
