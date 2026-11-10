"""Apply an inspected model-proposed cable group through classic merge and Excel.

The bounded evaluator admits only the source-reviewed shared-specification group;
other proposed groups remain unapplied pending their own source review.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_merge.runner import merge_similar_findings
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS
HERE=Path(__file__).parent
ROOT=Path('/tmp/auditmanager_eom_model_grouped_output_20261111')
SOURCE=Path('/tmp/auditmanager_eom_reviewed_cable_output_20261111/03_findings.json')
PROPOSAL=Path('/tmp/auditmanager_eom_semantic_grouping_control_20261111/result.json')


def main():
    result=json.loads(PROPOSAL.read_text());assert not result['is_error']
    proposals=result['json_data']['groups']
    group,=[g for g in proposals if set(g['member_ids'])=={'F-050','F-051'}]
    before=json.loads(SOURCE.read_text());byid={f['id']:f for f in before['findings']}
    members=[deepcopy(byid[fid]) for fid in group['member_ids']]
    assert len({f['severity'] for f in members})==1
    assert {f['report_section'] for f in members}=={'technical_findings'}
    assert all(any(e.get('block_id')=='blk_67c8cc82d7e24f738797d9b75ce14e97' for e in f['evidence']) for f in members)
    ROOT.mkdir(exist_ok=True);merge=ROOT/'group';merge.mkdir(exist_ok=True)
    wire=deepcopy(members)
    for f in wire:
        f['description']=f['problem']+'\n'+f['description']
        f['problem']=group['shared_issue']
    (merge/'03_findings.json').write_text(json.dumps({'findings':wire},ensure_ascii=False,indent=2))
    stats=merge_similar_findings('EOM-model-proposed-group',output_dir=merge)
    assert stats['merged_groups']==1
    merged,=json.loads((merge/'03_findings.json').read_text())['findings']
    merged['id']='F-050'
    merged['grouping_original_findings']=members
    merged['grouping_provenance']={'mode':'model_proposal_with_bounded_source_review','model_result_sha256':hashlib.sha256(PROPOSAL.read_bytes()).hexdigest(),'proposal':group,'independent_project_acceptance':False}
    merged['merged_original_finding_ids']=[f['id'] for f in members]
    for f in members:
        for field in ('problem','description','solution'):assert f[field] in merged['description']
        assert all(e in merged['evidence'] for e in f['evidence'])
        assert set(f['source_finding_ids'])<=set(merged['source_finding_ids'])
    after=deepcopy(before);after['findings']=[merged if f['id']=='F-050' else f for f in after['findings'] if f['id']!='F-051']
    after.setdefault('meta',{})['total_findings']=len(after['findings'])
    after['grouping_control_receipt']={'original_count':51,'grouped_count':50,'unapplied_proposals':[g for g in proposals if g is not group]}
    new={f['id']:f for f in after['findings']}
    assert len(new)==50 and all(new[fid]==f for fid,f in byid.items() if fid not in ('F-050','F-051'))
    (ROOT/'03_findings.json').write_text(json.dumps(after,ensure_ascii=False,indent=2)+'\n')
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    path=ROOT/'report.xlsx';wb.save(path);wb.close()
    wb=load_workbook(path);rows=list(wb['EOM'].values);text='\n'.join(str(c) for row in rows for c in row if c is not None)
    column=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id');ids=[r[column] for r in rows if r[column] in new]
    assert len(ids)==len(set(ids))==50
    for f in members:
        assert f['description'] in text and f['solution'] in text
    for value in ('65 м','95 м','С2513','QF5.10','СЗ510'):assert value in text
    assert 'СЭ510' not in text
    wb.close()
    (HERE/'EOM_model_grouped_output_control.xlsx').write_bytes(path.read_bytes())
    report={'scope':'Actual Astra grouping proposal followed by explicit known-control source review, classic merge and Excel.',
        'proposal_all_findings':51,'proposal_groups':len(proposals),'applied_member_ids':group['member_ids'],'classic_findings_preserved_exactly':49,
        'final_rows':50,'both_cable_descriptions_solutions_and_evidence_preserved':True,
        'model_discovered_shared_group_without_expected_ids':True,'prior_figure_and_identifier_uncertainty_preserved':True,
        'unapplied_groups':[g['member_ids'] for g in proposals if g is not group],
        'xlsx_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'raw_model_result_sha256':hashlib.sha256(PROPOSAL.read_bytes()).hexdigest(),
        'limitations':['Presentation grouping does not prove the technical truth of every grouped sentence.',
            'Only the cable shared-source group has bounded source-review evidence here; voltage group is not accepted or discarded.',
            'Known-control adapter still required; generic production grouping admission is not implemented.',
            'No negative grouping pair or whole-project transfer acceptance established by this replay.'],
        'new_model_calls':0,'full_acceptance_proven':False}
    (HERE/'EOM_model_grouped_output_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
