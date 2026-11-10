"""Carry the bounded F-032 revision through admission and real Excel output."""
from copy import deepcopy
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_eom_instance_operations import BASE, immutable
SOURCE=Path('/tmp/auditmanager_eom_classic_semantics_retry1_20261111/F-032/candidate.json')
PRIOR=Path('/tmp/auditmanager_eom_instance_question_output_20261111/03_findings.json')
ROOT=Path('/tmp/auditmanager_eom_f032_output_20261111')
OUT=Path(__file__).parent

def main():
    ROOT.mkdir(exist_ok=True)
    candidate=json.loads(SOURCE.read_text());entry=candidate['entry']
    before=json.loads(PRIOR.read_text());document=deepcopy(before)
    snapshot=next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']=='F-032')
    index=next(i for i,f in enumerate(document['findings']) if f['id']=='F-032')
    original=document['findings'][index]
    added={'source_review_requests','source_evidence_review_status'}
    assert {k:v for k,v in snapshot.items() if k not in added}==original
    document['findings'][index]=snapshot
    md=(BASE/'02_work/document.md').read_text()
    assert D('98.5')+D('6.1')+D('3.5')==D('108.1')
    assert D('55.4')+D('2.2')+D('5.4')==D('63.0')
    assert D('105.4')*D('0.22')==D('23.188')
    assert D('108.1')*D('0.22')==D('23.782')
    after,receipts=apply_review_admissions(document,[entry],md)
    assert receipts==[{'finding_id':'F-032','status':'applied','action':'revise'}],receipts
    assert len(after['findings'])==len(before['findings'])==51
    assert [f for f in after['findings'] if f['id']!='F-032']==[f for f in before['findings'] if f['id']!='F-032']
    item=after['findings'][index]
    assert 'правило минимальной мощности представленными материалами не установлено' in item['description']
    assert 'условном' in item['description']
    assert item['id']==original['id'] and item['source_finding_ids']==original['source_finding_ids']
    immutable(ROOT/'03_findings.json',after)
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    wb.save(ROOT/'report.xlsx');wb.close()
    wb=load_workbook(ROOT/'report.xlsx');rows=list(wb['EOM'].values)
    cols={key:i for i,(key,*_) in enumerate(PROJ_COLUMNS)}
    selected=[row for row in rows if row[cols['finding_id']]=='F-032'];assert len(selected)==1
    for key in ('problem','solution','risk','norm'):
        assert selected[0][cols[key]]==item[key],key
    assert selected[0][cols['description']]==item['description']+'\nНорма: '+item['norm']
    assert selected[0][cols['severity']]=='⚪ '+item['severity']
    wb.close()
    report={'scope':'Known-candidate F-032 revision; not independent detection or full-project acceptance.',
        'source_candidate_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'baseline_sha256':hashlib.sha256(PRIOR.read_bytes()).hexdigest(),'receipts':receipts,
        'unchanged_other_findings':50,'final_findings':51,'source_identity_preserved':True,
        'all_reviewed_display_fields_exported':True,'original_archived':True,
        'checked_source_blocks':['blk_fb5f780dae7a475ca6c249f62b64771e','blk_f2b273baee994702906a6e5d94a43063','blk_29be4ff018f040daaa2000c0a3ebde08'],
        'evidence':'Authoritative MD tables confirm 105.4 vs 108.1 and 68.6 vs 63.0. Arithmetic recomputed; no universal minimum inferred.',
        'limitations':['Area priority and final required loads unresolved.','Normative applicability remains unverified.','Existing limitations of earlier 51-row baseline remain.'],
        'full_acceptance':False,'new_model_calls':0}
    immutable(OUT/'EOM_F032_output_replay.json',report)
    immutable(OUT/'EOM_F032_candidate.json',candidate)
    (OUT/'EOM_F032_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
