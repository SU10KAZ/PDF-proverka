"""Known-case reviewed grouping through actual merge/verifier/Excel; no model calls.

Explicit evaluator grouping is NOT proof of autonomous semantic grouping quality.
Raw model candidates remain sealed; only the grouping title/class are normalized.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_merge.runner import merge_similar_findings
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet

HERE=Path(__file__).parent
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/AK/versions/v001')
ROOT=Path('/tmp/auditmanager_ak_systemic_output_control_20261110')
SELECT=[(0,2,0),(1,4,0),(2,2,0),(3,3,2),(4,3,0)]

def main():
    ROOT.mkdir(exist_ok=True);groupdir=ROOT/'group';groupdir.mkdir(exist_ok=True)
    baseline=json.loads((BASE/'03_analysis/runs/control/03_findings.json').read_text())
    old={f['id']:f for f in baseline['findings']};members=[];sources=[]
    for n,(packet,version,index) in enumerate(SELECT,1):
        path=HERE/f'AK_cabinet_{packet:03d}_operation_v{version}_validated.json'
        raw=json.loads(path.read_text())['findings'][index]
        assert 'клемм' in (raw['problem']+' '+raw['description']).casefold()
        item=deepcopy(raw);item.update(id=f'F-{n:03d}',problem='Неоднозначная адресация клемм в шкафах',
            severity=old['F-054']['severity'],category='documentation')
        if packet in (0,2):
            previous=old['F-054' if packet==0 else 'F-055']
            item['source_finding_ids']=list(dict.fromkeys(previous.get('source_finding_ids',[])+item.get('source_finding_ids',[])))
        members.append(item);sources.append({'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'finding_index':index})
    (groupdir/'03_findings.json').write_text(json.dumps({'findings':members},ensure_ascii=False,indent=2))
    assert merge_similar_findings('AK-known-group',output_dir=groupdir)['merged_groups']==1
    grouped,=json.loads((groupdir/'03_findings.json').read_text())['findings']
    grouped['id']='F-054';grouped['merged_original_finding_ids']=['F-054','F-055']
    grouped['control_grouping']={'mode':'explicit_known_case_evaluator_decision','autonomous_semantic_grouping_proven':False}
    for member in members:
        assert member['description'] in grouped['description']
        assert member['solution'] in grouped['description']
        assert set(member['source_finding_ids'])<=set(grouped['source_finding_ids'])
    combined=deepcopy(baseline)
    combined['findings']=[grouped if f['id']=='F-054' else f for f in combined['findings'] if f['id']!='F-055']
    combined.setdefault('meta',{})['total_findings']=len(combined['findings'])
    for name in ['01_blocks_analysis.json','02_text_analysis.json','document_graph.json']:
        shutil.copyfile(BASE/'03_analysis/runs/control'/name,ROOT/name)
    (ROOT/'02_work').mkdir(exist_ok=True);shutil.copyfile(BASE/'02_work/document.md',ROOT/'02_work/document.md')
    (ROOT/'03_findings.json').write_text(json.dumps(combined,ensure_ascii=False,indent=2))
    stats=_run_deterministic_phase(ROOT,'AK-systemic-output-control')
    assert not any(stats.get(k) for k in ['critic_error','corrector_error','source_review_queue_error'])
    after=json.loads((ROOT/'03_findings.json').read_text());current={f['id']:f for f in after['findings']}
    assert len(current)==64 and 'F-055' not in current
    for fid,original in old.items():
        if fid in ('F-054','F-055'):continue
        for field in ['problem','description','solution','severity','report_section']:
            assert current[fid].get(field)==original.get(field),(fid,field)
    for member in members:
        assert member['description'] in current['F-054']['description']
    wb=Workbook();build_project_sheet(wb,{'project_id':'AK','sheet_name':'AK','findings_json':after})
    path=ROOT/'report.xlsx';wb.save(path);wb.close()
    wb=load_workbook(path);text='\n'.join(str(c) for row in wb['AK'].values for c in row if c is not None);wb.close()
    for member in members:
        assert member['description'] in text
        assert member['solution'] in text
    report={'scope':'Five real model-produced cabinet candidates through explicit known-case grouping, deterministic verifier and Excel.',
        'inputs':sources,'baseline_findings':65,'output_findings':64,'untouched_classic_findings_preserved':63,
        'systemic_finding_id':'F-054','merged_baseline_ids':['F-054','F-055'],'cabinet_candidate_descriptions_preserved':5,
        'source_candidate_ids_preserved':True,'xlsx_path':str(path),'xlsx_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'verifier_stats':stats,'new_model_calls':0,'autonomous_semantic_grouping_proven':False,'full_control_complete':False,
        'limits':['Known evaluator grouping, not autonomous model decision.',
            'Second-cabinet PDS3/PDS4clause comes from actual detailed model response, not an evaluator-inserted answer.',
            'No full model verifier or production acceptance; grouped row count is not independent defect count.']}
    (HERE/'AK_systemic_output_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    shutil.copyfile(path,HERE/'AK_systemic_output_control.xlsx')
    print(json.dumps({k:v for k,v in report.items() if k not in ['inputs','verifier_stats']},ensure_ascii=False))

if __name__=='__main__':main()
