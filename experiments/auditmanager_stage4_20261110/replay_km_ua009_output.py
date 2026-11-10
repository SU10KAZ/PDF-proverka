"""Real discovered candidate, manual report formatting, local output retention only."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile

from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS


def main():
    here=Path(__file__).resolve().parent
    result_path=here/'KM_inventory_packet006_result.json'
    result=json.loads(result_path.read_text())
    raw=result['json_data']['text_findings'][0]
    control=json.loads((here/'KM_UA009_source_inventory_recovery.json').read_text())
    assert control['narrow_claim_verified'] and raw['id']=='F1'
    for source in control['same_version_source_evidence']:
        assert hashlib.sha256(Path(source['path']).read_bytes()).hexdigest()==source['sha256']
    finding={'problem':raw['statement'],'description':raw['comparison_basis']+' '+raw['limitation'],
        'solution':'Уточнить базу отметок и согласовать обозначение этой площадки на плане и разрезах.',
        'severity':'РЕКОМЕНДАТЕЛЬНОЕ','category':'coordination','report_section':'technical_findings',
        'sheet':'7','page':9,'norm':'','norm_quote':'','risk':'',
        'source_finding_ids':['object_inventory_006:F1'],'source_block_ids':raw['source_refs'],
        'related_block_ids':raw['source_refs'],
        'evidence':[{'type':'image','block_id':e['source_ref'],'observation':e['graphic_observation']} for e in raw['evidence']],
        'provenance':{'origin':'source_inventory_candidate_manual_report_formatting',
            'raw_result_sha256':hashlib.sha256(result_path.read_bytes()).hexdigest(),
            'manual_fields':['solution','severity','category','report_section','sheet','page'],
            'automatic_report_generation_verified':False}}
    source=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/03_analysis/runs/control')
    original=json.loads((source/'03_findings.json').read_text())
    candidate=combine_findings_with_targeted(deepcopy(original),[('known_source_inventory_retention',{'findings':[finding]})])
    outcomes=[]
    for label,data in [('baseline',original),('candidate',candidate)]:
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory)
            for name in ('01_blocks_analysis.json','document_graph.json'):
                (out/name).write_bytes((source/name).read_bytes())
            (out/'03_findings.json').write_text(json.dumps(data,ensure_ascii=False))
            stats=_run_deterministic_phase(out,'KM-UA009-retention')
            assert not stats['critic_error'] and not stats['corrector_error']
            findings=json.loads((out/'03_findings.json').read_text())['findings']
            outcomes.append({'label':label,'stats':stats,'findings':findings})
    before,after=outcomes
    assert after['findings'][:-1]==before['findings']
    retained=after['findings'][-1]
    for key in ('problem','description','solution','source_block_ids','source_finding_ids','report_section'):
        assert retained[key]==finding[key],key
    artifact_dir=Path('/tmp/auditmanager_km_ua009_output_retention_20261110')
    artifact_dir.mkdir(exist_ok=True)
    wb=Workbook()
    build_project_sheet(wb,{'project_id':'KM-retention','sheet_name':'KM','findings_json':{'findings':after['findings']}})
    path=artifact_dir/'KM_candidate.xlsx';wb.save(path);wb.close()
    saved=load_workbook(path);rows=list(saved['KM'].values)
    col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    ids={f['id'] for f in after['findings']};exported=[row[col] for row in rows if row[col] in ids]
    assert len(exported)==len(ids) and set(exported)==ids
    candidate_rows=[row for row in rows if row[col]==retained['id']]
    assert len(candidate_rows)==1 and retained['problem'] in candidate_rows[0]
    saved.close()
    report={'scope':'Discovered source-only candidate manually formatted for output retention replay',
        'baseline_count':len(before['findings']),'candidate_count':len(after['findings']),
        'all_baseline_findings_preserved':True,'candidate_core_and_sources_preserved':True,
        'excel_ids_unique_and_complete':True,'candidate_visible_in_saved_excel':True,
        'retained_candidate':retained,'verifier_stats':[o['stats'] for o in outcomes],
        'excel_path':str(path),'excel_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'new_model_calls':0,'automatic_report_generation_verified':False,
        'full_semantic_and_absence_model_passes_executed':False,'whole_project_quality_acceptance':False,
        'limits':'Tests merge, deterministic verifier/corrector and saved Excel only. Does not prove automatic formatting, complete audit or release readiness.'}
    (here/'KM_UA009_output_retention.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps({k:report[k] for k in ('baseline_count','candidate_count','all_baseline_findings_preserved','candidate_visible_in_saved_excel')}))


if __name__=='__main__':
    main()
