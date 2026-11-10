"""Two actual cable candidates through address correction, deterministic phase and Excel.

Known-control adapter only; no autonomous grouping or complete semantic gate claimed.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_eom_cable_operations import BASE
ROOT=Path('/tmp/auditmanager_eom_cable_output_20261111')
HERE=Path(__file__).parent
SOURCES=[Path('/tmp/auditmanager_eom_cable_operations_20261110/block_000/circuit_13/context_2/validated.json'),Path('/tmp/auditmanager_eom_shared_source_control_20261111/validated.json')]
ADDRESS=Path('/tmp/auditmanager_eom_line_address_control_20261111/result.json')

def main():
    ROOT.mkdir(exist_ok=True);(ROOT/'02_work').mkdir(exist_ok=True)
    address_raw=json.loads(ADDRESS.read_text());assert not address_raw['is_error']
    address=address_raw['json_data'];assert address['confidence']['stable_connection']=='высокая'
    # Control-specific applicability checks; not a generic semantic verifier.
    assert address['stable_connection']['panel']=='ВРУ-3, РП5'
    assert address['stable_connection']['protective_device'].startswith('QF5.10')
    assert address['md_identifier']=='СЗ510'
    target_ref='blk_513e35c4f8a7432986e9a498d4573996'
    assert any(e['block_id']==target_ref for e in address['evidence'])
    payloads=[json.loads(p.read_text()) for p in SOURCES]
    second=deepcopy(payloads[1]);assert len(second['findings'])==1
    finding=second['findings'][0];assert 'СЭ510' in finding['problem']
    assert any(e['block_id']==target_ref for e in finding['evidence'])
    before=deepcopy(finding)
    display='ВРУ-3 / РП5 / QF5.10 / ЩУ/Л-3.2 (в MD: '+address['md_identifier']+')'
    for key in ('problem','description','solution'):
        if isinstance(finding.get(key),str):finding[key]=finding[key].replace('СЭ510',display)
    for e in finding['evidence']:
        if isinstance(e.get('location'),str):e['location']=e['location'].replace('СЭ510',display)
    finding['address_resolution']={'mode':'known_control_source_verified_adapter',
        'raw_candidate_sha256':hashlib.sha256(json.dumps(before,ensure_ascii=False,sort_keys=True).encode()).hexdigest(),
        'address_result_sha256':hashlib.sha256(ADDRESS.read_bytes()).hexdigest(),
        'previous_reported_identifier':'СЭ510','graphic_identifier':address['graphic_identifier'],
        'authoritative_md_identifier':address['md_identifier'],'resolved_report_address':address['resolved_report_address'],
        'literal_graphic_character_resolved':False,'stable_connection_verified':True,
        'autonomous_production_correction_proven':False}
    existing_refs={e['block_id'] for e in finding['evidence']}
    for e in address['evidence']:
        if e['block_id'] in ('blk_bbe9060267e84022afbb7bccfc8749b2','blk_5b30188592764230a7cd894838a60430') and e['block_id'] not in existing_refs:
            finding['evidence'].append({'block_id':e['block_id'],'location':e['location'],'description':e['observation']})
    (ROOT/'address_corrected_candidate.json').write_text(json.dumps(second,ensure_ascii=False,indent=2)+'\n')
    source=BASE/'03_analysis/runs/control';baseline=json.loads((source/'03_findings.json').read_text())
    combined=combine_findings_with_targeted(baseline,[('object_operations_cable_0',payloads[0]),('object_operations_cable_1_address_resolved',second)])
    assert len(combined['findings'])==len(baseline['findings'])+2
    original_ids={f['id'] for f in baseline['findings']}
    recovered=[f for f in combined['findings'] if f['id'] not in original_ids];assert len(recovered)==2
    for name in ('01_blocks_analysis.json','02_text_analysis.json','document_graph.json'):shutil.copyfile(source/name,ROOT/name)
    shutil.copyfile(BASE/'02_work/document.md',ROOT/'02_work/document.md')
    (ROOT/'03_findings.json').write_text(json.dumps(combined,ensure_ascii=False,indent=2)+'\n')
    stats=_run_deterministic_phase(ROOT,'EOM-cable-output-control')
    assert not any(stats.get(k) for k in ('critic_error','corrector_error','source_review_queue_error'))
    after=json.loads((ROOT/'03_findings.json').read_text());current={f['id']:f for f in after['findings']}
    assert len(current)==len(combined['findings'])
    for f in baseline['findings']+recovered:
        for field in ('problem','description','finding','solution','recommendation','severity','report_section'):
            assert f.get(field)==current[f['id']].get(field),(f['id'],field)
    wb=Workbook();build_project_sheet(wb,{'project_id':'EOM','sheet_name':'EOM','findings_json':after})
    path=ROOT/'report.xlsx';wb.save(path);wb.close()
    wb=load_workbook(path);rows=list(wb['EOM'].values);text='\n'.join(str(c) for row in rows for c in row if c is not None)
    column=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
    ids=[row[column] for row in rows if row[column] in current];assert len(ids)==len(current)==len(set(ids))
    for f in recovered:assert f['description'] in text
    for value in ('С2513','65 м','95 м','QF5.10','СЗ510'):assert value in text,value
    assert 'СЭ510' not in text
    wb.close()
    result={'scope':'Actual two model-produced candidates with bounded source-reviewed address adapter through real combine/deterministic/Excel functions.',
        'baseline_preserved':len(baseline['findings']),'output_findings':len(current),'recovered_ids':[f['id'] for f in recovered],
        'both_line_descriptions_preserved':True,'incorrect_literal_not_rendered':True,'MD_spelling_preserved':True,
        'ambiguous_graphic_character_kept_in_provenance':True,'all_excel_ids_unique':True,'verifier_stats':stats,
        'xlsx_path':str(path),'xlsx_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'source_bindings':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in [*SOURCES,ADDRESS]],
        'full_semantic_gate_complete':False,'autonomous_grouping_proven':False,
        'remaining_work':['Two rows express one shared specification issue; semantic grouping/workload acceptance remains open.',
            'Address adapter is control-specific and needs generic integration before full audits.',
            'Model absence verifier and pending source review queue are not completed by this export.'],
        'new_model_calls':0,'full_acceptance_proven':False}
    (HERE/'EOM_cable_output_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    shutil.copyfile(path,HERE/'EOM_cable_output_control.xlsx')
    print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__':main()
