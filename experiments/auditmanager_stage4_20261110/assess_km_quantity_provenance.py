"""Evaluator-only checks of literal source cells; never sent to audit executors."""
import hashlib
import json
from pathlib import Path
ROOT=Path('/tmp/auditmanager_km_quantity_provenance_control_20261111')
OUT=Path(__file__).parent

def assess():
    request=json.loads((ROOT/'request.json').read_text())
    packet=json.loads(request['messages'][-1]['content'])
    sources={b['block_id']:b['content'] for b in packet['source_context']}
    result=json.loads((ROOT/'result.json').read_text());assert not result['is_error']
    data=result['json_data'];operation=next(r for r in data['object_operation_results'] if r['operation']=='quantity_balance')
    derivation=operation['quantity_derivation'];rows=derivation['anchor_records'];assert len(rows)==8
    inspected=[];subtotal=0;unknown=[]
    for row in rows:
        text=sources[row['source_block']]
        table=[[c.strip() for c in line.strip().strip('|').split('|')] for line in text.splitlines() if line.startswith('|')]
        headers=[r for r in table if 'Количество' in r];assert len(headers)==1
        column=headers[0].index('Количество')
        anchors=[r for r in table if any('HSA-M10' in c for c in r)];assert len(anchors)==1
        literal=anchors[0][column];assert literal==row['literal_cell']
        if literal:
            expected=int(literal)*row['multiplicity'];assert expected==row['subtotal'];subtotal+=expected
        else:
            assert row['value'] is None;unknown.append(row['source_block'])
        inspected.append({'source_block':row['source_block'],'column_index_zero_based':column,'column_header':'Количество','literal_cell':literal,'source_row':anchors[0],'model_cell_matches_MD':True})
    assert subtotal==derivation['anchor_balance']['known_subtotal']==172
    assert set(unknown)=={'blk_ba972bbe49b944208eba2b5a9161faa9','blk_e587f49a7bce4c7bac350d00c17ba616'}
    assert any('ОГМ26' in f['description'] and 'ОГМ25' in f['description'] for f in data['findings'])
    assert not any(f['operation']=='quantity_balance' and f.get('report_section')=='technical_findings' for f in data['findings'])
    report={'scope':'Same-context known-control comparison; one request per variant, not blind or full-audit acceptance.',
        'previous_failure':'KM_LK6_quantity_assessment.json','new_status':'bounded_negative_control_passed',
        'source_rows_checked':inspected,'known_anchor_subtotal':subtotal,'unknown_anchor_terms':unknown,
        'false_confirmed_228_removed':True,'repeated_OGM22_25_instances_retained':True,
        'useful_marking_conflict_retained':True,'quantity_clarification_emitted':False,
        'remaining_positive_control_work':'Resolve applicable anchor detail or preserve a justified quantity clarification; final output path still pending.',
        'new_geometry_candidate':'50 mm plan/section level difference: literal values visually checked, common surface basis/applicability and classic duplication not yet established.',
        'enforcement_boundary':'Prompt instruction plus this source-cell evaluation; no general deterministic semantic enforcement for all quantities.',
        'causal_boundary':'Same source context and model, changed generic quantity instruction; single stochastic observation.',
        'full_acceptance_proven':False}
    for name in ('result','validated'):(OUT/f'KM_quantity_provenance_{name}.json').write_bytes((ROOT/(name+'.json')).read_bytes())
    (OUT/'KM_quantity_provenance_assessment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'source_cells_verified':len(inspected),'known_subtotal':subtotal,'unknown_terms':len(unknown),'false_228_removed':True}))
if __name__=='__main__':assess()
