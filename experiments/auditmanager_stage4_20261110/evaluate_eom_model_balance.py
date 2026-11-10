"""Known four-circuit control computed from sealed model extractions.

Evaluation target selection and reviewed aliases are local to this case. They
are never imported into independent extraction requests or universal rules.
"""
import hashlib
import json
import re
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.object_balance import evaluate_object_balances

GRID=Path('/tmp/auditmanager_eom_graphic_recovery_20261110_grid')
SPLIT=Path('/tmp/auditmanager_eom_balance_inventory_control_20261110_split/EOM')
GRAPHIC='blk_4e968dd71c7149aca85e4195d5c3de49'
TABLE='blk_c5f0556f94c14503bfe178ef5941c077'


def evaluate(provider='astra'):
    grid = GRID if provider == 'astra' else Path('/tmp/auditmanager_eom_graphic_grid_claude_control_20261110')
    graphic=json.loads((grid/'coverage.json').read_text())
    table_result_path=SPLIT/'execution/object_inventory_002/result.json'
    table_result=json.loads(table_result_path.read_text())
    request=json.loads((SPLIT/'object_inventory_002.json').read_text())
    blocks=json.loads(request['messages'][1]['content'])['source_blocks']
    text=next(b['content'] for b in blocks if b['block_id']==TABLE)
    section=text[text.index('| РПЗ |'):text.index('| РП4 |')]
    source_rows={line.split('|')[1].strip():line for line in section.splitlines()
                 if re.match(r'^\|\s*C\d+\s*\|',line)}
    # Use actual quoted rows from model object records, and verify each against
    # the full authoritative subsection; no hand-filled right-hand inventory.
    extracted={}
    for obj in table_result['json_data']['object_registry']:
        if TABLE not in obj['source_refs']:continue
        for parameter in obj.get('observed_parameters',[]):
            quote=parameter.get('quote','')
            if not isinstance(quote,str):continue
            match=re.match(r'^\|\s*(C\d+)\s*\|',quote)
            if not match or match[1] not in source_rows:continue
            assert quote in section
            assert match[1] not in extracted, 'Ambiguous table object identity'
            extracted[match[1]]={'object_id':obj['id'],'quote':quote,'source_ref':TABLE}
    assert set(extracted)==set(source_rows), 'Model did not extract every row in table scope'
    selected={'C2325','C2326','C2327','C2328'}
    items=[];lineage=[]
    for obj in graphic['objects']:
        if provider == 'astra':
            values = [p.get('value') for p in obj.get('observed_parameters', [])]
        else:
            fields = {p.get('name'): p.get('value') for p in obj.get('observed_parameters', [])}
            values = [{'панель': fields.get('вышестоящий щит'),
                       'марка_цепи': fields.get('метка цепи'), 'роль': fields.get('роль'),
                       'original_parameters': obj.get('observed_parameters', [])}]
        for value in values:
            if not isinstance(value,dict) or value.get('панель')!='РП3':continue
            mark=value.get('марка_цепи','')
            if not isinstance(mark,str):continue
            key=mark.replace('С','C')
            if key not in selected:continue
            role={'рабочая':'active','активная':'active','Резерв':'reserve'}[value['роль']]
            items.append({'key':['РП3',key],'role':role,'source_refs':[GRAPHIC]})
            lineage.append({'object_id':obj['id'],'literal_mark':mark,'parameters':value})
    assert len(items)==4 and {i['key'][1] for i in items}==selected
    operation={'id':'EOM27_model_extracted_four_circuits','kind':'set_balance',
        'scope':'Only four known control branches C2325–C2328 within source-bound VRU2/RP3; all RP3 table rows inspected.',
        'left':{'scope_refs':[GRAPHIC],'items':items},
        'right':{'scope_refs':[TABLE],'items':[{'key':['РП3',mark],'role':'active','source_refs':[TABLE]}
                                              for mark in extracted if mark in selected]}}
    result=evaluate_object_balances([operation],{GRAPHIC,TABLE})
    computed,=result['operations'];assert computed['status']=='computed'
    assert [r['key'][1] for r in computed['only_left']]==['C2326','C2327']
    assert computed['reserves']['left']==[['РП3','C2325'],['РП3','C2328']]
    tiles=json.loads((GRID/'manifest.json').read_text())['source_tiles']['tiles']
    return {'provider':provider,'operation':operation,'execution':result,'model_graphic_lineage':lineage,
        'model_table_rows':extracted,'full_table_scope_row_count':len(source_rows),
        'graphic_result_sha256':hashlib.sha256((grid/'result.json').read_bytes()).hexdigest(),
        'table_result_sha256':hashlib.sha256(table_result_path.read_bytes()).hexdigest(),
        'directly_viewed_control_tiles':[t for t in tiles if t['grid_row'] in (0,1) and t['grid_column']==5],
        'bounded_source_review':'Two active2kWbranches, their protective apparatus and reserves checked on tiles5/13. Table contains24explicit individual rows; no target active row. Cable main sections3x4 and3x6 agree with this graphic; this is not whole-circuit engineering verification.',
        'case_aliases':'Cyrillic С and Latin C; graphic РП3 and authoritative TEXT РПЗ are compared only in this explicitly reviewed source scope. TEXT unchanged.',
        'control_pair_passed':True,'independent_full_project_discovery':False,
        'full_graphic_invalid_objects':len(graphic['invalid_objects']),
        'limits':['Bounded control chosen from known case; not complete scheme comparison.',
                  f"{len(graphic['invalid_objects'])} invalid graphic records remain excluded; record validity is not physical identity proof.",
                  'Aggregation into other load rows has not been ruled out; do not add4kW or claim overload.',
                  'No production release or full six-project improvement established.']}


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--provider',choices=['astra','claude'],default='astra')
    args=parser.parse_args()
    report=evaluate(args.provider)
    filename='EOM_model_extracted_balance_control.json' if args.provider=='astra' else 'EOM_model_extracted_balance_claude_control.json'
    Path(__file__).with_name(filename).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'control_pair_passed':report['control_pair_passed'],'table_rows':report['full_table_scope_row_count'],
                      'unresolved_invalid_graphic_objects':report['full_graphic_invalid_objects']}))
