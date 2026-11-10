"""Structural accounting only; reading accuracy requires source review."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry
from backend.app.pipeline.stages.text_analysis.inventory_record_ids import separate_colliding_record_ids

ROOT=Path('/tmp/auditmanager_ak_cabinet_inventory_control_20261110/AK')
OUT=Path(__file__).with_name('AK_cabinet_inventory_progress.json')

def assess():
    manifest=json.loads((ROOT/'manifest.json').read_text())
    rows=[]
    for packet in manifest['packets']:
        path=ROOT/'execution'/packet['stage']/'result.json'
        row={'stage':packet['stage'],'source_blocks':packet['block_ids'],'state':'result_not_saved'}
        rows.append(row)
        if not path.exists():continue
        result=json.loads(path.read_text())
        row.update(result_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),duration_ms=result.get('duration_ms'),
                   input_tokens=result.get('input_tokens'),output_tokens=result.get('output_tokens'))
        if result.get('is_error') or not isinstance(result.get('json_data'),dict):
            row['state']='failed';continue
        raw=result['json_data']
        normalized,repairs=separate_colliding_record_ids(raw)
        coverage=reconcile_object_registry(normalized,set(packet['block_ids']))
        row.update(state='response_received',objects=len(coverage['objects']),invalid_objects=len(coverage['invalid_objects']),
                   record_id_repairs=repairs,unresolved_source_blocks=coverage['unresolved_source_blocks'],
                   unresolved_operations=coverage['unresolved_operations'],source_inventory=coverage['source_inventory'],
                   candidate_count=len(raw.get('text_findings',[])),semantic_verified=False)
    report={'scope':'Five original AK14 source drawings; known-source selection, no answers in model request.',
            'packets':rows,'completed_responses':sum(r['state']=='response_received' for r in rows),
            'total_packets':len(rows),'semantic_verified':False,'stage4_complete':False,
            'next_checks':['Inspect actual terminal addressing and circuit boundaries against graphics.',
                'Run identity operation; inventory response does not establish that this check ran.',
                'Preserve all five addresses when combining a systemic finding; do not count detector/view duplicates.',
                'No general suppression of repeated terminals without a confirmed negative control.']}
    OUT.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return report

if __name__=='__main__':
    report=assess();print(json.dumps({k:v for k,v in report.items() if k!='packets'},ensure_ascii=False))
