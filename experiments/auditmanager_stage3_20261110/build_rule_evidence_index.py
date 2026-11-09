"""Bind the existing 145-rule assessment to current code and adjudicated runtime targets.
This creates an evidence index, not automatic approval of implementation or transfer.
"""
import hashlib
import json
import re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
OLD=ROOT/'experiments/auditmanager_v4_transfer_20261105'
S2=ROOT/'experiments/auditmanager_stage2_20261110'

def read(p): return json.loads(p.read_text())
def ref(p): return {'path':str(p.relative_to(ROOT)), 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}

# Semantic links reviewed against each bounded stage2 target; not inferred from words alone.
LINKS={
 'AK14':['C-03','C-16','C-22','AK-002'],
 'AR14_K3_support_levels':['C-07','C-09','AR-003'],
 'AR35_unlabelled_areas':['C-05','C-06','AR-002'],
 'AR39_lower_profile_and_rope':['C-12','AR-002','AR-009'],
 'AR59_rope_load_path':['C-12','C-21','AR-009'],
 'EOM11':['C-04','C-06','C-10','EOM-001','EOM-009'],
 'EOM111_outdoor_unit_identifier':['C-03','EOM-007'],
 'EOM27_active_socket_circuits':['C-08','C-10','EOM-004'],
 'EOM5_shunt_trip_identity':['C-14','EOM-008','EOM-010'],
 'ITP17_grooved_connection_identity':['C-11','C-14','ITP-015','ITP-022'],
 'ITP58_GVS_branch_flows':['C-09','ITP-008','ITP-009'],
 'ITP68_grooved_scope':['C-13','ITP-021'],
 'ITP72_wall_and_velocity':['C-07','ITP-017'],
 'KM14_LK6_repetitions_conditional_totals':['C-08','C-10','C-18','KM-005','KM-006'],
 'KM5_S3_bent_handrail_developed_length':['C-07','KM-001','KM-002','KM-003'],
 'KM9_LK5_landing_elevation':['C-07','KM-008'],
 'VK50_specification_duplicates':['C-03','C-08','VK-007','VK-009'],
 'VK20_sensor_count_identity':['C-10','VK-005'],
 'VK34_transition_assembly':['C-11','VK-001','VK-006','VK-016'],
 'VK51_apartment_scope':['C-04','VK-010'],
}

def main():
 matrix=read(OLD/'matrix_145.json');sources=read(OLD/'implementation_sources.json');assert len(matrix)==145 and len({r['ID'] for r in matrix})==145
 code={}
 for source in sources:
  p=ROOT/source['path'];code[source['id']]={**ref(p),'historical_sha256':source['sha256'],'unchanged_since_report':hashlib.sha256(p.read_bytes()).hexdigest()==source['sha256'],'symbol':source['symbol'],'historical_line':source['line']}
 targets=[]
 for disc in ['AK','AR','EOM','ITP','KM','VK']:
  p=S2/f'historical_{disc}_matching.json'
  for i,t in enumerate(read(p)['results']):targets.append({'target_id':t['target_id'],'route':t['route'],'final_match':t['final_match'],'evidence':ref(p),'pointer':f'/results/{i}','rule_ids':LINKS[t['target_id'].split(':')[0]]})
 assert len(targets)==21
 ids={r['ID'] for r in matrix};assert all(set(t['rule_ids'])<=ids for t in targets)
 rows=[]
 for original in matrix:
  used=re.findall(r'(?:^|\n)([A-Z0-9_]+): ',original['Источник_реализации'])
  assert used and all(x in code for x in used),(original['ID'],used)
  refs=[dict(source_id=x,**code[x]) for x in used]
  rows.append({'id':original['ID'],'discipline':original['Дисциплина'],'meaning':original['Смысл'],'historical_assessment':original,'implementation_sources_current':refs,'source_identity_status':'unchanged' if all(x['unchanged_since_report'] for x in refs) else 'changed_requires_claim_review','bounded_runtime_targets':[t for t in targets if original['ID'] in t['rule_ids']],'current_behavior_verified_for_entire_rule':False,'transfer_decision_status':'pending_semantic_review','warning':'A matching source hash preserves the old static evidence only. A target match or gap tests its bounded operation, not the whole rule.'})
 out={'purpose':'Current evidence index for stage3; all145 rows retained, no automatic transfer decisions','source_matrix':ref(OLD/'matrix_145.json'),'rows':rows,'sources':code,'summary':{'rules':len(rows),'sources':len(code),'unchanged_sources':sum(x['unchanged_since_report'] for x in code.values()),'rules_with_runtime_target_links':sum(bool(x['bounded_runtime_targets']) for x in rows),'target_links_cover_unique_targets':len(targets)},'stage3_complete':False,'production_changed':False}
 (HERE/'rule_evidence_index.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps(out['summary']))
if __name__=='__main__':main()
