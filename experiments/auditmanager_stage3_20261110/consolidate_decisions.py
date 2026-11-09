"""Verify evidence identity and consolidate reviewed transfer decisions (no inference)."""
import hashlib
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent

def read(p):return json.loads(p.read_text())
def ref(p):return {'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
def verify(r):
 p=Path(r['path']);p=p if p.is_absolute() else ROOT/p
 assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256'],str(p)

def main():
 index=read(HERE/'rule_evidence_index.json');hyp=read(HERE/'selected_hypotheses.json')
 files=[HERE/'common_rule_decisions.json']+[HERE/f'{d}_rule_decisions.json' for d in ['AK','ITP','AR','KM','VK','EOM']]
 decisions=[r for f in files for r in read(f)['decisions']];by={r['id']:r for r in decisions};assert len(decisions)==len(by)==145
 assert set(by)=={r['id'] for r in index['rows']}
 hids={h['id'] for h in hyp['hypotheses']}|{'evaluation'}
 for h in hyp['hypotheses']:
  assert h['positive_controls'] and h['negative_controls_and_limits'] and h['implementation_locations']
  for r in h['evidence']:verify(r)
 for r in index['sources'].values():verify(r)
 rows=[]
 for source in index['rows']:
  d=by[source['id']];assert set(d['hypotheses'])<=hids
  assert d['behavior'] and d['verification'] and d['recommendation']
  for t in source['bounded_runtime_targets']:verify(t['evidence'])
  old=source['historical_assessment']
  rows.append({'id':d['id'],'discipline':source['discipline'],'meaning':source['meaning'],'v4_operation':old['Проверка_v4'],'current_representation':old['Представлено_в_AuditManager'],'implementation_status':old['Статус'],'implementation_sources':source['implementation_sources_current'],'evidence_boundary':{'static':'30/31 source files unchanged since report; changed corrector matches diagnosed run. Status describes partial/static support, not complete operation execution.','runtime_targets':source['bounded_runtime_targets'],'whole_rule_runtime_verified':False},'historical_cases':old['Исторические_случаи'],'historical_fragments':old['Проверенные_фрагменты'],'recommendation':d['recommendation'],'proposed_behavior':d['behavior'],'destination':d['destination'],'change_point':old['Место_изменения'],'hypotheses':d['hypotheses'],'expected_benefit':old['Польза'],'regression_risk':old['Риск'],'new_control_boundary':d.get('control_and_nontransfer_boundary',d.get('regression_risk')),'verification':d['verification'],'historical_control_pairs':old['Контрольные_пары'],'pair_evidence_limit':'Historical pair labels require the frozen atom adjudication before acceptance. New synthetic controls do not substitute a confirmed historical pair.','implementation_accepted':False})
 out={'purpose':'Reviewed transfer design for all145rules; not a finding recall score or proof of implementation','rules':rows,'inputs':[ref(f) for f in files]+[ref(HERE/'rule_evidence_index.json'),ref(HERE/'selected_hypotheses.json')],'production_changed':False}
 (HERE/'transfer_matrix_145.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps({'rules':len(rows),'hypotheses':len(hyp['hypotheses']),'current_source_hashes_verified':len(index['sources']),'runtime_linked_rules':sum(bool(r['evidence_boundary']['runtime_targets']) for r in rows)}))
if __name__=='__main__':main()
