import json,re,hashlib,shutil
from pathlib import Path
from decimal import Decimal as D
root=Path('/tmp/auditmanager_eom_source_review_astra_20261110')
out=Path('experiments/auditmanager_stage4_20261110')
r=json.loads((root/'result.json').read_text()); request=json.loads((root/'request.json').read_text())
payload={p['finding_id']:p for p in json.loads(request['messages'][1]['content'])}
reviews={p['finding_id']:p for p in r['json_data']['reviews']}
checks=[]
for fid in reviews:
 for claim in reviews[fid]['claims']:
  for ev in claim.get('evidence',[]):
   source=next(s for s in payload[fid]['sources'] if s['block_id']==ev['block_id'])
   assert ev['quote'] in source['authoritative_text']
for col,cid,expected in [(2,'C1','94.5'),(3,'C2','89.9')]:
 source=payload['F-003']['sources'][0]['authoritative_text']
 rows={}
 for line in source.splitlines():
  cells=[c.strip() for c in line.strip().strip('|').split('|')]
  if re.fullmatch(r'C25\d+',cells[0]):
   assert cells[0] not in rows
   rows[cells[0]]=D(cells[col].replace(',','.'))
 claim=next(c for c in reviews['F-003']['claims'] if c['claim_id']==cid)
 op=claim['operations'][0]
 modelrows={k:D(str(v)) for k,v in op['operands_by_row'].items()}
 assert rows==modelrows and len(rows)==24
 assert sum(rows.values())==D(expected)==D(str(op['result']))
 checks.append({'finding_id':'F-003','claim_id':cid,'source_rows':len(rows),'all_model_operands_match_source':True,'sum':str(sum(rows.values()))})
op=reviews['F-040']['claims'][0]['operations'][0]
modelrows={}
for group in op['operands_grouped_by_equal_value']:
 assert D(str(group['value_each']))*len(group['rows'])==D(str(group['subtotal']))
 for row in group['rows']:
  assert row not in modelrows
  modelrows[row]=D(str(group['value_each']))
for source in payload['F-040']['sources']:
 rows={}
 for line in source['authoritative_text'].splitlines():
  cells=[c.strip() for c in line.strip().strip('|').split('|')]
  if re.fullmatch(r'15\d\d',cells[0]):
   assert cells[0] not in rows
   rows[cells[0]]=D(cells[3].replace('м²','').strip().replace(',','.'))
 assert set(rows)=={str(i) for i in range(1501,1590)}
 assert rows==modelrows
 assert sum(rows.values())==D('1636.5')==D(str(op['result']))
 checks.append({'finding_id':'F-040','block_id':source['block_id'],'source_rows':89,'all_model_operands_match_source':True,'sum':str(sum(rows.values()))})
for name in ('result.json','validation.json','attempt.json','manifest.json'):
 shutil.copyfile(root/name,out/('EOM_astra_pilot_'+name))
report={'scope':'Known-fragment verifier pilot; not independent discovery or six-project acceptance','request_sha256':hashlib.sha256((root/'request.json').read_bytes()).hexdigest(),'model':r['model'],'duration_ms':r['duration_ms'],'input_tokens':r['input_tokens'],'output_tokens':r['output_tokens'],'cached_tokens':r['cached_tokens'],'cost_source':r['cost_source'],'monetary_cost_known':False,'all_exact_quotes_verified':True,'arithmetic_checks':checks,'control_pair_passed':True,'semantic_assessment':{'F-003':'Retain arithmetic discrepancy 0.4 kW. Cause, fire simultaneity and VRU-2 identity remain unproven. Model correctly separates these claims. C6 combines justified checking with unproven mandatory changes; do not suppress the supported core because C6 is needs_data.','F-040':'Reject alleged sum mismatch: both full 89-row inventories sum to 1636.5. Missing plan is not evidence of a new defect and must not automatically create an engineer question.'},'automatic_application':False,'stage4_complete':False,'limitations':['One request, two known cases, one discipline, no graphics','No demonstrated independent finding recall','No proof of OS-level history isolation','Receipt validation alone does not accept semantics','No extrapolation of single-call duration to whole-project audits']}
(out/'EOM_astra_pilot_assessment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
statepath=out/'stage4_readiness.json'
state=json.loads(statepath.read_text())
state['eom_astra_known_pair_pilot']={'status':'passed_bounded_control_pair','report':str(out/'EOM_astra_pilot_assessment.json'),'model':r['model'],'duration_ms':r['duration_ms'],'new_independent_audit':False,'automatic_application':False,'remaining':'Graphic/context follow-up controls, semantic application integration and full six-project candidate audits remain incomplete.'}
statepath.write_text(json.dumps(state,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False,indent=2))
