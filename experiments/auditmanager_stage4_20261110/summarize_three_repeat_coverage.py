from pathlib import Path
import json,copy
from collections import Counter
from experiments.auditmanager_stage4_20261110.score_atomic_review import score,sha
from experiments.auditmanager_stage4_20261110.summarize_baseline_first_review import build
from experiments.auditmanager_stage4_20261110.verify_output_review_history import encode
h=Path('experiments/auditmanager_stage4_20261110');e=h/'evaluator_release_errata_v2.json';frozen=json.loads(e.read_text());results=[]
for rep in [1,2,3]:
 if rep<3:
  p=h/f'baseline_r{rep}_review_summary.json';old=json.loads(p.read_text());sources=[x['source'] for x in old['projects']]
  assert old['evaluator_sha256']==sha(e)
 else:
  sources=[]
  for disc in ['AK','AR','ITP','KM','VK','EOM']:
   qp=Path('/tmp/auditmanager_comparison_evaluation_20261111/baseline_r3')/(disc+'_output_review_queue.json')
   b=build(qp,h/(disc+'_r3_output_review_queue_receipt.json'));sources.extend(x['source'] for x in b['projects'])
 projects=[];totals={route:Counter() for route in frozen['truth_denominators']}
 for source in sources:
  lp=Path(source['ledger_path']);fp=Path(source['final_path']);assert sha(lp)==source['ledger_sha256'];assert sha(fp)==source['final_sha256']
  ledger=json.loads(lp.read_text());adapters=[]
  for row in ledger['targets']:
   if row['section_assessment']=='held_disputed_output_section_only':
    assert any(row['target_id'] in x['target_ids'] and 'report_section_accuracy' in x['held_dimensions'] for x in frozen['holds'])
    row['section_assessment']='unchecked';adapters.append(row['target_id'])
  v=score(e,fp,ledger)
  for route,x in v['routes'].items():assert x['review_complete'];totals[route].update(x['counts'])
  projects.append({'discipline':source['discipline'],'source':source,'routes':v['routes'],'section_accuracy_counts':v['section_accuracy_counts'],'section_adapters':adapters})
 assert len(projects)==6
 results.append({'repeat':rep,'projects':projects,'totals':{r:dict(c) for r,c in totals.items()}})
artifact={'status':'baseline_coverage_three_repeats_not_trial_acceptance','evaluator_path':str(e),'evaluator_sha256':sha(e),'repeats':results,'production_acceptance':False,'limits':['Each repeat uses the same 359 targets, including one truth hold; these are not independent projects.','Full target coverage is not correctness of every addition in a finding.','Technical targets, questions and negative controls must not be pooled as defect recall.','Negative absence does not establish successful filtering. Partial negative matches must remain visible.','Output claim-review errata remain separate; this report does not aggregate free-form verdict labels into precision.','Report sections, useful retention, model cost/time, engineer burden, experimental comparison and new-project transfer remain unaccepted.']}
p=h/'baseline_three_repeat_coverage_summary.json';assert not p.exists();p.write_bytes(encode(artifact))
for rep in results: print(rep['repeat'],rep['totals'])
