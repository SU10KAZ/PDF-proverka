# -*- coding: utf-8 -*-
"""Regression guard: the fields added to ink_changes for rule v2 (`inside_pad`,
`margin`, `border_seg`, the `v2` aggregate) must not change ANY rule-v1 number, so
that the main run mov_runs/cf_*.jsonl stays comparable with the code as it now
stands.  Re-runs four recorded instances and diffs the score dict.
    python probes/mov_regress_v1.py
"""
import sys, json
P='experiments/stage_comparison_vector_objects_v03_opus'
sys.path.insert(0,P+'/probes')
import mov_common as MC, grp_common as G, v03_objects as O, v03_counterfactual as C
import mov_m2_cf as M2
rows=[json.loads(l) for l in open(P+'/artifacts/mov_runs/cf_2.jsonl',encoding='utf-8')]
want=[r for r in rows if r.get('block_id','').startswith('6GNF') and r.get('tag') in
      ('B1_translate@0.02','C3_move_object@large@0.005','B3_crop_jitter@0.02','A1_path_split')]
print('checking',len(want))
pb=G.prepared_block(want[0]['doc_id'],want[0]['version'],want[0]['block_id'])
ex=G.extract(pb); L=O.build_objects(ex)
for r in want:
    ex2,man=C.apply(ex,L,r['cf_id'],**r['params'])
    out,rep,LA,LB=MC.compare(ex,ex2,modes=('strict',))
    sc=M2.score(out,man,ex,ex2)
    old=r['score']
    diff={k:(old.get(k),sc.get(k)) for k in ('n_findings','moved_ink_share','lost_ink_share','new_ink_share','move_detected','localised') if old.get(k)!=sc.get(k)}
    print(r['tag'],'verdict',r['verdict'],out['verdict'],'DIFF' if diff else 'SAME', diff)
