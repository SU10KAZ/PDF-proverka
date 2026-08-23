# -*- coding: utf-8 -*-
"""G8 — is object_id stable?  Share of ids preserved after each class-A rewrite.

The id is sha1(class, descriptor rounded to 2 decimals, centre quantised to 0.5 pt).
Usage:  grp_g8_objectid.py
"""
import sys,json,random,statistics
sys.path.insert(0,'/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_objects_v03_opus/probes')
import grp_common as G
s=json.load(open(G.ART/'grp_sample.json',encoding='utf-8'))
blocks=[b for b in s['blocks'] if 100<=b['n_seg']<=6000]
rng=random.Random(1); rng.shuffle(blocks); blocks=blocks[:40]
res={}
for b in blocks:
    pb=G.prepared_block(b['doc_id'],b['version'],b['block_id'])
    if pb is None: continue
    ex=G.extract(pb)
    if not ex.segments: continue
    s0=G.rw_identity(ex.segments,random.Random(7)); L0=G.layer_of(s0,ex.texts)
    ids0=set(o['object_id'] for o in L0.objects)
    for nm in ('A1_path_split','A2_path_merge','A5_order_shuffle','A8_lineweight','A6_round_0.01','A6_round_0.25','A4b_circle_to_chords5'):
        segs=G.REWRITES[nm](ex.segments,random.Random(7))
        if G.rewrite_bite(nm,ex.segments,segs)<=0: continue
        L=G.layer_of(segs,ex.texts)
        ids=set(o['object_id'] for o in L.objects)
        res.setdefault(nm,[]).append(len(ids0&ids)/max(1,len(ids0)))
out={k:{'n':len(v),'median':round(statistics.median(v),4),'mean':round(statistics.mean(v),4),'min':round(min(v),4)} for k,v in res.items()}
print(json.dumps(out,ensure_ascii=False,indent=1))
json.dump({'n_blocks':len(blocks),'object_id_preserved':out},open(G.ART/'grp_object_id_stability.json','w',encoding='utf-8'),ensure_ascii=False,indent=1)
