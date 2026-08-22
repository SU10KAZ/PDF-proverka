"""VERIFY C5: trivial baselines and tuning-sensitivity for the retrieval half of the claim.

Adversarial points:
  1. bucket hash recall 0.70 with 0 cross-collisions - what does a ONE-number baseline get?
  2. is the quantisation (round(x*2)/2) a sweet spot found on the same 20 blocks?
  3. which 7 pairs does the bucket hash recover - are they the two SAME-FILE pairs (O1)?
  4. 34-dim descriptor rank-1 19/20 - what does 1-dim / 2-dim get? and what happens on the only
     two pairs that exercise real change (O2)?
  5. 'no_grid -> 20/20' is the best of 7 variants scored on the same 20 items.
"""
import json, math, hashlib, itertools, collections, sys
from pathlib import Path
A = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_architecture_opus/artifacts")
F = json.loads((A/"signoise_03_block_features.json").read_text(encoding="utf-8"))
names = list(F)
SAME_FILE = {"ar_plan", "ar_wall_sections"}          # O1
REAL_CHANGE = {"ss_scheme_text_changed", "eom_singleline_changed"}   # O2
ANGLE_BINS = ("h_0","d_45","v_90","d_135","other")

def report(keys, label):
    g = collections.defaultdict(list)
    for n in names: g[keys[n]].append(n)
    hits, cross = [], []
    for a,b in itertools.combinations(names,2):
        if keys[a]!=keys[b]: continue
        (hits if a.split("/")[0]==b.split("/")[0] else cross).append((a,b))
    return {"label":label, "recall": len(hits)/10, "hits":[h[0].split("/")[0] for h in hits],
            "cross": len(cross), "buckets": len(g), "largest": max(len(v) for v in g.values())}

def bucket(f, q=2.0, use=("seg","txt","cmp","ang","lab","asp")):
    c=f["coarse"]; p=[]
    if "seg" in use: p.append(round(c["log_segments"]*q)/q)
    if "txt" in use: p.append(round(c["log_texts"]*q)/q)
    if "cmp" in use: p.append(round(c["log_components"]*q)/q)
    if "ang" in use: p.append(max(ANGLE_BINS, key=lambda n: c["angle_shares"][n]))
    if "lab" in use: p.append(round(c["category_shares"]["label"]*4)/4)
    if "asp" in use: p.append(round(min(c["aspect_ratio"],8.0)*2)/2)
    return hashlib.sha256(json.dumps(p).encode()).hexdigest()[:16]

print("=== 1/3. probe's 6-number bucket hash vs trivial baselines (q=2) ===")
for use in [("seg","txt","cmp","ang","lab","asp"), ("seg",), ("txt",), ("seg","txt"), ("seg","txt","cmp")]:
    r = report({n: bucket(F[n], 2.0, use) for n in names}, "+".join(use))
    print(f"  {r['label']:28s} recall {r['recall']:.2f}  cross-collisions {r['cross']}  "
          f"buckets {r['buckets']:2d}  recovered={sorted(r['hits'])}")

print("\n=== 2. quantisation sensitivity of the SAME 6-number design ===")
for q in (0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0):
    r = report({n: bucket(F[n], q) for n in names}, f"q={q}")
    print(f"  q={q:4.1f}  recall {r['recall']:.2f}  cross-collisions {r['cross']}  buckets {r['buckets']:2d}  "
          f"recovered={sorted(r['hits'])}")

print("\n=== 4/5. nearest-neighbour retrieval: trivial descriptors vs the 34-dim one ===")
def vec(f, groups):
    c=f["coarse"]; o=[]
    if "angles" in groups: o += [c["angle_shares"][n] for n in ANGLE_BINS]
    if "grid" in groups:   o += list(c["grid_shares"])
    if "counts" in groups: o += [c["log_segments"], c["log_components"], c["log_texts"]]
    if "topology" in groups: o += [c["endpoint_ratio"], c["branch_ratio"], c["t_junction_ratio"], c["closed_ratio"]]
    if "textcat" in groups: o += [c["category_shares"][n] for n in ("label","numeric","engineering_value")]
    if "shape" in groups: o += [c["length_mean_norm"], c["length_p90_norm"], c["aspect_ratio"]]
    if "segonly" in groups: o += [c["log_segments"]]
    if "segtxt" in groups: o += [c["log_segments"], c["log_texts"]]
    return o
def z(rows):
    n,d=len(rows),len(rows[0]); out=[[0.0]*d for _ in range(n)]
    for j in range(d):
        col=[r[j] for r in rows]; m=sum(col)/n
        sd=math.sqrt(sum((v-m)**2 for v in col)/n) or 1.0
        for i in range(n): out[i][j]=(col[i]-m)/sd
    return out
def retr(groups):
    rows=z([vec(F[n],groups) for n in names])
    cp={n: n.split("/")[0]+"/"+("right" if n.endswith("left") else "left") for n in names}
    ranks={}
    for i,n in enumerate(names):
        d=sorted(((math.dist(rows[i],rows[j]), names[j]) for j in range(len(names)) if j!=i))
        order=[x[1] for x in d]; ranks[n]=order.index(cp[n])+1
    top1=sum(1 for v in ranks.values() if v==1)
    rc=[n for n in names if n.split("/")[0] in REAL_CHANGE]
    sf=[n for n in names if n.split("/")[0] in SAME_FILE]
    return top1, ranks, sum(1 for n in rc if ranks[n]==1), len(rc), sum(1 for n in sf if ranks[n]==1), len(sf)
for label,g in [("34-dim full",("angles","grid","counts","topology","textcat","shape")),
                ("no_grid (claim's best)",("angles","counts","topology","textcat","shape")),
                ("counts_only (3 dims)",("counts",)),
                ("log_segments ONLY (1 dim)",("segonly",)),
                ("log_segments+log_texts (2 dims)",("segtxt",))]:
    t,ranks,rc1,rcn,sf1,sfn = retr(g)
    print(f"  {label:32s} top1 {t}/20   on the {rcn} REAL-CHANGE queries: {rc1}/{rcn}   "
          f"on the {sfn} SAME-FILE queries: {sf1}/{sfn}")
    if t < 20:
        print("      misses:", {n:r for n,r in ranks.items() if r!=1})
