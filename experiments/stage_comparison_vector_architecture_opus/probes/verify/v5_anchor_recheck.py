"""VERIFY C4: re-measure anchor ambiguity WITHOUT the 300-span cap and with the metric the claim
actually needs.

Adversarial points tested:
  1. probe 7 samples description['texts'][:300] - the FIRST 300 spans in PDF order, not a random
     sample - for exactly the three blocks the claim quotes (ar_plan 836, ss_plan_dense 522,
     vk_nodes 421). Re-run on ALL spans.
  2. the claim uses 'primitives inside the 0.035 anchor radius' to argue the `high` label
     (assigned at <= 0.012) is uninformative. Count within 0.012 too - that is the radius the
     label is about.
  3. a tie between primitives is only harmful if the tied primitives are DIFFERENT things.
     Measure how far apart the tied candidates are (max pairwise centroid distance, and whether
     they are pieces of one local cluster).
"""
import json, sys, math, collections
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
from experiments.stage_comparison_vector_blocks import extractor as E

DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")
MAXD, HIGH, EPS = 0.035, 0.012, 0.002
BLOCKS = ["ss_plan_dense", "vk_node_plan", "ar_plan", "vk_nodes", "vk_plan", "ar_wall_sections"]

for name in BLOCKS:
    d = json.loads((DESC/name/"left"/"vector_block.json").read_text(encoding="utf-8"))
    P = d["geometry"]["primitives"]
    segs = E._all_segments(P)
    cell = MAXD
    grid = collections.defaultdict(list)
    for i, s in enumerate(segs):
        b = E._bbox([s["p1"], s["p2"]])
        for gx in range(math.floor((b[0]-MAXD)/cell), math.floor((b[2]+MAXD)/cell)+1):
            for gy in range(math.floor((b[1]-MAXD)/cell), math.floor((b[3]+MAXD)/cell)+1):
                grid[(gx, gy)].append(i)
    # primitive centroids for the tie-spread test
    cen = {}
    for p in P:
        bb = p["normalized"]["bbox"]
        cen[p["id"]] = ((bb[0]+bb[2])/2, (bb[1]+bb[3])/2)

    ties, within35, within12, near = [], [], [], []
    tie_spread, tie_spread_high = [], []
    ties_high, within12_high = [], []
    for t in d["texts"]:
        pt = (t["x_norm"], t["y_norm"])
        best = {}
        for i in grid.get((math.floor(pt[0]/cell), math.floor(pt[1]/cell)), []):
            s = segs[i]
            dist = E._point_segment_distance(pt, [s["p1"], s["p2"]])
            k = s["primitive_id"]
            if k not in best or dist < best[k]:
                best[k] = dist
        if not best:
            ties.append(0); within35.append(0); within12.append(0); continue
        mn = min(best.values())
        near.append(mn)
        tied = [k for k, v in best.items() if v <= mn+EPS]
        ties.append(len(tied))
        within35.append(sum(1 for v in best.values() if v <= MAXD))
        within12.append(sum(1 for v in best.values() if v <= HIGH))
        if len(tied) > 1:
            pts = [cen[k] for k in tied]
            spread = max(math.dist(a, b) for a in pts for b in pts)
            tie_spread.append(spread)
        if mn <= HIGH:                      # this span is what the extractor calls `high`
            ties_high.append(len(tied))
            within12_high.append(sum(1 for v in best.values() if v <= HIGH))
            if len(tied) > 1:
                pts = [cen[k] for k in tied]
                tie_spread_high.append(max(math.dist(a, b) for a in pts for b in pts))
    n = len(ties)
    amb = sum(1 for c in ties if c > 1)/max(n,1)
    def med(xs): 
        xs = sorted(xs); return xs[len(xs)//2] if xs else None
    print(f"{name}: texts_all={n} (probe used {min(n,300)})")
    print(f"   mean within 0.035 = {sum(within35)/n:.2f}   (probe, first 300: see artifact)")
    print(f"   mean within 0.012 = {sum(within12)/n:.2f}   median {med(within12)}")
    print(f"   ambiguous (>=2 tied within {EPS}) = {100*amb:.1f} %   mean ties {sum(ties)/n:.3f}")
    if ties_high:
        print(f"   ON `high` SPANS ONLY (n={len(ties_high)}): mean ties {sum(ties_high)/len(ties_high):.3f}, "
              f"ambiguous {100*sum(1 for c in ties_high if c>1)/len(ties_high):.1f} %, "
              f"mean primitives within 0.012 = {sum(within12_high)/len(within12_high):.2f}")
    if tie_spread:
        s = sorted(tie_spread)
        print(f"   tie spread (max pairwise centroid dist of tied candidates): median {s[len(s)//2]:.4f}, "
              f"p90 {s[int(0.9*len(s))]:.4f}, share tied-candidates-within-0.02 = "
              f"{100*sum(1 for x in tie_spread if x <= 0.02)/len(tie_spread):.1f} %")
    del d
