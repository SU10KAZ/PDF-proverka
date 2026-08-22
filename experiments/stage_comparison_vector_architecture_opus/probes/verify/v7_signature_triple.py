"""REFUTATION TEST for C5(a): 'the three signature levels carry zero information about each other,
level_2 and level_3 equality agree 190/190, only two triples exist'.

190/190 agreement is measured on a target with essentially no variance: 189 of 190 pairings are
(F,F,F). A constant 'always disagree' predictor scores 189/190. The nested design (l1 = raw+style,
l2 = normalized+style, l3 = counters only) PREDICTS the intermediate triples (F,T,T) and (F,F,T);
they are simply absent from a 20-block corpus that contains no near-duplicate-but-not-identical
blocks. Construct one and the 'zero information' reading collapses.

Case built here: the same drawing, same crop, shifted on the sheet (the 0.5 % crop jitter the probe
itself measures in signoise_05) -> l1 F, l2 F, l3 T.
"""
import json, sys, collections
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
from experiments.stage_comparison_vector_blocks import extractor as E

DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")

def sig(d):
    topo = E._topology(d["geometry"]["primitives"], d["topology"]["tolerance_norm"], 8000)
    return E._signatures(d["geometry"]["primitives"], d["texts"], topo), topo

def shift(d, dx, dy):
    d = json.loads(json.dumps(d))
    for p in d["geometry"]["primitives"]:
        for sp in ("raw", "normalized"):
            b = p[sp]["bbox"]; p[sp]["bbox"] = [b[0]+dx, b[1]+dy, b[2]+dx, b[3]+dy]
            p[sp]["segments"] = [[[a[0]+dx, a[1]+dy], [c[0]+dx, c[1]+dy]] for a, c in p[sp]["segments"]]
    for t in d["texts"]:
        t["x_norm"] += dx; t["y_norm"] += dy
        t["bbox"] = [t["bbox"][0]+dx, t["bbox"][1]+dy, t["bbox"][2]+dx, t["bbox"][3]+dy]
    return d

for name in ("ss_table_graphic", "ss_simple_node", "ss_scheme_text_changed"):
    d = json.loads((DESC/name/"left"/"vector_block.json").read_text(encoding="utf-8"))
    s0, t0 = sig(d)
    d2 = shift(d, 0.005, 0.0)          # 0.5 % of the block, the probe's own perturbation size
    s1, t1 = sig(d2)
    trip = (s0["level_1_exact_vector"] == s1["level_1_exact_vector"],
            s0["level_2_normalized_geometry"] == s1["level_2_normalized_geometry"],
            s0["level_3_structural_topology"] == s1["level_3_structural_topology"])
    print(f"{name:24s} same drawing shifted 0.5 %: (l1,l2,l3) equal = {trip}"
          f"   l3_payload identical = {s0['level_3_payload'] == s1['level_3_payload']}")

print("\n=== base-rate check on the probe's own 190 pairings ===")
F = json.loads((Path('/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_architecture_opus/artifacts')/"signoise_03_block_features.json").read_text(encoding="utf-8"))
import itertools
names = list(F)
c = collections.Counter()
for a, b in itertools.combinations(names, 2):
    c[tuple(F[a]["signatures"][l] == F[b]["signatures"][l] for l in ("l1","l2","l3"))] += 1
print(dict(c))
n = sum(c.values()); pos = sum(v for k, v in c.items() if k[1] or k[2])
print(f"pairings where l2 OR l3 is True: {pos}/{n}. "
      f"A constant 'both False' predictor agrees on {n - sum(v for k,v in c.items() if k[1]!=k[2])}/{n} "
      f"= the same 190/190 the claim reports.")
