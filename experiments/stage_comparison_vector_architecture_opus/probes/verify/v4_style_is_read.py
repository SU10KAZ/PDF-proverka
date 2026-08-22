"""REFUTATION TEST for C2 'write-only ... blanking either changes nothing anywhere'.

extractor._canonical_primitive() reads primitive['style'] (stroke_width, stroke, fill, dashes) and
primitive['raw'] and feeds BOTH into structural_signature.level_1_exact_vector /
level_2_normalized_geometry - hashes the comparator DOES read (level_1 decides IDENTICAL).
extractor._summary() reads style for stroke_paths/filled_paths; _size_metrics() reads raw+style.

So the ablation's 0/10 only shows the comparator does not RE-READ the stored copy after the
signature was already computed from it. Test 1: does style change the signature at all on this
corpus? Test 2: does a style-only change (solid -> dashed line = a real engineering change) move
the verdict?
"""
import json, sys, collections
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
from experiments.stage_comparison_vector_blocks import extractor as E, comparator as C

DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")

def canon_nostyle(p, normalized, q):
    space = p["normalized" if normalized else "raw"]
    segs = []
    for s in space["segments"]:
        segs.append(tuple(sorted(tuple(round(float(v)/q) for v in pt) for pt in s)))
    return (p["type"], tuple(sorted(segs)))

print("=== TEST 1: does style add discriminative power to the level_1/level_2 token set? ===")
print(f"{'block':32s} {'prims':>7s} {'distinct tok WITH style':>24s} {'WITHOUT style':>14s}")
tot_w = tot_wo = 0
for pd in sorted(DESC.iterdir()):
    for side in ("left","right"):
        p = pd/side/"vector_block.json"
        if not p.exists(): continue
        d = json.loads(p.read_text(encoding="utf-8"))
        P = d["geometry"]["primitives"]
        w  = len({repr(E._canonical_primitive(x, True, 0.001)) for x in P})
        wo = len({repr(canon_nostyle(x, True, 0.001)) for x in P})
        tot_w += w; tot_wo += wo
        if w != wo:
            print(f"{pd.name+'/'+side:32s} {len(P):7,} {w:24,} {wo:14,}   <-- style separates {w-wo} tokens")
        del d
print(f"corpus distinct normalized tokens: WITH style {tot_w:,}  WITHOUT style {tot_wo:,}")

print("\n=== TEST 2: a style-only change (dashes) on a real block ===")
L = json.loads((DESC/"ss_simple_node"/"left"/"vector_block.json").read_text(encoding="utf-8"))
R = json.loads((DESC/"ss_simple_node"/"right"/"vector_block.json").read_text(encoding="utf-8"))
base = C.compare_descriptions(L, R)
print("baseline ss_simple_node status:", base["status"], "| differences:", base["differences"])

# make the right side's first primitive dashed - geometry untouched
R2 = json.loads(json.dumps(R))
R2["geometry"]["primitives"][0]["style"]["dashes"] = "[3 2] 0"
R2["geometry"]["primitives"][0]["style"]["stroke_width"] = 0.75
# recompute the signatures exactly as the extractor does
sig = E._signatures(R2["geometry"]["primitives"], R2["texts"], R2["topology"])
R2["structural_signature"] = sig
R2["primitive_summary"] = E._summary(R2["geometry"]["primitives"], R2["texts"], R2["topology"])
out = C.compare_descriptions(L, R2)
print("style-only change, style KEPT in signature :", out["status"],
      "| exact_sig_equal =", out["exact_vector_signature_equal"],
      "| l2_equal =", out["normalized_signature_equal"], "| differences:", out["differences"])

# now the world the claim proposes: style dropped from the contract, so the signature cannot see it
_orig = E._canonical_primitive
E._canonical_primitive = lambda p, n, q: canon_nostyle(p, n, q)
R3 = json.loads(json.dumps(R2))
R3["structural_signature"] = E._signatures(R3["geometry"]["primitives"], R3["texts"], R3["topology"])
L3 = json.loads(json.dumps(L))
L3["structural_signature"] = E._signatures(L3["geometry"]["primitives"], L3["texts"], L3["topology"])
out2 = C.compare_descriptions(L3, R3)
E._canonical_primitive = _orig
print("style-only change, style DROPPED from sig  :", out2["status"],
      "| exact_sig_equal =", out2["exact_vector_signature_equal"],
      "| l2_equal =", out2["normalized_signature_equal"], "| differences:", out2["differences"])

print("\n=== TEST 3: primitive_summary.stroke_paths/filled_paths also read style ===")
d = json.loads((DESC/"ar_plan"/"left"/"vector_block.json").read_text(encoding="utf-8"))
P = d["geometry"]["primitives"]
s0 = E._summary(P, d["texts"], d["topology"])
for x in P: x["style"] = {}
s1 = E._summary(P, d["texts"], d["topology"])
print("ar_plan/left  with style:", {k: s0[k] for k in ("stroke_paths","filled_paths")},
      " style blanked:", {k: s1[k] for k in ("stroke_paths","filled_paths")})
