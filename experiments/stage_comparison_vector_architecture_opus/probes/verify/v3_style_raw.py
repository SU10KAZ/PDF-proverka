"""VERIFY C2/C3: recompute style/raw byte shares, palette redundancy, and the five 'inert' fields.

Extra adversarial checks the probe did not do:
  * per-block share of style/raw (is 'half the contract' a corpus artefact of the 20k-capped blocks?)
  * distinct-style growth with length rank inside the capped blocks (the cap keeps the LONGEST
    primitives, O11 - so the palette may be measured on the most style-homogeneous subset)
  * how much of the style/raw evidence comes from blocks that are truncated at the storage cap
"""
import json, sys, collections
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")

def cb(v): return len(json.dumps(v, ensure_ascii=False, separators=(",",":")).encode())
def entry(k, v): return cb(k)+1+cb(v)+1     # same accounting as probe 1

INERT = ("anchors","hatch_like_structures","dimensions","labels","size_metrics")
tot = style_e = raw_e = 0
inert_e = collections.Counter()
style_val = 0; palette = 0; prims = 0; distinct_sum = 0
rows = []
capped_blocks = []
half_growth = []
for pair_dir in sorted(DESC.iterdir()):
    for side in ("left","right"):
        p = pair_dir/side/"vector_block.json"
        if not p.exists(): continue
        d = json.loads(p.read_text(encoding="utf-8"))
        name = f"{pair_dir.name}/{side}"
        t = cb(d); tot += t
        P = d["geometry"]["primitives"]
        se = sum(entry("style", x["style"]) for x in P)
        re_ = sum(entry("raw", x["raw"]) for x in P)
        style_e += se; raw_e += re_
        for k in INERT: inert_e[k] += entry(k, d[k])
        sv = sum(cb(x["style"]) for x in P)
        dist = {json.dumps(x["style"], sort_keys=True, separators=(",",":")) for x in P}
        style_val += sv; palette += sum(len(s.encode()) for s in dist)
        prims += len(P); distinct_sum += len(dist)
        ex = d["geometry"]["extraction"]
        cappedq = bool(ex.get("storage_capped"))
        if cappedq: capped_blocks.append((name, len(P), ex.get("primitives_uncapped")))
        # style diversity vs length rank INSIDE the retained set (retained = longest)
        order = sorted(P, key=lambda x: x["length_norm"], reverse=True)
        n = len(order)
        if n >= 100:
            q = n//4
            dq = [len({json.dumps(x["style"], sort_keys=True, separators=(",",":")) for x in order[i*q:(i+1)*q]}) for i in range(4)]
            half_growth.append((name, n, dq, cappedq))
        rows.append((name, t, se, re_, 100*se/t, 100*re_/t, len(P), len(dist), cappedq))
        del d

print(f"corpus compact bytes         : {tot:,}")
print(f"style entry bytes            : {style_e:,}  = {100*style_e/tot:.3f} %   (probe: 32,341,142 / 27.025 %)")
print(f"raw   entry bytes            : {raw_e:,}  = {100*raw_e/tot:.3f} %   (probe: 27,299,148 / 22.812 %)")
print(f"style+raw                    : {100*(style_e+raw_e)/tot:.3f} %")
print(f"style values / palette       : {style_val:,} / {palette:,} = {style_val/palette:.1f}x  (probe 30,899,153 / 125,208 = 246.8x)")
print(f"primitives / distinct styles : {prims:,} / {distinct_sum} = {prims/distinct_sum:.1f}x")
inert_tot = sum(inert_e.values())
print(f"five 'inert' fields          : {inert_tot:,} B = {100*inert_tot/tot:.3f} %  (probe 1,304,264 / 1.09 %)")
for k,v in inert_e.most_common(): print(f"    {k:24s} {v:,}")

print("\nblocks truncated at DEFAULT_STORAGE_CAP:", capped_blocks)
capped_names = {n for n,_,_ in capped_blocks}
cs = sum(r[2] for r in rows if r[0] in capped_names)
print(f"style bytes from capped blocks: {cs:,} / {style_e:,} = {100*cs/style_e:.1f} %")
cp = sum(r[6] for r in rows if r[0] in capped_names)
print(f"primitives from capped blocks : {cp:,} / {prims:,} = {100*cp/prims:.1f} %")

print("\nper-block share of the block's own bytes:")
print(f"{'block':32s} {'bytes':>12s} {'style%':>7s} {'raw%':>7s} {'prims':>7s} {'styles':>7s} capped")
for n,t_,se,re_,sp,rp,np_,ds,cq in rows:
    print(f"{n:32s} {t_:12,} {sp:7.2f} {rp:7.2f} {np_:7,} {ds:7,} {cq}")
sh = sorted(r[4] for r in rows)
print(f"style share per block: min {sh[0]:.2f} %  median {(sh[9]+sh[10])/2:.2f} %  max {sh[-1]:.2f} %")
sh = sorted(r[5] for r in rows)
print(f"raw   share per block: min {sh[0]:.2f} %  median {(sh[9]+sh[10])/2:.2f} %  max {sh[-1]:.2f} %")

print("\ndistinct styles per length-quartile of the RETAINED primitives (Q1=longest .. Q4=shortest kept):")
for n, cnt, dq, cq in half_growth:
    print(f"  {n:32s} n={cnt:6,} {dq}  capped={cq}")
