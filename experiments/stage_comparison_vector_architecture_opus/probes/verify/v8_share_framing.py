"""VERIFY framing of C1 and C2:
  * '63.8 % of the contract is never read' is a BYTE share on this corpus. What is the share by
    distinct key path, and how corpus-dependent is the byte share?
  * '~247x redundant' is palette bytes only. What is the factor once you pay for the per-primitive
    index that a palette scheme needs?
"""
import json, sys, collections
from pathlib import Path
DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")
def cb(v): return len(json.dumps(v, ensure_ascii=False, separators=(",",":")).encode())

READ = {"block_id","vector_quality","primitive_summary.primitive_count",
 "geometry.extraction.source_item_counts.l","geometry.extraction.source_item_counts.re",
 "geometry.primitives[].id","geometry.primitives[].type","geometry.primitives[].closed",
 "geometry.primitives[].segment_count","geometry.primitives[].length_norm",
 "geometry.primitives[].angle_degrees","geometry.primitives[].normalized.bbox",
 "geometry.primitives[].normalized.segments","texts[].text","texts[].category",
 "texts[].x_norm","texts[].y_norm","repeated_elements[].pattern_id","repeated_elements[].count",
 "structural_signature.level_1_exact_vector","structural_signature.level_2_normalized_geometry",
 "structural_signature.level_3_structural_topology"} | {f"topology.{k}" for k in
 ("node_count","edge_count","connected_components","endpoints","branch_points","t_junctions",
  "x_crossings_unconnected","closed_contours","nested_contours")}

paths = set()
def walk(v, p):
    if isinstance(v, dict):
        if not v: paths.add(p); return
        for k, x in v.items(): walk(x, f"{p}.{k}" if p else k)
    elif isinstance(v, list):
        if v and isinstance(v[0], (dict, list)): walk(v[0], p+"[]")
        else: paths.add(p)
    else: paths.add(p)

for pd in sorted(DESC.iterdir()):
    for side in ("left","right"):
        f = pd/side/"vector_block.json"
        if f.exists(): walk(json.loads(f.read_text(encoding="utf-8")), "")
print(f"distinct leaf key paths in the contract: {len(paths)}")
print(f"leaf key paths the comparator reads     : {len(READ & paths)}  ({100*len(READ & paths)/len(paths):.1f} % of paths)")
print(f"  -> the claim's 63.8 % is a BYTE share; by key path the unread share is "
      f"{100*(1-len(READ & paths)/len(paths)):.1f} %")
missing = READ - paths
if missing: print("  (read paths not seen in data:", missing, ")")

print()
# palette + index accounting
tot_style = tot_pal = tot_idx = 0
for pd in sorted(DESC.iterdir()):
    for side in ("left","right"):
        f = pd/side/"vector_block.json"
        if not f.exists(): continue
        d = json.loads(f.read_text(encoding="utf-8"))
        P = d["geometry"]["primitives"]
        dist = {}
        for p in P:
            k = json.dumps(p["style"], sort_keys=True, separators=(",",":"))
            dist.setdefault(k, len(dist))
        tot_style += sum(cb(p["style"]) for p in P)
        tot_pal += sum(len(k.encode()) for k in dist)
        # compact JSON array of integer indices, one per primitive
        idx = [dist[json.dumps(p["style"], sort_keys=True, separators=(",",":"))] for p in P]
        tot_idx += cb(idx)
        del d
print(f"style values {tot_style:,} B ; palette {tot_pal:,} B ; per-primitive index array {tot_idx:,} B")
print(f"factor quoted by the claim (palette only) : {tot_style/tot_pal:.1f}x")
print(f"factor once the index is paid for         : {tot_style/(tot_pal+tot_idx):.1f}x")
