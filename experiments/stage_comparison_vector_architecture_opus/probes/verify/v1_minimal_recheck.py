"""VERIFY C1: independent re-derivation of the 'minimal contract' and its byte share.

Differences from the probe:
  * the minimal key set is derived by ME from reading comparator.py, not copied;
  * the identity test compares the ENTIRE comparison dict (json sort_keys), not 7 selected keys;
  * bytes are also reported as on-disk file bytes, not only re-serialised compact JSON.
"""
import json, sys
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
from experiments.stage_comparison_vector_blocks import comparator as C

DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")
PAIRS = ["ss_scheme_text_changed","ss_plan_dense","ss_simple_node","ss_table_graphic","ar_plan",
         "ar_wall_sections","vk_plan","vk_nodes","vk_node_plan","eom_singleline_changed"]
TOPO = ("node_count","edge_count","connected_components","endpoints","branch_points",
        "t_junctions","x_crossings_unconnected","closed_contours","nested_contours")

def cb(v): return len(json.dumps(v, ensure_ascii=False, separators=(",",":")).encode())

def minimal(d):
    return {
      "block_id": d["block_id"],
      "vector_quality": d["vector_quality"],
      "primitive_summary": {"primitive_count": d["primitive_summary"]["primitive_count"]},
      "geometry": {
        "extraction": {"source_item_counts": {k: d["geometry"]["extraction"]["source_item_counts"].get(k,0) for k in ("l","re")}},
        "primitives": [
          {"id":p["id"],"type":p["type"],"closed":p["closed"],"segment_count":p["segment_count"],
           "length_norm":p["length_norm"],"angle_degrees":p["angle_degrees"],
           "normalized":{"bbox":p["normalized"]["bbox"],"segments":p["normalized"]["segments"]}}
          for p in d["geometry"]["primitives"]],
      },
      "texts": [{"text":t["text"],"category":t["category"],"x_norm":t["x_norm"],"y_norm":t["y_norm"]} for t in d["texts"]],
      "topology": {k: d["topology"].get(k,0) for k in TOPO},
      "repeated_elements": [{"pattern_id":r["pattern_id"],"count":r["count"]} for r in d["repeated_elements"]],
      "structural_signature": {k: d["structural_signature"][k] for k in
          ("level_1_exact_vector","level_2_normalized_geometry","level_3_structural_topology")},
    }

def dump(o): return json.dumps(o, ensure_ascii=False, sort_keys=True, separators=(",",":"))

full_b = min_b = disk_b = 0
mismatch = []
rows = []
for pair in PAIRS:
    L = json.loads((DESC/pair/"left"/"vector_block.json").read_text(encoding="utf-8"))
    R = json.loads((DESC/pair/"right"/"vector_block.json").read_text(encoding="utf-8"))
    ref = C.compare_descriptions(L, R)
    ml, mr = minimal(L), minimal(R)
    fb = cb(L)+cb(R); mb = cb(ml)+cb(mr)
    db = (DESC/pair/"left"/"vector_block.json").stat().st_size + (DESC/pair/"right"/"vector_block.json").stat().st_size
    del L, R
    got = C.compare_descriptions(ml, mr)
    same = dump(ref) == dump(got)
    if not same: mismatch.append(pair)
    full_b += fb; min_b += mb; disk_b += db
    rows.append((pair, ref["status"], same, fb, mb, db, 100*(1-mb/fb)))
    print(f"{pair:24s} {ref['status']:30s} FULL_DICT_IDENTICAL={same} {fb:,} -> {mb:,} ({100*(1-mb/fb):.1f}%)")

print()
print("mismatching pairs (whole-output):", mismatch)
print(f"corpus compact: {full_b:,} -> {min_b:,}  reduction {100*(1-min_b/full_b):.3f} %")
print(f"corpus on-disk (pretty json): {disk_b:,} B")
med = sorted(100*(1-m/f) for _,_,_,f,m,_,_ in rows)
print("per-pair reduction: min %.1f max %.1f median %.1f (unweighted mean %.1f)" % (
    med[0], med[-1], (med[4]+med[5])/2, sum(med)/len(med)))
