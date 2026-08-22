"""VERIFY C1/C3: instrument compare_descriptions and record EVERY key path it actually reads.

Wraps the two input descriptions in dicts that log __getitem__/get/in and lazily wrap children.
This is a code-truth answer independent of the probe's manual reading of comparator.py.
"""
import json, sys, collections
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
from experiments.stage_comparison_vector_blocks import comparator as C

DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")
READ = collections.Counter()

class TD(dict):
    __slots__ = ("_p",)
    def __init__(self, data, path):
        super().__init__(data)
        self._p = path
    def _w(self, k, v):
        READ[f"{self._p}.{k}" if self._p else k] += 1
        return wrap(v, f"{self._p}.{k}" if self._p else k)
    def __getitem__(self, k):
        return self._w(k, dict.__getitem__(self, k))
    def get(self, k, d=None):
        if dict.__contains__(self, k):
            return self._w(k, dict.__getitem__(self, k))
        READ[f"{self._p}.{k}(missing)" if self._p else f"{k}(missing)"] += 1
        return d
    def items(self):
        for k in dict.keys(self):
            yield k, self._w(k, dict.__getitem__(self, k))
    def values(self):
        for k in dict.keys(self):
            yield self._w(k, dict.__getitem__(self, k))

class TL(list):
    __slots__ = ("_p",)
    def __init__(self, data, path):
        super().__init__(data)
        self._p = path
    def __getitem__(self, i):
        v = list.__getitem__(self, i)
        if isinstance(i, slice):
            return TL(v, self._p)
        return wrap(v, self._p + "[]")
    def __iter__(self):
        for v in list.__iter__(self):
            yield wrap(v, self._p + "[]")

def wrap(v, path):
    if isinstance(v, TD) or isinstance(v, TL):
        return v
    if isinstance(v, dict):
        return TD(v, path)
    if isinstance(v, list):
        return TL(v, path)
    return v

PAIRS = ["ss_simple_node", "ss_scheme_text_changed", "eom_singleline_changed", "ss_table_graphic"]
for pair in PAIRS:
    L = json.loads((DESC/pair/"left"/"vector_block.json").read_text(encoding="utf-8"))
    R = json.loads((DESC/pair/"right"/"vector_block.json").read_text(encoding="utf-8"))
    out = C.compare_descriptions(wrap(L, ""), wrap(R, ""))
    print(pair, out["status"])

paths = sorted(READ)
print("\n=== key paths READ by compare_descriptions (branches exercised: IDENTICAL / SSVC / STRUCTURE_CHANGED / NEAR_IDENTICAL) ===")
for p in paths:
    print(f"  {p:60s} {READ[p]:,}")

MINIMAL = {
 "block_id","vector_quality","primitive_summary","primitive_summary.primitive_count",
 "geometry","geometry.extraction","geometry.extraction.source_item_counts",
 "geometry.extraction.source_item_counts.l","geometry.extraction.source_item_counts.re",
 "geometry.primitives","geometry.primitives[].id","geometry.primitives[].type",
 "geometry.primitives[].closed","geometry.primitives[].segment_count",
 "geometry.primitives[].length_norm","geometry.primitives[].angle_degrees",
 "geometry.primitives[].normalized","geometry.primitives[].normalized.bbox",
 "geometry.primitives[].normalized.segments",
 "texts","texts[].text","texts[].category","texts[].x_norm","texts[].y_norm",
 "topology","repeated_elements","repeated_elements[].pattern_id","repeated_elements[].count",
 "structural_signature","structural_signature.level_1_exact_vector",
 "structural_signature.level_2_normalized_geometry","structural_signature.level_3_structural_topology",
}
TOPO = {"node_count","edge_count","connected_components","endpoints","branch_points",
        "t_junctions","x_crossings_unconnected","closed_contours","nested_contours"}
MINIMAL |= {f"topology.{k}" for k in TOPO}

extra = [p for p in paths if p.rstrip("(missing)").replace("(missing)","") not in MINIMAL]
print("\nkey paths READ but NOT in the probe's minimal set:", extra)
unread = sorted(MINIMAL - set(paths))
print("keys KEPT in minimal but never observed as read here:", unread)
