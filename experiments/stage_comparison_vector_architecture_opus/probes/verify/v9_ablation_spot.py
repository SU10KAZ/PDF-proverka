"""Spot-reproduce the ablation rows cited by C2/C3 on the 4 cheap pairs (the code trace already
proves these keys are never referenced by compare_descriptions, so this is a belt-and-braces run)."""
import json, sys
from pathlib import Path
sys.path.insert(0, "/home/coder/projects/PDF-proverka")
from experiments.stage_comparison_vector_blocks import comparator as C
DESC = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_blocks/artifacts/descriptions")
PAIRS = ["ss_simple_node","ss_scheme_text_changed","ss_table_graphic","eom_singleline_changed"]
def sh(d, **kw): o = dict(d); o.update(kw); return o
def prim(d, key, val):
    return sh(d, geometry=dict(d["geometry"], primitives=[dict(p, **{key: val}) for p in d["geometry"]["primitives"]]))
ABL = {
 "anchors_blank": lambda d: sh(d, anchors=[]),
 "hatch_blank": lambda d: sh(d, hatch_like_structures=[]),
 "dimensions_labels_blank": lambda d: sh(d, dimensions=[], labels=[]),
 "size_metrics_blank": lambda d: sh(d, size_metrics={}),
 "primitive_style_blank": lambda d: prim(d, "style", {}),
 "primitive_raw_blank": lambda d: prim(d, "raw", {}),
}
def dump(o): return json.dumps(o, ensure_ascii=False, sort_keys=True, separators=(",",":"))
for pair in PAIRS:
    L = json.loads((DESC/pair/"left"/"vector_block.json").read_text(encoding="utf-8"))
    R = json.loads((DESC/pair/"right"/"vector_block.json").read_text(encoding="utf-8"))
    base = dump(C.compare_descriptions(L, R))
    out = []
    for name, fn in ABL.items():
        out.append(f"{name}={'same' if dump(C.compare_descriptions(fn(L), fn(R)))==base else 'CHANGED'}")
    print(f"{pair:24s} " + "  ".join(out))
