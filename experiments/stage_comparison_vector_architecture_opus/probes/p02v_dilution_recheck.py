"""p02v_ VERIFIER: re-run the falsify_ localized dilution on ALL 10 Track A blocks,
not the 5 the probe chose, to test the universal quantifier in the claim
"Every primitive inside a central window of 20% of block width can be erased
 from real blocks and the comparator still answers NEAR_IDENTICAL".
Writes only artifacts/p02v_dilution_all10.json.
"""
import json, time
from pathlib import Path
from experiments.stage_comparison_vector_architecture_opus.probes import falsify_dilution as fd

ART = Path(__file__).resolve().parents[1] / "artifacts"
TRACK_A = fd.TRACK_A
pairs = sorted(p.parent.parent.name for p in TRACK_A.glob("*/left/vector_block.json"))
print("pairs:", pairs, flush=True)
out = {}
for pair in pairs:
    t0 = time.time()
    try:
        rows = fd.localized_check(pair)
    except Exception as e:
        rows = [{"error": repr(e)}]
    out[pair] = rows
    last_ni = None
    for r in rows:
        if r.get("status") in ("IDENTICAL", "NEAR_IDENTICAL"):
            last_ni = r
    print(f"{pair:24} last NEAR_IDENTICAL window="
          f"{last_ni['window_side_norm'] if last_ni else None} "
          f"| rows={[(r.get('window_side_norm'), r.get('status')) for r in rows]} "
          f"| {time.time()-t0:.0f}s", flush=True)
(ART / "p02v_dilution_all10.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
