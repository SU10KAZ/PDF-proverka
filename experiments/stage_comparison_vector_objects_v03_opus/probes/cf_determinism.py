# -*- coding: utf-8 -*-
"""Rule 2 of the brief: every counterfactual must be deterministic.

Applies each cf_id twice (fresh extract + fresh object layer each time, separate
processes are not needed because the seed is derived from content) and compares the
sha256 of the resulting segment list, the text list and the manifest.
"""
from __future__ import annotations
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import cf_build_set as B        # noqa: E402


def digest(ex2, man):
    h = hashlib.sha256()
    for s in ex2.segments:
        h.update(f"{s['p0'][0]:.6f},{s['p0'][1]:.6f},{s['p1'][0]:.6f},{s['p1'][1]:.6f},"
                 f"{s.get('w')},{s.get('color')}|".encode())
    for t in ex2.texts:
        h.update(f"{t['text']}|{t['bbox']}|".encode())
    m = json.dumps(man, sort_keys=True, ensure_ascii=False, default=str)
    return h.hexdigest(), hashlib.sha256(m.encode()).hexdigest()


def main():
    carriers = B.pick_carriers()[:6]
    out = {"n_carriers": len(carriers), "checks": [], "mismatch": 0, "n": 0}
    for rec in carriers:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            continue
        for attempt in range(2):
            ex = G.extract(pb)
            ol = O.build_objects(ex)
            for cf_id, kw, tag in B.plan():
                try:
                    ex2, man = C.apply(ex, ol, cf_id, **kw)
                except Exception:
                    continue
                d = digest(ex2, man)
                key = f"{rec['block_id']}|{tag}"
                if attempt == 0:
                    out.setdefault("_first", {})[key] = d
                else:
                    first = out.get("_first", {}).get(key)
                    out["n"] += 1
                    if first != d:
                        out["mismatch"] += 1
                        out["checks"].append({"key": key, "first": first, "second": d})
        C.cleanup_scratch()
    out.pop("_first", None)
    json.dump(out, open(ART / "cf_determinism.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "checks"}, ensure_ascii=False))
    print("mismatching:", out["checks"][:5])


if __name__ == "__main__":
    main()
