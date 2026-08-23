# -*- coding: utf-8 -*-
"""What the WELD guard costs and buys, replayed offline from the stored records.

Claim under test: ink welded into surviving geometry (a branch, a bridge, a closed gap)
must NOT be called an added / removed OBJECT.  Two variants of the guard are scored on
exactly the same records:

    off        - every ADDED/REMOVED record speaks as an object
    add_only   - the guard applies to the addition side only
    both       - the guard applies to both sides   (the shipped rule)

Family grouping is ignored here, so "an object was added" covers both the single and the
"N of one kind" wording; that keeps the three variants comparable.
"""
from __future__ import annotations
import json
import glob
import collections
from pathlib import Path

ART = Path(__file__).resolve().parent.parent / "artifacts"
TRUE_ADD = {"C2_add_object@small", "C2x2_same_object"}
TRUE_REM = {"C1_remove_object@small"}
NEG = {"NEG", "A1_path_split", "D1_text_edit", "D3_label_rename"}
SHAPE_MUTE = ("GAP_OPENED", "BRANCH_REMOVED")


def main():
    rows = [json.loads(l) for f in sorted(glob.glob(str(ART / "ldg_runs" / "cf_*.jsonl")))
            for l in open(f, encoding="utf-8")]
    ok = [r for r in rows if "changes" in r]
    out = {}
    for variant in ("off", "add_only", "both"):
        st = collections.Counter()
        by_inst = collections.defaultdict(collections.Counter)
        for r in ok:
            add, rem = [], []
            for c in r["changes"]:
                if c.get("shape") in ("GAP_CLOSED", "BRANCH_ADDED"):
                    continue                       # spoken by the connector phrase
                w = bool(c.get("welded"))
                if c["type"] == "ADDED_OBJECT":
                    if variant in ("add_only", "both") and w:
                        continue
                    add.append(c)
                elif c["type"] == "REMOVED_OBJECT":
                    if c.get("shape") in SHAPE_MUTE:
                        continue
                    if variant == "both" and w:
                        continue
                    rem.append(c)
            for kind, recs, truth in (("added", add, TRUE_ADD), ("removed", rem, TRUE_REM)):
                if not recs:
                    continue
                good = r["inst"] in truth
                st[f"{kind}_fires"] += 1
                st[f"{kind}_" + ("true" if good else "false")] += 1
                by_inst[kind][r["inst"]] += 1
        res = {}
        for kind in ("added", "removed"):
            f = st[f"{kind}_fires"]
            res[kind] = {"fires": f, "true": st[f"{kind}_true"], "false": st[f"{kind}_false"],
                         "precision": round(st[f"{kind}_true"] / f, 4) if f else None,
                         "by_instance": dict(by_inst[kind])}
        out[variant] = res
    json.dump(out, open(ART / "ldg_weld_ablation.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    for v, res in out.items():
        for kind in ("added", "removed"):
            d = res[kind]
            print(f"{v:9s} {kind:8s} fires={d['fires']:4d} true={d['true']:4d} "
                  f"false={d['false']:4d} P={d['precision']}")
    print(json.dumps(out["off"]["removed"]["by_instance"], ensure_ascii=False))
    print(json.dumps(out["off"]["added"]["by_instance"], ensure_ascii=False))


if __name__ == "__main__":
    main()
