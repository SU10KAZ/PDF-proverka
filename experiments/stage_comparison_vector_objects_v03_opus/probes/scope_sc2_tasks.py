# -*- coding: utf-8 -*-
"""scope · SC2/SC3 — build the task list: which components get compared in three frames."""
from __future__ import annotations
import json, random, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"

SEED = 20260823
N_MULTI = 40
N_NESTED = 20
N_CONTROL = 20

M14 = ["AR-490254e9", "OV-2cc2a382", "AR-49c7c898", "AR-899c6321", "AR-aab8d4f9",
       "OV-a230726c", "EOM-81ca2c16", "EOM-27848006", "SS-5f190ee6", "SS-cc32de7d"]


def mine_rows():
    out = {}
    for f in ("mine_extract.jsonl", "mine_extract2.jsonl"):
        p = ART / f
        if p.exists():
            for line in open(p, encoding="utf-8"):
                r = json.loads(line)
                out[r["pair_id"]] = r
    return out


def main():
    comps = [json.loads(l) for l in open(ART / "scope_components.jsonl", encoding="utf-8")]
    rnd = random.Random(SEED)
    tasks = []
    seen = set()

    def add(c, tag, forced=None):
        k = (c["doc_id"], c["ver_a"], c["ver_b"], c["page_a"], c["page_b"],
             tuple(sorted(b["id"] for b in c["blocks_a"])),
             tuple(sorted(b["id"] for b in c["blocks_b"])))
        if k in seen:
            for t in tasks:
                if t["_k"] == list(k[:5]) + [list(k[5]), list(k[6])]:
                    t["tags"].append(tag)
            return
        seen.add(k)
        t = dict(c)
        t["task_id"] = f"T{len(tasks):04d}"
        t["tags"] = [tag]
        t["forced_pair"] = forced
        t["_k"] = list(k[:5]) + [list(k[5]), list(k[6])]
        tasks.append(t)

    # 1. the ten M14 pairs the mine probe flagged (their component)
    MR = mine_rows()
    for pid in M14:
        r = MR[pid]
        hit = [c for c in comps
               if c["doc_id"] == r["doc_id"] and c["page_a"] == r["page_a"] and c["page_b"] == r["page_b"]
               and any(b["id"] == r["block_a"] for b in c["blocks_a"])
               and any(b["id"] == r["block_b"] for b in c["blocks_b"])]
        if not hit:
            print("M14 pair without a component:", pid)
            continue
        add(hit[0], f"m14:{pid}", forced=[r["block_a"], r["block_b"]])

    # 2. multi-block components (the real 1:N / N:1 / N:M population)
    multi = [c for c in comps if c["kind"] in ("1:N", "N:1", "N:M") and c["n_a"] + c["n_b"] <= 8
             and not c["rot_mismatch"]]
    by_d = {}
    for c in multi:
        by_d.setdefault(c["discipline"], []).append(c)
    picked = []
    order = sorted(by_d, key=lambda d: -len(by_d[d]))
    while len(picked) < N_MULTI:
        got = False
        for d in order:
            if by_d[d] and len(picked) < N_MULTI:
                picked.append(by_d[d].pop(rnd.randrange(len(by_d[d]))))
                got = True
        if not got:
            break
    for c in picked:
        add(c, "multi")

    nested = [c for c in comps if c["kind"] == "1:1_nested" and not c["rot_mismatch"]]
    for c in rnd.sample(nested, min(N_NESTED, len(nested))):
        add(c, "nested")

    ctrl = [c for c in comps if c["kind"] == "1:1_aligned" and not c["rot_mismatch"]]
    for c in rnd.sample(ctrl, min(N_CONTROL, len(ctrl))):
        add(c, "control_aligned")

    for t in tasks:
        t.pop("_k", None)
    json.dump({"schema_version": "scope_tasks/1", "seed": SEED, "n": len(tasks),
               "tasks": tasks}, open(ART / "scope_tasks.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    from collections import Counter
    print(len(tasks), Counter(t["kind"] for t in tasks), Counter(x.split(":")[0] for t in tasks for x in t["tags"]))


if __name__ == "__main__":
    main()
