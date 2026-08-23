# -*- coding: utf-8 -*-
"""Aggregation for probe `ldg`: type census (L1), evidence audit (L2), phrase precision (L3).

    python probes/ldg_agg.py
"""
from __future__ import annotations
import json
import glob
import collections
from pathlib import Path

ART = Path(__file__).resolve().parent.parent / "artifacts"
HIT = 0.30            # bbox overlap that counts as 'points at the true change'

NEGATIVE = {"NEG", "A1_path_split", "D1_text_edit", "D3_label_rename"}
TEXT_ONLY = {"D1_text_edit", "D3_label_rename"}
TRUE_EVENT = {                     # instance -> (phrase the wording must be, count)
    "C1_remove_object@small": ("OBJECT_REMOVED", 1),
    "C2_add_object@small": ("OBJECT_ADDED", 1),
    "C2x2_same_object": ("ADDED_SAME_KIND", 2),
    "C9_add_branch": ("BRANCH_ADDED", 1),
    "C9x2_add_two_branches": ("BRANCH_ADDED", 2),
    "C10_remove_opening": ("OPENING_REMOVED", 1),
    "C3_move_object@small": ("CONFIG_CHANGED", None),
    "C6_reshape_object@small": ("CONFIG_CHANGED", None),
    "C7_split_object": ("CONFIG_CHANGED", None),
    "C8_merge_objects": ("CONFIG_CHANGED", None),
}
# a wording that is acceptable but weaker than the true event (never counted as the
# strict wording hit, reported separately)
WEAKER = {"CONFIG_CHANGED"}

# ------------------------------------------------------------------ verdict matrix
# For every (phrase, instance) pair: is the SENTENCE the expert would read true?
#   correct     - names the event that was performed, with the right number
#   undercount  - right event, fewer than were performed
#   weaker_true - a true but less specific sentence
#   false       - asserts something that did not happen
def verdict(pid, inst, n):
    if inst in NEGATIVE:
        return "false"
    if pid == "OPENING_REMOVED":
        return "correct" if (inst == "C10_remove_opening" and n == 1) else "false"
    if pid == "OPENING_ADDED":
        return "false"                      # no counterfactual of the plan opens a gap
    if pid == "BRANCH_ADDED":
        if inst == "C9_add_branch":
            return "correct" if n == 1 else "false"
        if inst == "C9x2_add_two_branches":
            return "correct" if n == 2 else ("undercount" if n == 1 else "false")
        return "false"
    if pid == "BRANCH_REMOVED":
        return "false"                      # no counterfactual removes a branch
    if pid == "ADDED_SAME_KIND":
        if inst == "C2x2_same_object":
            return "correct" if n == 2 else ("undercount" if n < 2 else "false")
        if inst == "C9x2_add_two_branches":
            return "weaker_true"            # two identical segments really were added
        return "false"
    if pid == "OBJECT_ADDED":
        if inst == "C2_add_object@small":
            return "correct" if n == 1 else "false"
        if inst == "C2x2_same_object":
            return "undercount"
        return "false"                      # a branch / a bridge / a closed gap is not
                                            # a new OBJECT
    if pid == "OBJECT_REMOVED":
        if inst == "C1_remove_object@small":
            return "correct" if n == 1 else "false"
        return "false"
    if pid == "CONFIG_CHANGED":
        if inst in ("C3_move_object@small", "C6_reshape_object@small",
                    "C7_split_object", "C8_merge_objects"):
            return "correct"
        return "weaker_true"
    return "false"


def rows():
    for f in sorted(glob.glob(str(ART / "ldg_runs" / "cf_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if r.get("skip") or r.get("error"):
                yield r
                continue
            yield r


def main():
    R = [r for r in rows()]
    ok = [r for r in R if "phrases" in r]
    skips = collections.Counter(r["inst"] for r in R if r.get("skip"))
    errs = collections.Counter(r["inst"] for r in R if r.get("error"))
    carriers = {r["block_id"] for r in ok}
    discs = {r["discipline"] for r in ok}

    # ---------------------------------------------------------------- L1 type census
    types = collections.Counter()
    types_on = collections.Counter()
    types_off = collections.Counter()
    per_inst_on = collections.defaultdict(collections.Counter)
    per_inst_off = collections.defaultdict(collections.Counter)
    shapes = collections.Counter()
    shapes_on = collections.Counter()
    per_inst_shape = collections.defaultdict(collections.Counter)
    for r in ok:
        for arm, key in (("prod", "changes"),):
            for c in r.get(key, []):
                t = c["type"]
                types[t] += 1
                on = c.get("on_target", 0) >= HIT
                (types_on if on else types_off)[t] += 1
                (per_inst_on if on else per_inst_off)[r["inst"]][t] += 1
                if c.get("shape"):
                    shapes[c["shape"]] += 1
                    if on:
                        shapes_on[c["shape"]] += 1
                    per_inst_shape[r["inst"]][c["shape"]] += 1

    # ---------------------------------------------------------------- L2 evidence
    n_rec = sum(len(r.get("changes", [])) for r in ok)
    n_ink = sum(1 for r in ok for c in r.get("changes", [])
                if ("ink_lost" in c["ev"] or "ink_new" in c["ev"]))
    n_att = sum(1 for r in ok for c in r.get("changes", []) if "attachment" in c["ev"])
    viol = sum(len(r.get("validate_violations") or []) for r in ok)
    text_rows = [r for r in ok if r["inst"] in TEXT_ONLY]
    text_recs = sum(len(r.get("changes", [])) for r in text_rows)
    text_recs_low = sum(r.get("n_changes_low", 0) for r in text_rows)
    neg_rows = [r for r in ok if r["inst"] in NEGATIVE]
    neg_recs = collections.Counter()
    for r in neg_rows:
        neg_recs[(r["inst"], r["noise"])] += len(r.get("changes", []))

    # ---------------------------------------------------------------- L3 phrases
    ph = collections.defaultdict(lambda: {"fires": 0, "loc_ok": 0, "word_ok": 0,
                                          "count_ok": 0, "on_negative": 0,
                                          "fires_by_inst": collections.Counter(),
                                          "false_examples": []})
    truth_seen = collections.Counter()
    caught = collections.defaultdict(set)      # phrase -> instances where TP happened
    for arm in ("phrases", "phrases_low"):
        for r in ok:
            inst = r["inst"]
            neg = inst in NEGATIVE
            te = TRUE_EVENT.get(inst)
            if arm == "phrases":
                truth_seen[inst] += 1
            for q in r.get(arm, []):
                d = ph[(arm, q["id"])]
                d["fires"] += 1
                d["fires_by_inst"][inst] += 1
                on = q.get("on_target", 0) >= HIT
                if neg:
                    d["on_negative"] += 1
                    if len(d["false_examples"]) < 6:
                        d["false_examples"].append([r["block_id"], inst, r["noise"],
                                                    q["text"]])
                    continue
                if on:
                    d["loc_ok"] += 1
                if te and q["id"] == te[0] and on:
                    d["word_ok"] += 1
                    if te[1] is None or q["n"] == te[1]:
                        d["count_ok"] += 1
                        caught[(arm, q["id"])].add((r["block_id"], inst, r["noise"]))
                elif on and q["id"] in WEAKER and te:
                    pass
                elif len(d["false_examples"]) < 6:
                    d["false_examples"].append([r["block_id"], inst, r["noise"], q["text"],
                                                round(q.get("on_target", 0), 2)])
    phrases = {}
    for (arm, pid), d in sorted(ph.items()):
        f = d["fires"]
        phrases[f"{arm}:{pid}"] = {
            "fires": f,
            "precision_localised": round(d["loc_ok"] / f, 4) if f else None,
            "precision_wording": round(d["word_ok"] / f, 4) if f else None,
            "precision_wording_and_count": round(d["count_ok"] / f, 4) if f else None,
            "fires_on_negative_control": d["on_negative"],
            "fires_by_instance": dict(d["fires_by_inst"]),
            "false_examples": d["false_examples"],
        }
    # recall per phrase: of the instances whose true event is this phrase, how many fired
    recall = {}
    for arm in ("phrases", "phrases_low"):
        for inst, (pid, cnt) in TRUE_EVENT.items():
            tot = sum(1 for r in ok if r["inst"] == inst)
            hit = sum(1 for r in ok if r["inst"] == inst and
                      any(q["id"] == pid and q.get("on_target", 0) >= HIT and
                          (cnt is None or q["n"] == cnt) for q in r.get(arm, [])))
            hit_any = sum(1 for r in ok if r["inst"] == inst and
                          any(q.get("on_target", 0) >= HIT for q in r.get(arm, [])))
            recall[f"{arm}:{inst}"] = {"n": tot, "phrase_exact": hit,
                                       "recall_phrase": round(hit / tot, 4) if tot else None,
                                       "any_phrase_on_target": hit_any,
                                       "recall_any": round(hit_any / tot, 4) if tot else None}

    # ------------------------------------------------------- L3b sentence verdicts
    verd = collections.defaultdict(collections.Counter)
    ndist = collections.defaultdict(collections.Counter)
    offtarget = collections.Counter()
    for arm in ("phrases", "phrases_low"):
        for r in ok:
            for q in r.get(arm, []):
                v = verdict(q["id"], r["inst"], q["n"])
                if q.get("on_target", 0) < HIT and r["inst"] not in NEGATIVE:
                    v = "false"                 # points somewhere else than the change
                    offtarget[(arm, q["id"])] += 1
                verd[(arm, q["id"])][v] += 1
                ndist[(arm, q["id"], r["inst"])][q["n"]] += 1
    sentences = {}
    for (arm, pid), c in sorted(verd.items()):
        tot = sum(c.values())
        sentences[f"{arm}:{pid}"] = {
            "fires": tot, **{k: c.get(k, 0) for k in
                             ("correct", "undercount", "weaker_true", "false")},
            "precision_strict": round(c.get("correct", 0) / tot, 4),
            "share_false": round(c.get("false", 0) / tot, 4),
            "share_not_false": round(1 - c.get("false", 0) / tot, 4),
            "fires_off_target": offtarget.get((arm, pid), 0),
        }
    counts = {f"{a}:{p}|{i}": dict(sorted(c.items()))
              for (a, p, i), c in sorted(ndist.items())}

    # ------------------------------------------------- L1b welded / connector census
    weld = collections.defaultdict(collections.Counter)
    for r in ok:
        for c in r.get("changes", []):
            if c.get("on_target", 0) < HIT and r["inst"] not in NEGATIVE:
                continue
            key = "welded" if c.get("welded") else "free"
            weld[r["inst"]][f"{c['type']}|{key}"] += 1
    # ------------------------------------------------- L3c geometric (domain-free) claim
    geo = collections.defaultdict(collections.Counter)
    CONNECTOR_CF = {"C9_add_branch", "C9x2_add_two_branches", "C10_remove_opening",
                    "C8_merge_objects"}
    for r in ok:
        for q in r.get("phrases", []):
            if q["id"] in ("BRANCH_ADDED", "OPENING_REMOVED"):
                geo[q["id"]]["fires"] += 1
                geo[q["id"]]["on_connector_cf" if r["inst"] in CONNECTOR_CF
                             else "on_other_cf"] += 1

    out = {
        "corpus": {"rows": len(ok), "carriers": len(carriers), "disciplines": sorted(discs),
                   "instances": sorted({r["inst"] for r in ok}),
                   "noises": sorted({r["noise"] for r in ok}),
                   "skips": dict(skips), "errors": dict(errs)},
        "L1_types": {"total": dict(types), "on_target": dict(types_on),
                     "off_target": dict(types_off),
                     "per_instance_on_target": {k: dict(v) for k, v in per_inst_on.items()},
                     "per_instance_off_target": {k: dict(v) for k, v in per_inst_off.items()},
                     "shapes_total": dict(shapes), "shapes_on_target": dict(shapes_on),
                     "shapes_per_instance": {k: dict(v) for k, v in per_inst_shape.items()}},
        "L2_evidence": {"records": n_rec, "with_ink_evidence": n_ink,
                        "with_attachment_evidence": n_att,
                        "validator_violations": viol,
                        "text_only_instances_rows": len(text_rows),
                        "text_only_records_prod": text_recs,
                        "text_only_records_no_floor": text_recs_low,
                        "records_on_negative_controls": {f"{k[0]}|{k[1]}": v
                                                         for k, v in sorted(neg_recs.items())}},
        "L3_phrases": phrases,
        "L3_recall": recall,
        "L3_sentence_verdicts": sentences,
        "L3_count_distribution": counts,
        "L1b_welded_census": {k: dict(v) for k, v in weld.items()},
        "L3c_geometric_claim": {k: dict(v) for k, v in geo.items()},
    }
    json.dump(out, open(ART / "ldg_cf_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(out["corpus"], ensure_ascii=False))
    print(json.dumps(out["L1_types"]["total"], ensure_ascii=False))
    print(json.dumps(out["L2_evidence"], ensure_ascii=False))
    for k, v in sentences.items():
        print(f"{k:34s} fires={v['fires']:4d} correct={v['correct']:4d} "
              f"under={v['undercount']:3d} weak={v['weaker_true']:3d} "
              f"FALSE={v['false']:4d}  P={v['precision_strict']:.3f} "
              f"notfalse={v['share_not_false']:.3f}")


if __name__ == "__main__":
    main()
