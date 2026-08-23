# -*- coding: utf-8 -*-
"""CNS-6 — accuracy of the taxonomy rules against blocks looked at by eye.

DEV  (R, n=64)  random stratified sample used to SET the thresholds.
HOLD (H, n=39)  fresh random sample, thresholds frozen -> honest accuracy.
T    (n=30)     stratified by PREDICTED class -> per-class precision, incl. rare classes.
"""
from __future__ import annotations
import json, os, sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes.cns_rules import classify, classify_v1
from experiments.stage_comparison_vector_objects_v03_opus.probes.cns_curvetext import page_text_lines

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
MAP = {"mixed": "vector_raster_mix", "legend": "legend_notes"}


def load_idx():
    idx = {}
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            idx[(b["doc_id"], b["version"], b["block_id"])] = b
    return idx


def enrich(feats, idx):
    for f in feats:
        if f.get("n_text", 1) == 0:
            b = idx.get((f["doc_id"], f["version"], f["block_id"]))
            f["page_text_lines"] = page_text_lines(b["pdf"], b["page_index"]) if b else 0


def evaluate(feats, labels, fn):
    fm = {f["block_id"]: f for f in feats}
    conf = Counter(); wrong = []
    for m in labels:
        e = MAP.get(m["eye_class"], m["eye_class"])
        c, rid = fn(fm[m["block_id"]])
        conf[(e, c)] += 1
        if c != e:
            wrong.append({"label": m["label"], "eye": e, "pred": c, "rule": rid, "note": m.get("eye_note")})
    acc = sum(v for (a, b), v in conf.items() if a == b) / max(1, len(labels))
    return {"n": len(labels), "accuracy": round(acc, 4),
            "confusion": {f"{a}->{b}": v for (a, b), v in sorted(conf.items())},
            "errors": wrong}


def main():
    idx = load_idx()
    res = {}
    feats_T = None
    for tag, name in (("R", "DEV_random_64"), ("H", "HOLDOUT_random_39"), ("T", "TARGETED_rare_30")):
        fp = ART / f"cns_feat_{tag}.json"
        if tag == "T":
            from experiments.stage_comparison_vector_objects_v03_opus.probes.cns_features import features
            blocks = json.load(open(ART / "cns_renders/T_blocks.json", encoding="utf-8"))
            feats = []
            for b in blocks:
                try:
                    feats.append(features(b))
                except Exception as exc:
                    feats.append({"block_id": b["block_id"], "error": str(exc)})
            json.dump(feats, open(ART / "cns_feat_T.json", "w", encoding="utf-8"), ensure_ascii=False)
        else:
            feats = json.load(open(fp, encoding="utf-8"))
        enrich(feats, idx)
        labels = json.load(open(ART / f"cns_eye_labels_{tag}.json", encoding="utf-8"))
        res[name] = {"v1_frozen": evaluate(feats, labels, classify_v1),
                     "v2_repaired": evaluate(feats, labels, classify)}
        if tag == "T":
            fm = {f["block_id"]: f for f in feats}
            per = {}
            for m in labels:
                b = m["bucket"]
                c, _ = classify(fm[m["block_id"]])
                e = MAP.get(m["eye_class"], m["eye_class"])
                per.setdefault(b, {"n": 0, "v1_pred_correct": 0, "v2_pred_correct": 0})
                per[b]["n"] += 1
                c1, _ = classify_v1(fm[m["block_id"]])
                per[b]["v1_pred_correct"] += int(c1 == e)
                per[b]["v2_pred_correct"] += int(c == e)
            res[name]["precision_per_predicted_bucket"] = per
    (ART / "cns_rule_accuracy.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    for k, v in res.items():
        print(k, "v1", v["v1_frozen"]["accuracy"], "v2", v["v2_repaired"]["accuracy"], "n", v["v1_frozen"]["n"])
        if "precision_per_predicted_bucket" in v:
            print(json.dumps(v["precision_per_predicted_bucket"], ensure_ascii=False))


if __name__ == "__main__":
    main()
