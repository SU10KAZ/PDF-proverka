"""VVG — does a cheap deterministic gate predict which descriptions need Vision verification?

ARM 3, Track B (Opus). Research only.

Inputs
  artifacts/vvg_case_signals.json          gate signals for the 60 ARM-1 manifest cases
  artifacts/vvg_fresh/*.json               14 fresh blocks extracted by ARM 3
  artifacts/vvd_runs/main/*.json           ARM 1's verification outcomes (labels)
  artifacts/vvg_runs/*.json                ARM 3's verification outcomes on the fresh blocks

Output
  artifacts/vvg_eval.json

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_eval
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_architecture_opus.probes import vvg_signals as sg

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"

# (signal, direction, human name).  direction "hi" = large value is risky.
CANDIDATE_SIGNALS: list[tuple[str, str, str]] = [
    ("retained_fraction_min", "lo", "geometry retained after the caps (O11)"),
    ("retained_primitive_fraction", "lo", "primitives retained after the storage cap"),
    ("retained_topology_fraction", "lo", "segments retained by the topology cap"),
    ("readable_text_ratio", "lo", "per-span readable-text ratio (O8a)"),
    ("garbled_text_ratio", "hi", "per-span garbled-text ratio (O8a)"),
    ("micro_segment_fraction", "hi", "fraction of micro-segments (O12)"),
    ("group_instability", "hi", "object-count movement under a +-20-25 % tolerance change"),
    ("unbound_text_ratio", "hi", "texts with no segment within 0.012"),
    ("ambiguous_text_ratio", "hi", "texts whose nearest geometry is contested by >=2 primitives"),
    ("anchor_conf_high_share", "hi", "share of anchors the extractor calls 'high' (O3, inverted)"),
    ("text_per_segment", "hi", "text items per segment"),
    ("segments_per_text", "hi", "segments per text item"),
    ("log10_segments", "hi", "log10 of segment count (block density)"),
    ("text_items", "hi", "number of text items"),
    ("text_area_share", "hi", "share of block area covered by text bboxes"),
    ("boundary_edges_touched", "hi", "block edges the content runs into"),
    ("repeat_families", "lo", "number of repeating shape families"),
    ("repeat_top_share", "hi", "share of repeats concentrated in the top family"),
    ("repeat_circle24_share", "hi", "share of repeat families with the 24-point circle constant (O5)"),
    ("median_text_anchor_distance", "hi", "median text-to-nearest-segment distance"),
    ("group_count_at_tol", "hi", "object count at the extractor's own tolerance"),
    ("raster_area_share", "hi", "share of the block covered by bitmap images the vector layer cannot see"),
    ("raster_images_in_block", "hi", "number of bitmap images intersecting the block"),
]

BINARY_SIGNALS: list[tuple[str, str]] = [
    ("cap_storage", "storage cap reached"),
    ("cap_topology", "topology cap reached"),
    ("components_truncated", "component list truncated"),
    ("any_cap", "any cap reached"),
    ("quality_not_good", "vector_quality != GOOD"),
    ("hatch_saturated", "hatch candidates saturated at 30 (O6)"),
    ("repeat_families_le1", "at most one repeating shape family"),
    ("block_level_undecodable_flag", "Track A's block-level UNDECODABLE flag"),
    ("has_raster", "the block contains at least one bitmap image"),
    ("frame_mismatch", "page /Rotate != 0, so description frame != crop frame (O13)"),
    ("no_text_but_geometry", "geometry present but zero text spans (text drawn as curves)"),
    ("no_geometry", "zero vector segments in the block"),
    ("degenerate_quality", "vector_quality == VECTOR_DATA_INSUFFICIENT"),
]


# ------------------------------------------------------------------ label load

def load_labels() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted((ART / "vvd_runs").rglob("*.json")):
        rec = json.loads(path.read_text(encoding="utf-8"))
        cid = rec.get("case_id")
        if not cid or not rec.get("status"):
            continue
        tag = rec.get("run_tag") or path.parent.name
        if tag not in ("main", "vvd_runs"):
            out.setdefault(f"{cid}@{tag}", {}).update(_label_row(rec, cid, tag))
            continue
        out[cid] = _label_row(rec, cid, tag)
    for path in sorted((ART / "vvg_runs").glob("*.json")):
        rec = json.loads(path.read_text(encoding="utf-8"))
        if not rec.get("status"):
            continue
        out[rec["block"]] = _label_row(rec, rec["block"], "fresh")
    return out


def _label_row(rec: dict[str, Any], cid: str, tag: str) -> dict[str, Any]:
    verdict = rec.get("verdict") or {}
    return {
        "id": cid,
        "run_tag": tag,
        "status": rec["status"],
        "confidence": verdict.get("confidence"),
        "suspicious": [s.get("claim_id") for s in verdict.get("suspicious", []) if isinstance(s, dict)],
        "missing": verdict.get("missing", []),
        "payload_tokens": rec.get("usage_payload_attributable"),
        "usage_raw": rec.get("usage_raw"),
        "wall_seconds": rec.get("wall_seconds"),
        "duration_ms": rec.get("duration_ms"),
        "crop_bytes": rec.get("crop_bytes"),
    }


# ------------------------------------------------------------------ table build

def build_table() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cases = json.loads((ART / "vvg_case_signals.json").read_text(encoding="utf-8"))["rows"]
    for row in cases:
        r = dict(row)
        r["id"] = row["case_id"]
        r["group"] = row["block"]
        r["set"] = "manifest"
        rows.append(r)

    fresh_sig_path = ART / "vvg_fresh_signals.json"
    if fresh_sig_path.exists():
        fresh = json.loads(fresh_sig_path.read_text(encoding="utf-8"))["rows"]
    else:
        fresh = []
        index = json.loads((ART / "vvg_fresh_index.json").read_text(encoding="utf-8"))["blocks"]
        for block in index:
            d = sg.load_description(ROOT / block["description"])
            r = {"id": block["id"], "group": block["id"], "set": "fresh",
                 "block": block["id"], "mutation": "clean", "family": "fresh_control",
                 "discipline": block["discipline"], "corrupted": False,
                 "crop_png": block["crop_png"], "synthetic": False,
                 "disclose_limits": True, "changed_claims": []}
            r.update(sg.compute_signals(d))
            fresh.append(r)
        fresh_sig_path.write_text(json.dumps({"rows": fresh}, ensure_ascii=False, indent=1) + "\n",
                                  encoding="utf-8")
    for r in fresh:
        r = dict(r)
        r["set"] = "fresh"
        r["group"] = r["id"]
        rows.append(r)

    raster = json.loads((ART / "vvg_raster_signal.json").read_text(encoding="utf-8"))["blocks"]
    frame = json.loads((ART / "vvg_frame_signal.json").read_text(encoding="utf-8"))["blocks"]
    for r in rows:
        key = r.get("block") if r["set"] == "manifest" else r["id"]
        r.update(raster.get(key, {"raster_images_in_block": 0, "raster_area_share": 0.0,
                                  "has_raster": False}))
        r.update({k: v for k, v in frame.get(key, {}).items() if k != "set"})
        r["no_text_but_geometry"] = bool(r.get("text_items", 0) == 0 and r.get("segments", 0) > 0)
        r["no_geometry"] = bool(r.get("segments", 0) == 0)
        r["degenerate_quality"] = bool(r.get("vector_quality") == "VECTOR_DATA_INSUFFICIENT")

    labels = load_labels()
    for r in rows:
        lab = labels.get(r["id"])
        r["label_status"] = lab["status"] if lab else None
        r["label_confidence"] = lab.get("confidence") if lab else None
        r["label_suspicious"] = lab.get("suspicious") if lab else None
        r["label_missing"] = lab.get("missing") if lab else None
        r["payload_tokens"] = lab.get("payload_tokens") if lab else None
        r["wall_seconds"] = lab.get("wall_seconds") if lab else None
        r["usage_raw"] = lab.get("usage_raw") if lab else None
        r["crop_bytes_run"] = lab.get("crop_bytes") if lab else None
        r["y_objection"] = None if not lab else int(lab["status"] != "VERIFIED")
        r["y_failed"] = None if not lab else int(lab["status"] == "FAILED")
        # ground-truth corruption: only defined for the synthetic manifest
        r["y_corrupted"] = int(bool(r.get("corrupted"))) if r["set"] == "manifest" else None
        # the verifier contradicted at least one claim (as opposed to only reporting
        # picture content the 14-claim sheet does not cover)
        r["y_suspicious"] = None if not lab else int(bool(lab.get("suspicious")))
        r["y_missing_only"] = None if not lab else int(
            bool(lab.get("missing")) and not lab.get("suspicious"))
        # C8/C10/C13/C14 are the four claims the verifier contests on almost every block
        # (they encode extractor definitions no human shares).  This label keeps only
        # contradictions of the remaining claims.  DERIVED from measured verdicts.
        soft = {"C8", "C10", "C13", "C14"}
        r["y_core_contradiction"] = None if not lab else int(
            bool(set(lab.get("suspicious") or []) - soft))
    return rows


# --------------------------------------------------------------------- metrics

def _counts(pred: list[int], y: list[int]) -> dict[str, Any]:
    tp = sum(1 for p, t in zip(pred, y) if p and t)
    fp = sum(1 for p, t in zip(pred, y) if p and not t)
    fn = sum(1 for p, t in zip(pred, y) if not p and t)
    tn = sum(1 for p, t in zip(pred, y) if not p and not t)
    n = len(y)
    gated = (tp + fp) / n if n else 0.0
    recall = tp / (tp + fn) if (tp + fn) else None
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn, "n": n,
        "gated_fraction": round(gated, 4),
        "skipped_fraction": round(1 - gated, 4),
        "precision": round(tp / (tp + fp), 4) if (tp + fp) else None,
        "recall": round(recall, 4) if recall is not None else None,
        # of the blocks the gate SKIPS, how many would the verifier have objected to
        "missed_in_skipped": fn,
        "skip_purity": round(tn / (tn + fn), 4) if (tn + fn) else None,
        "missed_defect_fraction": round(fn / (tp + fn), 4) if (tp + fn) else None,
        # recall divided by the recall a coin flip gating the same fraction would give
        "lift_over_random": round(recall / gated, 3) if (recall is not None and gated > 0) else None,
        "f1": round(2 * tp / (2 * tp + fp + fn), 4) if (2 * tp + fp + fn) else None,
    }


def sweep(values: list[float], y: list[int], direction: str) -> list[dict[str, Any]]:
    thresholds = sorted(set(values))
    points = []
    for t in thresholds:
        pred = [int(v >= t) if direction == "hi" else int(v <= t) for v in values]
        pt = _counts(pred, y)
        pt["threshold"] = t
        pt["direction"] = direction
        points.append(pt)
    return points


def auc(values: list[float], y: list[int], direction: str) -> float | None:
    pos = [v for v, t in zip(values, y) if t]
    neg = [v for v, t in zip(values, y) if not t]
    if not pos or not neg:
        return None
    wins = 0.0
    for p in pos:
        for n in neg:
            if direction == "hi":
                wins += 1.0 if p > n else (0.5 if p == n else 0.0)
            else:
                wins += 1.0 if p < n else (0.5 if p == n else 0.0)
    return round(wins / (len(pos) * len(neg)), 4)


def base_rate(y: list[int]) -> dict[str, Any]:
    n = len(y)
    pos = sum(y)
    return {"n": n, "positives": pos, "base_rate": round(pos / n, 4) if n else None}


# ------------------------------------------------------------------ combination

def rule_predict(rows: list[dict[str, Any]], rule: tuple[str, str, float]) -> list[int]:
    name, direction, thr = rule
    out = []
    for r in rows:
        v = r.get(name)
        v = 0.0 if v is None else float(v)
        out.append(int(v >= thr) if direction == "hi" else int(v <= thr))
    return out


def or_predict(rows: list[dict[str, Any]], rules) -> list[int]:
    preds = [rule_predict(rows, r) for r in rules]
    return [int(any(p[i] for p in preds)) for i in range(len(rows))]


def candidate_rules(rows: list[dict[str, Any]], max_per_signal: int = 12):
    rules = []
    for name, direction, _ in CANDIDATE_SIGNALS:
        vals = sorted({float(r.get(name) or 0.0) for r in rows})
        if len(vals) > max_per_signal:
            step = len(vals) / max_per_signal
            vals = [vals[min(len(vals) - 1, int(i * step))] for i in range(max_per_signal)]
        for v in vals:
            rules.append((name, direction, v))
    for name, _ in BINARY_SIGNALS:
        rules.append((name, "hi", 1.0))
    return rules


def search_combination(rows, y, max_rules=3, max_gated=1.0, beam=40):
    rules = candidate_rules(rows)
    scored = []
    for rule in rules:
        pred = rule_predict(rows, rule)
        c = _counts(pred, y)
        scored.append((c, [rule]))
    scored.sort(key=lambda item: (-(item[0]["recall"] or 0), item[0]["gated_fraction"]))
    best = [s for s in scored if s[0]["gated_fraction"] <= max_gated]
    frontier = [s[1] for s in scored[:beam]]
    for _ in range(max_rules - 1):
        new_frontier = []
        for combo in frontier:
            for rule in rules:
                if rule in combo:
                    continue
                cand = combo + [rule]
                pred = or_predict(rows, cand)
                c = _counts(pred, y)
                if c["gated_fraction"] <= max_gated:
                    best.append((c, cand))
                new_frontier.append((c, cand))
        new_frontier.sort(key=lambda item: (-(item[0]["recall"] or 0), item[0]["gated_fraction"]))
        frontier = [c for _, c in new_frontier[:beam]]
    best.sort(key=lambda item: (-(item[0]["f1"] or 0), item[0]["gated_fraction"]))
    return best


# ------------------------------------------------------------------------ main

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ART / "vvg_eval.json"))
    ap.add_argument("--set", default="all", choices=["all", "manifest", "fresh"])
    ap.add_argument("--label", default="y_objection", choices=["y_objection", "y_failed", "y_corrupted", "y_suspicious",
                                 "y_core_contradiction"])
    args = ap.parse_args()

    rows = build_table()
    labelled = [r for r in rows if r.get(args.label) is not None]
    if args.set != "all":
        labelled = [r for r in labelled if r["set"] == args.set]
    y = [r[args.label] for r in labelled]

    result: dict[str, Any] = {
        "label_field": args.label,
        "label_definition": ("y_objection = the vision verifier returned anything other than "
                             "VERIFIED for this description"),
        "rows_total": len(rows),
        "rows_labelled": len(labelled),
        "base_rate_all": base_rate(y),
    }

    for subset in ("manifest", "fresh"):
        sub = [r for r in labelled if r["set"] == subset]
        result[f"base_rate_{subset}"] = base_rate([r[args.label] for r in sub])
    ctl = [r for r in labelled if r.get("family") in ("control", "fresh_control")]
    mut = [r for r in labelled if r.get("family") == "mutation"]
    result["base_rate_controls"] = base_rate([r[args.label] for r in ctl])
    result["base_rate_mutations"] = base_rate([r[args.label] for r in mut])

    # --- per-signal
    per_signal = []
    for name, direction, human in CANDIDATE_SIGNALS:
        vals = [float(r.get(name) or 0.0) for r in labelled]
        points = sweep(vals, y, direction)
        best = max((p for p in points if p["f1"] is not None), key=lambda p: p["f1"], default=None)
        per_signal.append({
            "signal": name, "direction": direction, "meaning": human,
            "auc": auc(vals, y, direction),
            "best_f1_point": best,
            "points": points,
            "distinct_values": len(set(vals)),
        })
    for name, human in BINARY_SIGNALS:
        vals = [1.0 if r.get(name) else 0.0 for r in labelled]
        pred = [int(v >= 1.0) for v in vals]
        c = _counts(pred, y)
        c["threshold"] = 1.0
        c["direction"] = "hi"
        per_signal.append({
            "signal": name, "direction": "hi", "meaning": human, "binary": True,
            "auc": auc(vals, y, "hi"), "best_f1_point": c, "points": [c],
            "distinct_values": len(set(vals)),
        })
    per_signal.sort(key=lambda s: -(s["auc"] or 0))
    result["per_signal"] = per_signal

    # --- combinations (in-sample, plus block-grouped cross validation)
    combos = search_combination(labelled, y, max_rules=3)
    result["best_combinations"] = [
        {"rules": [{"signal": r[0], "direction": r[1], "threshold": r[2]} for r in rules],
         **counts} for counts, rules in combos[:15]
    ]

    budgets = {}
    for budget in (0.15, 0.25, 0.33, 0.5, 0.67, 0.8):
        cand = search_combination(labelled, y, max_rules=3, max_gated=budget)
        cand = [c for c in cand if c[0]["gated_fraction"] <= budget]
        cand.sort(key=lambda item: (-(item[0]["recall"] or 0), item[0]["gated_fraction"]))
        singles = []
        for name, direction, _ in CANDIDATE_SIGNALS:
            vals = [float(r.get(name) or 0.0) for r in labelled]
            pts = [p for p in sweep(vals, y, direction) if p["gated_fraction"] <= budget]
            if pts:
                pts.sort(key=lambda p: (-(p["recall"] or 0), p["gated_fraction"]))
                singles.append({"signal": name, **pts[0]})
        for name, _ in BINARY_SIGNALS:
            pred = [1 if r.get(name) else 0 for r in labelled]
            c = _counts(pred, y)
            if c["gated_fraction"] <= budget:
                singles.append({"signal": name, "threshold": 1.0, "direction": "hi", **c})
        singles.sort(key=lambda p: (-(p["recall"] or 0), p["gated_fraction"]))
        budgets[str(budget)] = {
            "budget": budget,
            "best_single": singles[0] if singles else None,
            "best_combination": ({"rules": [{"signal": r[0], "direction": r[1], "threshold": r[2]}
                                            for r in cand[0][1]], **cand[0][0]} if cand else None),
        }
    result["operating_points"] = budgets

    # leave-one-group-out on the best single rule family, to expose selection optimism
    groups = sorted({r["group"] for r in labelled})
    logo = []
    for g in groups:
        train = [r for r in labelled if r["group"] != g]
        test = [r for r in labelled if r["group"] == g]
        ytr = [r[args.label] for r in train]
        yte = [r[args.label] for r in test]
        if not any(ytr) or not test:
            continue
        picked = search_combination(train, ytr, max_rules=2)
        if not picked:
            continue
        counts, rules = picked[0]
        pred = or_predict(test, rules)
        logo.append({"held_out_group": g, "rules": [list(r) for r in rules],
                     "train_f1": counts["f1"], "test": _counts(pred, yte)})
    agg = {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    for item in logo:
        for k in agg:
            agg[k] += item["test"][k]
    n = sum(agg.values())
    agg_metrics = {
        **agg, "n": n,
        "gated_fraction": round((agg["tp"] + agg["fp"]) / n, 4) if n else None,
        "precision": round(agg["tp"] / (agg["tp"] + agg["fp"]), 4) if (agg["tp"] + agg["fp"]) else None,
        "recall": round(agg["tp"] / (agg["tp"] + agg["fn"]), 4) if (agg["tp"] + agg["fn"]) else None,
    }
    result["leave_one_block_out"] = {"folds": logo, "pooled": agg_metrics}

    # --- irreducible error: identical signal vectors with different labels
    sig_names = [s for s, _, _ in CANDIDATE_SIGNALS] + [s for s, _ in BINARY_SIGNALS]
    buckets: dict[tuple, list[dict[str, Any]]] = {}
    for r in labelled:
        key = tuple(round(float(r.get(s) or 0.0), 6) for s in sig_names)
        buckets.setdefault(key, []).append(r)
    collisions = []
    ambiguous = 0
    for key, members in buckets.items():
        labels = {m[args.label] for m in members}
        if len(labels) > 1:
            ambiguous += len(members)
            collisions.append({
                "members": [{"id": m["id"], "mutation": m["mutation"],
                             "status": m["label_status"]} for m in members],
            })
    result["signal_collisions"] = {
        "distinct_signal_vectors": len(buckets),
        "rows_in_ambiguous_vectors": ambiguous,
        "max_accuracy_ceiling": round(1 - _ceiling_error(buckets, args.label) / len(labelled), 4),
        "examples": collisions[:12],
    }

    out = Path(args.out)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items()
                      if k not in ("per_signal", "best_combinations", "leave_one_block_out",
                                   "signal_collisions")}, ensure_ascii=False, indent=1))
    print("\nTop signals by AUC:")
    for s in per_signal[:12]:
        b = s["best_f1_point"] or {}
        print(f"  {s['signal']:<32} dir={s['direction']:<2} auc={s['auc']} "
              f"bestF1={b.get('f1')} prec={b.get('precision')} rec={b.get('recall')} "
              f"gated={b.get('gated_fraction')} thr={b.get('threshold')}")
    print(f"\nwrote {out}")


def _ceiling_error(buckets, label_field: str) -> int:
    err = 0
    for members in buckets.values():
        ones = sum(m[label_field] for m in members)
        err += min(ones, len(members) - ones)
    return err


if __name__ == "__main__":
    main()
