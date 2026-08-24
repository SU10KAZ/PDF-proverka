#!/usr/bin/env python3
"""VVD — scoring for ARM 1.

Merges the raw verifier records (``vvd_results_<tag>.json``) with the hand-written strict
judgements in ``artifacts/vvd_judgements.json`` and emits the tables that go into
``vvd_FINDINGS.md``:

* confusion matrix expected x returned status
* per-mutation detection / miss with the strict rule applied
* false-alarm buckets on the clean controls
* cost distribution (payload-attributable and raw), median and p90

Strict rule (applied by hand in vvd_judgements.json, replayed here):
a PARTIAL/FAILED that names the WRONG problem is a MISS.
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

STATUSES = ["VERIFIED", "PARTIAL", "FAILED", None]


def pct(a: int, b: int) -> str:
    return f"{a}/{b} ({100.0 * a / b:.0f}%)" if b else "0/0"


def quant(vals, p):
    s = sorted(vals)
    if not s:
        return None
    return s[min(len(s) - 1, int(round(p * (len(s) - 1))))]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tags", nargs="*", default=["main"])
    args = ap.parse_args()
    judge = json.loads((vv.ARTIFACTS / "vvd_judgements.json").read_text(encoding="utf-8"))
    jcases = judge["cases"]

    rows = []
    for tag in args.tags:
        data = json.loads((vv.ARTIFACTS / f"vvd_results_{tag}.json").read_text(encoding="utf-8"))
        for r in data["rows"]:
            key = f"{tag}/{r['record']}"
            r["judgement_key"] = key
            j = jcases.get(key, {})
            r["detection"] = j.get("detection")
            r["control_bucket"] = j.get("control_bucket")
            r["judge_note"] = j.get("note")
            rows.append(r)

    out = {"tags": args.tags, "n": len(rows)}

    # ---- confusion matrix (main tag only, by family)
    for tag in args.tags:
        sub = [r for r in rows if r["tag"] == tag]
        cm = collections.Counter((r["expected_status"], r["status"]) for r in sub)
        out.setdefault("confusion", {})[tag] = {f"{k[0]}->{k[1]}": v for k, v in sorted(cm.items())}

    # ---- detection per mutation kind
    det = collections.defaultdict(lambda: {"n": 0, "hit": 0, "miss": 0, "status_ok": 0,
                                           "cases_hit": [], "cases_miss": []})
    for r in rows:
        if r["family"] != "mutation":
            continue
        k = f"{r['tag']}:{r['mutation']}"
        d = det[k]
        d["n"] += 1
        d["status_ok"] += int(bool(r["status_acceptable"]))
        if r["detection"] == "HIT":
            d["hit"] += 1
            d["cases_hit"].append(r["record"])
        else:
            d["miss"] += 1
            d["cases_miss"].append(r["record"])
    out["detection_by_kind"] = {k: v for k, v in sorted(det.items())}

    # per block
    detb = collections.defaultdict(lambda: {"n": 0, "hit": 0})
    for r in rows:
        if r["family"] != "mutation":
            continue
        d = detb[f"{r['tag']}:{r['block']}"]
        d["n"] += 1
        d["hit"] += int(r["detection"] == "HIT")
    out["detection_by_block"] = {k: v for k, v in sorted(detb.items())}

    # ---- novel objections relative to the block's own clean control (same tag)
    ctrl_ids = {}
    for r in rows:
        if r["family"] == "control":
            ctrl_ids[(r["tag"], r["block"])] = set(r["suspicious_ids"])
    novel = []
    for r in rows:
        if r["family"] != "mutation":
            continue
        base = ctrl_ids.get((r["tag"], r["block"]))
        if base is None:
            base = ctrl_ids.get(("main", r["block"]), set())
            r["novel_baseline"] = "main_control"
        else:
            r["novel_baseline"] = "same_tag_control"
        n = sorted(set(r["suspicious_ids"]) - base)
        r["novel_objections"] = n
        r["novel_hits_changed_claim"] = bool(set(n) & set(r["changed_claims"]))
        novel.append({"tag": r["tag"], "record": r["record"], "mutation": r["mutation"],
                      "block": r["block"], "suspicious_ids": r["suspicious_ids"],
                      "control_ids": sorted(base), "novel": n,
                      "novel_hits_changed_claim": r["novel_hits_changed_claim"],
                      "detection": r["detection"]})
    out["novel_objections"] = novel
    nov = collections.defaultdict(lambda: {"n": 0, "any_novel": 0, "novel_on_changed": 0})
    for r in rows:
        if r["family"] != "mutation":
            continue
        d = nov[f"{r['tag']}:{r['mutation']}"]
        d["n"] += 1
        d["any_novel"] += int(bool(r["novel_objections"]))
        d["novel_on_changed"] += int(r["novel_hits_changed_claim"])
    out["novel_by_kind"] = {k: v for k, v in sorted(nov.items())}

    # ---- controls
    ctrl = collections.defaultdict(lambda: collections.Counter())
    ctrl_rows = []
    for r in rows:
        if r["family"] != "control":
            continue
        ctrl[r["tag"]][r["status"]] += 1
        ctrl[r["tag"]][f"bucket_{r['control_bucket']}"] += 1
        ctrl_rows.append({"tag": r["tag"], "record": r["record"], "block": r["block"],
                          "status": r["status"], "bucket": r["control_bucket"],
                          "suspicious_ids": r["suspicious_ids"], "note": r["judge_note"]})
    out["controls"] = {k: dict(v) for k, v in ctrl.items()}
    out["control_rows"] = ctrl_rows

    # ---- real defect cases
    out["real_defect"] = [{"tag": r["tag"], "record": r["record"], "block": r["block"],
                           "expected": r["expected_status"], "status": r["status"],
                           "acceptable": r["status_acceptable"], "detection": r["detection"],
                           "note": r["judge_note"]}
                          for r in rows if r["family"] == "real_defect"]

    # ---- cost
    cost = {}
    for tag in args.tags:
        sub = [r for r in rows if r["tag"] == tag and r["usage_payload_attributable"]]
        pay = [r["usage_payload_attributable"] for r in sub]
        raw = [sum(int((r["usage_raw"] or {}).get(k, 0) or 0)
                   for k in ("input_tokens", "cache_creation_input_tokens",
                             "cache_read_input_tokens", "output_tokens")) for r in sub]
        think = [int(((r["usage_raw"] or {}).get("output_tokens_details") or {})
                     .get("thinking_tokens", 0) or 0) for r in sub]
        cread = [int((r["usage_raw"] or {}).get("cache_read_input_tokens", 0) or 0) for r in sub]
        wall = [r["wall_seconds"] for r in sub if r["wall_seconds"]]
        cost[tag] = {
            "n": len(sub),
            "payload_median": statistics.median(pay), "payload_p90": quant(pay, 0.9),
            "payload_min": min(pay), "payload_max": max(pay), "payload_sum": sum(pay),
            "raw_total_median": statistics.median(raw), "raw_total_p90": quant(raw, 0.9),
            "cache_read_median": statistics.median(cread), "cache_read_sum": sum(cread),
            "thinking_median": statistics.median(think), "thinking_p90": quant(think, 0.9),
            "wall_median": statistics.median(wall), "wall_p90": quant(wall, 0.9),
            "wall_min": min(wall), "wall_max": max(wall), "wall_sum": sum(wall),
        }
    out["cost"] = cost

    dest = vv.ARTIFACTS / "vvd_scores.json"
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2)[:9000])
    print("written:", dest)


if __name__ == "__main__":
    main()
