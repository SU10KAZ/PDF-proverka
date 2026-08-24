#!/usr/bin/env python3
"""ARM 2 — assemble Pipeline A and Pipeline B from the recorded artefacts and score both.
Research only."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path("/home/coder/projects/PDF-proverka")
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_architecture_opus.probes import vvp_score as S  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes import vvp_pipelines as P  # noqa: E402

ART = S.ART
VERIFY = ART / "vvp_verify"
VISION = ART / "vvp_vision"

SCALE = {"IDENTICAL": 0, "NEAR_IDENTICAL": 1, "STRUCTURE_SAME_VALUES_CHANGED": 2,
         "STRUCTURE_CHANGED": 3}


def verdict_grade(got: str | None, human: str) -> str:
    if got is None:
        return "wrong"
    if got == human:
        return "correct"
    if got not in SCALE:
        return "wrong"
    return "partial" if abs(SCALE[got] - SCALE[human]) == 1 else "wrong"


def _load(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def verification(pair_id: str, side: str) -> dict[str, Any] | None:
    return _load(VERIFY / f"{pair_id}__{side}.json")


def vision_compare(pair_id: str) -> dict[str, Any] | None:
    return _load(VISION / f"compare__{pair_id}.json")


def gapfill(pair_id: str) -> dict[str, Any] | None:
    return _load(VISION / f"gapfill__{pair_id}.json")


def _tok(rec: dict[str, Any] | None) -> int:
    return int((rec or {}).get("usage_payload_attributable") or 0)


def _raw(rec: dict[str, Any] | None) -> int:
    u = (rec or {}).get("usage_raw") or {}
    return sum(int(u.get(k, 0)) for k in ("input_tokens", "cache_creation_input_tokens",
                                          "cache_read_input_tokens", "output_tokens"))


def _wall(rec: dict[str, Any] | None) -> float:
    return float((rec or {}).get("wall_seconds") or 0.0)


def vision_change_lines(rec: dict[str, Any] | None) -> list[str]:
    ans = (rec or {}).get("answer") or {}
    out = []
    for c in ans.get("changes") or []:
        kind = c.get("kind", "?")
        out.append(f"[VISION/{kind}] {c.get('text','')}")
    for g in ans.get("gap_findings") or []:
        same = g.get("same_in_both")
        out.append(f"[VISION-GAP/{g.get('kind','?')}|same={same}] {g.get('text','')}")
    return out


def classify_vision_line(pair_id: str, line: str) -> dict[str, Any]:
    """A Vision line is false if it asserts a project change on a pair whose ground truth
    has none, or if it asserts something no ground-truth change covers on a pair that changed.
    Lines tagged crop/presentation are counted separately - they are honest hedges, not claims
    about the project."""
    gt = S.GT[pair_id]["real_changes"]
    if "|same=True" in line or "|same=true" in line:
        return {"line": line, "class": "hedge", "rule": "V0", "why": "gap resolved as identical"}
    if line.startswith("[VISION/crop]") or line.startswith("[VISION/presentation]") or \
       line.startswith("[VISION-GAP/crop") or line.startswith("[VISION-GAP/presentation"):
        return {"line": line, "class": "hedge", "rule": "V1",
                "why": "flagged as crop/presentation, not asserted as a project change"}
    low = line.lower()
    for c in gt:
        if sum(1 for k in c["keywords"] if k.lower() in low) >= 2:
            return {"line": line, "class": "substantive", "rule": "V2",
                    "why": f"carries ground-truth change {c['id']}", "gt": c["id"]}
    return {"line": line, "class": "false", "rule": "V3",
            "why": "asserts a project change not present in the ground truth"}


def build(pair_id: str) -> dict[str, Any]:
    cmp_ = S.load_comparison(pair_id)
    human = S.GT[pair_id]["human_verdict"]
    det = S.deterministic_layer(pair_id)
    det_lines = [x["line"] for x in det["lines"]]
    gate = P.difficulty(pair_id)
    vl, vr = verification(pair_id, "left"), verification(pair_id, "right")
    vc = vision_compare(pair_id)
    gf = gapfill(pair_id)

    # ---------------- Pipeline A
    a_calls, a_tok, a_raw, a_wall = 0, 0, 0, 0.0
    if gate["escalate"]:
        a_calls, a_tok, a_raw, a_wall = 1, _tok(vc), _raw(vc), _wall(vc)
        a_verdict_adjudicate = ((vc or {}).get("answer") or {}).get("verdict")
        a_lines = det_lines + vision_change_lines(vc)
    else:
        a_verdict_adjudicate = cmp_["status"]
        a_lines = det_lines
    a_verdict_supplement = cmp_["status"]

    # ---------------- Pipeline B
    sl, sr = (vl or {}).get("status"), (vr or {}).get("status")
    b_tok = _tok(vl) + _tok(vr)
    b_raw = _raw(vl) + _raw(vr)
    b_wall = _wall(vl) + _wall(vr)
    b_calls = 2
    if sl == "VERIFIED" and sr == "VERIFIED":
        route = "DETERMINISTIC"
        b_verdict, b_lines = cmp_["status"], det_lines
    elif "FAILED" in (sl, sr):
        route = "VISION_ONLY"
        b_calls += 1
        b_tok += _tok(vc); b_raw += _raw(vc); b_wall += _wall(vc)
        b_verdict = ((vc or {}).get("answer") or {}).get("verdict")
        b_lines = vision_change_lines(vc)
    else:
        route = "GAPFILL"
        b_calls += 1
        b_tok += _tok(gf); b_raw += _raw(gf); b_wall += _wall(gf)
        b_verdict = ((gf or {}).get("answer") or {}).get("verdict_after_gapfill") or cmp_["status"]
        b_lines = det_lines + vision_change_lines(gf)

    def score(lines: list[str]) -> dict[str, Any]:
        cls = []
        for ln in lines:
            cls.append(classify_vision_line(pair_id, ln) if ln.startswith("[VISION")
                       else S.classify_line(pair_id, ln))
        st = S.score_statements(pair_id, lines)
        return {
            "lines": cls,
            "n_lines": len(cls),
            "n_false": sum(1 for x in cls if x["class"] == "false"),
            "n_substantive": sum(1 for x in cls if x["class"] == "substantive"),
            "n_stat": sum(1 for x in cls if x["class"] == "stat"),
            "n_hedge": sum(1 for x in cls if x["class"] == "hedge"),
            "statements": st,
            "surfaced": [s["id"] for s in st if s["level"] == "surfaced"],
            "token_only": [s["id"] for s in st if s["level"] == "token_only"],
            "missed": [s["id"] for s in st if s["level"] == "missed"],
        }

    return {
        "pair_id": pair_id,
        "human_verdict": human,
        "comparator_status": cmp_["status"],
        "gate": gate,
        "verification": {"left": sl, "right": sr,
                         "left_suspicious": [s.get("claim_id") for s in
                                             (((vl or {}).get("verdict") or {}).get("suspicious") or [])],
                         "right_suspicious": [s.get("claim_id") for s in
                                              (((vr or {}).get("verdict") or {}).get("suspicious") or [])],
                         "left_missing": ((vl or {}).get("verdict") or {}).get("missing") or [],
                         "right_missing": ((vr or {}).get("verdict") or {}).get("missing") or []},
        "A": {"escalated": gate["escalate"], "verdict": a_verdict_adjudicate,
              "verdict_supplement_policy": a_verdict_supplement,
              "grade": verdict_grade(a_verdict_adjudicate, human),
              "grade_supplement_policy": verdict_grade(a_verdict_supplement, human),
              "vision_calls": a_calls, "tokens_payload": a_tok, "tokens_raw": a_raw,
              "wall_seconds": round(a_wall, 1), **score(a_lines)},
        "B": {"route": route, "verdict": b_verdict, "grade": verdict_grade(b_verdict, human),
              "vision_calls": b_calls, "tokens_payload": b_tok, "tokens_raw": b_raw,
              "wall_seconds": round(b_wall, 1), **score(b_lines)},
    }


def main() -> None:
    out = {p: build(p) for p in S.PAIR_ORDER}
    (ART / "vvp_pipeline_results.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    hdr = (f"{'pair':24s}|{'human':30s}| A verdict                     g   lines F  surf/tok/miss | "
           f"B route        B verdict                     g   lines F  surf/tok/miss")
    print(hdr); print("-" * len(hdr))
    for p in S.PAIR_ORDER:
        r = out[p]
        a, b = r["A"], r["B"]
        print(f"{p:24s}|{r['human_verdict']:30s}| {str(a['verdict']):29s} {a['grade'][0].upper()} "
              f"{a['n_lines']:5d} {a['n_false']:3d} {len(a['surfaced'])}/{len(a['token_only'])}/{len(a['missed'])}"
              f"         | {b['route']:14s} {str(b['verdict']):29s} {b['grade'][0].upper()} "
              f"{b['n_lines']:5d} {b['n_false']:3d} {len(b['surfaced'])}/{len(b['token_only'])}/{len(b['missed'])}")
    for k in ("A", "B"):
        calls = sum(out[p][k]["vision_calls"] for p in S.PAIR_ORDER)
        tok = sum(out[p][k]["tokens_payload"] for p in S.PAIR_ORDER)
        raw = sum(out[p][k]["tokens_raw"] for p in S.PAIR_ORDER)
        wall = sum(out[p][k]["wall_seconds"] for p in S.PAIR_ORDER)
        false = sum(out[p][k]["n_false"] for p in S.PAIR_ORDER)
        grades = [out[p][k]["grade"] for p in S.PAIR_ORDER]
        surf = sum(len(out[p][k]["surfaced"]) for p in S.PAIR_ORDER)
        tokonly = sum(len(out[p][k]["token_only"]) for p in S.PAIR_ORDER)
        miss = sum(len(out[p][k]["missed"]) for p in S.PAIR_ORDER)
        print(f"\nPipeline {k}: calls={calls} payload_tokens={tok} raw_tokens={raw} "
              f"wall={wall:.0f}s false_lines={false} "
              f"verdicts correct/partial/wrong={grades.count('correct')}/{grades.count('partial')}/{grades.count('wrong')} "
              f"statements surfaced/token_only/missed={surf}/{tokonly}/{miss}")


if __name__ == "__main__":
    main()
