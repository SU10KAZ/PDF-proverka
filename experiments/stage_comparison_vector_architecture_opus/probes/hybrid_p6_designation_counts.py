#!/usr/bin/env python3
"""Probe HYBRID-6: can «Количество аппаратов 12 → 14» be derived, and from what?

Three candidate sources are tested on all 10 benchmark pairs:
  S1 repeated_elements pattern ids      (Track A's only counting layer)
  S2 raw text spans, designation regex  (letter-prefix + index, e.g. QF1)
  S3 text LINES, designation regex      (probe HYBRID-3 object layer)

For each source: how many "count changed" statements it produces, and how many
of those are FALSE (pairs whose human verdict is IDENTICAL / NEAR_IDENTICAL must
produce none; ar_plan/ar_wall_sections are literally the same PDF bytes — O1).

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p6_designation_counts
"""
from __future__ import annotations

import collections
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TA = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

from experiments.stage_comparison_vector_architecture_opus.probes import (
    hybrid_p3_object_layer_gain as p3,
)

DESIG = re.compile(r"^\s*([A-Za-zА-Яа-яЁё]{1,8})\s*[-–—]?\s*(\d{1,3})\s*$")
STAMP_WORDS = {
    "Согласовано", "Подп. и дата", "Взам. инв. №", "Инв. № подл.", "ГАП", "ГКП", "Изм.",
    "Кол.уч.", "Лист", "№ док.", "Подп.", "Дата", "Стадия", "Листов", "Разраб.", "Пров.",
    "Н.контр.", "Утв.", "Гл.спец.ОВ", "Гл.спец.ВК", "Гл.спец.ЭОМ", "Взам. инв.",
}


def load(pid, side):
    return json.loads((TA / "descriptions" / pid / side / "vector_block.json").read_text("utf-8"))


def desig_counts(strings) -> collections.Counter:
    c = collections.Counter()
    for s in strings:
        m = DESIG.match(s)
        if m:
            c[m.group(1)] += 1
    return c


def pattern_counts(desc):
    return collections.Counter({e["pattern_id"]: e["count"] for e in desc["repeated_elements"]})


def diff_counts(l: collections.Counter, r: collections.Counter):
    keys = set(l) | set(r)
    return {k: [l.get(k, 0), r.get(k, 0)] for k in sorted(keys) if l.get(k, 0) != r.get(k, 0)}


def main() -> None:
    pairs = json.loads((TA / "block_pairs.json").read_text("utf-8"))["pairs"]
    human = json.loads((TA / "human_validation.json").read_text("utf-8"))
    verdicts = {}
    for row in (human if isinstance(human, list) else human.get("pairs", human.get("results", []))):
        verdicts[row.get("pair_id")] = row.get("human_expected") or row.get("expected") or row.get("verdict")
    res = {}
    for p in pairs:
        pid = p["pair_id"]
        l, r = load(pid, "left"), load(pid, "right")
        s1 = diff_counts(pattern_counts(l), pattern_counts(r))
        s2 = diff_counts(
            desig_counts(t["text"] for t in l["texts"]),
            desig_counts(t["text"] for t in r["texts"]),
        )
        s3 = diff_counts(
            desig_counts(x["text"] for x in p3.to_lines(l)),
            desig_counts(x["text"] for x in p3.to_lines(r)),
        )
        res[pid] = {
            "human_expected": p.get("human_expected") or verdicts.get(pid),
            "same_pdf_bytes": l["source"]["pdf_sha256"] == r["source"]["pdf_sha256"],
            "S1_pattern_count_changes": len(s1),
            "S2_span_designation_changes": s2,
            "S3_line_designation_changes": s3,
        }
    # false-positive tally on pairs where nothing may be counted as changed
    quiet = [k for k, v in res.items() if v["human_expected"] in ("IDENTICAL", "NEAR_IDENTICAL")]
    summary = {
        "quiet_pairs": quiet,
        "S1_false_statements": sum(res[k]["S1_pattern_count_changes"] for k in quiet),
        "S2_false_statements": sum(len(res[k]["S2_span_designation_changes"]) for k in quiet),
        "S3_false_statements": sum(len(res[k]["S3_line_designation_changes"]) for k in quiet),
        "S1_on_same_pdf_bytes": {k: res[k]["S1_pattern_count_changes"] for k in res if res[k]["same_pdf_bytes"]},
        "S2_on_same_pdf_bytes": {k: res[k]["S2_span_designation_changes"] for k in res if res[k]["same_pdf_bytes"]},
        "S3_on_same_pdf_bytes": {k: res[k]["S3_line_designation_changes"] for k in res if res[k]["same_pdf_bytes"]},
        "eom_S3": res["eom_singleline_changed"]["S3_line_designation_changes"],
        "eom_S2": res["eom_singleline_changed"]["S2_span_designation_changes"],
        "eom_S1": res["eom_singleline_changed"]["S1_pattern_count_changes"],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_designation_counts.json").write_text(
        json.dumps({"per_pair": res, "summary": summary}, ensure_ascii=False, indent=2) + "\n", "utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print()
    for k, v in res.items():
        print(f"{k:24s} {str(v['human_expected'])[:28]:30s} S1={v['S1_pattern_count_changes']:>3} "
              f"S2={len(v['S2_span_designation_changes']):>3} S3={len(v['S3_line_designation_changes']):>3} "
              f"{json.dumps(v['S3_line_designation_changes'], ensure_ascii=False)[:110]}")


if __name__ == "__main__":
    main()
