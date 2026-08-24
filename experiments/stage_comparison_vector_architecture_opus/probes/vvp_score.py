#!/usr/bin/env python3
"""ARM 2 scoring: classify every emitted line and score both pipelines against
artifacts/vvp_ground_truth.json.  Research only."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path("/home/coder/projects/PDF-proverka")
sys.path.insert(0, str(ROOT))
EXP = ROOT / "experiments" / "stage_comparison_vector_architecture_opus"
ART = EXP / "artifacts"
TRACK_A = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"

CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

GT = json.loads((ART / "vvp_ground_truth.json").read_text(encoding="utf-8"))["pairs"]

PAIR_ORDER = [
    "ss_simple_node", "ar_plan", "ar_wall_sections", "ss_plan_dense", "ss_table_graphic",
    "vk_plan", "vk_node_plan", "vk_nodes", "ss_scheme_text_changed", "eom_singleline_changed",
]

# Lines whose class the rules cannot decide are listed here with a hand verdict and a reason.
# (kept empty unless a rule genuinely cannot decide; every entry is stated in vvp_FINDINGS.md)
MANUAL: dict[tuple[str, str], tuple[str, str]] = {}


def load_comparison(pair_id: str) -> dict[str, Any]:
    return json.loads((TRACK_A / "comparisons" / pair_id / "comparison.json").read_text(encoding="utf-8"))


def _gt_keywords(pair_id: str) -> list[tuple[str, list[str]]]:
    return [(c["id"], [k.lower() for k in c["keywords"]]) for c in GT[pair_id]["real_changes"]]


def classify_line(pair_id: str, line: str) -> dict[str, Any]:
    """Classify one comparator difference line.

    substantive - the line carries (part of) a ground-truth change
    false       - the line asserts a difference the ground truth says does not exist,
                  or is a catalogued artefact (O5 motifs, O7 primitive count, O8 mojibake)
    stat        - a true-but-unreadable machine statistic on a pair that really did change
    """
    key = (pair_id, line)
    if key in MANUAL:
        cls, why = MANUAL[key]
        return {"line": line, "class": cls, "rule": "MANUAL", "why": why}
    has_real = bool(GT[pair_id]["real_changes"])

    if line.startswith("Число примитивов"):
        return {"line": line, "class": "false", "rule": "R1",
                "why": "primitive count is PDF path packaging (O7), not content"}
    if line.startswith("Изменены повторяющиеся motifs"):
        return {"line": line, "class": "false", "rule": "R2",
                "why": "repeated-motif fingerprints are resampling/rect artefacts (O5)"}
    if line.startswith("Топология изменилась"):
        return ({"line": line, "class": "stat", "rule": "R3a",
                 "why": "true statistic on a pair that really changed, but not a readable statement"}
                if has_real else
                {"line": line, "class": "false", "rule": "R3b",
                 "why": "topology similarity moved on a pair with no real change"})
    if line.startswith("Текст/значение"):
        body = line[len("Текст/значение "):]
        if CTRL.search(body):
            return {"line": line, "class": "false", "rule": "R4a",
                    "why": "mojibake value pair printed from a text layer the comparator itself calls unreliable (O8/S12)"}
        low = body.lower()
        for cid, kws in _gt_keywords(pair_id):
            if sum(1 for k in kws if k in low) >= 2:
                return {"line": line, "class": "substantive", "rule": "R4b",
                        "why": f"carries ground-truth change {cid}", "gt": cid}
        return {"line": line, "class": "false", "rule": "R4c",
                "why": "text value pair not present in the ground-truth change list"}
    if line.startswith("Добавлено text items") or line.startswith("Удалено text items"):
        low = line.lower()
        hits = [cid for cid, kws in _gt_keywords(pair_id) if sum(1 for k in kws if k in low) >= 2]
        if hits:
            return {"line": line, "class": "substantive", "rule": "R5a",
                    "why": "the added/removed list contains ground-truth change tokens",
                    "gt": hits}
        return {"line": line, "class": "false", "rule": "R5b",
                "why": "added/removed list carries no ground-truth change token"}
    return {"line": line, "class": "false", "rule": "R0", "why": "unclassified line"}


def deterministic_layer(pair_id: str) -> dict[str, Any]:
    cmp_ = load_comparison(pair_id)
    lines = [classify_line(pair_id, d) for d in cmp_["differences"]]
    return {
        "pair_id": pair_id,
        "status": cmp_["status"],
        "lines": lines,
        "n_lines": len(lines),
        "n_false": sum(1 for x in lines if x["class"] == "false"),
        "n_substantive": sum(1 for x in lines if x["class"] == "substantive"),
        "n_stat": sum(1 for x in lines if x["class"] == "stat"),
        "gt_hit": sorted({c for x in lines for c in ([x["gt"]] if isinstance(x.get("gt"), str)
                                                     else x.get("gt", []))}),
    }


def main() -> None:
    out = {p: deterministic_layer(p) for p in PAIR_ORDER}
    (ART / "vvp_deterministic_lines.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{'pair':26s} {'status':30s} lines false subst stat  gt_hit")
    for p in PAIR_ORDER:
        r = out[p]
        gt_all = [c["id"] for c in GT[p]["real_changes"]]
        print(f"{p:26s} {r['status']:30s} {r['n_lines']:5d} {r['n_false']:5d} "
              f"{r['n_substantive']:5d} {r['n_stat']:4d}  {r['gt_hit']} / {gt_all}")


if __name__ == "__main__":
    main()


# --------------------------------------------------------------- statement scoring

def _hits(text: str, kws: list[str]) -> int:
    low = text.lower()
    return sum(1 for k in kws if k in low)


def score_statements(pair_id: str, lines: list[str],
                     overrides: dict[str, tuple[str, str]] | None = None) -> list[dict[str, Any]]:
    """For each ground-truth change, is it readable out of `lines`?

    surfaced   - one single line names the change well enough to be read as the statement
    token_only - the tokens occur (in an aggregate list, or split over lines) but the
                 statement cannot be read off without reconstructing it
    missed     - fewer than two of its keywords occur anywhere in the output
    """
    overrides = overrides or {}
    out = []
    for change in GT[pair_id]["real_changes"]:
        kws = [k.lower() for k in change["keywords"]]
        best_line, best = None, 0
        for line in lines:
            h = _hits(line, kws)
            if h > best:
                best, best_line = h, line
        total = _hits(" \n ".join(lines), kws)
        if best >= 2 and best_line is not None and not best_line.lower().startswith(
                ("добавлено text items", "удалено text items")):
            level = "surfaced"
        elif total >= 2:
            level = "token_only"
        else:
            level = "missed"
        why = "rule"
        if change["id"] in overrides:
            level, why = overrides[change["id"]]
        out.append({"id": change["id"], "ru": change["ru"], "level": level,
                    "best_line": best_line, "keyword_hits_in_best_line": best,
                    "keyword_hits_total": total, "why": why})
    return out
