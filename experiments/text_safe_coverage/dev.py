"""Pinned SECTION/OWNER DEV experiments; predictions precede truth scoring."""
import argparse
from pathlib import Path
import time

from experiments.text_comparison_v1.common import read, write, file_hash
from experiments.text_comparison_v1.dev_score import TRUTH_SHA, metrics
from .sections import materialize, predict


def run(root, approach, name=None):
    root = Path(root)
    source = read(root / "DEV_TEXT_ONLY.json")
    if source["source_sha256"] != TRUTH_SHA or file_hash(source["source"]) != TRUTH_SHA:
        raise ValueError("DEV truth pin mismatch")
    cases = source["cases"]
    if any(c["kind"] not in {"SECTION", "OWNER"} for c in cases):
        raise ValueError("Non-TEXT truth prohibited")
    output = root / (name or ("dev_" + approach))
    if output.exists():
        raise ValueError("Immutable DEV run already exists")
    started = time.perf_counter()
    rows, quality = [], []
    for doc in read(root / "DEV_DOCUMENTS.json"):
        result = materialize(doc, approach=approach)
        version = doc["document_version"]
        write(output / version / "sections.json", result)
        quality.append({"document_version": version, **result["quality"]})
        for case in cases:
            if case["document_version"] == version:
                prediction = predict({k: case[k] for k in ("kind", "source_anchors")}, result)
                rows.append({"case_id": case["case_id"], "kind": case["kind"],
                             "document_version": version, "anchors": case["source_anchors"],
                             "prediction": prediction, "truth": case["final_answer"]})
    rows.sort(key=lambda r: r["case_id"])
    result = {"approach": approach, "truth_sha256": TRUTH_SHA,
              "section": metrics([r for r in rows if r["kind"] == "SECTION"]),
              "owner": metrics([r for r in rows if r["kind"] == "OWNER"]),
              "combined": metrics(rows), "quality": quality, "cases": rows}
    write(output / "scorecard.json", result)
    write(output / "performance.json", {"seconds": time.perf_counter() - started,
                                         "model_calls": 0, "input_tokens": 0, "output_tokens": 0})
    print({k: result[k] for k in ("approach", "section", "owner")}, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--approach", choices=["hierarchy", "layout", "continuity", "structural"], required=True)
    p.add_argument("--name")
    a = p.parse_args()
    run(a.root, a.approach, a.name)
