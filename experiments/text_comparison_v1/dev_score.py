"""DEV-only SECTION/OWNER scorer. No TABLE answers enter predictions or rules."""
from collections import Counter
from pathlib import Path
import argparse
import hashlib

from .common import file_hash, read, write
from .sections import materialize

TRUTH_SHA = "71ed12f693442665a64c073a56f3d13df321bfdd0303f4a378286437616906d7"


def predict(case, result):
    refs = {r["line_id"]: r for r in result["ownership"]}
    anchors = case["source_anchors"]
    rows = []
    for a in anchors:
        row = refs.get(a["line_id"])
        if row is None or any(row["source_ref"].get(k) != a[k] for k in
                ("page", "block_id", "markdown_line", "line_sha256")):
            return {"answer": "REVIEW", "reason": "SOURCE_ANCHOR_MISMATCH"}
        rows.append(row)
    left, right = rows
    if case["kind"] == "OWNER":
        # DEV asks whether the preceding fragment belongs to the following
        # heading. A table cannot belong to its following narrative section.
        if left["route"] == "TABLE" and right["route"] == "TEXT" and right["status"] == "PROVEN":
            return {"answer": "NO", "reason": "TABLE_EXCLUDED_FROM_FOLLOWING_NARRATIVE_OWNER"}
    if any(r["route"] != "TEXT" or r["status"] != "PROVEN" for r in rows):
        return {"answer": "REVIEW", "reason": "NON_NARRATIVE_OR_UNRESOLVED_OWNER"}
    edges = [d for d in result["decisions"] if anchors[0]["line_id"] < d["right_anchor"] <= anchors[1]["line_id"]]
    if any(d["decision"] == "REVIEW" for d in edges):
        return {"answer": "REVIEW", "reason": "REVIEW_BOUNDARY_ON_PATH"}
    if left["section_owner"] == right["section_owner"]:
        return {"answer": "YES", "reason": "PROVEN_SAME_DIRECT_OWNER"}
    # Same Foundation AnchorResolver contract: a repeated ancestor heading
    # resumes its section; direct child owner IDs need not be identical.
    if edges and all(d["decision"] == "SAME" for d in edges):
        return {"answer": "YES", "reason": "REPEATED_ANCESTOR_NO_NEW_BOUNDARY",
                "direct_owners_equal": False}
    return {"answer": "NO", "reason": "PROVEN_NEW_BOUNDARY"}


def metrics(rows):
    labelled = [r for r in rows if r["truth"] in {"YES", "NO"}]
    proven = [r for r in labelled if r["prediction"]["answer"] != "REVIEW"]
    correct = sum(r["truth"] == r["prediction"]["answer"] for r in proven)
    return {"N": len(labelled), "proven": len(proven), "proven_correct": correct,
            "proven_accuracy": correct / len(proven) if proven else None,
            "coverage": len(proven) / len(labelled) if labelled else None,
            "legacy_score_review_incorrect": correct / len(labelled) if labelled else None,
            "review": len(labelled) - len(proven), "review_rate": (len(labelled) - len(proven)) / len(labelled) if labelled else None,
            "false_split": sum(r["truth"] == "YES" and r["prediction"]["answer"] == "NO" for r in proven),
            "false_merge": sum(r["truth"] == "NO" and r["prediction"]["answer"] == "YES" for r in proven),
            "truth_distribution": dict(Counter(r["truth"] for r in labelled))}


def run(truth_path, documents_path, output):
    if file_hash(truth_path) != TRUTH_SHA:
        raise ValueError("Wrong DEV truth")
    cases = [c for c in read(truth_path)["cases"] if c["kind"] in {"SECTION", "OWNER"}]
    docs = {d["document_version"]: d for d in read(documents_path)}
    rows, quality = [], []
    output = Path(output)
    if output.exists():
        raise ValueError("Immutable DEV run already exists")
    for version in sorted({c["document_version"] for c in cases}):
        result = materialize(docs[version])
        write(output / version / "sections.json", result)
        quality.append({"document_version": version, **result["quality"]})
        for c in cases:
            if c["document_version"] == version:
                rows.append({"case_id": c["case_id"], "kind": c["kind"], "truth": c["final_answer"],
                             "anchors": c["source_anchors"], "document_version": version, "prediction": predict(c, result)})
    rows.sort(key=lambda r: r["case_id"])
    scorecard = {"truth_sha256": TRUTH_SHA, "purpose": "DEV_ONLY_NOT_RELEASE_VALIDATION",
                 "section": metrics([r for r in rows if r["kind"] == "SECTION"]),
                 "owner": metrics([r for r in rows if r["kind"] == "OWNER"]),
                 "combined": metrics(rows), "documents": len(quality), "quality": quality, "cases": rows}
    write(output / "scorecard.json", scorecard)
    print({k: scorecard[k] for k in ("section", "owner", "combined", "documents")}, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--truth", required=True)
    p.add_argument("--documents", required=True)
    p.add_argument("--output", required=True)
    a = p.parse_args()
    run(a.truth, a.documents, a.output)
