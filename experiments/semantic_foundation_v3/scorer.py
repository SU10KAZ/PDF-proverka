"""Scorer V3. Predictions must be frozen before a separate invocation reads answers."""
from collections import Counter
from pathlib import Path
import argparse
import hashlib
import json

from . import frozen_v1 as v1


def abstain(code):
    return {"decision": "REVIEW", "basis": "DEFAULT", "evidence_codes": [code]}


class AnchorResolver:
    def __init__(self, result):
        self.result = result
        ledger = result["ledger"]
        self.columns = ledger["columns"]
        self.blocks = ledger["blocks"]
        source = ledger["sources"]["work_md"]
        if v1.file_sha(Path(source["path"])) != source["sha256"]:
            raise ValueError("Source content changed")
        self.raw_lines = Path(source["path"]).read_text(errors="replace").splitlines()
        self.locators = {}
        for i, ref in enumerate(self.columns["block_ref"]):
            block = self.blocks[ref]
            self.locators[(block["page"], block["block_id"], self.columns["markdown_line"][i])] = i

    def resolve(self, anchor):
        i = self.locators.get((anchor["page"], anchor["block_id"], anchor["markdown_line"]))
        if i is None:
            return None
        raw = self.raw_lines[anchor["markdown_line"] - 1]
        if hashlib.sha256(raw.encode()).hexdigest() != anchor["line_sha256"]:
            return None
        return i

    def predict(self, case):
        if case["document_version"] != self.result["ledger"]["document_version"]:
            return abstain("DOCUMENT_VERSION_MISMATCH")
        a, b = [self.resolve(x) for x in case["anchors"]]
        if a is None or b is None:
            return abstain("MISSING_SOURCE_ANCHOR")
        if a > b:
            return abstain("REVERSED_ANCHORS")
        if case["kind"] not in {"SECTION", "TABLE"}:
            return abstain("OWNERSHIP_QUESTION_NOT_BOUNDARY")
        owners = [self.columns["owner"][i] for i in (a, b)]
        if not all(isinstance(o, int) for o in owners):
            return abstain("EXCLUDED_OR_UNOWNED_ANCHOR")
        units = self.result["semantics"]["units"]
        expected_kind = {"SECTION": "SECTION_FRAGMENT", "TABLE": "TABLE_SEGMENT"}[case["kind"]]
        if any(units[o]["kind"] != expected_kind for o in owners):
            return abstain("WRONG_ANCHOR_KIND")
        if owners[0] == owners[1]:
            if expected_kind == "SECTION_FRAGMENT" and units[owners[0]]["status"] != "PROVEN":
                return abstain("PROVISIONAL_OWNERSHIP_IS_NOT_SAME")
            return {"decision": "SAME", "basis": "PROVEN", "evidence_codes": ["WITHIN_ONE_PROVEN_UNIT"]}
        edges = [d for d in self.result["decisions"]["decisions"]
                 if d["kind"] == case["kind"] and a < d["right_anchor"] <= b]
        if not edges or any(d["decision"] == "REVIEW" for d in edges):
            return abstain("REVIEW_EDGE_ON_PATH" if edges else "NO_PUBLISHED_EDGE")
        return {"decision": "NEW" if any(d["decision"] == "NEW" for d in edges) else "SAME",
                "basis": "PROVEN", "evidence_codes": sorted({c for d in edges for c in d["evidence_codes"]})}


def score(predictions, answers):
    """Unanswered/UNSURE are unscored and counted, never invented as labels."""
    invalid = {x for x in answers.values() if x not in {None, "UNSURE", "SAME", "NEW"}}
    if invalid:
        raise ValueError(f"Invalid truth labels: {invalid}")
    if set(answers) - set(predictions):
        raise ValueError("Truth has cases outside the frozen prediction universe")
    for p in predictions.values():
        if p["decision"] not in {"SAME", "NEW", "REVIEW"} or p["basis"] not in {"PROVEN", "DEFAULT"}:
            raise ValueError("Invalid prediction")
        if p["decision"] == "REVIEW" and p["basis"] == "PROVEN":
            raise ValueError("REVIEW cannot be PROVEN")
    labelled = {k: v for k, v in answers.items() if v in {"SAME", "NEW"}}
    proven = {k: predictions[k] for k in labelled
              if predictions[k]["basis"] == "PROVEN" and predictions[k]["decision"] != "REVIEW"}
    correct = sum(p["decision"] == labelled[k] for k, p in proven.items())
    legacy_correct = sum(predictions[k]["decision"] == v for k, v in labelled.items())
    counts = Counter(labelled.values())
    return {"labelled": len(labelled), "prediction_universe": len(predictions),
            "unanswered": len(predictions) - len(answers) + sum(v is None for v in answers.values()),
            "unsure": sum(v == "UNSURE" for v in answers.values()),
            "proven": len(proven), "proven_correct": correct,
            "accuracy_on_proven": correct / len(proven) if proven else None,
            "coverage": len(proven) / len(labelled) if labelled else None,
            "legacy_review_incorrect": legacy_correct / len(labelled) if labelled else None,
            "majority_class_share": max(counts.values()) / len(labelled) if labelled else None,
            "review_distribution": dict(Counter(predictions[k]["decision"] for k in labelled))}


def freeze_predictions(path, predictions, namespace, packet_sha256):
    path = Path(path)
    if namespace != "SEMANTIC_FOUNDATION_V3_DEV":
        raise ValueError("Foundation scorer CLI is DEV-only; a fresh holdout protocol is required")
    data = v1.canonical_bytes({"namespace": namespace, "packet_sha256": packet_sha256, "predictions": predictions})
    with path.open("xb") as handle:
        handle.write(data)
    path.with_suffix(path.suffix + ".sha256").write_text(hashlib.sha256(data).hexdigest() + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--answers", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    expected = args.predictions.with_suffix(args.predictions.suffix + ".sha256").read_text().strip()
    if v1.file_sha(args.predictions) != expected:
        raise ValueError("Prediction freeze changed")
    frozen = json.loads(args.predictions.read_text())
    if frozen["namespace"] != "SEMANTIC_FOUNDATION_V3_DEV":
        raise ValueError("Only the separate DEV namespace is accepted")
    # This is the only answer read, after the prediction freeze was checked.
    truth = json.loads(args.answers.read_text())
    if truth["namespace"] != frozen["namespace"] or truth["packet_sha256"] != frozen["packet_sha256"]:
        raise ValueError("Truth namespace/packet mismatch")
    args.output.write_bytes(v1.canonical_bytes(score(frozen["predictions"], truth["answers"])))


if __name__ == "__main__":
    main()
