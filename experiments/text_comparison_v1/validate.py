"""Independent persisted source/schema/partition checks for final artifacts."""
from collections import Counter
from pathlib import Path
import argparse
import hashlib
import subprocess
import sys

from .common import file_hash, read, write


def validate(root, repo):
    root, repo = Path(root), Path(repo)
    sys.path.insert(0, str(root / "validation_deps"))
    from jsonschema import Draft202012Validator
    schema = read(root / "reports/TEXT_SECTION_V3_SCHEMA.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    counts, errors = Counter(), []
    section_ids = {}
    inputs = read(root / "FROZEN_PROJECT_INPUTS.json")
    documents = {p[side]["document_version"]: p[side] for p in inputs["pairs"] for side in ("old", "new")}
    for version, doc in documents.items():
        for kind in ("work_md", "blocks", "pdf"):
            receipt = doc["artifacts"][kind]
            if file_hash(receipt["path"]) != receipt["sha256"]:
                errors.append([version, "changed_input", kind])
        raw_lines = Path(doc["artifacts"]["work_md"]["path"]).read_text().splitlines()
        mat = read(root / "run1/documents" / version / "sections.json")
        raw_blocks = read(doc["artifacts"]["blocks"]["path"])["blocks"]
        owners = {r["line_id"]: r for r in mat["ownership"]}
        if len(owners) != len(mat["ownership"]) or len(owners) != mat["quality"]["source_lines"]:
            errors.append([version, "ownership_not_partition"])
        section_ids[version] = [s["instance_id"] for s in mat["sections"]]
        for s in mat["sections"]:
            counts["sections_schema_validated"] += 1
            for error in validator.iter_errors(s):
                errors.append([version, "schema", list(error.path), error.message])
            for b in s["ordered_text_blocks"]:
                for ref in b["source_refs"]:
                    counts["section_source_refs_checked"] += 1
                    line_id = ref["line_id"]
                    if owners[line_id]["route"] != "TEXT":
                        errors.append([version, "non_narrative_ref", line_id])
                    block = mat["ledger"]["blocks"][mat["ledger"]["columns"]["block_ref"][line_id]]
                    raw_types = {str(raw_blocks[j].get("block_type", "")).casefold() for j in block["raw_block_indices"]}
                    if raw_types & {"image", "graphic", "drawing", "sheet", "table"}:
                        errors.append([version, "raw_nonnarrative_block", line_id])
                    if ref["document_version"] != version or hashlib.sha256(raw_lines[ref["markdown_line"] - 1].encode()).hexdigest() != ref["line_sha256"]:
                        errors.append([version, "bad_source_ref", line_id])
        for extraction in read(root / "run1/documents" / version / "facts.json").values():
            for fact in extraction["facts"]:
                counts["facts_checked"] += 1
                if not fact["source_refs"]:
                    errors.append([version, "fact_without_source"])
                for ref in fact["source_refs"]:
                    counts["fact_source_refs_checked"] += 1
                    if ref["document_version"] != version or hashlib.sha256(raw_lines[ref["markdown_line"] - 1].encode()).hexdigest() != ref["line_sha256"]:
                        errors.append([version, "bad_fact_source"])
    for pair in inputs["pairs"]:
        relations = read(root / "run1/pairs" / pair["pair_key"] / "relations.json")
        for side in ("old", "new"):
            actual = [i for r in relations for i in r[side + "_sections"]]
            expected = section_ids[pair[side]["document_version"]]
            if Counter(actual) != Counter(expected) or len(actual) != len(set(actual)):
                errors.append([pair["pair_key"], "relation_partition", side])
        counts["document_pairs_checked"] += 1
    holdout = read(root / "holdout/BLIND_SECTION_RELATION_PACKET.json")["cases"]
    if len({c["case_id"] for c in holdout}) != len(holdout) or any(c["answer"] is not None for c in holdout):
        errors.append(["holdout", "duplicate_or_answered"])
    counts["blind_holdout_cases"] = len(holdout)
    initial = read(root / "INITIAL_STATE.json")
    for relative in subprocess.check_output(["git", "ls-tree", "-r", "--name-only", initial["research_head"],
                                            "experiments/table_materialization_v3"], cwd=repo, text=True).splitlines():
        original = subprocess.check_output(["git", "show", f"{initial['research_head']}:{relative}"], cwd=repo)
        if (repo / relative).read_bytes() != original:
            errors.append([relative, "table_v3_changed"])
        counts["table_v3_files_unchanged"] += 1
    for path, expected in initial["protected"].items():
        if file_hash(path) != expected:
            errors.append([path, "protected_changed"])
    for path, expected in read(root / "CANDIDATE_FREEZE.json")["files"].items():
        if file_hash(repo / path) != expected:
            errors.append([path, "candidate_changed"])
    manifest = read(root / "DELIVERABLE_MANIFEST.json")
    for name, expected in manifest["required_files"].items():
        if file_hash(root / "reports" / name) != expected:
            errors.append([name, "report_hash_changed"])
    out = {"pass": not errors, "counts": dict(counts), "errors": errors,
           "schema_validator": "jsonschema 4.25.1, Draft 2020-12", "source_validation": "Exact original Markdown line hashes; raw blocks types; frozen PDFs/Markdown/blocks hashes",
           "scope_limit": "Source structure checks cannot prove that upstream OCR never flattened an unlabelled table; no table/graphic-labeled block entered comparison."}
    write(root / "reports/FINAL_VALIDATION.json", out)
    print(out, flush=True)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--repo", required=True)
    a = p.parse_args()
    validate(a.root, a.repo)
