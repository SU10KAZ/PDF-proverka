"""Offline corpus runner and independent persisted-artifact verification; no truth imports."""
from collections import Counter
from pathlib import Path
import argparse
import json
import re
import resource
import time

from . import frozen_v1 as v1
from .materialize import materialize_document

ARTIFACTS = ("ledger", "semantics", "decisions")
V1_ARTIFACTS = ("comparison_units", "text_sections", "table_identities")


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(v1.canonical_bytes(value))


def receipt(path):
    path = Path(path)
    return {"path": str(path), "sha256": v1.file_sha(path), "bytes": path.stat().st_size}


def run(documents, output, producer):
    output = Path(output)
    if output.exists():
        raise ValueError("Runs are immutable; select a new output directory")
    index, started = [], time.perf_counter()
    names = ARTIFACTS if producer == "foundation" else V1_ARTIFACTS
    materialize = materialize_document if producer == "foundation" else v1.materialize_document
    for document in documents:
        tick = time.perf_counter()
        for name in ("work_md", "blocks"):
            source = document["artifacts"][name]
            if v1.file_sha(Path(source["path"])) != source["sha256"]:
                raise ValueError("Pinned source changed")
        result = materialize(document)
        row = {"document_version": document["document_version"], "artifacts": {}, "quality": result["quality"]}
        for name in names:
            p = output / document["document_version"] / (name + ".json")
            write(p, result[name])
            row["artifacts"][name] = receipt(p)
        row["seconds"] = time.perf_counter() - tick
        index.append(row)
        del result
    performance = {"seconds": time.perf_counter() - started, "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                   "artifact_bytes": sum(a["bytes"] for r in index for a in r["artifacts"].values()),
                   "documents": len(documents), "producer": producer}
    write(output / "index.json", index)
    write(output / "performance.json", performance)
    write(output / "producer.json", {"files": [receipt(p) for p in sorted(Path(__file__).parent.glob("*.py"))],
                                    "documents_sha256": v1.digest(documents)})
    print(json.dumps(performance))


def source_scan(path):
    """Independent scanner, not the materializer's parser or ledger inventory."""
    atoms, blocks = {}, set()
    page, block, started = None, None, False
    for number, text in enumerate(Path(path).read_text(errors="replace").splitlines(), 1):
        if re.fullmatch(r"## Page\s+\d+\s*", text):
            page, block = int(text.split()[2]), None
        elif text.startswith("### BLOCK #"):
            match = re.fullmatch(r"### BLOCK #\S+ \[([^]]+)\]:\s*(\S+)\s*", text)
            if match and page is not None:
                block, started = match.group(2), False
                blocks.add((page, block, number))
        elif block is not None:
            if text.startswith(("> **Stamp:**", "> **Created:**", "> **Crop:**")):
                continue
            if not started and (text.startswith(">") or not text.strip()):
                continue
            started = True
            if text.strip():
                atoms[(page, block, number)] = text
    return atoms, blocks


def audit(documents, foundation, baseline):
    rows, totals = [], Counter()
    for document in documents:
        version = document["document_version"]
        r = {name: read(Path(foundation) / version / (name + ".json")) for name in ARTIFACTS}
        ledger, semantics, columns = r["ledger"], r["semantics"], r["ledger"]["columns"]
        atoms, blocks = source_scan(document["artifacts"]["work_md"]["path"])
        represented = [(ledger["blocks"][ref]["page"], ledger["blocks"][ref]["block_id"], columns["markdown_line"][i])
                       for i, ref in enumerate(columns["block_ref"])]
        block_refs = {(b["page"], b["block_id"], b["header_line"]) for b in ledger["blocks"] if "header_line" in b}
        raw = read(document["artifacts"]["blocks"]["path"])
        raw_refs = {i for b in ledger["blocks"] for i in b["raw_block_indices"]}
        owners = columns["owner"]
        units = semantics["units"]
        invalid = sum(not (0 <= owner < len(units)) if isinstance(owner, int)
                      else owner not in {"FURNITURE", "EXCLUDED", "REVIEW"} for owner in owners)
        mismatches = sum(columns["normalized_text"][i] != v1.normalize(v1.strip_markup(atoms.get(loc, "")))
                         for i, loc in enumerate(represented))
        base = read(Path(baseline) / version / "comparison_units.json")
        expected_sheets = {"units": [u for u in base["units"] if u["unit_type"] == "SHEET"],
                           "page_routing": [u for u in base["page_routing"] if u["unit_type"] == "SHEET"]}
        row = {"document_version": version, "pages": len(semantics["pages"]), "nonempty_lines": len(represented),
               "duplicate_semantic_lines": len(represented) - len(set(represented)),
               "lost_source_lines": len(set(atoms) - set(represented)), "extra_source_lines": len(set(represented) - set(atoms)),
               "lost_markdown_blocks": len(blocks - block_refs), "lost_raw_blocks": len(set(range(len(raw.get("blocks", [])))) - raw_refs),
               "invalid_owners": invalid, "unowned_unexplained_lines": sum(o == "UNOWNED" or (isinstance(o, str) and not columns["evidence_codes"][i]) for i, o in enumerate(owners)),
               "normalized_source_mismatches": mismatches, "sheet_contract_mismatches": int(semantics["sheets_v1"] != expected_sheets)}
        # All V1 page properties, not just SHEET classifier outcomes.
        parsed, _ = v1.parse_markdown(Path(document["artifacts"]["work_md"]["path"]))
        raw_pages = {int(p["page_index"]) + 1: p for p in raw.get("pages", [])}
        for number in raw_pages:
            parsed.setdefault(number, v1.ParsedPage(number))
        row["page_contract_mismatches"] = sum(
            semantics["pages"][str(n)]["evidence"] != v1.page_evidence(p, raw_pages.get(n, {}), raw.get("blocks", []), parsed)
            for n, p in parsed.items())
        rows.append(row)
        totals.update({k: v for k, v in row.items() if isinstance(v, int)})
    failure_keys = set(totals) - {"pages", "nonempty_lines"}
    return {"pass": not any(totals[k] for k in failure_keys), "documents": len(documents),
            "totals": dict(totals), "documents_detail": rows,
            "method": "Independent raw Markdown scanner and raw block inventory; persisted ownership; complete V1 page evidence and SHEET artifact equality."}


def replay(left, right):
    a, b = read(Path(left) / "index.json"), read(Path(right) / "index.json")
    differences, checked = [], 0
    if [r["document_version"] for r in a] != [r["document_version"] for r in b]:
        differences.append("document_order_or_membership")
    for x, y in zip(a, b):
        for name in ARTIFACTS:
            checked += 1
            if v1.file_sha(Path(x["artifacts"][name]["path"])) != v1.file_sha(Path(y["artifacts"][name]["path"])):
                differences.append([x["document_version"], name])
    return {"pass": not differences, "semantic_files_compared": checked, "differences": differences,
            "excluded_nonsemantic_files": ["index.json (timings/paths)", "performance.json", "producer.json (paths)"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--documents", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--producer", choices=["foundation", "v1"], default="foundation")
    args = parser.parse_args()
    run(read(args.documents), args.output, args.producer)


if __name__ == "__main__":
    main()
