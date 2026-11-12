"""Offline exact-quote check; does not adjudicate findings or inspect graphics."""
import argparse
import collections
import hashlib
import json
import re
from pathlib import Path


def fingerprint(path):
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def check(md_path, findings_path):
    source = md_path.read_text()
    headers = list(re.finditer(r"^### BLOCK #\d+ \[([^]]+)\]: (blk_\w+)\s*$", source, re.M))
    blocks = {}
    for index, header in enumerate(headers):
        block_id = header[2]
        if block_id in blocks:
            raise ValueError(f"Ambiguous duplicate block ID: {block_id}")
        end = headers[index + 1].start() if index + 1 < len(headers) else len(source)
        blocks[block_id] = (header[1], source[header.end():end])
    if not blocks:
        raise ValueError("No source blocks parsed")
    findings = json.loads(findings_path.read_text())["findings"]
    rows = []
    for index, finding in enumerate(findings):
        for evidence_index, evidence in enumerate(finding.get("evidence", [])):
            block_id = evidence.get("block_id")
            block = blocks.get(block_id)
            quote = evidence.get("quote")
            if not block:
                status = "missing_block"
            elif block[0] not in ("TEXT", "TABLE"):
                status = "graphic_not_text_check"
            elif not quote:
                status = "no_quote"
            elif not isinstance(quote, str):
                status = "invalid_quote_type"
            elif quote in block[1]:
                status = "exact"
            elif " ".join(quote.split()) in " ".join(block[1].split()):
                status = "whitespace_only"
            else:
                status = "unmatched"
            rows.append({"finding_id": finding["id"], "pointer": f"/findings/{index}/evidence/{evidence_index}",
                         "block_id": block_id, "block_kind": block[0] if block else None,
                         "status": status, "quote_sha256": hashlib.sha256(quote.encode()).hexdigest()
                         if isinstance(quote, str) else None})
    return {"version": 1, "scope": "Exact quote provenance only; sealed output",
            "sources": {"authoritative_md": fingerprint(md_path), "final": fingerprint(findings_path)},
            "method": "Case-sensitive literal substring in addressed MD block; whitespace-only matches reported separately. No punctuation, units, numeric or OCR correction.",
            "parsed_blocks": len(blocks), "finding_count": len(findings), "evidence_count": len(rows),
            "counts": dict(collections.Counter(row["status"] for row in rows)), "rows": rows,
            "limits": ["Matching a quote does not prove object identity, applicability, arithmetic, interpretation, severity or completeness.",
                       "Graphics are excluded from textual corroboration; their summaries are not treated as evidence.",
                       "A source reference without a quote is not an error by itself and requires separate claim review.",
                       "This does not validate locations written in prose or visible finding-to-finding references."],
            "whole_output_precision_finalized": False, "quality_score": None,
            "runtime_changed": False, "model_calls": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", type=Path, required=True)
    parser.add_argument("--findings", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve() in {args.md.resolve(), args.findings.resolve()}:
        parser.error("Output must not overwrite source evidence")
    result = check(args.md, args.findings)
    result["checker"] = fingerprint(Path(__file__))
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"finding_count": result["finding_count"], "evidence_count": result["evidence_count"],
                      "counts": result["counts"]}, ensure_ascii=False))
