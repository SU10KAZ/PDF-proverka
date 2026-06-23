#!/usr/bin/env python3
"""READ-ONLY scan for v2 expert_review split-brain.

Find versions where ``versions/<vid>/_output/expert_review.json`` contains
reviewed decisions missing from canonical ``04_review/expert_review.json``.
The script never writes to projects_v2; optional ``--json-out`` may point to
/tmp or another operator-provided report path.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


def _default_v2_root() -> Path:
    env = os.environ.get("AUDIT_PROJECTS_V2_DIR")
    if env:
        return Path(env).resolve()
    return Path.cwd() / "projects_v2"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _reviewed_decisions(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    data = _load_json(path)
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for item in data.get("decisions") or []:
        if not isinstance(item, dict):
            continue
        if item.get("decision") not in ("accepted", "rejected"):
            continue
        item_id = str(item.get("item_id") or "")
        if not item_id:
            continue
        item_type = str(item.get("item_type") or "")
        out[(item_type, item_id)] = item
    return out


def _document_code(doc_dir: Path) -> str:
    data = _load_json(doc_dir / "document.json")
    return str(data.get("document_code") or doc_dir.name)


def scan(v2_root: Path) -> dict[str, Any]:
    objects = v2_root / "objects"
    rows: list[dict[str, Any]] = []
    versions_scanned = 0
    if not objects.is_dir():
        return {"v2_root": str(v2_root), "versions_scanned": 0, "split_versions": []}

    for version_dir in sorted(objects.glob("*/disciplines/*/documents/*/versions/*")):
        if not version_dir.is_dir():
            continue
        versions_scanned += 1
        output_review = version_dir / "_output" / "expert_review.json"
        if not output_review.is_file():
            continue
        canonical_review = version_dir / "04_review" / "expert_review.json"
        output_decisions = _reviewed_decisions(output_review)
        if not output_decisions:
            continue
        canonical_decisions = _reviewed_decisions(canonical_review)
        missing_keys = sorted(set(output_decisions) - set(canonical_decisions))
        if not missing_keys:
            continue
        doc_dir = version_dir.parent.parent
        rows.append({
            "document_code": _document_code(doc_dir),
            "version_id": version_dir.name,
            "version_dir": str(version_dir),
            "canonical_review": str(canonical_review),
            "output_review": str(output_review),
            "missing_count": len(missing_keys),
            "missing_decisions": [
                {
                    "item_type": item_type,
                    "item_id": item_id,
                    "decision": output_decisions[(item_type, item_id)].get("decision"),
                }
                for item_type, item_id in missing_keys
            ],
        })
    return {
        "v2_root": str(v2_root),
        "versions_scanned": versions_scanned,
        "split_count": len(rows),
        "split_versions": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v2-root", type=Path, default=_default_v2_root())
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()

    report = scan(args.v2_root.resolve())
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print("=== v2 expert_review split scan ===")
    print(f"v2_root: {report['v2_root']}")
    print(f"versions_scanned: {report['versions_scanned']}")
    print(f"split_count: {report['split_count']}")
    for row in report["split_versions"]:
        print(
            f"  [split] {row['document_code']}/{row['version_id']} "
            f"missing={row['missing_count']} version_dir={row['version_dir']}"
        )
    if args.json_out:
        print(f"-> {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
