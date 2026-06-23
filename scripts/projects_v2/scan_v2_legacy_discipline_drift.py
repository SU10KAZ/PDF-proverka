#!/usr/bin/env python3
"""READ-ONLY scan for v2 discipline vs legacy-path discipline drift.

The scanner reports documents where the v2 folder discipline or
document.json discipline disagrees with the discipline inferred from
document.json legacy_project_path. It never moves, deletes, or edits live
projects; optional --json-out writes only the report file requested by the
operator.
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


def _default_legacy_root() -> Path:
    env = os.environ.get("AUDIT_PROJECTS_DIR") or os.environ.get("PROJECTS_DIR")
    if env:
        return Path(env).resolve()
    return Path.cwd() / "projects"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _legacy_location(legacy_path: str, legacy_root: Path) -> dict[str, str | None]:
    if not legacy_path:
        return {"legacy_object": None, "legacy_discipline": None, "legacy_unit": None}
    path = Path(legacy_path)
    try:
        rel = path.resolve().relative_to(legacy_root.resolve())
        parts = rel.parts
        if len(parts) >= 3:
            return {
                "legacy_object": parts[0],
                "legacy_discipline": parts[1],
                "legacy_unit": parts[2],
            }
    except Exception:
        pass
    parts = path.parts
    project_indexes = [i for i, part in enumerate(parts) if part == "projects"]
    for idx in reversed(project_indexes):
        if len(parts) > idx + 3:
            return {
                "legacy_object": parts[idx + 1],
                "legacy_discipline": parts[idx + 2],
                "legacy_unit": parts[idx + 3],
            }
    return {"legacy_object": None, "legacy_discipline": None, "legacy_unit": None}


def scan(v2_root: Path, legacy_root: Path) -> dict[str, Any]:
    objects = v2_root / "objects"
    rows: list[dict[str, Any]] = []
    documents_scanned = 0
    if not objects.is_dir():
        return {
            "v2_root": str(v2_root),
            "legacy_root": str(legacy_root),
            "documents_scanned": 0,
            "drift_count": 0,
            "drift_documents": [],
        }

    for doc_dir in sorted(objects.glob("*/disciplines/*/documents/*")):
        if not doc_dir.is_dir():
            continue
        dj = _load_json(doc_dir / "document.json")
        if not dj:
            continue
        documents_scanned += 1
        v2_object = doc_dir.parents[3].name
        v2_discipline = doc_dir.parent.parent.name
        document_discipline = str(dj.get("discipline") or "") or None
        document_code = str(dj.get("document_code") or doc_dir.name)
        legacy_path = str(dj.get("legacy_project_path") or "")
        loc = _legacy_location(legacy_path, legacy_root)
        legacy_discipline = loc["legacy_discipline"]
        reasons: list[str] = []
        if legacy_path and legacy_discipline is None:
            reasons.append("legacy_discipline_unresolved")
        if legacy_discipline and v2_discipline != legacy_discipline:
            reasons.append("v2_folder_vs_legacy")
        if legacy_discipline and document_discipline and document_discipline != legacy_discipline:
            reasons.append("document_json_vs_legacy")
        if document_discipline and document_discipline != v2_discipline:
            reasons.append("document_json_vs_v2_folder")
        if not reasons:
            continue
        rows.append({
            "document_code": document_code,
            "object_folder": v2_object,
            "v2_discipline": v2_discipline,
            "document_json_discipline": document_discipline,
            "legacy_discipline": legacy_discipline,
            "legacy_object": loc["legacy_object"],
            "legacy_unit": loc["legacy_unit"],
            "legacy_project_path": legacy_path,
            "v2_document_dir": str(doc_dir),
            "reasons": reasons,
        })
    return {
        "v2_root": str(v2_root),
        "legacy_root": str(legacy_root),
        "documents_scanned": documents_scanned,
        "drift_count": len(rows),
        "drift_documents": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v2-root", type=Path, default=_default_v2_root())
    parser.add_argument("--legacy-root", type=Path, default=_default_legacy_root())
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()

    report = scan(args.v2_root.resolve(), args.legacy_root.resolve())
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print("=== v2 legacy discipline drift scan ===")
    print(f"v2_root: {report['v2_root']}")
    print(f"legacy_root: {report['legacy_root']}")
    print(f"documents_scanned: {report['documents_scanned']}")
    print(f"drift_count: {report['drift_count']}")
    for row in report["drift_documents"]:
        print(
            f"  [drift] {row['document_code']}: "
            f"v2={row['v2_discipline']} document_json={row['document_json_discipline']} "
            f"legacy={row['legacy_discipline']} reasons={','.join(row['reasons'])}"
        )
    if args.json_out:
        print(f"report: {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
