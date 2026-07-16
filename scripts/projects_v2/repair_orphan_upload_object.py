#!/usr/bin/env python3
"""Move v2-primary uploads from a hash-id orphan to the registered object.

The command is dry-run by default.  ``--apply`` creates a space-efficient
hard-link backup of the orphan object plus a normal copy of the migration
ledger, moves every document to the target object, updates document IDs and
ledger paths, and finally removes the now-empty orphan object directory.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_json(path: Path, data: dict) -> None:
    mode = path.stat().st_mode if path.exists() else 0o600
    fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(raw_tmp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _replace_paths(value, old: str, new: str):
    if isinstance(value, str):
        return value.replace(old, new)
    if isinstance(value, list):
        return [_replace_paths(item, old, new) for item in value]
    if isinstance(value, dict):
        return {key: _replace_paths(item, old, new) for key, item in value.items()}
    return value


def _documents(object_dir: Path) -> list[Path]:
    return sorted(object_dir.glob("disciplines/*/documents/*/document.json"))


def _plan(v2_root: Path, source_folder: str, target_folder: str,
          source_id: str, target_id: str) -> dict:
    source = v2_root / "objects" / source_folder
    target = v2_root / "objects" / target_folder
    ledger_path = v2_root / "_system" / "old_to_new_map.json"
    if not source.is_dir():
        raise RuntimeError(f"source object not found: {source}")
    if not target.is_dir():
        raise RuntimeError(f"target object not found: {target}")
    source_meta = _read_json(source / "object.json")
    target_meta = _read_json(target / "object.json")
    if str(source_meta.get("object_id")) != source_id:
        raise RuntimeError("source object_id does not match object.json")
    if str(target_meta.get("object_id")) != target_id:
        raise RuntimeError("target object_id does not match object.json")

    docs = []
    for document_json in _documents(source):
        meta = _read_json(document_json)
        if str(meta.get("object_id")) != source_id:
            raise RuntimeError(f"unexpected object_id in {document_json}")
        document_dir = document_json.parent
        discipline = document_dir.parents[1].name
        destination = target / "disciplines" / discipline / "documents" / document_dir.name
        if destination.exists():
            raise RuntimeError(f"target document already exists: {destination}")
        docs.append({
            "discipline": discipline,
            "document_code": document_dir.name,
            "source": document_dir,
            "destination": destination,
        })

    ledger = _read_json(ledger_path)
    rows = [row for row in ledger.get("migrations", [])
            if str(row.get("object_id")) == source_id]
    disk_keys = {(item["discipline"], item["document_code"]) for item in docs}
    ledger_keys = {(str(row.get("discipline")), str(row.get("document_code")))
                   for row in rows}
    if disk_keys != ledger_keys:
        raise RuntimeError(
            f"disk/ledger mismatch: disk_only={sorted(disk_keys - ledger_keys)!r}, "
            f"ledger_only={sorted(ledger_keys - disk_keys)!r}"
        )
    return {
        "source": source,
        "target": target,
        "target_display_name": str(target_meta.get("display_name") or
                                   target_meta.get("legacy_name") or target_folder),
        "ledger_path": ledger_path,
        "ledger": ledger,
        "ledger_rows": rows,
        "documents": docs,
    }


def _apply(plan: dict, v2_root: Path, source_id: str, target_id: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = (v2_root / "_system" / "destructive_backups" /
              f"{stamp}_repair_orphan_upload_{source_id}_to_{target_id}")
    backup.mkdir(parents=True, exist_ok=False)
    # The uploaded bundle is several GiB and 01_input is immutable. Hard links
    # preserve a rollback copy without duplicating file contents on a full disk.
    shutil.copytree(
        plan["source"], backup / "source_object.before",
        copy_function=os.link,
    )
    shutil.copy2(plan["ledger_path"], backup / "old_to_new_map.before.json")

    moved = []
    for item in plan["documents"]:
        destination = item["destination"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        item["source"].rename(destination)
        document_json = destination / "document.json"
        metadata = _read_json(document_json)
        metadata["object_id"] = target_id
        _atomic_json(document_json, metadata)
        moved.append(f"{item['discipline']}/{item['document_code']}")

    old_root = str(plan["source"].resolve())
    new_root = str(plan["target"].resolve())
    changed_rows = 0
    for row in plan["ledger"].get("migrations", []):
        if str(row.get("object_id")) != source_id:
            continue
        rewritten = _replace_paths(row, old_root, new_root)
        row.clear()
        row.update(rewritten)
        row["object_id"] = target_id
        row["object_name"] = plan["target_display_name"]
        changed_rows += 1
    plan["ledger"]["generated_at"] = datetime.now(timezone.utc).replace(
        microsecond=0).isoformat().replace("+00:00", "Z")
    _atomic_json(plan["ledger_path"], plan["ledger"])

    shutil.rmtree(plan["source"])
    manifest = {
        "schema_version": 1,
        "source_object_id": source_id,
        "target_object_id": target_id,
        "source_folder": plan["source"].name,
        "target_folder": plan["target"].name,
        "moved_documents": moved,
        "updated_ledger_rows": changed_rows,
        "backup_uses_hard_links": True,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    _atomic_json(backup / "repair_manifest.json", manifest)
    return backup


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v2-root", default="projects_v2")
    parser.add_argument("--source-folder", required=True)
    parser.add_argument("--target-folder", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    v2_root = Path(args.v2_root).resolve()
    plan = _plan(
        v2_root, args.source_folder, args.target_folder,
        args.source_id, args.target_id,
    )
    print(f"documents: {len(plan['documents'])}")
    print(f"ledger rows: {len(plan['ledger_rows'])}")
    print(f"source: {plan['source']}")
    print(f"target: {plan['target']}")
    if not args.apply:
        print("DRY-RUN: no changes made")
        return 0
    backup = _apply(plan, v2_root, args.source_id, args.target_id)
    print(f"APPLIED: moved {len(plan['documents'])} documents")
    print(f"backup: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
