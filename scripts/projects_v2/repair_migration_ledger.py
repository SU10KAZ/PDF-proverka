#!/usr/bin/env python3
"""Repair stale post-cutover paths/checksums in old_to_new_map.json.

The migration ledger was created before the canonical stage-artifact rename
(``01_text`` -> ``02_text`` and ``02_blocks`` -> ``01_blocks``) and before one
document folder was normalized.  In addition, mutable v2 analysis/review files
legitimately diverged from the frozen legacy copy after v2 became primary.

Dry-run is the default.  Apply mode requires a backup directory and explicit
confirmation.  It never copies project data: it only points ledger rows at
existing files and records independent current SHA-256 values for v2 and
legacy.  Any ambiguous document or unresolved file aborts before mutation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

CONFIRM_TOKEN = "REPAIR_MIGRATION_LEDGER"
ARTIFACT_RENAMES = {
    "01_text_analysis.json": "02_text_analysis.json",
    "02_blocks_analysis.json": "01_blocks_analysis.json",
    "02_blocks_for_text.json": "01_blocks_for_text.json",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: dict) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".ledger_repair_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(value, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _find_document(v2_root: Path, entry: dict) -> Path:
    recorded = Path(str(entry.get("v2_document_dir") or ""))
    if (recorded / "document.json").is_file():
        return recorded
    code = str(entry.get("document_code") or "")
    discipline = str(entry.get("discipline") or "")
    object_id = str(entry.get("object_id") or "")
    matches: list[Path] = []
    for candidate in (v2_root / "objects").glob(f"*/disciplines/*/documents/{code}"):
        if not (candidate / "document.json").is_file():
            continue
        try:
            doc = json.loads((candidate / "document.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if object_id and str(doc.get("object_id") or "") != object_id:
            continue
        if discipline and candidate.parents[1].name != discipline:
            continue
        matches.append(candidate)
    if len(matches) != 1:
        raise RuntimeError(
            f"{code}: expected one canonical document for stale path, got {len(matches)}"
        )
    return matches[0]


def _resolve_renamed_file(path: Path) -> Path | None:
    if path.is_file():
        return path
    canonical = ARTIFACT_RENAMES.get(path.name)
    if canonical:
        candidate = path.with_name(canonical)
        if candidate.is_file():
            return candidate
    return None


def build_repair(data: dict, v2_root: Path) -> tuple[dict, dict]:
    updated = json.loads(json.dumps(data, ensure_ascii=False))
    stats = {
        "entries": 0,
        "document_paths_repaired": 0,
        "new_paths_repaired": 0,
        "old_paths_repaired": 0,
        "checksum_divergences_recorded": 0,
        "files_verified": 0,
    }
    unresolved: list[str] = []

    for entry in updated.get("migrations", []):
        stats["entries"] += 1
        old_doc = Path(str(entry.get("v2_document_dir") or ""))
        new_doc = _find_document(v2_root, entry)
        if new_doc != old_doc:
            entry["v2_document_dir"] = str(new_doc)
            stats["document_paths_repaired"] += 1

        for item in entry.get("files", []):
            old_new_path = Path(str(item.get("new_path") or ""))
            candidates: list[Path] = []
            try:
                candidates.append(new_doc / old_new_path.relative_to(old_doc))
            except ValueError:
                candidates.append(old_new_path)
            new_path = next(
                (resolved for candidate in candidates
                 if (resolved := _resolve_renamed_file(candidate)) is not None),
                None,
            )
            if new_path is None:
                unresolved.append(f"missing v2 file: {old_new_path}")
                continue
            if new_path != old_new_path:
                item["new_path"] = str(new_path)
                stats["new_paths_repaired"] += 1

            old_path = Path(str(item.get("old_path") or ""))
            resolved_old = _resolve_renamed_file(old_path)
            if resolved_old is not None and resolved_old != old_path:
                item["old_path"] = str(resolved_old)
                old_path = resolved_old
                stats["old_paths_repaired"] += 1

            v2_sha = _sha256(new_path)
            item["sha256"] = v2_sha
            if old_path.is_file():
                legacy_sha = _sha256(old_path)
                item["legacy_sha256"] = legacy_sha
                relation = "identical" if legacy_sha == v2_sha else "diverged_after_cutover"
                item["checksum_relation"] = relation
                if relation != "identical":
                    stats["checksum_divergences_recorded"] += 1
            else:
                item.pop("legacy_sha256", None)
                item["checksum_relation"] = "legacy_absent"
            stats["files_verified"] += 1

    if unresolved:
        raise RuntimeError("\n".join(unresolved))
    updated["generated_at"] = datetime.now(timezone.utc).isoformat()
    updated["maintenance"] = {
        "operation": "repair_migration_ledger_post_cutover",
        **stats,
    }
    return updated, stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--v2-root", default=str(_repo_root() / "projects_v2"))
    ap.add_argument("--backup-dir")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--confirm")
    args = ap.parse_args(argv)

    root = Path(args.v2_root).resolve()
    ledger = root / "_system" / "old_to_new_map.json"
    source = json.loads(ledger.read_text(encoding="utf-8"))
    repaired, stats = build_repair(source, root)
    print("=== projects_v2 migration ledger repair ===")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    if not args.apply:
        print("DRY-RUN: no files changed")
        return 0
    if args.confirm != CONFIRM_TOKEN:
        print(f"[ERROR] --apply requires --confirm {CONFIRM_TOKEN}")
        return 2
    if not args.backup_dir:
        print("[ERROR] --apply requires --backup-dir")
        return 2
    backup = Path(args.backup_dir).resolve()
    backup.mkdir(parents=True, exist_ok=False)
    shutil.copy2(ledger, backup / "old_to_new_map.before.json")
    _atomic_json(backup / "old_to_new_map.after.json", repaired)
    _atomic_json(backup / "manifest.json", {"v2_root": str(root), **stats})
    _atomic_json(ledger, repaired)
    print(f"[OK] repaired ledger; backup: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
