#!/usr/bin/env python3
"""Remove synthetic pytest writes from a projects_v2 store, with backup.

Dry-run is the default.  ``--apply`` requires an explicit backup directory and
confirmation token.  Only two narrowly defined classes are touched:

* ledger rows whose legacy path belongs to ``/tmp/pytest-*`` or whose v2
  object folder starts with ``test_``/``pytest_``;
* top-level v2 object folders starting with ``test_``/``pytest_`` or whose
  own ``object.json.legacy_path`` points into ``/tmp/pytest-*``.

Before mutation the full ledger and all candidate directories are archived.
Real object folders and real legacy rows are never selected by this script.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

CONFIRM_TOKEN = "REMOVE_PYTEST_POLLUTION"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _object_folder(v2_root: Path, value: str) -> str | None:
    try:
        rel = Path(value).resolve().relative_to((v2_root / "objects").resolve())
    except (OSError, ValueError):
        return None
    return rel.parts[0] if rel.parts else None


def is_polluted_entry(entry: dict, v2_root: Path) -> bool:
    legacy = str(entry.get("legacy_folder_path") or "").replace("\\", "/")
    if "/tmp/pytest-" in legacy or "/tmp/pytest_of_" in legacy:
        return True
    folder = _object_folder(v2_root, str(entry.get("v2_document_dir") or ""))
    if not folder:
        return False
    return folder.startswith(("test_", "pytest_")) or is_synthetic_object_dir(
        v2_root / "objects" / folder
    )


def is_synthetic_object_dir(path: Path) -> bool:
    if path.name.startswith(("test_", "pytest_")):
        return True
    try:
        meta = json.loads((path / "object.json").read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    legacy = str(meta.get("legacy_path") or "").replace("\\", "/")
    return "/tmp/pytest-" in legacy or "/tmp/pytest_of_" in legacy


def collect(v2_root: Path) -> tuple[dict, list[dict], list[Path]]:
    map_path = v2_root / "_system" / "old_to_new_map.json"
    data = json.loads(map_path.read_text(encoding="utf-8"))
    polluted = [e for e in data.get("migrations", []) if is_polluted_entry(e, v2_root)]
    objects_root = v2_root / "objects"
    dirs = sorted(
        p for p in objects_root.iterdir()
        if p.is_dir() and not p.is_symlink() and is_synthetic_object_dir(p)
    )
    return data, polluted, dirs


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".cleanup_", suffix=".json")
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


def apply_cleanup(v2_root: Path, backup_dir: Path) -> dict:
    data, polluted, dirs = collect(v2_root)
    backup_dir.mkdir(parents=True, exist_ok=False)
    map_path = v2_root / "_system" / "old_to_new_map.json"
    shutil.copy2(map_path, backup_dir / "old_to_new_map.before.json")

    archive = backup_dir / "pytest_objects.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        for directory in dirs:
            tf.add(directory, arcname=f"objects/{directory.name}", recursive=True)

    kept = [e for e in data.get("migrations", []) if not is_polluted_entry(e, v2_root)]
    updated = dict(data)
    updated["migrations"] = kept
    updated["generated_at"] = datetime.now(timezone.utc).isoformat()
    updated["maintenance"] = {
        "operation": "cleanup_pytest_pollution",
        "removed_entries": len(polluted),
        "removed_object_dirs": len(dirs),
        "backup_dir": str(backup_dir),
    }
    _atomic_json(map_path, updated)
    for directory in dirs:
        shutil.rmtree(directory)

    manifest = {
        "v2_root": str(v2_root),
        "removed_entries": len(polluted),
        "removed_object_dirs": [p.name for p in dirs],
        "remaining_entries": len(kept),
        "ledger_backup": str(backup_dir / "old_to_new_map.before.json"),
        "objects_backup": str(archive),
    }
    _atomic_json(backup_dir / "manifest.json", manifest)

    _, remaining_polluted, remaining_dirs = collect(v2_root)
    if remaining_polluted or remaining_dirs:
        raise RuntimeError("post-cleanup verification failed")
    return manifest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--v2-root", default=str(_repo_root() / "projects_v2"))
    ap.add_argument("--backup-dir")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--confirm")
    args = ap.parse_args(argv)

    root = Path(args.v2_root).resolve()
    data, polluted, dirs = collect(root)
    print("=== projects_v2 pytest pollution cleanup ===")
    print(f"v2 root              : {root}")
    print(f"ledger entries       : {len(data.get('migrations', []))}")
    print(f"polluted entries     : {len(polluted)}")
    print(f"synthetic object dirs: {len(dirs)}")
    if not args.apply:
        print("DRY-RUN: no files changed")
        return 0
    if args.confirm != CONFIRM_TOKEN:
        print(f"[ERROR] --apply requires --confirm {CONFIRM_TOKEN}")
        return 2
    if not args.backup_dir:
        print("[ERROR] --apply requires --backup-dir")
        return 2
    result = apply_cleanup(root, Path(args.backup_dir).resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
