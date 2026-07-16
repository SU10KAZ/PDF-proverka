#!/usr/bin/env python3
"""Guarded retirement workflow for the legacy ``projects/`` tree.

The default and all inspection commands are non-destructive.  The only
destructive command, ``execute``, requires an exact confirmation phrase, a
fully verified external backup, an unchanged source manifest, green migration
gates, and a stopped backend.  It first renames ``projects/`` to a quarantine
name, runs v2-only smoke tests, restores the old name on failure, and removes
the quarantine directory only after the smoke tests pass.

Typical flow::

    python scripts/projects_v2/retire_legacy_projects.py manifest \
      --output /media/backup/projects-manifest.json
    # Copy projects/ to an external directory, or create .tar/.tar.gz/.tgz.
    python scripts/projects_v2/retire_legacy_projects.py verify-backup \
      --manifest /media/backup/projects-manifest.json \
      --backup /media/backup/projects-20260713.tar.gz \
      --receipt /media/backup/projects-backup-receipt.json
    python scripts/projects_v2/retire_legacy_projects.py preflight \
      --manifest /media/backup/projects-manifest.json \
      --receipt /media/backup/projects-backup-receipt.json

``execute`` is intentionally not shown here; see ``--help`` after reviewing
the generated preflight report and stopping all backend/audit workers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterable

CONFIRM = "DELETE_LEGACY_PROJECTS_AFTER_VERIFIED_BACKUP"
PARITY_ACK = "ACKNOWLEDGE_LEGACY_ONLY_DATA_REMAINS_IN_VERIFIED_BACKUP"
SCHEMA = 1
CHUNK = 1024 * 1024


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha_stream(fh: BinaryIO) -> str:
    h = hashlib.sha256()
    while chunk := fh.read(CHUNK):
        h.update(chunk)
    return h.hexdigest()


def _sha_file(path: Path) -> str:
    with path.open("rb") as fh:
        return _sha_stream(fh)


def _json_sha(obj: dict) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n",
                   encoding="utf-8")
    os.replace(tmp, path)


def build_manifest(source: Path) -> dict:
    source = source.resolve()
    if not source.is_dir():
        raise RuntimeError(f"legacy source is not a directory: {source}")
    files: list[dict] = []
    symlinks: list[dict] = []
    empty_dirs: list[str] = []
    total = 0
    for root, dirs, names in os.walk(source, followlinks=False):
        root_path = Path(root)
        traversable_dirs: list[str] = []
        for name in sorted(dirs):
            p = root_path / name
            if p.is_symlink():
                symlinks.append({"path": p.relative_to(source).as_posix(),
                                 "target": os.readlink(p)})
            else:
                traversable_dirs.append(name)
        dirs[:] = traversable_dirs
        if not traversable_dirs and not names:
            empty_dirs.append(root_path.relative_to(source).as_posix())
        for name in sorted(names):
            p = root_path / name
            if p.is_symlink():
                symlinks.append({"path": p.relative_to(source).as_posix(),
                                 "target": os.readlink(p)})
                continue
            if not p.is_file():
                raise RuntimeError(f"special file is not allowed in legacy source: {p}")
            size = p.stat().st_size
            files.append({
                "path": p.relative_to(source).as_posix(),
                "bytes": size,
                "sha256": _sha_file(p),
            })
            total += size
    files.sort(key=lambda x: x["path"])
    symlinks.sort(key=lambda x: x["path"])
    manifest = {
        "schema_version": SCHEMA,
        "kind": "legacy_projects_source_manifest",
        "generated_at": _utc_now(),
        "source_root": str(source),
        "source_name": source.name,
        "file_count": len(files),
        "total_bytes": total,
        "empty_dirs": sorted(empty_dirs),
        "symlinks": symlinks,
        "files": files,
    }
    manifest["content_id"] = _json_sha({
        "source_name": source.name,
        "file_count": len(files),
        "total_bytes": total,
        "empty_dirs": manifest["empty_dirs"],
        "symlinks": symlinks,
        "files": files,
    })
    return manifest


def load_manifest(path: Path) -> dict:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if obj.get("kind") != "legacy_projects_source_manifest" or not obj.get("content_id"):
        raise RuntimeError(f"invalid source manifest: {path}")
    expected = _json_sha({
        "source_name": obj["source_name"],
        "file_count": obj["file_count"],
        "total_bytes": obj["total_bytes"],
        "empty_dirs": obj.get("empty_dirs", []),
        "symlinks": obj.get("symlinks", []),
        "files": obj["files"],
    })
    if expected != obj["content_id"]:
        raise RuntimeError("manifest content_id mismatch")
    return obj


def _compare_entry(entry: dict, size: int, digest: str) -> None:
    if size != entry["bytes"]:
        raise RuntimeError(
            f"size mismatch for {entry['path']}: expected {entry['bytes']}, got {size}")
    if digest != entry["sha256"]:
        raise RuntimeError(f"sha256 mismatch for {entry['path']}")


def verify_directory(manifest: dict, backup: Path) -> dict:
    root = backup.resolve()
    nested = root / manifest["source_name"]
    if nested.is_dir():
        root = nested
    if not root.is_dir():
        raise RuntimeError(f"backup directory not found: {root}")
    for entry in manifest["files"]:
        p = root / PurePosixPath(entry["path"])
        if not p.is_file() or p.is_symlink():
            raise RuntimeError(f"backup file missing or invalid: {entry['path']}")
        _compare_entry(entry, p.stat().st_size, _sha_file(p))
    for entry in manifest.get("symlinks", []):
        p = root / PurePosixPath(entry["path"])
        if not p.is_symlink():
            raise RuntimeError(f"backup symlink missing: {entry['path']}")
        if os.readlink(p) != entry["target"]:
            raise RuntimeError(f"symlink target mismatch for {entry['path']}")
    return {"backup_kind": "directory", "backup_content_sha256": None,
            "resolved_content_root": str(root)}


def _safe_tar_names(tf: tarfile.TarFile) -> dict[str, tarfile.TarInfo]:
    out: dict[str, tarfile.TarInfo] = {}
    for member in tf.getmembers():
        raw = member.name.replace("\\", "/")
        norm = PurePosixPath(raw)
        if norm.is_absolute() or ".." in norm.parts:
            raise RuntimeError(f"unsafe archive member: {member.name}")
        key = "/".join(part for part in norm.parts if part not in ("", "."))
        if member.isfile() or member.issym():
            out[key] = member
        elif member.islnk():
            raise RuntimeError(f"hard links are not allowed in backup archive: {member.name}")
    return out


def verify_tar(manifest: dict, backup: Path) -> dict:
    with tarfile.open(backup, mode="r:*") as tf:
        members = _safe_tar_names(tf)
        prefix = manifest["source_name"] + "/"
        with_prefix = sum(1 for e in manifest["files"] if prefix + e["path"] in members)
        without_prefix = sum(1 for e in manifest["files"] if e["path"] in members)
        if with_prefix == len(manifest["files"]):
            selected_prefix = prefix
        elif without_prefix == len(manifest["files"]):
            selected_prefix = ""
        else:
            raise RuntimeError(
                "archive does not contain every manifest file under a consistent prefix "
                f"(with projects/: {with_prefix}, without: {without_prefix})")
        for entry in manifest["files"]:
            member = members[selected_prefix + entry["path"]]
            fh = tf.extractfile(member)
            if fh is None:
                raise RuntimeError(f"cannot read archive member: {member.name}")
            with fh:
                digest = _sha_stream(fh)
            _compare_entry(entry, member.size, digest)
        for entry in manifest.get("symlinks", []):
            key = selected_prefix + entry["path"]
            member = members.get(key)
            if member is None or not member.issym():
                raise RuntimeError(f"backup symlink missing: {entry['path']}")
            if member.linkname != entry["target"]:
                raise RuntimeError(f"symlink target mismatch for {entry['path']}")
    return {"backup_kind": "tar", "backup_content_sha256": _sha_file(backup),
            "resolved_content_root": selected_prefix.rstrip("/") or "."}


def _same_device(source: Path, backup: Path) -> bool:
    probe = backup if backup.exists() else backup.parent
    return source.stat().st_dev == probe.stat().st_dev


def verify_backup(manifest_path: Path, backup: Path, *, allow_same_device: bool) -> dict:
    manifest_path = manifest_path.resolve()
    manifest = load_manifest(manifest_path)
    source = Path(manifest["source_root"])
    backup = backup.resolve()
    if not backup.exists():
        raise RuntimeError(f"backup not found: {backup}")
    same_device = _same_device(source, backup)
    if same_device and not allow_same_device:
        raise RuntimeError(
            "backup is on the same filesystem device as projects/. "
            "Use a genuinely external mount; --allow-same-device is for tests only.")
    if backup.is_dir():
        detail = verify_directory(manifest, backup)
    elif tarfile.is_tarfile(backup):
        detail = verify_tar(manifest, backup)
    else:
        raise RuntimeError("supported backups: directory, .tar, .tar.gz, .tgz")
    return {
        "schema_version": SCHEMA,
        "kind": "legacy_projects_backup_receipt",
        "verified_at": _utc_now(),
        "manifest_path": str(manifest_path),
        "manifest_content_id": manifest["content_id"],
        "source_root": manifest["source_root"],
        "backup_path": str(backup),
        "external_device_verified": not same_device,
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
        **detail,
    }


def verify_source_unchanged(manifest: dict, source: Path) -> None:
    current = build_manifest(source)
    if current["content_id"] != manifest["content_id"]:
        raise RuntimeError(
            "projects/ changed after the manifest was created; create and verify a new backup")


def _load_dotenv(repo: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    p = repo / ".env"
    if not p.is_file():
        return values
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _effective_setting(repo: Path, name: str) -> str | None:
    return os.environ.get(name) or _load_dotenv(repo).get(name)


def _run_gate(label: str, command: list[str], repo: Path, env: dict | None = None) -> dict:
    proc = subprocess.run(command, cwd=repo, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    tail = proc.stdout.strip().splitlines()[-12:]
    if proc.returncode != 0:
        raise RuntimeError(f"gate failed: {label}\n" + "\n".join(tail))
    return {"label": label, "command": command, "tail": tail}


def _active_consumers(repo: Path, source: Path) -> list[dict]:
    blockers: list[dict] = []
    tokens = ("uvicorn", "gunicorn", "celery", "process_project", "batch_migrate",
              "audit_worker", "rq worker")
    me = os.getpid()
    for proc_dir in Path("/proc").glob("[0-9]*"):
        pid = int(proc_dir.name)
        if pid == me:
            continue
        try:
            cmd = (proc_dir / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", "replace")
            cwd = (proc_dir / "cwd").resolve()
        except (OSError, PermissionError):
            continue
        repo_related = cwd == repo or repo in cwd.parents
        if repo_related and any(token in cmd for token in tokens):
            blockers.append({"pid": pid, "reason": "active worker/server", "cmd": cmd})
            continue
        try:
            for fd in (proc_dir / "fd").iterdir():
                try:
                    target = fd.resolve()
                except OSError:
                    continue
                if target == source or source in target.parents:
                    blockers.append({"pid": pid, "reason": f"open fd under projects: {target}",
                                     "cmd": cmd})
                    break
        except (OSError, PermissionError):
            pass
    return blockers


def migration_gates(repo: Path) -> list[dict]:
    if _effective_setting(repo, "AUDIT_STORAGE_BACKEND") != "projects_v2":
        raise RuntimeError("AUDIT_STORAGE_BACKEND must be projects_v2")
    if _effective_setting(repo, "AUDIT_PROJECTS_V2_WRITE_MODE") != "projects_v2_primary":
        raise RuntimeError("AUDIT_PROJECTS_V2_WRITE_MODE must be projects_v2_primary")
    py = sys.executable
    base = repo / "scripts" / "projects_v2"
    gates = [
        _run_gate("migration coverage", [py, str(base / "verify_migration_coverage.py")], repo),
        _run_gate("migration ledger", [py, str(base / "validate_migration.py")], repo),
        _run_gate("drift", [py, str(base / "scan_migrated_drift.py"),
                             "--stable-seconds", "0"], repo),
        _run_gate("v2-only tests", [py, "-m", "pytest", "-q",
                                     "tests/test_projects_v2_only_compat.py",
                                     "tests/test_projects_v2_only_harness.py",
                                     "tests/test_projects_v2_final_acceptance.py"], repo),
    ]
    drift = json.loads((repo / "projects_v2" / "_system" /
                        "migrated_drift_scan_report.json").read_text(encoding="utf-8"))
    if drift.get("summary", {}).get("drift_documents") != 0:
        raise RuntimeError("drift gate reports non-zero documents")
    return gates


def parity_diagnostic(repo: Path) -> dict:
    """Refresh full-corpus parity and summarize legacy-only runtime data.

    Literal parity is not expected after v2-primary has moved forward, but
    findings/version losses need a separate, visible acknowledgement before an
    accelerated retirement.  The verified external backup remains the exact
    recovery source for those legacy-only snapshots.
    """
    script = repo / "scripts" / "projects_v2" / "check_ui_contract_parity.py"
    command = [sys.executable, str(script), "--all"]
    proc = subprocess.run(command, cwd=repo, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    # The parity tool intentionally returns 1 when it found mismatches.  That
    # is diagnostic data handled below, not a failure to produce the report.
    if proc.returncode not in (0, 1):
        raise RuntimeError("full-corpus parity diagnostic failed\n" +
                           "\n".join(proc.stdout.strip().splitlines()[-12:]))
    gate = {"label": "full-corpus parity diagnostic", "command": command,
            "returncode": proc.returncode,
            "tail": proc.stdout.strip().splitlines()[-12:]}
    report = json.loads((repo / "projects_v2" / "_system" /
                         "full_corpus_parity_report.json").read_text(encoding="utf-8"))
    counts = report.get("doc_status_counts", {}) or {}
    findings = report.get("findings_losses", []) or []
    versions = report.get("version_losses", []) or []
    missing_v2 = [r.get("document_code") for r in report.get("results", [])
                  if r.get("doc_status") == "MISSING_IN_V2"]
    return {
        "gate": gate,
        "contract_ok": bool(report.get("contract_ok")),
        "documents_checked": report.get("documents_checked"),
        "doc_status_counts": counts,
        "findings_loss_documents": findings,
        "version_loss_documents": versions,
        "missing_in_v2_documents": missing_v2,
        "requires_explicit_ack": bool(findings or versions or missing_v2),
    }


def load_receipt(path: Path, manifest: dict) -> dict:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if obj.get("kind") != "legacy_projects_backup_receipt":
        raise RuntimeError("invalid backup receipt")
    if obj.get("manifest_content_id") != manifest["content_id"]:
        raise RuntimeError("receipt belongs to a different source manifest")
    if not obj.get("external_device_verified"):
        raise RuntimeError("receipt does not prove an external-device backup")
    return obj


def full_preflight(manifest_path: Path, receipt_path: Path) -> dict:
    repo = _repo_root()
    manifest = load_manifest(manifest_path)
    receipt = load_receipt(receipt_path, manifest)
    source = Path(manifest["source_root"]).resolve()
    if source != (repo / "projects").resolve():
        raise RuntimeError(f"manifest source is not this repository's projects/: {source}")
    # Re-read every source and backup byte. A stale receipt alone is insufficient.
    verify_source_unchanged(manifest, source)
    fresh = verify_backup(manifest_path, Path(receipt["backup_path"]),
                          allow_same_device=False)
    if fresh.get("backup_content_sha256") != receipt.get("backup_content_sha256"):
        raise RuntimeError("backup archive changed after receipt creation")
    gates = migration_gates(repo)
    parity = parity_diagnostic(repo)
    blockers = _active_consumers(repo, source)
    parity_risk = parity["requires_explicit_ack"]
    return {
        "schema_version": SCHEMA,
        "kind": "legacy_projects_retirement_preflight",
        "generated_at": _utc_now(),
        "ready_without_parity_ack": not blockers and not parity_risk,
        "ready_with_verified_backup_and_parity_ack": not blockers,
        "source_root": str(source),
        "manifest_content_id": manifest["content_id"],
        "backup_path": receipt["backup_path"],
        "backup_external": True,
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
        "gates": gates,
        "parity_diagnostic": parity,
        "active_blockers": blockers,
        "parity_note": (
            "Legacy/v2 literal parity is diagnostic, not an execute gate: the external "
            "backup is the lossless rollback source while projects_v2 is canonical."),
    }


def _post_rename_smoke(repo: Path) -> dict:
    env = os.environ.copy()
    env["AUDIT_STORAGE_BACKEND"] = "projects_v2"
    env["AUDIT_PROJECTS_V2_WRITE_MODE"] = "projects_v2_primary"
    env["AUDIT_PROJECTS_DIR"] = str(repo / "projects")  # intentionally absent
    return _run_gate(
        "post-rename v2-only smoke",
        [sys.executable, "-m", "pytest", "-q",
         "tests/test_projects_v2_only_compat.py",
         "tests/test_projects_v2_only_harness.py"],
        repo, env=env)


def execute_retirement(manifest_path: Path, receipt_path: Path, confirm: str,
                       parity_ack: str | None = None) -> dict:
    if confirm != CONFIRM:
        raise RuntimeError(f"refused: --confirm must equal {CONFIRM}")
    preflight = full_preflight(manifest_path, receipt_path)
    if preflight["active_blockers"]:
        details = "; ".join(f"pid={b['pid']} {b['reason']}" for b in preflight["active_blockers"])
        raise RuntimeError("stop backend/audit workers before retirement: " + details)
    parity = preflight["parity_diagnostic"]
    if parity["requires_explicit_ack"] and parity_ack != PARITY_ACK:
        raise RuntimeError(
            "full-corpus parity still contains legacy-only findings/versions. "
            f"Review the preflight report; accelerated retirement requires "
            f"--acknowledge-parity-risk {PARITY_ACK}")
    repo = _repo_root()
    source = repo / "projects"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quarantine = repo / f"projects_legacy_archive_{stamp}"
    if quarantine.exists():
        raise RuntimeError(f"quarantine path already exists: {quarantine}")
    os.replace(source, quarantine)
    try:
        smoke = _post_rename_smoke(repo)
    except Exception:
        if source.exists():
            raise RuntimeError(
                f"post-rename smoke failed and automatic rollback is blocked: {source} exists")
        os.replace(quarantine, source)
        raise
    shutil.rmtree(quarantine)
    result = {
        "schema_version": SCHEMA,
        "kind": "legacy_projects_retirement_result",
        "completed_at": _utc_now(),
        "deleted": str(quarantine),
        "source_absent": not source.exists(),
        "manifest_content_id": preflight["manifest_content_id"],
        "backup_path": preflight["backup_path"],
        "post_rename_smoke": smoke,
    }
    _write_json(repo / "projects_v2" / "_system" /
                "legacy_projects_retirement_result.json", result)
    return result


def _human_bytes(value: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return str(value)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Guarded legacy projects/ retirement")
    sub = ap.add_subparsers(dest="command", required=True)
    p_manifest = sub.add_parser("manifest", help="hash every legacy source file")
    p_manifest.add_argument("--source", type=Path, default=_repo_root() / "projects")
    p_manifest.add_argument("--output", type=Path, required=True)
    p_verify = sub.add_parser("verify-backup", help="verify every backup file and write receipt")
    p_verify.add_argument("--manifest", type=Path, required=True)
    p_verify.add_argument("--backup", type=Path, required=True)
    p_verify.add_argument("--receipt", type=Path, required=True)
    p_verify.add_argument("--allow-same-device", action="store_true",
                          help="tests only; such a receipt cannot authorize deletion")
    p_pre = sub.add_parser("preflight", help="repeat full verification and all deletion gates")
    p_pre.add_argument("--manifest", type=Path, required=True)
    p_pre.add_argument("--receipt", type=Path, required=True)
    p_pre.add_argument("--output", type=Path,
                       default=_repo_root() / "projects_v2" / "_system" /
                       "legacy_projects_retirement_preflight.json")
    p_exec = sub.add_parser("execute", help="quarantine, v2-only smoke, then delete")
    p_exec.add_argument("--manifest", type=Path, required=True)
    p_exec.add_argument("--receipt", type=Path, required=True)
    p_exec.add_argument("--confirm", required=True)
    p_exec.add_argument("--acknowledge-parity-risk", default=None,
                        help="required only when preflight reports legacy-only runtime data")
    args = ap.parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "manifest":
            obj = build_manifest(args.source)
            _write_json(args.output.resolve(), obj)
            print(f"[OK] manifest: {args.output.resolve()}")
            print(f"files={obj['file_count']} bytes={obj['total_bytes']} "
                  f"({_human_bytes(obj['total_bytes'])}) content_id={obj['content_id']}")
        elif args.command == "verify-backup":
            obj = verify_backup(args.manifest, args.backup,
                                allow_same_device=args.allow_same_device)
            _write_json(args.receipt.resolve(), obj)
            print(f"[OK] backup verified byte-for-byte: {obj['backup_path']}")
            print(f"receipt: {args.receipt.resolve()} external={obj['external_device_verified']}")
        elif args.command == "preflight":
            obj = full_preflight(args.manifest, args.receipt)
            _write_json(args.output.resolve(), obj)
            ready = obj["ready_without_parity_ack"]
            print(f"[{'OK' if ready else 'REVIEW'}] preflight ready_without_ack={ready}")
            print(f"report: {args.output.resolve()}")
            parity = obj["parity_diagnostic"]
            print("parity risks: "
                  f"findings={len(parity['findings_loss_documents'])} "
                  f"versions={len(parity['version_loss_documents'])} "
                  f"missing_v2={len(parity['missing_in_v2_documents'])}")
            if obj["active_blockers"]:
                for blocker in obj["active_blockers"]:
                    print(f"  pid={blocker['pid']} {blocker['reason']}")
                return 3
        else:
            obj = execute_retirement(args.manifest, args.receipt, args.confirm,
                                     args.acknowledge_parity_risk)
            print(f"[OK] legacy projects retired; backup retained at {obj['backup_path']}")
        return 0
    except (OSError, RuntimeError, tarfile.TarError, json.JSONDecodeError) as exc:
        print(f"[REFUSED] {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
