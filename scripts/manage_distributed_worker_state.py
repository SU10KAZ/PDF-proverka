#!/usr/bin/env python3
"""Prepare and attest the host-side shared Worker-state contract.

Only this deployment/bootstrap tool mutates UID/GID/mode/ACL.  The hardened
runtime consumes the root-owned receipt and remains validation-only.
"""
from __future__ import annotations

import argparse
import grp
import json
import os
import socket
import sqlite3
import stat
import struct
from datetime import datetime, timezone
from pathlib import Path

from backend.app.services.distributed_workers.state_permissions import (
    SHARED_DIRECTORY_MODE,
    SHARED_FILE_MODE,
    SQLITE_SIDECAR_SUFFIXES,
    STATIC_DIRECTORY_NAMES,
    TRUSTED_RECEIPT_MODE,
    TRUSTED_RECEIPT_OWNER_GID,
    TRUSTED_RECEIPT_OWNER_UID,
    TRUSTED_RECEIPT_PARENT_MODE,
    build_host_receipt,
    canonical_receipt_bytes,
    encode_default_acl,
    host_object_record,
    validate_shared_directory,
    validate_shared_file,
)

_ACL_ACCESS = "system.posix_acl_access"
_ACL_DEFAULT = "system.posix_acl_default"
_ACL_UNDEFINED_ID = 0xFFFFFFFF


def _absolute_scoped(path: Path, label: str) -> Path:
    if not path.is_absolute() or path == Path("/") or len(path.parts) < 3:
        raise SystemExit(f"{label} must be an absolute, non-root scoped path")
    return path


def _plain(path: Path, *, directory: bool) -> os.stat_result:
    info = path.lstat()
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if stat.S_ISLNK(info.st_mode) or not expected:
        raise SystemExit(f"refusing non-plain state path: {path}")
    return info


def _access_acl(parent: Path, service_uids: tuple[int, ...]) -> bytes:
    mode = stat.S_IMODE(_plain(parent, directory=True).st_mode)
    owner_perm = (mode >> 6) & 0o7
    group_perm = (mode >> 3) & 0o7
    other_perm = mode & 0o7
    mask_perm = group_perm
    entries = [(0x01, owner_perm, _ACL_UNDEFINED_ID)]
    for uid in sorted(set(service_uids)):
        entries.append((0x02, 0o1, uid))
        mask_perm |= 0o1
    entries.extend(
        ((0x04, group_perm, _ACL_UNDEFINED_ID), (0x10, mask_perm, _ACL_UNDEFINED_ID),
         (0x20, other_perm, _ACL_UNDEFINED_ID))
    )
    raw = bytearray(struct.pack("<I", 2))
    for entry in entries:
        raw.extend(struct.pack("<HHI", *entry))
    return bytes(raw)


def _validate_parent_acl(parent: Path, service_uids: tuple[int, ...]) -> None:
    try:
        raw = os.getxattr(parent, _ACL_ACCESS, follow_symlinks=False)
    except OSError as exc:
        raise SystemExit(f"shared-state parent traverse ACL is missing: {parent}") from exc
    if len(raw) < 4 or (len(raw) - 4) % 8 or struct.unpack_from("<I", raw)[0] != 2:
        raise SystemExit(f"shared-state parent traverse ACL is malformed: {parent}")
    entries: dict[tuple[int, int], int] = {}
    for offset in range(4, len(raw), 8):
        tag, permissions, identifier = struct.unpack_from("<HHI", raw, offset)
        entries[(tag, identifier)] = permissions
    named = {
        identifier: permissions
        for (tag, identifier), permissions in entries.items()
        if tag == 0x02
    }
    if named != {uid: 0o1 for uid in sorted(set(service_uids))}:
        raise SystemExit(f"shared-state parent traverse ACL mismatch: {parent}")
    mode = stat.S_IMODE(_plain(parent, directory=True).st_mode)
    if (
        entries.get((0x01, _ACL_UNDEFINED_ID)) != (mode >> 6) & 0o7
        or entries.get((0x10, _ACL_UNDEFINED_ID)) != (mode >> 3) & 0o7
        or entries.get((0x20, _ACL_UNDEFINED_ID)) != mode & 0o7
    ):
        raise SystemExit(f"shared-state parent ACL/mode mismatch: {parent}")


def _known_database_files(data_dir: Path) -> list[Path]:
    result = []
    for name in ("workers.db", *("workers.db" + suffix for suffix in SQLITE_SIDECAR_SUFFIXES)):
        path = data_dir / name
        if path.exists() or path.is_symlink():
            result.append(path)
    for path in sorted(data_dir.glob("workers.db.before_v*_to_v*")):
        if path not in result:
            result.append(path)
    return result


def _state_objects(data_dir: Path) -> list[Path]:
    directories = [data_dir / name if name else data_dir for name in STATIC_DIRECTORY_NAMES]
    files = _known_database_files(data_dir)
    if data_dir / "workers.db" not in files:
        raise SystemExit("shared-state preparation requires an existing workers.db")
    return directories + files


def _validate_group(args: argparse.Namespace) -> None:
    try:
        entry = grp.getgrnam(args.shared_group)
    except KeyError as exc:
        raise SystemExit(f"required shared group does not exist: {args.shared_group}") from exc
    if entry.gr_gid != args.shared_gid:
        raise SystemExit(
            f"shared group identity mismatch: {args.shared_group} has gid={entry.gr_gid}; "
            f"expected {args.shared_gid}"
        )


def _require_backend_inactive(host: str, port: int) -> None:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            pass
    except OSError:
        return
    raise SystemExit(
        f"refusing shared-state preparation while backend listener is active: {host}:{port}"
    )


def _require_database_quiescent(data_dir: Path) -> tuple[int, int, int]:
    """Recover/checkpoint SQLite through SQLite itself before host mutation.

    WAL is durable database state and must never be unlinked blindly.  A
    successful TRUNCATE checkpoint proves that no active reader/writer blocks
    recovery and moves every committed frame into the main database.  The
    subsequent integrity check is performed before metadata normalization or
    receipt minting.  Closing this read/write connection lets SQLite perform
    its own safe WAL/SHM cleanup; any retained sidecar is normalized later.
    """
    database_path = data_dir / "workers.db"
    _plain(database_path, directory=False)
    for suffix in SQLITE_SIDECAR_SUFFIXES:
        path = database_path.with_name(database_path.name + suffix)
        if path.exists() or path.is_symlink():
            _plain(path, directory=False)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"{database_path.as_uri()}?mode=rw",
            uri=True,
            timeout=0.0,
            isolation_level=None,
        )
        connection.execute("PRAGMA busy_timeout = 0")
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is None or len(checkpoint) != 3 or int(checkpoint[0]) != 0:
            raise SystemExit(
                "refusing shared-state preparation while SQLite is busy: "
                f"checkpoint={tuple(checkpoint) if checkpoint is not None else None}"
            )
        integrity = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            raise SystemExit(
                "refusing shared-state preparation for a database that failed "
                f"integrity_check: {integrity[:3]}"
            )
        return tuple(int(value) for value in checkpoint)
    except sqlite3.Error as exc:
        raise SystemExit(
            f"refusing shared-state preparation for non-quiescent/invalid SQLite state: {exc}"
        ) from exc
    finally:
        if connection is not None:
            connection.close()


def _receipt_parent(path: Path) -> Path:
    path = _absolute_scoped(path, "--receipt")
    parent = path.parent
    if not parent.exists():
        _plain(parent.parent, directory=True)
        # A deployment may deliberately run with umask 0077.  mkdir's mode is
        # therefore not the contract until it is normalized explicitly.
        parent.mkdir(mode=0o700)
        os.chown(parent, TRUSTED_RECEIPT_OWNER_UID, TRUSTED_RECEIPT_OWNER_GID)
        os.chmod(parent, TRUSTED_RECEIPT_PARENT_MODE)
    info = _plain(parent, directory=True)
    if (
        info.st_uid != TRUSTED_RECEIPT_OWNER_UID
        or info.st_gid != TRUSTED_RECEIPT_OWNER_GID
        or stat.S_IMODE(info.st_mode) != TRUSTED_RECEIPT_PARENT_MODE
    ):
        raise SystemExit(
            "receipt parent must have the exact trusted owner/group/mode: "
            f"uid={TRUSTED_RECEIPT_OWNER_UID} gid={TRUSTED_RECEIPT_OWNER_GID} "
            f"mode={TRUSTED_RECEIPT_PARENT_MODE:04o}"
        )
    return parent


def _write_receipt(path: Path, payload: bytes) -> None:
    parent = _receipt_parent(path)
    temporary = parent / f".{path.name}.tmp-{os.getpid()}"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.write(fd, payload)
        os.fsync(fd)
        os.fchmod(fd, TRUSTED_RECEIPT_MODE)
        os.fchown(fd, TRUSTED_RECEIPT_OWNER_UID, TRUSTED_RECEIPT_OWNER_GID)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _receipt(args: argparse.Namespace) -> dict[str, object]:
    return build_host_receipt(
        data_dir=args.data_dir,
        owner_uid=args.owner_uid,
        shared_gid=args.shared_gid,
        shared_group=args.shared_group,
        prepared_at_utc=datetime.now(timezone.utc).isoformat(),
        objects=[host_object_record(path, data_dir=args.data_dir) for path in _state_objects(args.data_dir)],
    )


def _validate_receipt_host(args: argparse.Namespace) -> dict[str, object]:
    parent = _plain(args.receipt.parent, directory=True)
    if (
        parent.st_uid != TRUSTED_RECEIPT_OWNER_UID
        or parent.st_gid != TRUSTED_RECEIPT_OWNER_GID
        or stat.S_IMODE(parent.st_mode) != TRUSTED_RECEIPT_PARENT_MODE
    ):
        raise SystemExit("trusted preparation receipt parent is not protected")
    info = _plain(args.receipt, directory=False)
    if (
        info.st_uid != TRUSTED_RECEIPT_OWNER_UID
        or info.st_gid != TRUSTED_RECEIPT_OWNER_GID
        or stat.S_IMODE(info.st_mode) != TRUSTED_RECEIPT_MODE
    ):
        raise SystemExit("trusted preparation receipt owner/mode mismatch")
    try:
        receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("trusted preparation receipt is invalid") from exc
    expected = _receipt(args)
    expected["prepared_at_utc"] = receipt.get("prepared_at_utc")
    if receipt != expected:
        raise SystemExit("trusted preparation receipt does not match current host metadata")
    return receipt


def _validate_host_state(args: argparse.Namespace, *, require_receipt: bool) -> None:
    data_dir = _absolute_scoped(args.data_dir, "--data-dir")
    _validate_group(args)
    if args.service_uid:
        _validate_parent_acl(data_dir.parent, tuple(args.service_uid))
    for name in STATIC_DIRECTORY_NAMES:
        path = data_dir / name if name else data_dir
        validate_shared_directory(path, owner_uid=args.owner_uid, shared_gid=args.shared_gid)
    allowed_owners = {args.owner_uid, *args.service_uid}
    for path in _known_database_files(data_dir):
        validate_shared_file(
            path, shared_gid=args.shared_gid, allowed_owner_uids=allowed_owners
        )
    if data_dir / "workers.db" not in _known_database_files(data_dir):
        raise SystemExit("shared-state validation requires workers.db")
    if require_receipt:
        _validate_receipt_host(args)


def prepare(args: argparse.Namespace) -> None:
    data_dir = _absolute_scoped(args.data_dir, "--data-dir")
    _require_backend_inactive(args.backend_host, args.backend_port)
    _validate_group(args)
    checkpoint = _require_database_quiescent(data_dir)
    parent = data_dir.parent
    _plain(parent, directory=True)
    if args.service_uid:
        os.setxattr(
            parent, _ACL_ACCESS, _access_acl(parent, tuple(args.service_uid)),
            follow_symlinks=False,
        )
    for name in STATIC_DIRECTORY_NAMES:
        path = data_dir / name if name else data_dir
        path.mkdir(mode=0o700, exist_ok=True)
        _plain(path, directory=True)
        os.chown(path, args.owner_uid, args.shared_gid, follow_symlinks=False)
        os.chmod(path, SHARED_DIRECTORY_MODE, follow_symlinks=False)
        os.setxattr(path, _ACL_DEFAULT, encode_default_acl(), follow_symlinks=False)
    for path in _known_database_files(data_dir):
        _plain(path, directory=False)
        os.chown(path, -1, args.shared_gid, follow_symlinks=False)
        os.chmod(path, SHARED_FILE_MODE, follow_symlinks=False)
    _validate_host_state(args, require_receipt=False)
    _write_receipt(args.receipt, canonical_receipt_bytes(_receipt(args)))
    _validate_host_state(args, require_receipt=True)
    print(f"SQLITE_QUIESCENCE_PASS checkpoint={checkpoint} integrity=ok")
    print("DISTRIBUTED_WORKER_STATE_PREPARATION_PASS")


def validate(args: argparse.Namespace) -> None:
    _validate_host_state(args, require_receipt=True)
    print("DISTRIBUTED_WORKER_STATE_HOST_VALIDATION_PASS")


def validate_host(args: argparse.Namespace) -> None:
    """Validate exact host metadata without creating or trusting a receipt."""
    _validate_host_state(args, require_receipt=False)
    print("DISTRIBUTED_WORKER_STATE_HOST_METADATA_PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(required=True)
    for command, function in (
        ("prepare", prepare),
        ("validate", validate),
        ("validate-host", validate_host),
    ):
        item = sub.add_parser(command)
        item.add_argument("--data-dir", type=Path, required=True)
        item.add_argument("--owner-uid", type=int, required=True)
        item.add_argument("--shared-gid", type=int, required=True)
        item.add_argument("--shared-group", required=True)
        item.add_argument("--service-uid", type=int, action="append", default=[])
        item.add_argument("--receipt", type=Path, required=True)
        item.add_argument("--backend-host", default="127.0.0.1")
        item.add_argument("--backend-port", type=int, default=8081)
        item.set_defaults(func=function)
    args = parser.parse_args()
    if (
        args.owner_uid < 0 or args.shared_gid < 0
        or any(uid < 0 for uid in args.service_uid)
        or not 1 <= args.backend_port <= 65535
    ):
        raise SystemExit("UID/GID/port values are out of range")
    args.data_dir = _absolute_scoped(args.data_dir, "--data-dir")
    args.receipt = _absolute_scoped(args.receipt, "--receipt")
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
