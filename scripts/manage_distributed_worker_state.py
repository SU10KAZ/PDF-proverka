#!/usr/bin/env python3
"""Prepare/validate the one canonical shared Worker-state permission boundary.

Production use is an explicit operator/deployment action before hardened services
start.  It is intentionally non-recursive and only touches the typed static
directories plus known SQLite files.  Runtime imports only validation helpers and
never calls this script.
"""
from __future__ import annotations

import argparse
import os
import stat
import struct
from pathlib import Path

from backend.app.services.distributed_workers.state_permissions import (
    SHARED_DIRECTORY_MODE,
    SHARED_FILE_MODE,
    STATIC_DIRECTORY_NAMES,
    encode_default_acl,
    validate_or_complete_shared_file,
    validate_shared_directory,
)

_ACL_ACCESS = "system.posix_acl_access"
_ACL_DEFAULT = "system.posix_acl_default"
_ACL_UNDEFINED_ID = 0xFFFFFFFF


def _absolute_scoped(path: Path) -> Path:
    if not path.is_absolute() or path == Path("/") or len(path.parts) < 3:
        raise SystemExit("--data-dir must be an absolute, non-root scoped path")
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
    for name in ("workers.db", "workers.db-wal", "workers.db-shm"):
        path = data_dir / name
        if path.exists() or path.is_symlink():
            result.append(path)
    for path in sorted(data_dir.glob("workers.db.before_v*_to_v*")):
        if path not in result:
            result.append(path)
    return result


def prepare(args: argparse.Namespace) -> None:
    data_dir = _absolute_scoped(args.data_dir)
    parent = data_dir.parent
    _plain(parent, directory=True)
    if args.service_uid:
        os.setxattr(
            parent,
            _ACL_ACCESS,
            _access_acl(parent, tuple(args.service_uid)),
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
    validate(args)


def validate(args: argparse.Namespace) -> None:
    data_dir = _absolute_scoped(args.data_dir)
    if args.service_uid:
        _validate_parent_acl(data_dir.parent, tuple(args.service_uid))
    for name in STATIC_DIRECTORY_NAMES:
        path = data_dir / name if name else data_dir
        validate_shared_directory(path, owner_uid=args.owner_uid, shared_gid=args.shared_gid)
    for path in _known_database_files(data_dir):
        # In validate mode a wrong creator-owned mode must still fail instead of
        # being repaired.  Check directly before calling the common verifier.
        info = _plain(path, directory=False)
        if info.st_gid != args.shared_gid or stat.S_IMODE(info.st_mode) != SHARED_FILE_MODE:
            raise SystemExit(
                f"shared database metadata mismatch: {path} gid={info.st_gid} "
                f"mode={stat.S_IMODE(info.st_mode):04o}"
            )
        validate_or_complete_shared_file(path, shared_gid=args.shared_gid)
    print("DISTRIBUTED_WORKER_STATE_PERMISSION_PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(required=True)
    for command, function in (("prepare", prepare), ("validate", validate)):
        item = sub.add_parser(command)
        item.add_argument("--data-dir", type=Path, required=True)
        item.add_argument("--owner-uid", type=int, required=True)
        item.add_argument("--shared-gid", type=int, required=True)
        item.add_argument("--service-uid", type=int, action="append", default=[])
        item.set_defaults(func=function)
    args = parser.parse_args()
    if args.owner_uid < 0 or args.shared_gid < 0 or any(uid < 0 for uid in args.service_uid):
        raise SystemExit("UID/GID values must be non-negative")
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
