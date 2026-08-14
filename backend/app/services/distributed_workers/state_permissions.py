"""Fail-closed metadata contract for multi-identity Worker state.

Persistent directory creation belongs to the privileged deployment boundary.
Hardened application processes only validate that contract.  The one runtime
mutation in this module is deliberately limited to completing mode 0660 on a
plain regular file already owned by the caller and already in the configured
shared group; it never changes ownership or directory SGID.
"""
from __future__ import annotations

import errno
import os
import stat
import struct
from pathlib import Path

from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
)


SHARED_DIRECTORY_MODE = 0o2770
SHARED_FILE_MODE = 0o660
STATIC_DIRECTORY_NAMES = (
    "",
    "source_packages",
    "incoming",
    "result_staging",
    "validated_results",
    "rejected_results",
    "superseded_results",
    "job_logs",
)

_ACL_XATTR_DEFAULT = "system.posix_acl_default"
_ACL_VERSION = 2
_ACL_UNDEFINED_ID = 0xFFFFFFFF
_ACL_USER_OBJ = 0x01
_ACL_GROUP_OBJ = 0x04
_ACL_MASK = 0x10
_ACL_OTHER = 0x20
_EXPECTED_DEFAULT_ACL = {
    (_ACL_USER_OBJ, _ACL_UNDEFINED_ID): 0o7,
    (_ACL_GROUP_OBJ, _ACL_UNDEFINED_ID): 0o7,
    (_ACL_MASK, _ACL_UNDEFINED_ID): 0o7,
    (_ACL_OTHER, _ACL_UNDEFINED_ID): 0o0,
}


class SharedStatePermissionError(DistributedWorkersConfigError):
    """The deployment-owned persistent-state metadata is not safe/usable."""


def encode_default_acl() -> bytes:
    """Return the exact Linux POSIX default ACL used by the bootstrap tool."""
    result = bytearray(struct.pack("<I", _ACL_VERSION))
    for tag, permissions in (
        (_ACL_USER_OBJ, 0o7),
        (_ACL_GROUP_OBJ, 0o7),
        (_ACL_MASK, 0o7),
        (_ACL_OTHER, 0o0),
    ):
        result.extend(struct.pack("<HHI", tag, permissions, _ACL_UNDEFINED_ID))
    return bytes(result)


def _decode_acl(raw: bytes, path: Path) -> dict[tuple[int, int], int]:
    if len(raw) < 4 or (len(raw) - 4) % 8:
        raise SharedStatePermissionError(
            f"distributed Worker state has malformed default ACL: {path}"
        )
    version = struct.unpack_from("<I", raw)[0]
    if version != _ACL_VERSION:
        raise SharedStatePermissionError(
            f"distributed Worker state has unsupported default ACL: {path}"
        )
    entries: dict[tuple[int, int], int] = {}
    for offset in range(4, len(raw), 8):
        tag, permissions, identifier = struct.unpack_from("<HHI", raw, offset)
        key = (tag, identifier)
        if key in entries or permissions & ~0o7:
            raise SharedStatePermissionError(
                f"distributed Worker state has malformed default ACL: {path}"
            )
        entries[key] = permissions
    return entries


def _plain_stat(path: Path, *, directory: bool) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise SharedStatePermissionError(
            f"distributed Worker shared state is not prepared: missing {path}"
        ) from exc
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if stat.S_ISLNK(info.st_mode) or not expected:
        kind = "directory" if directory else "file"
        raise SharedStatePermissionError(
            f"distributed Worker state {kind} is not a plain path: {path}"
        )
    return info


def validate_shared_directory(path: Path, *, owner_uid: int, shared_gid: int) -> None:
    """Validate deployment-owned owner/group/mode/default-ACL without writes."""
    info = _plain_stat(path, directory=True)
    mode = stat.S_IMODE(info.st_mode)
    if info.st_uid != owner_uid or info.st_gid != shared_gid or mode != SHARED_DIRECTORY_MODE:
        raise SharedStatePermissionError(
            "distributed Worker shared directory metadata mismatch: "
            f"{path} has uid={info.st_uid} gid={info.st_gid} mode={mode:04o}; "
            f"expected uid={owner_uid} gid={shared_gid} mode={SHARED_DIRECTORY_MODE:04o}"
        )
    try:
        raw_acl = os.getxattr(path, _ACL_XATTR_DEFAULT, follow_symlinks=False)
    except OSError as exc:
        if exc.errno in {errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP}:
            raise SharedStatePermissionError(
                f"distributed Worker shared directory lacks required default ACL: {path}"
            ) from exc
        raise
    if _decode_acl(raw_acl, path) != _EXPECTED_DEFAULT_ACL:
        raise SharedStatePermissionError(
            f"distributed Worker shared directory default ACL mismatch: {path}"
        )


def validate_or_complete_shared_file(path: Path, *, shared_gid: int) -> None:
    """Validate mode/gid or make the only safe creator-owned file correction."""
    info = _plain_stat(path, directory=False)
    mode = stat.S_IMODE(info.st_mode)
    if info.st_gid != shared_gid:
        raise SharedStatePermissionError(
            f"distributed Worker shared file GID mismatch: {path} has "
            f"gid={info.st_gid}; expected gid={shared_gid}"
        )
    if mode == SHARED_FILE_MODE:
        return
    # No executable/special/other bits may be hidden by a normalizing chmod,
    # and a process may only complete permissions on its own newly-created file.
    if mode & ~SHARED_FILE_MODE or info.st_uid != os.geteuid():
        raise SharedStatePermissionError(
            f"distributed Worker shared file mode is unsafe or not caller-owned: "
            f"{path} has uid={info.st_uid} mode={mode:04o}; expected mode=0660"
        )
    try:
        os.chmod(path, SHARED_FILE_MODE, follow_symlinks=False)
    except OSError as exc:
        raise SharedStatePermissionError(
            f"cannot complete safe distributed Worker file mode 0660: {path}"
        ) from exc
    final = _plain_stat(path, directory=False)
    if final.st_gid != shared_gid or stat.S_IMODE(final.st_mode) != SHARED_FILE_MODE:
        raise SharedStatePermissionError(
            f"distributed Worker shared file verification failed: {path}"
        )
