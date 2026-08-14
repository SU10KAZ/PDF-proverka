"""Namespace-aware, validation-only contract for shared Worker state.

The privileged host preparation boundary owns UID/GID/mode/ACL mutation and
writes a root-owned receipt. Hardened runtimes never repair metadata. They
validate the immutable receipt, object identity and all security properties
that remain meaningful in their user namespace. An overflow ID is accepted
only as a namespace representation backed by that receipt; it is never a
source of trust by itself.
"""
from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import stat
import struct
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
)


CONTRACT_NAME = "auditmanager.distributed-workers.shared-state"
CONTRACT_VERSION = 1
TRUSTED_RECEIPT_OWNER_UID = 0
TRUSTED_RECEIPT_OWNER_GID = 0
TRUSTED_RECEIPT_MODE = 0o444
TRUSTED_RECEIPT_PARENT_MODE = 0o755
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

_ACL_XATTR_ACCESS = "system.posix_acl_access"
_ACL_XATTR_DEFAULT = "system.posix_acl_default"
_ACL_VERSION = 2
_ACL_UNDEFINED_ID = 0xFFFFFFFF
_ACL_USER_OBJ = 0x01
_ACL_USER = 0x02
_ACL_GROUP_OBJ = 0x04
_ACL_GROUP = 0x08
_ACL_MASK = 0x10
_ACL_OTHER = 0x20
_EXPECTED_DEFAULT_ACL = {
    (_ACL_USER_OBJ, _ACL_UNDEFINED_ID): 0o7,
    (_ACL_GROUP_OBJ, _ACL_UNDEFINED_ID): 0o7,
    (_ACL_MASK, _ACL_UNDEFINED_ID): 0o7,
    (_ACL_OTHER, _ACL_UNDEFINED_ID): 0o0,
}
_MAX_RECEIPT_BYTES = 1024 * 1024
logger = logging.getLogger(__name__)


class SharedStatePermissionError(DistributedWorkersConfigError):
    """The deployment-owned persistent-state contract is absent or unsafe."""


@dataclass(frozen=True)
class IdMapRange:
    namespace_start: int
    host_start: int
    length: int

    def visible_id(self, host_id: int) -> int | None:
        if self.host_start <= host_id < self.host_start + self.length:
            return self.namespace_start + host_id - self.host_start
        return None


def encode_default_acl() -> bytes:
    """Return the exact POSIX default ACL installed by host preparation."""
    result = bytearray(struct.pack("<I", _ACL_VERSION))
    for tag, permissions in (
        (_ACL_USER_OBJ, 0o7),
        (_ACL_GROUP_OBJ, 0o7),
        (_ACL_MASK, 0o7),
        (_ACL_OTHER, 0o0),
    ):
        result.extend(struct.pack("<HHI", tag, permissions, _ACL_UNDEFINED_ID))
    return bytes(result)


def _decode_acl(raw: bytes, path: Path, kind: str) -> dict[tuple[int, int], int]:
    if len(raw) < 4 or (len(raw) - 4) % 8:
        raise SharedStatePermissionError(
            f"distributed Worker state has malformed {kind} ACL: {path}"
        )
    version = struct.unpack_from("<I", raw)[0]
    if version != _ACL_VERSION:
        raise SharedStatePermissionError(
            f"distributed Worker state has unsupported {kind} ACL: {path}"
        )
    entries: dict[tuple[int, int], int] = {}
    for offset in range(4, len(raw), 8):
        tag, permissions, identifier = struct.unpack_from("<HHI", raw, offset)
        key = (tag, identifier)
        if key in entries or permissions & ~0o7:
            raise SharedStatePermissionError(
                f"distributed Worker state has malformed {kind} ACL: {path}"
            )
        entries[key] = permissions
    return entries


def _xattr(path: Path, name: str) -> bytes | None:
    try:
        return os.getxattr(path, name, follow_symlinks=False)
    except OSError as exc:
        if exc.errno in {errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP}:
            return None
        raise


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


def _validate_access_acl(path: Path, mode: int) -> None:
    raw = _xattr(path, _ACL_XATTR_ACCESS)
    if raw is None:
        return
    entries = _decode_acl(raw, path, "access")
    if any(tag in {_ACL_USER, _ACL_GROUP} for tag, _identifier in entries):
        raise SharedStatePermissionError(
            f"distributed Worker state has named access ACL entries: {path}"
        )
    expected = {
        (_ACL_USER_OBJ, _ACL_UNDEFINED_ID): (mode >> 6) & 0o7,
        (_ACL_GROUP_OBJ, _ACL_UNDEFINED_ID): (mode >> 3) & 0o7,
        (_ACL_OTHER, _ACL_UNDEFINED_ID): mode & 0o7,
    }
    if (_ACL_MASK, _ACL_UNDEFINED_ID) in entries:
        expected[(_ACL_MASK, _ACL_UNDEFINED_ID)] = (mode >> 3) & 0o7
    if entries != expected:
        raise SharedStatePermissionError(
            f"distributed Worker state access ACL/mode mismatch: {path}"
        )


def validate_shared_directory(path: Path, *, owner_uid: int, shared_gid: int) -> None:
    """Authoritative host-namespace directory validator; never writes."""
    info = _plain_stat(path, directory=True)
    mode = stat.S_IMODE(info.st_mode)
    if info.st_uid != owner_uid or info.st_gid != shared_gid or mode != SHARED_DIRECTORY_MODE:
        raise SharedStatePermissionError(
            "distributed Worker shared directory metadata mismatch: "
            f"{path} has uid={info.st_uid} gid={info.st_gid} mode={mode:04o}; "
            f"expected uid={owner_uid} gid={shared_gid} mode={SHARED_DIRECTORY_MODE:04o}"
        )
    raw_default = _xattr(path, _ACL_XATTR_DEFAULT)
    if raw_default is None:
        raise SharedStatePermissionError(
            f"distributed Worker shared directory lacks required default ACL: {path}"
        )
    if _decode_acl(raw_default, path, "default") != _EXPECTED_DEFAULT_ACL:
        raise SharedStatePermissionError(
            f"distributed Worker shared directory default ACL mismatch: {path}"
        )
    _validate_access_acl(path, mode)


def validate_shared_file(
    path: Path, *, shared_gid: int, allowed_owner_uids: set[int] | None = None
) -> None:
    """Authoritative host-namespace shared-file validator; never writes."""
    info = _plain_stat(path, directory=False)
    mode = stat.S_IMODE(info.st_mode)
    if info.st_gid != shared_gid or mode != SHARED_FILE_MODE:
        raise SharedStatePermissionError(
            f"distributed Worker shared file metadata mismatch: {path} has "
            f"uid={info.st_uid} gid={info.st_gid} mode={mode:04o}; expected "
            f"gid={shared_gid} mode={SHARED_FILE_MODE:04o}"
        )
    if allowed_owner_uids is not None and info.st_uid not in allowed_owner_uids:
        raise SharedStatePermissionError(
            f"distributed Worker shared file owner is not approved: {path} "
            f"uid={info.st_uid}"
        )
    _validate_access_acl(path, mode)


def _read_id_map(kind: str) -> tuple[IdMapRange, ...]:
    path = Path(f"/proc/self/{kind}_map")
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise SharedStatePermissionError(f"cannot read namespace {kind} map") from exc
    result = []
    for line in lines:
        fields = line.split()
        if len(fields) != 3 or not all(field.isdecimal() for field in fields):
            raise SharedStatePermissionError(f"malformed namespace {kind} map")
        result.append(IdMapRange(*(int(field) for field in fields)))
    if not result:
        raise SharedStatePermissionError(f"empty namespace {kind} map")
    return tuple(result)


def _overflow_id(kind: str) -> int:
    try:
        return int(Path(f"/proc/sys/kernel/overflow{kind}").read_text(encoding="ascii").strip())
    except (OSError, ValueError) as exc:
        raise SharedStatePermissionError(f"cannot read kernel overflow{kind}") from exc


def _visible_id(host_id: int, ranges: tuple[IdMapRange, ...]) -> int | None:
    for item in ranges:
        value = item.visible_id(host_id)
        if value is not None:
            return value
    return None


def _validate_namespace_id(
    *, actual: int, expected_host: int, kind: str, receipt_backed: bool
) -> str:
    ranges = _read_id_map(kind)
    expected_visible = _visible_id(expected_host, ranges)
    if expected_visible is not None:
        if actual != expected_visible:
            raise SharedStatePermissionError(
                f"namespace-visible {kind} mismatch: actual={actual} "
                f"expected={expected_visible} (host {expected_host})"
            )
        return "exact"
    overflow = _overflow_id(kind)
    if receipt_backed and actual == overflow:
        return "trusted_receipt_plus_unmapped_overflow"
    raise SharedStatePermissionError(
        f"unverifiable namespace {kind}: actual={actual} expected_host={expected_host}; "
        "overflow IDs are not trusted without an authoritative host receipt"
    )


def _assert_no_symlink_components(path: Path) -> None:
    if not path.is_absolute():
        raise SharedStatePermissionError(f"contract path must be absolute: {path}")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError as exc:
            raise SharedStatePermissionError(f"contract path is missing: {current}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise SharedStatePermissionError(f"contract path contains symlink: {current}")


def _validate_receipt_boundary(path: Path) -> os.stat_result:
    _assert_no_symlink_components(path)
    info = _plain_stat(path, directory=False)
    mode = stat.S_IMODE(info.st_mode)
    if mode != TRUSTED_RECEIPT_MODE:
        raise SharedStatePermissionError(
            f"trusted preparation receipt mode mismatch: {path} mode={mode:04o}"
        )
    _validate_namespace_id(
        actual=info.st_uid,
        expected_host=TRUSTED_RECEIPT_OWNER_UID,
        kind="uid",
        receipt_backed=True,
    )
    _validate_namespace_id(
        actual=info.st_gid,
        expected_host=TRUSTED_RECEIPT_OWNER_GID,
        kind="gid",
        receipt_backed=True,
    )
    parent_info = _plain_stat(path.parent, directory=True)
    _validate_namespace_id(
        actual=parent_info.st_uid,
        expected_host=TRUSTED_RECEIPT_OWNER_UID,
        kind="uid",
        receipt_backed=True,
    )
    _validate_namespace_id(
        actual=parent_info.st_gid,
        expected_host=TRUSTED_RECEIPT_OWNER_GID,
        kind="gid",
        receipt_backed=True,
    )
    parent_mode = stat.S_IMODE(parent_info.st_mode)
    if parent_mode != TRUSTED_RECEIPT_PARENT_MODE:
        raise SharedStatePermissionError(
            "trusted preparation receipt parent mode mismatch: "
            f"{path.parent} mode={parent_mode:04o}; "
            f"expected {TRUSTED_RECEIPT_PARENT_MODE:04o}"
        )
    if info.st_uid == os.geteuid() or parent_info.st_uid == os.geteuid():
        raise SharedStatePermissionError(
            "backend identity can forge trusted host-preparation receipt"
        )
    if os.access(path, os.W_OK, effective_ids=True):
        raise SharedStatePermissionError(
            "backend identity can write trusted host-preparation receipt"
        )
    return info


def _acl_digest(path: Path, name: str) -> str | None:
    raw = _xattr(path, name)
    return hashlib.sha256(raw).hexdigest() if raw is not None else None


def host_object_record(path: Path, *, data_dir: Path) -> dict[str, Any]:
    """Describe one already-validated object in the host namespace."""
    try:
        relative = path.relative_to(data_dir)
    except ValueError as exc:
        raise SharedStatePermissionError(f"receipt object escapes data dir: {path}") from exc
    directory = path.is_dir() and not path.is_symlink()
    info = _plain_stat(path, directory=directory)
    return {
        "path": "." if relative == Path(".") else relative.as_posix(),
        "kind": "directory" if directory else "file",
        "device": info.st_dev,
        "inode": info.st_ino,
        "uid": info.st_uid,
        "gid": info.st_gid,
        "mode": f"{stat.S_IMODE(info.st_mode):04o}",
        "access_acl_sha256": _acl_digest(path, _ACL_XATTR_ACCESS),
        "default_acl_sha256": (
            _acl_digest(path, _ACL_XATTR_DEFAULT) if directory else None
        ),
    }


def build_host_receipt(
    *, data_dir: Path, owner_uid: int, shared_gid: int, shared_group: str,
    prepared_at_utc: str, objects: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "contract": CONTRACT_NAME,
        "version": CONTRACT_VERSION,
        "data_dir": str(data_dir),
        "owner_uid": owner_uid,
        "shared_gid": shared_gid,
        "shared_group": shared_group,
        "prepared_at_utc": prepared_at_utc,
        "objects": objects,
    }


def canonical_receipt_bytes(receipt: dict[str, Any]) -> bytes:
    return (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _safe_relative(value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise SharedStatePermissionError("receipt object path must be a string")
    pure = PurePosixPath(value)
    if pure.is_absolute() or ".." in pure.parts:
        raise SharedStatePermissionError(f"unsafe receipt object path: {value!r}")
    return Path(".") if value == "." else Path(*pure.parts)


def _load_receipt(path: Path) -> dict[str, Any]:
    info = _validate_receipt_boundary(path)
    if info.st_size <= 0 or info.st_size > _MAX_RECEIPT_BYTES:
        raise SharedStatePermissionError("trusted preparation receipt size is invalid")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SharedStatePermissionError("trusted preparation receipt is invalid JSON") from exc
    if not isinstance(value, dict):
        raise SharedStatePermissionError("trusted preparation receipt must be an object")
    return value


def _record_by_path(receipt: dict[str, Any]) -> dict[Path, dict[str, Any]]:
    values = receipt.get("objects")
    if not isinstance(values, list):
        raise SharedStatePermissionError("trusted preparation receipt objects are invalid")
    result: dict[Path, dict[str, Any]] = {}
    for value in values:
        if not isinstance(value, dict):
            raise SharedStatePermissionError("trusted preparation receipt object is invalid")
        relative = _safe_relative(value.get("path"))
        if relative in result:
            raise SharedStatePermissionError(f"duplicate receipt object: {relative}")
        result[relative] = value
    return result


def _validate_runtime_object(
    *, path: Path, record: dict[str, Any], owner_uid: int, shared_gid: int
) -> str:
    directory = record.get("kind") == "directory"
    if not directory and record.get("kind") != "file":
        raise SharedStatePermissionError(f"invalid receipt object kind: {path}")
    info = _plain_stat(path, directory=directory)
    mode = stat.S_IMODE(info.st_mode)
    expected_mode = SHARED_DIRECTORY_MODE if directory else SHARED_FILE_MODE
    if mode != expected_mode or record.get("mode") != f"{expected_mode:04o}":
        raise SharedStatePermissionError(
            f"runtime-visible shared-state mode mismatch: {path} mode={mode:04o}"
        )
    if directory:
        raw_default = _xattr(path, _ACL_XATTR_DEFAULT)
        if raw_default is None:
            raise SharedStatePermissionError(
                f"distributed Worker shared directory lacks required default ACL: {path}"
            )
        if _decode_acl(raw_default, path, "default") != _EXPECTED_DEFAULT_ACL:
            raise SharedStatePermissionError(f"runtime-visible default ACL mismatch: {path}")
    _validate_access_acl(path, mode)
    expected_uid = owner_uid if directory else record.get("uid")
    if not isinstance(expected_uid, int) or record.get("uid") != expected_uid:
        raise SharedStatePermissionError(f"trusted receipt owner mismatch: {path}")
    uid_result = _validate_namespace_id(
        actual=info.st_uid, expected_host=expected_uid, kind="uid", receipt_backed=True
    )
    if record.get("gid") != shared_gid:
        raise SharedStatePermissionError(f"trusted receipt host GID mismatch: {path}")
    gid_result = _validate_namespace_id(
        actual=info.st_gid, expected_host=shared_gid, kind="gid", receipt_backed=True
    )
    for field, actual in (
        ("device", info.st_dev),
        ("inode", info.st_ino),
        ("access_acl_sha256", _acl_digest(path, _ACL_XATTR_ACCESS)),
        ("default_acl_sha256", _acl_digest(path, _ACL_XATTR_DEFAULT) if directory else None),
    ):
        if record.get(field) != actual:
            raise SharedStatePermissionError(
                f"trusted receipt object identity/security mismatch: {path} field={field}"
            )
    if directory:
        if not os.access(path, os.R_OK | os.W_OK | os.X_OK, effective_ids=True):
            raise SharedStatePermissionError(f"runtime lacks required directory access: {path}")
    elif not os.access(path, os.R_OK | os.W_OK, effective_ids=True):
        raise SharedStatePermissionError(f"runtime lacks required file access: {path}")
    return f"uid={uid_result},gid={gid_result}"


def validate_runtime_shared_state(
    *, data_dir: Path, owner_uid: int, shared_gid: int, receipt_path: Path
) -> dict[str, Any]:
    """Validate a host-prepared contract without chmod/chown or repair."""
    _assert_no_symlink_components(data_dir)
    receipt = _load_receipt(receipt_path)
    expected_header = {
        "contract": CONTRACT_NAME,
        "version": CONTRACT_VERSION,
        "data_dir": str(data_dir),
        "owner_uid": owner_uid,
        "shared_gid": shared_gid,
    }
    for field, expected in expected_header.items():
        if receipt.get(field) != expected:
            raise SharedStatePermissionError(f"trusted preparation receipt {field} mismatch")
    records = _record_by_path(receipt)
    required = {Path(".")} | {Path(name) for name in STATIC_DIRECTORY_NAMES if name}
    required.add(Path("workers.db"))
    missing = sorted(str(path) for path in required - records.keys())
    if missing:
        raise SharedStatePermissionError(
            f"trusted preparation receipt lacks required objects: {missing}"
        )
    results = {}
    for relative, record in records.items():
        path = data_dir if relative == Path(".") else data_dir / relative
        results[str(relative)] = _validate_runtime_object(
            path=path, record=record, owner_uid=owner_uid, shared_gid=shared_gid
        )
    root_info = data_dir.lstat()
    logger.info(
        "shared-state contract validated: version=%s expected_host_gid=%s "
        "runtime_visible_gid=%s overflow_gid_trusted_directly=false objects=%s",
        CONTRACT_VERSION,
        shared_gid,
        root_info.st_gid,
        len(results),
    )
    return {
        "contract": CONTRACT_NAME,
        "version": CONTRACT_VERSION,
        "receipt": str(receipt_path),
        "objects_validated": len(results),
        "namespace_results": results,
        "overflow_gid_trusted_directly": False,
        "runtime_mutations": [],
    }
