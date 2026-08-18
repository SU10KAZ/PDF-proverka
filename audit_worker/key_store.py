"""Worker-owned private-key storage for mTLS machine identity.

Linux baseline is explicitly OS-permission protected.  Windows uses DPAPI
machine scope and decrypts only to process memory for grpc channel creation.
Neither backend accepts a key through argv, environment or API.
"""
from __future__ import annotations

import ctypes
import os
import platform
import stat
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


class KeyStoreError(RuntimeError):
    pass


def _reject_symlink_path(path: Path) -> None:
    current = path.absolute()
    while True:
        if current.exists() and current.is_symlink():
            raise KeyStoreError(f"symlink is forbidden in key-store path: {current}")
        parent = current.parent
        if parent == current:
            break
        current = parent


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write(path: Path, data: bytes, mode: int) -> None:
    _reject_symlink_path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


class WorkerKeyStore(ABC):
    def __init__(self, root: Path) -> None:
        self.root = root

    @abstractmethod
    def store_private_key(self, private_key_pem: bytes) -> None: ...

    @abstractmethod
    def load_private_key(self) -> bytes: ...

    @abstractmethod
    def has_private_key(self) -> bool: ...

    def generate(self, *, replace: bool = False) -> bytes:
        if self.has_private_key() and not replace:
            return self.load_private_key()
        key = ec.generate_private_key(ec.SECP256R1())
        pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        self.store_private_key(pem)
        return pem


class LinuxPermissionKeyStore(WorkerKeyStore):
    """Plain PKCS#8 protected by a dedicated account and 0700/0600 modes."""

    @property
    def path(self) -> Path:
        return self.root / "client-key.pem"

    def _validate(self) -> None:
        _reject_symlink_path(self.root)
        _reject_symlink_path(self.path)
        if not self.root.is_dir():
            raise KeyStoreError("Linux key-store directory is missing")
        root_stat = self.root.stat()
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise KeyStoreError("Linux key-store directory must be mode 0700")
        if root_stat.st_uid != os.geteuid():
            raise KeyStoreError("Linux key-store directory owner mismatch")
        if self.path.exists():
            key_stat = self.path.stat()
            if not stat.S_ISREG(key_stat.st_mode):
                raise KeyStoreError("Linux private key is not a regular file")
            if stat.S_IMODE(key_stat.st_mode) != 0o600:
                raise KeyStoreError("Linux private key must be mode 0600")
            if key_stat.st_uid != os.geteuid():
                raise KeyStoreError("Linux private key owner mismatch")

    def store_private_key(self, private_key_pem: bytes) -> None:
        _reject_symlink_path(self.root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        _atomic_write(self.path, private_key_pem, 0o600)
        self._validate()

    def load_private_key(self) -> bytes:
        self._validate()
        data = self.path.read_bytes()
        if b"PRIVATE KEY" not in data:
            raise KeyStoreError("Linux key-store payload is not a private key")
        return data

    def has_private_key(self) -> bool:
        return self.path.exists()


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_ulong), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def _blob(data: bytes):
    buffer = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
    return _DataBlob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))), buffer


class WindowsDpapiKeyStore(WorkerKeyStore):
    """DPAPI machine-scope blob; plaintext exists only in Worker memory."""

    CRYPTPROTECT_LOCAL_MACHINE = 0x4
    CRYPTPROTECT_UI_FORBIDDEN = 0x1
    ENTROPY = b"auditmanager-worker-mtls-v1"

    @property
    def path(self) -> Path:
        return self.root / "client-key.dpapi"

    @staticmethod
    def _require_windows() -> None:
        if platform.system() != "Windows":
            raise KeyStoreError("Windows DPAPI backend is available only on Windows")

    def _protect(self, data: bytes) -> bytes:
        self._require_windows()
        in_blob, keep_in = _blob(data)
        entropy, keep_entropy = _blob(self.ENTROPY)
        out_blob = _DataBlob()
        crypt32 = ctypes.windll.crypt32
        kernel32 = ctypes.windll.kernel32
        ok = crypt32.CryptProtectData(
            ctypes.byref(in_blob), None, ctypes.byref(entropy), None, None,
            self.CRYPTPROTECT_LOCAL_MACHINE | self.CRYPTPROTECT_UI_FORBIDDEN,
            ctypes.byref(out_blob),
        )
        del keep_in, keep_entropy
        if not ok:
            raise ctypes.WinError()
        try:
            return ctypes.string_at(out_blob.pbData, out_blob.cbData)
        finally:
            kernel32.LocalFree(out_blob.pbData)

    def _unprotect(self, data: bytes) -> bytes:
        self._require_windows()
        in_blob, keep_in = _blob(data)
        entropy, keep_entropy = _blob(self.ENTROPY)
        out_blob = _DataBlob()
        crypt32 = ctypes.windll.crypt32
        kernel32 = ctypes.windll.kernel32
        ok = crypt32.CryptUnprotectData(
            ctypes.byref(in_blob), None, ctypes.byref(entropy), None, None,
            self.CRYPTPROTECT_UI_FORBIDDEN, ctypes.byref(out_blob),
        )
        del keep_in, keep_entropy
        if not ok:
            raise ctypes.WinError()
        try:
            return ctypes.string_at(out_blob.pbData, out_blob.cbData)
        finally:
            kernel32.LocalFree(out_blob.pbData)

    def store_private_key(self, private_key_pem: bytes) -> None:
        self._require_windows()
        encrypted = self._protect(private_key_pem)
        _atomic_write(self.path, encrypted, 0o600)

    def load_private_key(self) -> bytes:
        self._require_windows()
        return self._unprotect(self.path.read_bytes())

    def has_private_key(self) -> bool:
        return self.path.exists()


def platform_key_store(root: Path, backend: str = "auto") -> WorkerKeyStore:
    selected = (backend or "auto").lower()
    if selected == "auto":
        selected = "windows_dpapi" if platform.system() == "Windows" else "linux_permissions"
    if selected == "windows_dpapi":
        return WindowsDpapiKeyStore(root)
    if selected == "linux_permissions":
        if platform.system() == "Windows":
            raise KeyStoreError("plaintext PEM backend is forbidden on Windows")
        return LinuxPermissionKeyStore(root)
    raise KeyStoreError(f"unknown key-store backend: {backend}")
