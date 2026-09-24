"""Append-only, versioned shadow store of the Consolidator.

Layout (never under ``runs/``, no ``run_manifest.json``/``current_run.json``,
no ``project_change_v3_*`` names, so no production reader can mistake it for
a run)::

    <root>/<source_run_id>/.shadow.lock
    <root>/<source_run_id>/<consolidator_run_id>/SHADOW_RUN_MANIFEST.json  (written last)
                                               /…artifacts…

Every write is atomic (tmp → fsync → rename) and read back; a path is written
once; after ``finalize`` the run refuses every write and its files are made
read-only.  A completed run is read back only through ``load_completed``,
which re-verifies every artifact hash.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from pathlib import Path
from typing import Any

from .contracts import SHADOW_RUN_SCHEMA

MANIFEST = "SHADOW_RUN_MANIFEST.json"
RESULT = "SHADOW_RESULT.json"
LOCK = ".shadow.lock"
SHADOW_DIR_NAME = "projectchange_consolidator_shadow"
_SAFE_REL = re.compile(r"^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.:-]+)*$")
_RUN_ID = re.compile(r"^[0-9a-f]{32}$")
_SOURCE_ID = re.compile(r"^[A-Za-z0-9_.:-]+$")


class ShadowStoreError(RuntimeError):
    def __init__(self, code: str, message: str = ""):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def _dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=1, sort_keys=False).encode("utf-8")


def _atomic_write(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "xb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    try:
        os.link(tmp, path)          # fails if the target exists: a path is written once
    except FileExistsError as exc:
        tmp.unlink()
        raise ShadowStoreError("SHADOW_OVERWRITE_REFUSED", str(path)) from exc
    tmp.unlink()
    digest = hashlib.sha256(data).hexdigest()
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise ShadowStoreError("SHADOW_READBACK_MISMATCH", str(path))
    return digest


class ShadowRun:
    def __init__(self, store: "ShadowStore", source_run_id: str, run_id: str):
        self.store = store
        self.source_run_id = source_run_id
        self.consolidator_run_id = run_id
        self.dir = store.root / source_run_id / run_id
        self.artifacts: dict[str, str] = {}
        self.frozen = False

    def write(self, rel: str, value: Any) -> str:
        if self.frozen:
            raise ShadowStoreError("SHADOW_RUN_FROZEN", rel)
        if not _SAFE_REL.fullmatch(rel) or ".." in rel.split("/") or rel == MANIFEST:
            raise ShadowStoreError("SHADOW_BAD_PATH", rel)
        digest = _atomic_write(self.dir / rel, _dumps(value))
        self.artifacts[rel] = digest
        return digest

    def finalize(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """Write the manifest last (with every artifact hash), then freeze the run on disk."""
        if self.frozen:
            raise ShadowStoreError("SHADOW_RUN_FROZEN", MANIFEST)
        body = {**manifest, "schema": SHADOW_RUN_SCHEMA, "consolidator_run_id": self.consolidator_run_id,
                "source_run_id": self.source_run_id, "frozen": True,
                "artifacts": {rel: {"ref": rel, "sha256": sha} for rel, sha in sorted(self.artifacts.items())}}
        _atomic_write(self.dir / MANIFEST, _dumps(body))
        self.frozen = True
        for path in self.dir.rglob("*"):
            if path.is_file():
                path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        self.store.release(self.source_run_id)
        return body


class ShadowStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        if self.root.name == "runs" or "runs" in self.root.parts[-2:]:
            raise ShadowStoreError("SHADOW_ROOT_IN_RUNS", str(self.root))

    def create_run(self, source_run_id: str) -> ShadowRun:
        if not _SOURCE_ID.fullmatch(source_run_id or ""):
            raise ShadowStoreError("SHADOW_BAD_SOURCE_RUN_ID", repr(source_run_id))
        base = self.root / source_run_id
        base.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(base / LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError as exc:
            raise ShadowStoreError("SHADOW_LOCKED", f"another shadow run holds {base / LOCK}") from exc
        run_id = uuid.uuid4().hex
        os.write(fd, run_id.encode())
        os.close(fd)
        try:
            (base / run_id).mkdir(exist_ok=False)
        except Exception:
            self.release(source_run_id)
            raise
        return ShadowRun(self, source_run_id, run_id)

    def release(self, source_run_id: str) -> None:
        try:
            (self.root / source_run_id / LOCK).unlink()
        except FileNotFoundError:
            pass

    # ------------------------------------------------------------------ read side
    def manifests(self, source_run_id: str) -> list[dict[str, Any]]:
        base = self.root / source_run_id
        out = []
        if not base.is_dir():
            return out
        for d in sorted(base.iterdir()):
            if d.is_dir() and _RUN_ID.fullmatch(d.name) and (d / MANIFEST).is_file():
                try:
                    out.append(json.loads((d / MANIFEST).read_text(encoding="utf-8")))
                except (OSError, ValueError):
                    continue
        return out

    def load_completed(self, source_run_id: str, run_id: str, *, source_result_sha256: str) -> dict[str, Any]:
        """A COMPLETED, frozen run bound to exactly this source result; every artifact hash re-verified."""
        if not _RUN_ID.fullmatch(run_id or "") or not _SOURCE_ID.fullmatch(source_run_id or ""):
            raise ShadowStoreError("SHADOW_BAD_ID", f"{source_run_id}/{run_id}")
        d = self.root / source_run_id / run_id
        try:
            manifest = json.loads((d / MANIFEST).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ShadowStoreError("SHADOW_MANIFEST_MISSING", str(d)) from exc
        if manifest.get("schema") != SHADOW_RUN_SCHEMA or manifest.get("state") != "COMPLETED" or not manifest.get(
                "frozen"):
            raise ShadowStoreError("SHADOW_NOT_COMPLETED", f"{run_id}: {manifest.get('state')}")
        if manifest.get("source_run_id") != source_run_id or manifest.get("consolidator_run_id") != run_id:
            raise ShadowStoreError("SHADOW_IDENTITY_MISMATCH", run_id)
        if manifest.get("source_result_sha256") != source_result_sha256:
            raise ShadowStoreError("SHADOW_SOURCE_MISMATCH", f"{run_id} is bound to another source result")
        for rel, art in (manifest.get("artifacts") or {}).items():
            path = d / rel
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != art.get("sha256"):
                raise ShadowStoreError("SHADOW_ARTIFACT_MISMATCH", rel)
        if RESULT not in (manifest.get("artifacts") or {}):
            raise ShadowStoreError("SHADOW_RESULT_MISSING", run_id)
        result = json.loads((d / RESULT).read_text(encoding="utf-8"))
        if hashlib.sha256((d / RESULT).read_bytes()).hexdigest() != manifest.get("shadow_result_sha256"):
            raise ShadowStoreError("SHADOW_ARTIFACT_MISMATCH", RESULT)
        return {"manifest": manifest, "result": result}
