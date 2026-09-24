"""Explicit import of a FROZEN source run and its completed shadow consolidation next to a pair's shadow runs.

For a source run that is not a run of the production store (e.g. a frozen
research run of the same pair), the UI can only show «Исходные | Итоговые»
if both the source and the consolidation are available read-only and bound by
hashes.  This import:

* copies — never rewrites — the source result, its Miner region source, the
  semantic map, every source-package ``page.json`` and every graphic crop
  those pages reference, each verified by sha256 before and after the copy;
* refuses unless the source result belongs to this pair, the pair's CURRENT
  PDFs have exactly the source's PDF sha256, the source run id is not a run of
  the production store, and the shadow run is COMPLETED, frozen and bound to
  exactly this source result sha256;
* copies the shadow run byte-identically and re-verifies it through the same
  reader the UI uses;
* never touches ``runs/``, ``current_run.json``, Human Mapping or the catalog.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .source_view import load_frozen_bundle
from .storage import MANIFEST, ShadowStore
from .view import IMPORT_DIR, IMPORT_RECEIPT, IMPORT_SCHEMA, _pdf_binding_ok, load_source, shadow_root


class ImportRefused(RuntimeError):
    def __init__(self, code: str, message: str = ""):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def _sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _copy(src: Path, dst: Path, expected: str | None = None) -> dict[str, str]:
    before = _sha(src)
    if expected is not None and before != expected:
        raise ImportRefused("SOURCE_SHA_MISMATCH", str(src))
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        raise ImportRefused("IMPORT_OVERWRITE_REFUSED", str(dst))
    shutil.copyfile(src, dst)
    with open(dst, "rb") as fh:
        os.fsync(fh.fileno())
    after = _sha(dst)
    if after != before or _sha(src) != before:
        raise ImportRefused("IMPORT_COPY_MISMATCH", str(src))
    return {"from": str(src), "from_sha256": before, "sha256": after}


def _read_only(root: Path) -> None:
    for p in root.rglob("*"):
        if p.is_file():
            p.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def import_frozen_consolidation(session_id: str, pair_id: str, bundle_spec: dict[str, Any], shadow_run_dir: Path,
                                *, label: str, engine: dict[str, Any]) -> dict[str, Any]:
    from backend.app.services.project_change_v3 import run_storage

    bundle = load_frozen_bundle(bundle_spec)
    source_run_id = bundle.source_run_id
    if str(bundle.result.get("pair_id")) != pair_id:
        raise ImportRefused("SOURCE_PAIR_MISMATCH", f"{bundle.result.get('pair_id')} != {pair_id}")
    if (run_storage.run_dir(session_id, pair_id, source_run_id) / "run_manifest.json").exists():
        raise ImportRefused("SOURCE_IS_A_PRODUCTION_RUN", source_run_id)
    if not _pdf_binding_ok(session_id, pair_id, bundle.result):
        raise ImportRefused("PDF_BINDING_FAILED", "the pair's current PDFs differ from the source run's PDFs")
    shadow_run_dir = Path(shadow_run_dir)
    manifest = json.loads((shadow_run_dir / MANIFEST).read_bytes())
    if manifest.get("state") != "COMPLETED" or not manifest.get("frozen") or manifest.get(
            "source_run_id") != source_run_id or manifest.get("source_result_sha256") != bundle.result_sha256:
        raise ImportRefused("SHADOW_NOT_BOUND", "the shadow run is not a completed run of exactly this source")
    ShadowStore(shadow_run_dir.parent.parent).load_completed(
        source_run_id, manifest["consolidator_run_id"], source_result_sha256=bundle.result_sha256)

    production = run_storage.root(session_id, pair_id)
    protected = [production / "current_run.json", *sorted((production / "runs").rglob("*"))]
    before = {str(p): _sha(p) for p in protected if p.is_file()}

    root = shadow_root(session_id, pair_id)
    dest = root / source_run_id / IMPORT_DIR
    dest.mkdir(parents=True, exist_ok=False)
    files: list[dict[str, str]] = []
    kinds = {f["role"]: f for f in bundle.files}
    hint_role = "miner_results" if "miner_results" in kinds else "miner_checkpoint"
    for rel, role in (("SOURCE_RESULT.json", "result"), ("SOURCE_HINT_REGIONS.json", hint_role),
                      ("SOURCE_SEMANTIC_MAP.json", "semantic_map")):
        files.append({"rel": rel, "role": role, **_copy(Path(kinds[role]["path"]), dest / rel, kinds[role]["sha256"])})
    for rel, sha in sorted((bundle.page_sha256 or {}).items()):
        page_path = bundle.source_package_dir / rel
        files.append({"rel": f"source/{rel}", "role": "page", **_copy(page_path, dest / "source" / rel, sha)})
        record = json.loads(page_path.read_bytes())
        for block in record.get("blocks") or []:
            ref, crop_sha = block.get("graphic_crop_ref"), block.get("graphic_crop_sha256")
            if ref and crop_sha and Path(ref).is_file():
                target = Path(rel).parent / f"{block['block_id']}.png"
                files.append({"rel": f"source/{target}", "role": "graphic_crop",
                              **_copy(Path(ref), dest / "source" / target, crop_sha)})
    result = bundle.result
    receipt = {
        "schema": IMPORT_SCHEMA, "session_id": session_id, "pair_id": pair_id, "source_run_id": source_run_id,
        "label": label, "engine": engine, "hint_method": bundle.hint_method, "hint_source_kind": hint_role,
        "source_result_sha256": bundle.result_sha256,
        "source_files": bundle.files, "files": files, "file_count": len(files), "content_mutated": False,
        "pdf_binding": {"method": "source_pdf_sha256", "old_sha256": result["source_manifest"]["old_pdf_sha256"],
                        "new_sha256": result["source_manifest"]["new_pdf_sha256"],
                        "verified_against_current_pair_pdfs": True},
        "imported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (dest / IMPORT_RECEIPT).write_text(json.dumps(receipt, ensure_ascii=False, indent=1), encoding="utf-8")
    _read_only(dest)

    target = root / source_run_id / manifest["consolidator_run_id"]
    target.mkdir(parents=False, exist_ok=False)
    copied = []
    for p in sorted(shadow_run_dir.rglob("*")):
        if p.is_file():
            copied.append({"rel": str(p.relative_to(shadow_run_dir)),
                           **_copy(p, target / p.relative_to(shadow_run_dir))})
    _read_only(target)

    after = {str(p): _sha(p) for p in protected if p.is_file()}
    if before != after or set(before) != {str(p) for p in [production / "current_run.json",
                                                          *sorted((production / "runs").rglob("*"))] if p.is_file()}:
        raise ImportRefused("PRODUCTION_RUN_STORE_CHANGED", "runs/ or current_run.json changed during the import")
    source = load_source(session_id, pair_id, source_run_id)
    loaded = ShadowStore(root).load_completed(source_run_id, manifest["consolidator_run_id"],
                                              source_result_sha256=source.result_sha256)
    return {
        "schema": "projectchange-consolidator-ui-binding/1", "session_id": session_id, "pair_id": pair_id,
        "source_run_id": source_run_id, "consolidator_run_id": manifest["consolidator_run_id"],
        "source_result_sha256": bundle.result_sha256, "shadow_result_sha256": loaded["manifest"]["shadow_result_sha256"],
        "consolidator_result_sha256": _sha(target / "SHADOW_RESULT.json"),
        "source_import_dir": str(dest), "source_import_receipt_sha256": _sha(dest / IMPORT_RECEIPT),
        "shadow_run_dir": str(target), "shadow_files_copied": len(copied), "source_files_copied": len(files),
        "production_run_store_unchanged": True, "protected_files_checked": len(before),
        "reader_verification": "load_source + ShadowStore.load_completed PASS",
    }
