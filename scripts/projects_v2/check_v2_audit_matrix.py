#!/usr/bin/env python3
"""Run a safe v2-primary audit source matrix on shadow copies.

The script reads projects_v2/_system/old_to_new_map.json, chooses diverse real
v2 documents, builds minimal shadow version directories under /tmp, runs the
real prepare stage against those shadows, and mocks crop/Gemma artifacts using
the production Gemma contract validators. It never writes to real projects_v2.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DISCIPLINE_ORDER = [
    "GP",
    "AR",
    "KJ",
    "KM",
    "EOM",
    "OV",
    "VK",
    "SS",
    "TX",
    "ITP",
    "AI",
    "POS",
]


def _safe_name(value: str) -> str:
    value = re.sub(r"[^0-9A-Za-z._-]+", "_", value.strip())
    return value.strip("._-") or "document"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _link_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    try:
        os.symlink(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _copy_metadata(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.exists():
        shutil.copy2(src, dst)


def _version_dir(entry: dict[str, Any]) -> Path:
    return Path(entry["v2_document_dir"]) / "versions" / str(entry.get("version_id") or "v001")


def _has_v2_sources(entry: dict[str, Any]) -> tuple[bool, dict[str, str]]:
    from backend.app.services.storage.projects_v2_source_resolver import resolve_version_source_files

    version_dir = _version_dir(entry)
    sources = resolve_version_source_files(version_dir, entry.get("document_code"))
    paths = {
        "pdf": str(sources.pdf_path or ""),
        "md": str(sources.md_path or ""),
        "result_json": str(sources.result_json_path or ""),
        "project_info": str(sources.project_info_path or ""),
    }
    ok = (
        version_dir.is_dir()
        and sources.layout == "projects_v2"
        and sources.pdf_path is not None
        and sources.md_path is not None
        and sources.result_json_path is not None
        and sources.project_info_path is not None
    )
    return ok, paths


def _candidate_sort_key(entry: dict[str, Any]) -> tuple[int, int, str]:
    discipline = str(entry.get("discipline") or "")
    try:
        discipline_rank = DISCIPLINE_ORDER.index(discipline)
    except ValueError:
        discipline_rank = len(DISCIPLINE_ORDER)
    kind_rank = 0 if entry.get("kind") == "container" else 1
    return (discipline_rank, kind_rank, str(entry.get("document_code") or ""))


def choose_entries(migrations: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    usable: list[dict[str, Any]] = []
    seen_doc_versions: set[tuple[str, str, str]] = set()
    for entry in sorted(migrations, key=_candidate_sort_key):
        key = (
            str(entry.get("v2_document_dir") or ""),
            str(entry.get("document_code") or ""),
            str(entry.get("version_id") or ""),
        )
        if key in seen_doc_versions:
            continue
        seen_doc_versions.add(key)
        ok, _ = _has_v2_sources(entry)
        if ok:
            usable.append(entry)

    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, str]] = set()
    selected_disciplines: set[str] = set()

    for entry in usable:
        discipline = str(entry.get("discipline") or "")
        if discipline in selected_disciplines:
            continue
        selected.append(entry)
        selected_keys.add((
            str(entry.get("v2_document_dir") or ""),
            str(entry.get("document_code") or ""),
            str(entry.get("version_id") or ""),
        ))
        selected_disciplines.add(discipline)
        if len(selected) >= limit:
            return selected

    has_container = any(e.get("kind") == "container" for e in selected)
    if not has_container:
        for entry in usable:
            if entry.get("kind") == "container":
                selected.append(entry)
                break

    for entry in usable:
        key = (
            str(entry.get("v2_document_dir") or ""),
            str(entry.get("document_code") or ""),
            str(entry.get("version_id") or ""),
        )
        if key in selected_keys:
            continue
        selected.append(entry)
        selected_keys.add(key)
        if len(selected) >= limit:
            return selected

    return selected[:limit]


def build_shadow_version(entry: dict[str, Any], root: Path) -> Path:
    from backend.app.services.storage.projects_v2_source_resolver import resolve_version_source_files

    src_version = _version_dir(entry)
    doc_code = str(entry.get("document_code") or src_version.parent.parent.name)
    shadow = root / _safe_name(str(entry.get("discipline") or "XX")) / _safe_name(doc_code) / "versions" / str(entry.get("version_id") or "v001")
    if shadow.exists():
        shutil.rmtree(shadow)
    for dirname in ("01_input", "02_work", "03_analysis", "04_review", "05_export", "99_service"):
        (shadow / dirname).mkdir(parents=True, exist_ok=True)

    for rel in ("version.json", "01_input/project_info.json"):
        _copy_metadata(src_version / rel, shadow / rel)

    document_json = src_version.parent.parent / "document.json"
    if document_json.exists():
        _copy_metadata(document_json, shadow.parent.parent / "document.json")

    sources = resolve_version_source_files(src_version, doc_code)
    if sources.pdf_path:
        _link_or_copy(sources.pdf_path, shadow / "02_work" / "document.pdf")
    if sources.md_path:
        _link_or_copy(sources.md_path, shadow / "02_work" / "document.md")
    if sources.result_json_path:
        _link_or_copy(sources.result_json_path, shadow / "02_work" / "result.json")
    if sources.ocr_html_path:
        _link_or_copy(sources.ocr_html_path, shadow / "02_work" / "ocr.html")
    if sources.project_info_path:
        _copy_metadata(sources.project_info_path, shadow / "01_input" / "project_info.json")
    return shadow


@contextmanager
def patched_environ(values: dict[str, str]):
    old: dict[str, str | None] = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _extract_image_blocks(document_graph: Path, limit: int = 3) -> list[dict[str, Any]]:
    try:
        graph = _load_json(document_graph)
    except Exception:
        return []
    result: list[dict[str, Any]] = []
    pages = graph.get("pages") if isinstance(graph, dict) else None
    if not isinstance(pages, list):
        return result
    for page in pages:
        if not isinstance(page, dict):
            continue
        page_no = page.get("page") or page.get("page_number")
        for block in page.get("image_blocks") or []:
            if not isinstance(block, dict):
                continue
            block_id = str(block.get("id") or block.get("block_id") or "")
            if not block_id:
                continue
            result.append({
                "block_id": block_id,
                "block_type": "image",
                "page": page_no,
                "file": f"block_{_safe_name(block_id)}.png",
            })
            if len(result) >= limit:
                return result
    return result


def write_mock_crop_and_gemma(version_dir: Path, output_dir: Path, image_blocks: list[dict[str, Any]]) -> dict[str, Any]:
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
        GEMMA_BASE_CROP_POLICY,
        build_gemma_summary,
    )
    from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import (
        evaluate_gemma_enrichment,
        find_project_markdown,
    )
    from backend.app.services.storage.projects_v2_source_resolver import load_version_project_info

    index = {
        **GEMMA_BASE_CROP_POLICY,
        "blocks": image_blocks,
    }
    _write_json(output_dir / "blocks_gemma_100" / "index.json", index)
    md_path = find_project_markdown(version_dir, load_version_project_info(version_dir))
    if md_path is None:
        raise RuntimeError("mock gemma cannot find markdown")
    summary = build_gemma_summary(
        status="ok" if image_blocks else "no_blocks",
        project_dir=version_dir,
        md_path=md_path,
        model="mock-gemma-matrix",
        blocks_total=len(image_blocks),
        blocks_ok=len(image_blocks),
        blocks_failed=0,
    )
    _write_json(output_dir / "gemma_enrichment_summary.json", summary)
    return evaluate_gemma_enrichment(version_dir, load_version_project_info(version_dir))


def run_entry(entry: dict[str, Any], shadow_root: Path) -> dict[str, Any]:
    from backend.app.pipeline.stages.prepare import process_project
    from backend.app.services.storage.projects_v2_source_resolver import (
        load_version_project_info,
        resolve_version_source_files,
    )

    doc_code = str(entry.get("document_code") or "")
    src_version = _version_dir(entry)
    source_ok, source_paths = _has_v2_sources(entry)
    version_dir = build_shadow_version(entry, shadow_root)
    output_dir = version_dir / "03_analysis" / "runs" / "matrix_mock"
    env = {
        "AUDIT_STORAGE_BACKEND": "projects_v2",
        "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
        "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED": "true",
        "AUDIT_PROJECT_ID": doc_code,
        "AUDIT_VERSION_ID": str(entry.get("version_id") or "v001"),
        "AUDIT_VERSION_DIR": str(version_dir),
        "AUDIT_OUTPUT_DIR": str(output_dir),
        "AUDIT_ROOT_DIR": str(REPO_ROOT),
    }
    with patched_environ(env):
        info = load_version_project_info(version_dir)
        shadow_sources = resolve_version_source_files(version_dir, doc_code, project_info=info)
        prepare_ok = bool(process_project.process(str(version_dir), force=True))
        document_graph = output_dir / "document_graph.json"
        image_blocks = _extract_image_blocks(document_graph)
        gemma_state = write_mock_crop_and_gemma(version_dir, output_dir, image_blocks)

    return {
        "document_code": doc_code,
        "object_name": entry.get("object_name"),
        "discipline": entry.get("discipline"),
        "kind": entry.get("kind"),
        "version_id": entry.get("version_id"),
        "source_version_dir": str(src_version),
        "shadow_version_dir": str(version_dir),
        "output_dir": str(output_dir),
        "source_ok": source_ok,
        "source_paths": source_paths,
        "shadow_layout": shadow_sources.layout,
        "shadow_pdf": str(shadow_sources.pdf_path or ""),
        "shadow_md": str(shadow_sources.md_path or ""),
        "shadow_result_json": str(shadow_sources.result_json_path or ""),
        "prepare_ok": prepare_ok,
        "document_graph": document_graph.exists(),
        "mock_crop_blocks": len(image_blocks),
        "mock_gemma_ready": bool(gemma_state.get("ready")),
        "mock_gemma_status": gemma_state.get("status"),
        "mock_gemma_detail": gemma_state.get("detail"),
        "legacy_output_created": (version_dir / "_output").exists(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=REPO_ROOT / "projects_v2" / "_system" / "old_to_new_map.json")
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--shadow-root", type=Path, default=None)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    shadow_root = args.shadow_root or Path("/tmp") / f"v2_audit_matrix_{stamp}"
    shadow_root.mkdir(parents=True, exist_ok=True)

    ledger = _load_json(args.ledger)
    migrations = ledger.get("migrations") if isinstance(ledger, dict) else None
    if not isinstance(migrations, list):
        raise SystemExit(f"bad ledger shape: {args.ledger}")

    selected = choose_entries(migrations, args.limit)
    results: list[dict[str, Any]] = []
    for entry in selected:
        try:
            result = run_entry(entry, shadow_root)
            result["ok"] = (
                result["source_ok"]
                and result["prepare_ok"]
                and result["document_graph"]
                and result["mock_gemma_ready"]
                and not result["legacy_output_created"]
            )
        except Exception as exc:
            result = {
                "document_code": entry.get("document_code"),
                "discipline": entry.get("discipline"),
                "kind": entry.get("kind"),
                "version_id": entry.get("version_id"),
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        results.append(result)

    report = {
        "shadow_root": str(shadow_root),
        "selected_count": len(selected),
        "ok_count": sum(1 for item in results if item.get("ok")),
        "failed_count": sum(1 for item in results if not item.get("ok")),
        "results": results,
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.json_out:
        _write_json(args.json_out, report)
    print(text)
    return 0 if report["failed_count"] == 0 and report["selected_count"] >= min(args.limit, 1) else 1


if __name__ == "__main__":
    raise SystemExit(main())
