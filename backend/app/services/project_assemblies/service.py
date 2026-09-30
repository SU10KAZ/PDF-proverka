"""Build immutable, provenance-preserving documents from several projects.

The service is deliberately local and deterministic.  It never calls an OCR
or model provider.  A source is admitted only when the four files consumed by
Stage Comparison are already present in the new portal export format.
"""
from __future__ import annotations

import base64
import hashlib
import html
import json
import os
import re
import shutil
import stat
import tempfile
import uuid
import subprocess
import sys
import signal
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save
from backend.app.services.common.results_md import (
    BLOCK_HEADER_RE,
    PAGE_HEADER_RE,
    is_results_md_text,
    parse_results_md,
)
from backend.app.services.stage_comparison import sheet_matching, stage_upload
from backend.app.services.storage.projects_v2_source_resolver import resolve_version_source_files

SCHEMA = "project_assembly/1"
ORIGIN_SCHEMA = "project_assembly_origin/1"
SOURCE_SCHEMA = "project_assembly_source/1"
SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
MAX_SOURCES = 32
MAX_PAGES = 2000
MAX_PDF_BYTES = 2 * 1024**3
MIN_FREE_RESERVE = 5 * 1024**3


class AssemblyError(ValueError):
    def __init__(self, code: str, message: str, *, details: Any = None):
        super().__init__(message)
        self.code = code
        self.details = details


def enabled() -> bool:
    return os.environ.get("PROJECT_ASSEMBLIES_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _ref(payload: dict[str, str]) -> str:
    raw = _canonical({"schema": SOURCE_SCHEMA, **payload})
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_ref(value: str) -> dict[str, str]:
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        data = json.loads(raw)
    except Exception as exc:
        raise AssemblyError("BAD_SOURCE_REF", "Некорректная ссылка на источник") from exc
    if not isinstance(data, dict) or data.pop("schema", None) != SOURCE_SCHEMA:
        raise AssemblyError("BAD_SOURCE_REF", "Неизвестный формат ссылки на источник")
    allowed = {"kind", "object_id", "stage", "discipline", "document_code", "version_id"}
    if set(data) - allowed or not all(isinstance(v, str) for v in data.values()):
        raise AssemblyError("BAD_SOURCE_REF", "Некорректные поля ссылки на источник")
    return data


def _comparison_dir(object_id: str, *, create: bool = False) -> Path:
    _obj, comparison = stage_upload.resolve_object_dir(object_id, create=create)
    return comparison


def _root(object_id: str, *, create: bool = False) -> Path:
    root = _comparison_dir(object_id, create=create) / "assemblies"
    if create:
        root.mkdir(parents=True, exist_ok=True)
    return root


def _object_meta(object_id: str) -> tuple[Path, dict[str, Any]]:
    comparison = _comparison_dir(object_id, create=False)
    object_dir = comparison.parent
    try:
        meta = json.loads((object_dir / "object.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        meta = {}
    return object_dir, meta


def _doc_rows(root: Path, *, kind: str, object_id: str, stage: str = "", discipline: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not root.is_dir():
        return rows
    for meta_path in sorted(root.glob("*/document.json")):
        doc_dir = meta_path.parent
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        code = str(meta.get("document_code") or doc_dir.name)
        versions_dir = doc_dir / "versions"
        for version_dir in sorted((p for p in versions_dir.iterdir() if p.is_dir()), key=lambda p: p.name) if versions_dir.is_dir() else []:
            payload = {
                "kind": kind,
                "object_id": object_id,
                "stage": stage,
                "discipline": discipline or str(meta.get("discipline") or ""),
                "document_code": code,
                "version_id": version_dir.name,
            }
            files = resolve_version_source_files(version_dir, code)
            rows.append({
                "source_ref": _ref(payload),
                **payload,
                "label": f"{code} · {version_dir.name}",
                "current": version_dir.name == str(meta.get("current_version") or ""),
                "readiness": {
                    "pdf": bool(files.pdf_path),
                    "markdown": bool(files.md_path),
                    "blocks": bool(files.blocks_json_path),
                    "html": bool(files.ocr_html_path),
                },
            })
    return rows


def list_sources(object_id: str) -> dict[str, Any]:
    object_dir, _meta = _object_meta(object_id)
    rows: list[dict[str, Any]] = []
    for stage in ("stage_1", "stage_2"):
        rows.extend(_doc_rows(
            object_dir / "comparison" / stage / "documents",
            kind="comparison", object_id=object_id, stage=stage,
        ))
    disciplines = object_dir / "disciplines"
    if disciplines.is_dir():
        for discipline_dir in sorted(p for p in disciplines.iterdir() if p.is_dir()):
            rows.extend(_doc_rows(
                discipline_dir / "documents", kind="project", object_id=object_id,
                discipline=discipline_dir.name,
            ))
    rows.sort(key=lambda r: (r["kind"], r["discipline"], r["document_code"].casefold(), r["version_id"]))
    return {"schema": "project_assembly_sources/1", "object_id": object_id, "items": rows}


def _resolve_source(source_ref: str, expected_object_id: str) -> dict[str, Any]:
    ref = _decode_ref(source_ref)
    if ref.get("object_id") != expected_object_id:
        raise AssemblyError("SOURCE_OBJECT_MISMATCH", "Источник относится к другому объекту")
    object_dir, _meta = _object_meta(expected_object_id)
    if ref.get("kind") == "comparison":
        stage = ref.get("stage")
        if stage not in {"stage_1", "stage_2"}:
            raise AssemblyError("BAD_SOURCE_REF", "Некорректная стадия источника")
        doc_dir = object_dir / "comparison" / stage / "documents" / ref["document_code"]
    elif ref.get("kind") == "project":
        doc_dir = object_dir / "disciplines" / ref["discipline"] / "documents" / ref["document_code"]
    else:
        raise AssemblyError("BAD_SOURCE_REF", "Неизвестный тип источника")
    version_dir = doc_dir / "versions" / ref["version_id"]
    try:
        version_dir.resolve().relative_to(object_dir.resolve())
    except ValueError as exc:
        raise AssemblyError("BAD_SOURCE_REF", "Источник выходит за каталог объекта") from exc
    if not version_dir.is_dir():
        raise AssemblyError("SOURCE_NOT_FOUND", f"Версия источника не найдена: {ref['document_code']} {ref['version_id']}")
    files = resolve_version_source_files(version_dir, ref["document_code"])
    return {**ref, "version_dir": version_dir, "files": files}


def _preflight_one(source: dict[str, Any]) -> dict[str, Any]:
    import fitz

    files = source["files"]
    paths = {
        "pdf": files.pdf_path,
        "markdown": files.md_path,
        "blocks": files.blocks_json_path,
        "html": files.ocr_html_path,
    }
    missing = [name for name, path in paths.items() if path is None or not Path(path).is_file()]
    if missing:
        raise AssemblyError(
            "INCOMPATIBLE_OCR_EXPORT",
            f"{source['document_code']} {source['version_id']}: нужна новая выгрузка OCR ({', '.join(missing)})",
            details={"missing": missing},
        )
    paths = {key: Path(value) for key, value in paths.items()}
    md_text = paths["markdown"].read_text(encoding="utf-8")
    if not is_results_md_text(md_text):
        raise AssemblyError("LEGACY_OCR_FORMAT", f"{source['document_code']}: старый формат распознавания")
    md = parse_results_md(md_text)
    try:
        blocks = json.loads(paths["blocks"].read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AssemblyError("BAD_BLOCKS_JSON", f"{source['document_code']}: повреждён blocks.json") from exc
    if not isinstance(blocks, dict) or not isinstance(blocks.get("pages"), list) or not isinstance(blocks.get("blocks"), list):
        raise AssemblyError("BAD_BLOCKS_JSON", f"{source['document_code']}: нет pages[] или blocks[]")
    if md.document_name and blocks.get("document_name"):
        def normalized_document_name(value: Any) -> str:
            name = Path(str(value)).name.strip().casefold()
            while name.endswith(".pdf"):
                name = name[:-4]
            return re.sub(r"\s*\(\d+\)$", "", name).strip()

        if normalized_document_name(md.document_name) != normalized_document_name(blocks["document_name"]):
            raise AssemblyError(
                "OCR_PACKAGE_MISMATCH",
                f"{source['document_code']}: MD и blocks.json относятся к разным документам",
            )
    with fitz.open(paths["pdf"]) as pdf:
        page_count = pdf.page_count
    if md.page_numbers != list(range(1, page_count + 1)):
        raise AssemblyError(
            "PAGE_COVERAGE_MISMATCH",
            f"{source['document_code']}: страницы MD не покрывают PDF",
        )
    page_indexes = [row.get("page_index") for row in blocks["pages"] if isinstance(row, dict)]
    if len(page_indexes) != page_count or set(page_indexes) != set(range(page_count)):
        raise AssemblyError("PAGE_COVERAGE_MISMATCH", f"{source['document_code']}: pages[] не покрывает PDF")
    block_ids = [str(row.get("block_id") or "") for row in blocks["blocks"] if isinstance(row, dict)]
    if any(not block_id for block_id in block_ids) or len(block_ids) != len(set(block_ids)):
        raise AssemblyError("DUPLICATE_BLOCK_ID", f"{source['document_code']}: пустые или повторяющиеся block_id")
    md_ids = [block.block_id for block in md.blocks]
    json_content_ids = [
        str(row.get("block_id")) for row in blocks["blocks"]
        if isinstance(row, dict) and row.get("block_type") != "stamp"
    ]
    if set(md_ids) != set(json_content_ids):
        raise AssemblyError("OCR_PACKAGE_MISMATCH", f"{source['document_code']}: MD и blocks.json содержат разные блоки")
    for row in blocks["blocks"]:
        try:
            page_index = int(row["page_index"])
        except (KeyError, TypeError, ValueError) as exc:
            raise AssemblyError("BAD_BLOCK_PAGE", f"{source['document_code']}: блок без корректной страницы") from exc
        if page_index not in range(page_count):
            raise AssemblyError("BAD_BLOCK_PAGE", f"{source['document_code']}: блок за пределами PDF")
    html_text = paths["html"].read_text(encoding="utf-8")
    sheet_index = sheet_matching.extract_sheet_index_from_results_html(html_text)
    return {
        **{key + "_path": str(path) for key, path in paths.items()},
        "page_count": page_count,
        "block_count": len(blocks["blocks"]),
        "graphic_count": sum(1 for row in blocks["blocks"] if row.get("block_type") == "image"),
        "markdown_chars": len(md_text),
        "pdf_bytes": paths["pdf"].stat().st_size,
        "sha256": {key: _sha(path) for key, path in paths.items()},
        "sheet_index": sheet_index,
    }


def preview(object_id: str, source_refs: list[str]) -> dict[str, Any]:
    if not 1 <= len(source_refs) <= MAX_SOURCES:
        raise AssemblyError("SOURCE_COUNT", f"Нужно выбрать от 1 до {MAX_SOURCES} источников")
    if len(source_refs) != len(set(source_refs)):
        raise AssemblyError("DUPLICATE_SOURCE", "Одна версия источника выбрана несколько раз")
    rows = []
    page_offset = 0
    for source_ref in source_refs:
        source = _resolve_source(source_ref, object_id)
        check = _preflight_one(source)
        rows.append({
            "source_ref": source_ref,
            "kind": source["kind"],
            "discipline": source.get("discipline") or "",
            "document_code": source["document_code"],
            "version_id": source["version_id"],
            "page_start": page_offset + 1,
            "page_end": page_offset + check["page_count"],
            **{key: check[key] for key in ("page_count", "block_count", "graphic_count", "markdown_chars", "pdf_bytes", "sha256")},
        })
        page_offset += check["page_count"]
    totals = {
        key: sum(row[key] for row in rows)
        for key in ("page_count", "block_count", "graphic_count", "markdown_chars", "pdf_bytes")
    }
    if totals["page_count"] > MAX_PAGES:
        raise AssemblyError("PAGE_LIMIT", f"В сборке больше {MAX_PAGES} страниц")
    if totals["pdf_bytes"] > MAX_PDF_BYTES:
        raise AssemblyError("PDF_SIZE_LIMIT", "Суммарный размер PDF превышает 2 GiB")
    manifest_input = {"object_id": object_id, "sources": [{"source_ref": r["source_ref"], "sha256": r["sha256"]} for r in rows]}
    return {
        "schema": "project_assembly_preview/1",
        "object_id": object_id,
        "sources": rows,
        "totals": totals,
        "preview_token": hashlib.sha256(_canonical(manifest_input)).hexdigest(),
        "warnings": [],
    }


def _blob(root: Path, source: Path, expected_sha: str) -> Path:
    destination = root / "_blobs" / "sha256" / expected_sha[:2] / expected_sha
    if destination.is_file():
        if _sha(destination) != expected_sha:
            raise AssemblyError("BLOB_HASH_MISMATCH", "Повреждён локальный снимок источника")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=".blob-", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as out, source.open("rb") as inp:
            shutil.copyfileobj(inp, out, 1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        if _sha(temporary) != expected_sha:
            raise AssemblyError("SOURCE_CHANGED", f"Источник изменился во время фиксации: {source.name}")
        try:
            os.link(temporary, destination)
        except FileExistsError:
            pass
        destination.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _block_id(source: dict[str, Any], blocks_sha: str, original_id: str) -> str:
    identity = [source["kind"], source.get("stage") or source.get("discipline"), source["document_code"], source["version_id"], blocks_sha, original_id]
    return "blk_" + hashlib.sha256(_canonical(identity)).hexdigest()[:32]


def _project_md(text: str, *, page_offset: int, id_map: dict[str, str], ordinal_offset: int, page_count: int) -> tuple[list[str], int]:
    parsed = parse_results_md(text)
    lines = text.splitlines()
    boundaries = [
        (idx, int(match.group("page")), match.group("suffix") or "")
        for idx, line in enumerate(lines) if (match := PAGE_HEADER_RE.match(line))
    ]
    by_page: dict[int, list[str]] = {}
    suffixes: dict[int, str] = {}
    for pos, (start, page_no, suffix) in enumerate(boundaries):
        end = boundaries[pos + 1][0] if pos + 1 < len(boundaries) else len(lines)
        by_page[page_no] = lines[start + 1:end]
        suffixes[page_no] = suffix
    result: list[str] = []
    next_ordinal = ordinal_offset
    original_ordinals = {block.block_id: block.ordinal for block in parsed.blocks}
    for page_no in range(1, page_count + 1):
        # Лист и название из штампа остаются при странице и после сдвига номера.
        result.extend([f"## Page {page_offset + page_no}{suffixes.get(page_no, '')}", ""])
        for line in by_page.get(page_no, []):
            match = BLOCK_HEADER_RE.match(line)
            if match:
                old_id = match.group("block_id")
                next_ordinal += 1
                line = f"### BLOCK #{next_ordinal} [{match.group(2)}]: {id_map[old_id]}"
            elif line.lstrip().startswith("> **Crop:**"):
                continue
            else:
                for old_id, new_id in id_map.items():
                    if old_id in line:
                        line = line.replace(old_id, new_id)
            result.append(line)
        if result and result[-1] != "":
            result.append("")
    return result, next_ordinal


def _build_files(
    version_dir: Path,
    manifest: dict[str, Any],
    heartbeat: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    import fitz

    work = version_dir / "02_work"
    work.mkdir(parents=True, exist_ok=False)
    output_pdf = fitz.open()
    output_blocks: list[dict[str, Any]] = []
    output_pages: list[dict[str, Any]] = []
    origin_pages: list[dict[str, Any]] = []
    origin_blocks: dict[str, dict[str, Any]] = {}
    md_lines = [f"# Document: {manifest['name']}.pdf", "Path: Сборки / " + manifest["name"], ""]
    html_links: list[str] = []
    html_sections: list[str] = []
    page_offset = 0
    ordinal_offset = 0
    for source_index, source in enumerate(manifest["sources"]):
        blobs = {key: Path(value["blob_path"]) for key, value in source["files"].items()}
        blocks_data = json.loads(blobs["blocks"].read_text(encoding="utf-8"))
        blocks_sha = source["files"]["blocks"]["sha256"]
        id_map = {str(row["block_id"]): _block_id(source, blocks_sha, str(row["block_id"])) for row in blocks_data["blocks"]}
        with fitz.open(blobs["pdf"]) as src:
            output_pdf.insert_pdf(src, links=False, annots=True, widgets=True)
            page_count = src.page_count
        md_part, ordinal_offset = _project_md(
            blobs["markdown"].read_text(encoding="utf-8"),
            page_offset=page_offset, id_map=id_map, ordinal_offset=ordinal_offset, page_count=page_count,
        )
        md_lines.extend(md_part)
        sheets = {int(row["pdf_page"]): row for row in sheet_matching.extract_sheet_index_from_results_html(blobs["html"].read_text(encoding="utf-8"))}
        for local_page in range(1, page_count + 1):
            global_page = page_offset + local_page
            sheet = sheets.get(local_page) or {}
            label = sheet.get("display") or f"Page {global_page}"
            html_links.append(f'<li><a href="#page-{global_page - 1}">{html.escape(str(label))}</a></li>')
            html_sections.append(f'<section id="page-{global_page - 1}" data-source-index="{source_index}"><h2>{html.escape(str(label))}</h2></section>')
            origin_pages.append({
                "assembly_page": global_page,
                "source_index": source_index,
                "source_page": local_page,
                "sheet_number": sheet.get("sheet_number"),
                "sheet_title": sheet.get("title"),
            })
        for page in blocks_data["pages"]:
            projected = dict(page)
            projected["page_index"] = int(page["page_index"]) + page_offset
            projected["page_label"] = projected["page_index"] + 1
            output_pages.append(projected)
        for block in blocks_data["blocks"]:
            projected = dict(block)
            old_id = str(block["block_id"])
            new_id = id_map[old_id]
            projected["block_id"] = new_id
            projected["page_index"] = int(block["page_index"]) + page_offset
            if projected.get("ordinal") is not None:
                projected["ordinal"] = next(
                    (b.ordinal for b in parse_results_md("\n".join(md_part)).blocks if b.block_id == new_id),
                    projected.get("ordinal"),
                )
            projected.pop("crop_url", None)
            output_blocks.append(projected)
            origin_blocks[new_id] = {
                "source_index": source_index,
                "source_block_id": old_id,
                "source_page": int(block["page_index"]) + 1,
                "assembly_page": int(block["page_index"]) + 1 + page_offset,
                "coords_norm": block.get("coords_norm"),
                "block_type": block.get("block_type"),
            }
        if heartbeat is not None:
            heartbeat(source_index + 1, len(manifest["sources"]))
        page_offset += page_count
    output_pdf.set_metadata({"title": manifest["name"], "author": "Audit Manager", "creator": "Project Assemblies"})
    pdf_path = work / "document.pdf"
    output_pdf.save(pdf_path, garbage=0, deflate=False, no_new_id=True)
    output_pdf.close()
    (work / "document.md").write_text("\n".join(md_lines).rstrip() + "\n", encoding="utf-8")
    content_identity = hashlib.sha256(_canonical({
        "name": manifest["name"],
        "section": manifest["section"],
        "sources": [
            {"source_ref": source["source_ref"], "files": {role: item["sha256"] for role, item in source["files"].items()}}
            for source in manifest["sources"]
        ],
    })).hexdigest()
    blocks_payload = {
        "schema_version": 1,
        "document_id": "assembly_" + content_identity[:32],
        "pages": output_pages,
        "blocks": output_blocks,
    }
    (work / "blocks.json").write_text(json.dumps(blocks_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    html_text = "<!doctype html><html><head><meta charset=\"utf-8\"><title>" + html.escape(manifest["name"]) + "</title></head><body><nav><ol>" + "".join(html_links) + "</ol></nav>" + "".join(html_sections) + "</body></html>\n"
    (work / "ocr.html").write_text(html_text, encoding="utf-8")
    origin = {
        "schema": ORIGIN_SCHEMA,
        "manifest_sha256": manifest["manifest_sha256"],
        "sources": [{key: value for key, value in source.items() if key != "files"} for source in manifest["sources"]],
        "pages": origin_pages,
        "blocks": origin_blocks,
    }
    (work / "assembly_origin.json").write_text(json.dumps(origin, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        name: {"size": (work / name).stat().st_size, "sha256": _sha(work / name)}
        for name in ("document.pdf", "document.md", "blocks.json", "ocr.html", "assembly_origin.json")
    }


def _free_space_guard(root: Path, estimated: int) -> None:
    free = shutil.disk_usage(root).free
    required = estimated + MIN_FREE_RESERVE
    if free < required:
        raise AssemblyError("INSUFFICIENT_DISK", f"Недостаточно места: нужно {required}, доступно {free}")


@contextmanager
def _build_lock(root: Path):
    import fcntl

    lock_path = root.parent.parent.parent / "_system" / "project_assemblies.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AssemblyError("ASSEMBLY_BUSY", "Уже выполняется другая задача сборки") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _registry_lock(root: Path):
    """Serialize idempotency lookup and exclusive version publication."""
    import fcntl

    lock_path = root / ".registry.lock"
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_version(object_id: str, assembly_id: str, version_id: str) -> dict[str, Any]:
    if not SAFE_ID.fullmatch(assembly_id) or not SAFE_ID.fullmatch(version_id):
        raise AssemblyError("BAD_ID", "Некорректный идентификатор сборки")
    root = _root(object_id, create=True)
    assembly_dir = root / assembly_id
    pending = assembly_dir / "pending" / f"{version_id}.json"
    if not pending.is_file():
        raise AssemblyError("VERSION_NOT_FOUND", "Задание сборки не найдено")
    manifest = json.loads(pending.read_text(encoding="utf-8"))
    final = assembly_dir / "versions" / version_id
    if final.is_dir():
        return json.loads((final / "manifest.json").read_text(encoding="utf-8"))
    with _build_lock(root):
        for stale in (assembly_dir / "versions").glob(f".{version_id}.*.building"):
            shutil.rmtree(stale, ignore_errors=True)
        estimated = int(manifest["totals"]["pdf_bytes"] * 3 + manifest["totals"]["page_count"] * 2_000_000)
        _free_space_guard(root, estimated)
        temporary = assembly_dir / "versions" / f".{version_id}.{uuid.uuid4().hex}.building"
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.mkdir(exist_ok=False)
        state_path = assembly_dir / "jobs" / f"{version_id}.json"
        atomic_write_json(state_path, {"status": "RUNNING", "updated_at": _now(), "heartbeat": _now(), "version_id": version_id, "pid": os.getpid()})
        try:
            def heartbeat(completed_sources: int, total_sources: int) -> None:
                atomic_write_json(state_path, {
                    "status": "RUNNING",
                    "updated_at": _now(),
                    "heartbeat": _now(),
                    "version_id": version_id,
                    "pid": os.getpid(),
                    "completed_sources": completed_sources,
                    "total_sources": total_sources,
                })

            artifacts = _build_files(temporary, manifest, heartbeat=heartbeat)
            completed = {**manifest, "status": "READY", "completed_at": _now(), "artifacts": artifacts}
            (temporary / "manifest.json").write_text(json.dumps(completed, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, final)
            for path in final.rglob("*"):
                if path.is_file():
                    path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            pointer = assembly_dir / "current_version.txt"
            tmp_pointer = pointer.with_suffix(".tmp")
            tmp_pointer.write_text(version_id + "\n", encoding="utf-8")
            os.replace(tmp_pointer, pointer)
            atomic_write_json(state_path, {"status": "READY", "updated_at": _now(), "version_id": version_id, "artifacts": artifacts})
            return completed
        except Exception as exc:
            shutil.rmtree(temporary, ignore_errors=True)
            atomic_write_json(state_path, {"status": "FAILED", "updated_at": _now(), "version_id": version_id, "error": f"{type(exc).__name__}: {exc}"})
            raise


def launch_version(object_id: str, assembly_id: str, version_id: str) -> dict[str, Any]:
    assembly_dir = _root(object_id) / assembly_id
    pending = assembly_dir / "pending" / f"{version_id}.json"
    if not pending.is_file():
        raise AssemblyError("VERSION_NOT_FOUND", "Задание сборки не найдено")
    jobs = assembly_dir / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    log_path = jobs / f"{version_id}.worker.log"
    state_path = jobs / f"{version_id}.json"
    initial = {"status": "PREPARING", "updated_at": _now(), "version_id": version_id, "attempt": 1}
    atomic_write_json(state_path, initial)
    command = [
        sys.executable, "-m", "backend.app.services.project_assemblies.worker",
        object_id, assembly_id, version_id,
    ]
    log = log_path.open("ab")
    try:
        process = subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[4],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log.close()
    try:
        current = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        current = initial
    if current.get("status") == "PREPARING":
        current["pid"] = process.pid
        atomic_write_json(state_path, current)
    return current


def retry_version(object_id: str, assembly_id: str, version_id: str) -> dict[str, Any]:
    state_path = _root(object_id) / assembly_id / "jobs" / f"{version_id}.json"
    try:
        current = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        current = {}
    if current.get("status") in {"READY", "RUNNING", "PREPARING"}:
        raise AssemblyError("ASSEMBLY_BUSY", "Эта версия уже готова или выполняется")
    state = launch_version(object_id, assembly_id, version_id)
    state["attempt"] = int(current.get("attempt") or 1) + 1
    atomic_write_json(state_path, state)
    return state


def cancel_version(object_id: str, assembly_id: str, version_id: str) -> dict[str, Any]:
    state_path = _root(object_id) / assembly_id / "jobs" / f"{version_id}.json"
    try:
        current = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AssemblyError("VERSION_NOT_FOUND", "Состояние задачи не найдено") from exc
    pid = current.get("pid")
    if isinstance(pid, int) and pid > 1:
        try:
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().decode("utf-8", errors="replace")
            expected = f"backend.app.services.project_assemblies.worker\x00{object_id}\x00{assembly_id}\x00{version_id}"
            if expected in cmdline:
                os.kill(pid, signal.SIGTERM)
        except FileNotFoundError:
            pass
    state = {"status": "CANCELLED", "updated_at": _now(), "version_id": version_id, "attempt": current.get("attempt", 1)}
    atomic_write_json(state_path, state)
    return state


def _next_version(assembly_dir: Path) -> str:
    versions = assembly_dir / "versions"
    pending = assembly_dir / "pending"
    used = {path.stem if path.suffix == ".json" else path.name for root in (versions, pending) if root.is_dir() for path in root.iterdir()}
    number = 1
    while f"v{number:03d}" in used:
        number += 1
    return f"v{number:03d}"


def _prepare_assembly(
    *, object_id: str, name: str, section: str, source_refs: list[str],
    author: str = "", idempotency_key: str = "", assembly_id: str | None = None,
    composition_completeness: str = "UNKNOWN",
) -> tuple[dict[str, Any], bool]:
    clean_name = " ".join(name.split()).strip()
    if not clean_name:
        raise AssemblyError("NAME_REQUIRED", "Укажите название сборки")
    if composition_completeness not in {"COMPLETE", "INCOMPLETE", "UNKNOWN"}:
        raise AssemblyError("BAD_COMPLETENESS", "Некорректное заявление о полноте состава")
    check = preview(object_id, source_refs)
    root = _root(object_id, create=True)
    key = idempotency_key.strip()
    if key:
        registry = root / "idempotency.json"
        existing = (json.loads(registry.read_text(encoding="utf-8")) if registry.is_file() else {}).get(key)
        if existing:
            return get_assembly(object_id, existing["assembly_id"], existing["version_id"]), True
    assembly_id = assembly_id or ("asm_" + uuid.uuid4().hex[:20])
    if not SAFE_ID.fullmatch(assembly_id):
        raise AssemblyError("BAD_ID", "Некорректный идентификатор сборки")
    assembly_dir = root / assembly_id
    assembly_dir.mkdir(parents=True, exist_ok=bool(assembly_id and (assembly_dir / "assembly.json").is_file()))
    version_id = _next_version(assembly_dir)
    sources = []
    for row in check["sources"]:
        resolved = _resolve_source(row["source_ref"], object_id)
        file_rows = {}
        for role, path_key in (("pdf", "pdf_path"), ("markdown", "markdown_path"), ("blocks", "blocks_path"), ("html", "html_path")):
            source_path = Path(_preflight_one(resolved)[path_key])
            digest = row["sha256"][role]
            blob = _blob(root, source_path, digest)
            file_rows[role] = {"sha256": digest, "blob_path": str(blob), "source_name": source_path.name}
        sources.append({
            "source_ref": row["source_ref"], "kind": row["kind"], "object_id": object_id,
            "stage": resolved.get("stage") or "", "discipline": row["discipline"],
            "document_code": row["document_code"], "version_id": row["version_id"],
            "page_start": row["page_start"], "page_end": row["page_end"], "files": file_rows,
        })
    base = {
        "schema": SCHEMA, "assembly_id": assembly_id, "version_id": version_id,
        "object_id": object_id, "name": clean_name, "section": section.strip().upper() or "OTHER",
        "author": author, "created_at": _now(), "sources": sources, "totals": check["totals"],
        "composition_completeness": composition_completeness,
        "preview_token": check["preview_token"],
    }
    base["manifest_sha256"] = hashlib.sha256(_canonical({
        "schema": SCHEMA,
        "object_id": object_id,
        "name": clean_name,
        "section": base["section"],
        "composition_completeness": composition_completeness,
        "sources": [
            {
                "source_ref": source["source_ref"],
                "page_start": source["page_start"],
                "page_end": source["page_end"],
                "files": {role: row["sha256"] for role, row in source["files"].items()},
            }
            for source in sources
        ],
    })).hexdigest()
    (assembly_dir / "pending").mkdir(exist_ok=True)
    pending_path = assembly_dir / "pending" / f"{version_id}.json"
    with pending_path.open("x", encoding="utf-8") as handle:
        json.dump(base, handle, ensure_ascii=False, indent=2)
    (assembly_dir / "jobs").mkdir(exist_ok=True)
    atomic_write_json(assembly_dir / "jobs" / f"{version_id}.json", {
        "status": "QUEUED", "updated_at": _now(), "version_id": version_id, "attempt": 0,
    })
    if not (assembly_dir / "assembly.json").is_file():
        atomic_write_json(assembly_dir / "assembly.json", {
            "schema": SCHEMA, "assembly_id": assembly_id, "object_id": object_id,
            "name": clean_name, "section": base["section"], "created_at": base["created_at"],
        })
    if key:
        def remember(data):
            data = data if isinstance(data, dict) else {}
            data.setdefault(key, {"assembly_id": assembly_id, "version_id": version_id})
            return data
        load_modify_save(root / "idempotency.json", remember, default={})
    return base, False


def create_assembly(
    *, object_id: str, name: str, section: str, source_refs: list[str],
    author: str = "", idempotency_key: str = "", assembly_id: str | None = None,
    build: bool = True, composition_completeness: str = "UNKNOWN",
) -> dict[str, Any]:
    root = _root(object_id, create=True)
    with _registry_lock(root):
        prepared, existing = _prepare_assembly(
            object_id=object_id,
            name=name,
            section=section,
            source_refs=source_refs,
            author=author,
            idempotency_key=idempotency_key,
            assembly_id=assembly_id,
            composition_completeness=composition_completeness,
        )
    if existing:
        return prepared
    if build:
        return build_version(object_id, prepared["assembly_id"], prepared["version_id"])
    return {**prepared, **launch_version(object_id, prepared["assembly_id"], prepared["version_id"])}


def get_assembly(object_id: str, assembly_id: str, version_id: str | None = None) -> dict[str, Any]:
    assembly_dir = _root(object_id) / assembly_id
    if version_id is None:
        try:
            version_id = (assembly_dir / "current_version.txt").read_text(encoding="utf-8").strip()
        except OSError:
            version_id = ""
    ready = assembly_dir / "versions" / str(version_id) / "manifest.json"
    if ready.is_file():
        return json.loads(ready.read_text(encoding="utf-8"))
    state = assembly_dir / "jobs" / f"{version_id}.json"
    if state.is_file():
        status = json.loads(state.read_text(encoding="utf-8"))
        if status.get("status") in {"PREPARING", "RUNNING"}:
            pid = status.get("pid")
            if not isinstance(pid, int) or not Path(f"/proc/{pid}").exists():
                status = {
                    **status,
                    "status": "INTERRUPTED",
                    "error": "Процесс сборки был прерван; задачу можно повторить.",
                }
        pending = assembly_dir / "pending" / f"{version_id}.json"
        if pending.is_file():
            return {**json.loads(pending.read_text(encoding="utf-8")), **status}
        return status
    raise AssemblyError("ASSEMBLY_NOT_FOUND", "Сборка не найдена")


def list_assemblies(object_id: str) -> dict[str, Any]:
    root = _root(object_id)
    items = []
    if root.is_dir():
        for meta_path in sorted(root.glob("asm_*/assembly.json")):
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            try:
                current = get_assembly(object_id, meta["assembly_id"])
            except AssemblyError:
                job_paths = sorted((meta_path.parent / "jobs").glob("v*.json"))
                current = get_assembly(object_id, meta["assembly_id"], job_paths[-1].stem) if job_paths else {}
            artifacts = current.get("artifacts") or {}
            artifact_bytes = sum(int(row.get("size") or 0) for row in artifacts.values() if isinstance(row, dict))
            items.append({
                **meta,
                "current_version": current.get("version_id"),
                "status": current.get("status", "PREPARING"),
                "totals": current.get("totals"),
                "artifact_bytes": artifact_bytes,
                "error": current.get("error"),
            })
    return {"schema": "project_assemblies/1", "object_id": object_id, "items": items}


def _attachments_path(object_id: str, *, create: bool = False) -> Path:
    return _root(object_id, create=create) / "attachments.json"


def attachments(object_id: str) -> dict[str, Any]:
    path = _attachments_path(object_id)
    if not path.is_file():
        return {"schema": "project_assembly_attachments/1", "object_id": object_id, "stage_1": [], "stage_2": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AssemblyError("ATTACHMENTS_CORRUPT", "Повреждён реестр прикреплений") from exc
    return data


def attach(object_id: str, assembly_id: str, version_id: str, side: str) -> dict[str, Any]:
    if side not in {"stage_1", "stage_2"}:
        raise AssemblyError("BAD_SIDE", "Сторона должна быть stage_1 или stage_2")
    ready = get_assembly(object_id, assembly_id, version_id)
    if ready.get("status") != "READY":
        raise AssemblyError("ASSEMBLY_NOT_READY", "К сравнению можно прикрепить только готовую сборку")
    ref = {"assembly_id": assembly_id, "version_id": version_id, "attached_at": _now()}

    def mutate(data):
        if not isinstance(data, dict):
            data = {}
        data.setdefault("schema", "project_assembly_attachments/1")
        data.setdefault("object_id", object_id)
        data.setdefault("stage_1", [])
        data.setdefault("stage_2", [])
        data[side] = [row for row in data[side] if not (
            row.get("assembly_id") == assembly_id and row.get("version_id") == version_id
        )]
        data[side].append(ref)
        return data

    return load_modify_save(_attachments_path(object_id, create=True), mutate, default={})


def detach(object_id: str, assembly_id: str, version_id: str, side: str) -> dict[str, Any]:
    if side not in {"stage_1", "stage_2"}:
        raise AssemblyError("BAD_SIDE", "Сторона должна быть stage_1 или stage_2")

    def mutate(data):
        if not isinstance(data, dict):
            data = {}
        data.setdefault("schema", "project_assembly_attachments/1")
        data.setdefault("object_id", object_id)
        data.setdefault("stage_1", [])
        data.setdefault("stage_2", [])
        data[side] = [row for row in data[side] if not (
            row.get("assembly_id") == assembly_id and row.get("version_id") == version_id
        )]
        return data

    return load_modify_save(_attachments_path(object_id, create=True), mutate, default={})


def attached_documents(object_id: str, side: str) -> list[dict[str, Any]]:
    result = []
    for ref in attachments(object_id).get(side, []):
        try:
            manifest = get_assembly(object_id, str(ref.get("assembly_id") or ""), str(ref.get("version_id") or ""))
        except AssemblyError:
            continue
        if manifest.get("status") != "READY":
            continue
        work = _root(object_id) / manifest["assembly_id"] / "versions" / manifest["version_id"] / "02_work"
        result.append({
            "pdf_path": str(work / "document.pdf"),
            "md_path": str(work / "document.md"),
            "html_path": str(work / "ocr.html"),
            "relative": str(Path("assemblies") / manifest["assembly_id"] / "versions" / manifest["version_id"] / "document.pdf"),
            "filename": manifest["name"] + ".pdf",
            "document_code": manifest["name"],
            "discipline": manifest["section"],
            "version_id": manifest["version_id"],
            "assembly_ref": {
                "assembly_id": manifest["assembly_id"],
                "version_id": manifest["version_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "origin_path": str(work / "assembly_origin.json"),
                "composition_completeness": manifest.get("composition_completeness") or "UNKNOWN",
            },
            "assembly_members": [
                {
                    "kind": source.get("kind"),
                    "stage": source.get("stage"),
                    "document_code": source.get("document_code"),
                    "version_id": source.get("version_id"),
                }
                for source in manifest.get("sources") or []
            ],
        })
    return result


def object_id_for_stage_path(stage_path: str | Path) -> str | None:
    candidate = Path(stage_path).expanduser().resolve()
    if candidate.name not in {"stage_1", "stage_2"} or candidate.parent.name != "comparison":
        return None
    try:
        meta = json.loads((candidate.parent.parent / "object.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return str(meta.get("object_id") or "") or None


def attached_documents_for_stage_path(stage_path: str | Path) -> list[dict[str, Any]]:
    if not enabled():
        return []
    path = Path(stage_path).expanduser().resolve()
    object_id = object_id_for_stage_path(path)
    return attached_documents(object_id, path.name) if object_id else []


def attached_count(object_id: str, side: str) -> int:
    if not enabled():
        return 0
    return len(attached_documents(object_id, side))


def origin_for_page(object_id: str, assembly_id: str, version_id: str, page: int) -> dict[str, Any]:
    path = _root(object_id) / assembly_id / "versions" / version_id / "02_work" / "assembly_origin.json"
    if not path.is_file():
        raise AssemblyError("ASSEMBLY_NOT_FOUND", "Карта происхождения не найдена")
    origin = json.loads(path.read_text(encoding="utf-8"))
    record = next((row for row in origin.get("pages", []) if row.get("assembly_page") == page), None)
    if record is None:
        raise AssemblyError("PAGE_NOT_FOUND", "Страница сборки не найдена")
    source = origin["sources"][record["source_index"]]
    blocks = [{"block_id": block_id, **row} for block_id, row in origin.get("blocks", {}).items() if row.get("assembly_page") == page]
    return {"page": record, "source": source, "blocks": blocks}


def resolve_document_origin(document: dict[str, Any], page: int, block_id: str | None = None) -> dict[str, Any] | None:
    ref = document.get("assembly_ref") if isinstance(document, dict) else None
    if not isinstance(ref, dict):
        return None
    path = Path(str(ref.get("origin_path") or ""))
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    page_row = next((row for row in data.get("pages", []) if row.get("assembly_page") == page), None)
    if page_row is None:
        return None
    source_index = page_row.get("source_index")
    sources = data.get("sources") or []
    if not isinstance(source_index, int) or not (0 <= source_index < len(sources)):
        return None
    out = {"assembly_page": page, "source": sources[source_index], "page": page_row}
    if block_id:
        block = (data.get("blocks") or {}).get(block_id)
        if isinstance(block, dict):
            out["block"] = block
    return out
