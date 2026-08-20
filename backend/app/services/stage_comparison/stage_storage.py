"""Версионное хранилище документов одной стадии сравнения.

Layout повторяет полезную часть ``projects_v2``::

    stage_N/
      stage.json
      documents/<CODE>/
        document.json
        current_version.txt
        versions/vNNN/
          version.json
          01_input/       # неизменяемые оригиналы загрузки
          02_work/        # канонические document.pdf/.md/result.json
          99_service/

Контуры findings/optimization/review/export намеренно отсутствуют.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from backend.app.services.common import discipline_service


SCHEMA_VERSION = 1
STORAGE_PROFILE = "comparison_stage_v1"
_SOURCE_SUFFIXES = (
    ".pdf", ".md", ".html", ".htm", ".json",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_component(value: str, fallback: str = "document") -> str:
    value = re.sub(r"[\\/\x00]+", "_", str(value or "").strip())
    value = re.sub(r"\s+", " ", value).strip(" .")
    if not value or value in {".", ".."}:
        value = fallback
    return value[:180]


def ensure_stage_scaffold(
    stage_dir: Path,
    *,
    stage_name: str,
    object_id: str | None,
    object_name: str,
) -> dict:
    stage_dir.mkdir(parents=True, exist_ok=True)
    (stage_dir / "documents").mkdir(exist_ok=True)
    (stage_dir / "99_service" / "imports").mkdir(parents=True, exist_ok=True)
    meta_path = stage_dir / "stage.json"
    try:
        current = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        current = {}
    created_at = current.get("created_at") or _now()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "storage_profile": STORAGE_PROFILE,
        "object_id": object_id,
        "object_name": object_name,
        "stage": stage_name,
        "created_at": created_at,
        "updated_at": _now(),
        "stores_findings": False,
        "stores_optimizations": False,
    }
    _write_json(meta_path, payload)
    return payload


def ensure_comparison_object_scaffold(
    object_dir: Path,
    *,
    object_id: str | None,
    object_name: str,
    stages: tuple[str, ...] = ("stage_1", "stage_2"),
) -> dict:
    """Создать object-level метаданные и каркасы обеих стадий."""
    object_dir.mkdir(parents=True, exist_ok=True)
    meta_path = object_dir / "object.json"
    current = _load_json(meta_path)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "storage_profile": STORAGE_PROFILE,
        "object_id": object_id,
        "display_name": object_name,
        "folder_name": object_dir.name,
        "created_at": current.get("created_at") or _now(),
        "updated_at": _now(),
        "stages": list(stages),
        "stores_findings": False,
        "stores_optimizations": False,
    }
    _write_json(meta_path, payload)
    for stage_name in stages:
        ensure_stage_scaffold(
            object_dir / stage_name,
            stage_name=stage_name,
            object_id=object_id,
            object_name=object_name,
        )
    return payload


def is_versioned_stage(stage_dir: Path) -> bool:
    try:
        data = json.loads((stage_dir / "stage.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return data.get("storage_profile") == STORAGE_PROFILE


def _candidate_files(pdf: Path, all_pdfs_in_folder: int) -> list[Path]:
    folder = pdf.parent
    stem = pdf.stem.casefold()
    out = [pdf]
    try:
        files = sorted(p for p in folder.iterdir() if p.is_file())
    except OSError:
        return out
    for item in files:
        if item == pdf or item.suffix.lower() not in _SOURCE_SUFFIXES:
            continue
        low = item.name.casefold()
        shared = low in {"document.md", "result.json", "ocr.html", "project_info.json"}
        related = low.startswith(stem) or stem.startswith(item.stem.casefold())
        if related or (shared and all_pdfs_in_folder == 1):
            out.append(item)
    return out


def _pick(files: list[Path], *, suffixes: tuple[str, ...], exact: tuple[str, ...] = ()) -> Path | None:
    lowered = {name.casefold() for name in exact}
    for path in files:
        if path.name.casefold() in lowered:
            return path
    for path in files:
        if any(path.name.casefold().endswith(suffix.casefold()) for suffix in suffixes):
            return path
    return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


def _next_version(doc_dir: Path) -> tuple[str, int]:
    ids = []
    versions_root = doc_dir / "versions"
    if versions_root.is_dir():
        ids = [p.name for p in versions_root.iterdir() if p.is_dir() and re.fullmatch(r"v\d+", p.name)]
    nums = [int(vid[1:]) for vid in ids]
    no = max(nums, default=0) + 1
    return f"v{no:03d}", no


def _document_dir(stage_dir: Path, discipline: str, code: str) -> Path:
    direct = stage_dir / "documents" / code
    if direct.is_dir():
        return direct
    # Регистр в имени ZIP не должен создавать второй каталог документа.
    for meta in (stage_dir / "documents").glob("*/document.json"):
        data = _load_json(meta)
        if str(data.get("document_code") or "").casefold() == code.casefold():
            return meta.parent
    return direct


def _canonical_work_files(version_dir: Path, sources: list[Path], pdf: Path) -> dict[str, str]:
    work = version_dir / "02_work"
    work.mkdir(parents=True, exist_ok=True)
    mapping: dict[str, str] = {}
    selections = {
        "document.pdf": pdf,
        "document.md": _pick(sources, suffixes=("_document.md", "_results.md", ".md"), exact=("document.md",)),
        "result.json": _pick(sources, suffixes=("_result.json", ".result.json"), exact=("result.json",)),
        "blocks.json": _pick(sources, suffixes=("_blocks.json",), exact=("blocks.json",)),
        "ocr.html": _pick(sources, suffixes=("_ocr.html", "_results.html", "_results.htm"), exact=("ocr.html",)),
    }
    for canonical, source in selections.items():
        if source is None:
            continue
        shutil.copy2(source, work / canonical)
        mapping[canonical] = source.name
    return mapping


def import_extracted_tree(
    extracted_root: Path,
    stage_dir: Path,
    *,
    stage_name: str,
    object_id: str | None,
    object_name: str,
    upload_filename: str | None,
) -> dict:
    """Добавить содержимое распакованного ZIP как новые версии документов."""
    ensure_stage_scaffold(
        stage_dir,
        stage_name=stage_name,
        object_id=object_id,
        object_name=object_name,
    )
    pdfs = sorted(p for p in extracted_root.rglob("*.pdf") if p.is_file())
    per_folder: dict[Path, int] = {}
    for pdf in pdfs:
        per_folder[pdf.parent] = per_folder.get(pdf.parent, 0) + 1

    imported = []
    consumed: set[Path] = set()
    now = _now()
    for pdf in pdfs:
        sources = _candidate_files(pdf, per_folder[pdf.parent])
        consumed.update(sources)
        rel_parent = str(pdf.parent.relative_to(extracted_root)) if pdf.parent != extracted_root else ""
        detected = discipline_service.detect_discipline_detailed(
            folder_name=rel_parent,
            pdf_name=pdf.name,
            doc_text="",
        )
        discipline = _safe_component(detected.get("code") or "EOM", "EOM").upper()
        code = _safe_component(pdf.stem)
        doc_dir = _document_dir(stage_dir, discipline, code)
        version_id, version_no = _next_version(doc_dir)
        version_dir = doc_dir / "versions" / version_id
        for subdir in (
            version_dir / "01_input",
            version_dir / "02_work",
            version_dir / "99_service",
        ):
            subdir.mkdir(parents=True, exist_ok=True)

        input_files = []
        for source in sources:
            destination = version_dir / "01_input" / source.name
            shutil.copy2(source, destination)
            input_files.append({
                "name": source.name,
                "size": source.stat().st_size,
                "sha256": _sha256(source),
            })
        canonical = _canonical_work_files(version_dir, sources, pdf)
        project_info = {
            "project_id": code,
            "document_code": code,
            "name": code,
            "section": discipline,
            "pdf_file": pdf.name,
            "pdf_files": [pdf.name],
            "md_files": [p.name for p in sources if p.suffix.lower() == ".md"],
            "version_id": version_id,
            "version_label": f"V{version_no}",
            "version_source": "stage_archive_upload",
        }
        _write_json(version_dir / "01_input" / "project_info.json", project_info)
        _write_json(version_dir / "01_input" / "input_manifest.json", {
            "schema_version": 1,
            "uploaded_at": now,
            "upload_filename": upload_filename,
            "source_relative_pdf": str(pdf.relative_to(extracted_root)),
            "files": input_files,
        })
        _write_json(version_dir / "version.json", {
            "schema_version": 1,
            "version_id": version_id,
            "version_no": version_no,
            "label": f"V{version_no}",
            "source": "stage_archive_upload",
            "status": "source_only",
            "created_at": now,
            "canonical_files": canonical,
            "stores_findings": False,
            "stores_optimizations": False,
        })

        document = _load_json(doc_dir / "document.json")
        versions = [v for v in document.get("versions", []) if isinstance(v, dict)]
        versions.append({
            "version_id": version_id,
            "version_no": version_no,
            "label": f"V{version_no}",
            "status": "source_only",
            "source": "stage_archive_upload",
            "created_at": now,
        })
        document.update({
            "schema_version": 1,
            "document_code": code,
            "object_id": object_id,
            "discipline": discipline,
            "kind": "comparison_source",
            "versions": versions,
            "version_ids": [v["version_id"] for v in versions],
            "current_version": version_id,
            "stores_findings": False,
            "stores_optimizations": False,
        })
        _write_json(doc_dir / "document.json", document)
        (doc_dir / "current_version.txt").write_text(version_id, encoding="utf-8")
        imported.append({
            "discipline": discipline,
            "document_code": code,
            "version_id": version_id,
            "source_pdf": str(pdf.relative_to(extracted_root)),
        })

    all_files = {p for p in extracted_root.rglob("*") if p.is_file()}
    unassigned = sorted(all_files - consumed)
    import_id = datetime.now(timezone.utc).strftime("upload_%Y%m%dT%H%M%S%fZ")
    import_dir = stage_dir / "99_service" / "imports" / import_id
    for source in unassigned:
        rel = source.relative_to(extracted_root)
        destination = import_dir / "unassigned" / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    _write_json(import_dir / "import.json", {
        "schema_version": 1,
        "created_at": now,
        "upload_filename": upload_filename,
        "documents": imported,
        "unassigned_files": [str(p.relative_to(extracted_root)) for p in unassigned],
    })
    return {
        "documents_imported": len(imported),
        "versions_created": len(imported),
        "documents": imported,
        "unassigned_files_count": len(unassigned),
    }


def iter_current_documents(stage_dir: Path):
    """Yield metadata current-версий без review/findings артефактов."""
    meta_paths = list((stage_dir / "documents").glob("*/document.json"))
    # Чтение старого layout оставлено только для бесшовного открытия данных,
    # созданных до перехода на единый documents/. Новые импорты туда не пишут.
    meta_paths.extend((stage_dir / "disciplines").glob("*/documents/*/document.json"))
    for meta_path in sorted(meta_paths):
        document = _load_json(meta_path)
        version_id = str(document.get("current_version") or "").strip()
        if not version_id:
            try:
                version_id = (meta_path.parent / "current_version.txt").read_text(encoding="utf-8").strip()
            except OSError:
                continue
        version_dir = meta_path.parent / "versions" / version_id
        pdf = version_dir / "02_work" / "document.pdf"
        if not pdf.is_file():
            continue
        md = version_dir / "02_work" / "document.md"
        info = _load_json(version_dir / "01_input" / "project_info.json")
        yield {
            "document": document,
            "document_dir": meta_path.parent,
            "version_id": version_id,
            "version_dir": version_dir,
            "pdf_path": pdf,
            "md_path": md if md.is_file() else None,
            "source_filename": info.get("pdf_file") or f"{document.get('document_code')}.pdf",
        }


def current_document_count(stage_dir: Path) -> int:
    return sum(1 for _ in iter_current_documents(stage_dir))


__all__ = [
    "STORAGE_PROFILE",
    "ensure_stage_scaffold",
    "ensure_comparison_object_scaffold",
    "import_extracted_tree",
    "is_versioned_stage",
    "iter_current_documents",
    "current_document_count",
]
