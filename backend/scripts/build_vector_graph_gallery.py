#!/usr/bin/env python3
"""Build an ALIA/POS UI gallery from the reviewed graphic-block corpus.

The source corpus remains authoritative under ``experiments/блоки разных дисциплин``.
The gallery stores compact WebP previews and hard-links graph JSON sidecars, so it
can be rebuilt without duplicating the large experimental artifacts.
"""
from __future__ import annotations

import argparse
import collections
import copy
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fitz
from PIL import Image

ROOT = Path(__file__).resolve().parents[2]
CORPUS_ROOT = ROOT / "experiments" / "блоки разных дисциплин"
ALIA_OBJECT = ROOT / "projects_v2" / "objects" / "214_Alia_ASTERUS"
POS_DOCUMENTS = ALIA_OBJECT / "disciplines" / "POS" / "documents"
VERSION_ID = "v001"

DISCIPLINES = {
    "АР": "structured_architecture",
    "ВК": "structured_water",
    "ГП": "structured_general_plan",
    "КЖ": "structured_structure",
    "КМ": "structured_structure",
    "ОВ": "structured_hvac",
    "СС": "structured_alia_scheme",
    "ТХ": "structured_technology",
    "ЭОМ": "structured_electrical",
}

TASK = (
    "## Задача:\n"
    "Выше — точный структурный разбор векторного слоя блока. "
    "Используй его как приоритетный источник марок, размеров и связей."
)
ID_RE = re.compile(r"([A-ZА-Я0-9]+(?:-[A-ZА-Я0-9]+){2,})$", re.IGNORECASE)


@dataclass(frozen=True)
class GraphRecord:
    path: Path
    block_id: str
    source_pdf_name: str
    profile_id: str


@dataclass
class GalleryBlock:
    discipline: str
    pdf_path: Path
    source_block_id: str
    gallery_block_id: str
    title: str
    metadata: dict[str, Any]
    graph: GraphRecord | None
    profile_id: str
    prepared_package: dict[str, Any] | None = None
    page: int = 0
    page_label: str = ""


def json_read(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return default


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.gallery-{os.getpid()}.tmp")
    temp.write_text(text, encoding="utf-8")
    os.replace(temp, path)


def atomic_json(path: Path, payload: Any) -> None:
    atomic_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def atomic_hardlink(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file():
        try:
            if os.path.samefile(source, destination):
                return
        except OSError:
            pass
    temp = destination.with_name(f"{destination.name}.gallery-{os.getpid()}.tmp")
    try:
        temp.unlink()
    except FileNotFoundError:
        pass
    os.link(source, temp)
    os.replace(temp, destination)


def extract_block_id(pdf: Path) -> str:
    match = ID_RE.search(pdf.stem.strip())
    if match:
        return match.group(1).upper()
    digest = hashlib.sha1(pdf.name.encode("utf-8")).hexdigest()[:10].upper()
    return f"GALLERY-{digest}"


def source_title(pdf: Path, source_block_id: str) -> str:
    title = pdf.stem.strip()
    suffix = f" — {source_block_id}"
    if title.upper().endswith(suffix.upper()):
        title = title[: -len(suffix)]
    return title


def load_manifest_records(folder: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for path in sorted(folder.glob("*.json")):
        payload = json_read(path)
        candidates = payload if isinstance(payload, list) else []
        if isinstance(payload, dict) and isinstance(payload.get("records"), list):
            candidates = payload["records"]
        for item in candidates:
            if not isinstance(item, dict) or not item.get("output"):
                continue
            name = Path(str(item["output"])).name
            previous = records.get(name) or {}
            score = sum(bool(item.get(key)) for key in ("profile_id", "subtype", "description"))
            old_score = sum(bool(previous.get(key)) for key in ("profile_id", "subtype", "description"))
            if score >= old_score:
                records[name] = item
    return records


def discover_graphs(folder: Path) -> dict[str, list[GraphRecord]]:
    result: dict[str, list[GraphRecord]] = collections.defaultdict(list)
    for path in sorted(folder.glob("*_out/*.structure.json")):
        payload = json_read(path, {})
        if not isinstance(payload, dict):
            continue
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        block_id = str(source.get("block_id") or path.name[: -len(".structure.json")]).strip()
        if not block_id:
            continue
        result[block_id].append(GraphRecord(
            path=path,
            block_id=block_id,
            source_pdf_name=Path(str(source.get("pdf_file") or "")).name,
            profile_id=str(payload.get("profile_id") or "").strip(),
        ))
    return result


def choose_graph(
    pdf: Path,
    block_id: str,
    graphs: dict[str, list[GraphRecord]],
    pdf_id_counts: collections.Counter,
) -> GraphRecord | None:
    candidates = graphs.get(block_id) or []
    exact = [record for record in candidates if record.source_pdf_name == pdf.name]
    if exact:
        return sorted(exact, key=lambda item: str(item.path))[0]
    if len(candidates) == 1 and pdf_id_counts[block_id] == 1:
        return candidates[0]
    return None


def build_standalone_package(
    *,
    pdf_path: Path,
    discipline: str,
    gallery_block_id: str,
    source_block_id: str,
    title: str,
) -> dict[str, Any] | None:
    """Построить штатный CTX-пакет для самостоятельного одностраничного PDF."""
    from backend.app.pipeline.stages.block_grounding.block_source_router import (
        resolve_block_package,
    )

    with tempfile.TemporaryDirectory(prefix="vector-graph-gallery-", dir="/tmp") as temp_name:
        version_dir = (
            Path(temp_name) / "objects" / "gallery" / "disciplines" / discipline
            / "documents" / gallery_block_id / "versions" / VERSION_ID
        )
        work_dir = version_dir / "02_work"
        output_dir = version_dir / "03_analysis" / "latest"
        work_dir.mkdir(parents=True)
        output_dir.mkdir(parents=True)
        target_pdf = work_dir / "document.pdf"
        try:
            os.link(pdf_path, target_pdf)
        except OSError:
            shutil.copyfile(pdf_path, target_pdf)
        atomic_json(output_dir / "document_graph.json", {
            "schema_version": 1,
            "pages": [{
                "page": 1,
                "page_number": 1,
                "page_index": 0,
                "sheet_name": title,
                "image_blocks": [{
                    "id": gallery_block_id,
                    "block_id": gallery_block_id,
                    "block_type": "image",
                    "coords_norm": [0.0, 0.0, 1.0, 1.0],
                }],
            }],
        })
        package = resolve_block_package(
            output_dir,
            gallery_block_id,
            1,
            prefer_prepared=False,
        )

    graph = package.get("graph")
    if (
        not isinstance(graph, dict)
        or not str(package.get("source_kind") or "").startswith("structured_")
    ):
        return None
    source = graph.get("source")
    if isinstance(source, dict):
        source["pdf_file"] = pdf_path.name
    classification = dict(package.get("classification") or {})
    classification.update({
        "source": "standalone_pdf_ctx",
        "confidence": classification.get("confidence") or "high",
        "block_title": classification.get("block_title") or title,
        "source_block_id": source_block_id,
        "source_pdf": str(pdf_path.relative_to(ROOT)),
    })
    package["classification"] = classification
    return package


def block_has_graph(block: GalleryBlock) -> bool:
    return (
        block.graph is not None
        or isinstance((block.prepared_package or {}).get("graph"), dict)
    )


def discover_blocks(discipline: str) -> list[GalleryBlock]:
    folder = CORPUS_ROOT / discipline
    pdfs = sorted(folder.glob("*.pdf"), key=lambda path: path.name.casefold())
    ids = [extract_block_id(path) for path in pdfs]
    id_counts = collections.Counter(ids)
    seen: collections.Counter = collections.Counter()
    metadata_by_name = load_manifest_records(folder)
    graphs = discover_graphs(folder)
    blocks: list[GalleryBlock] = []

    for pdf, source_id in zip(pdfs, ids):
        seen[source_id] += 1
        gallery_id = source_id
        if id_counts[source_id] > 1:
            gallery_id = f"{source_id}--{seen[source_id]:02d}"
        metadata = dict(metadata_by_name.get(pdf.name) or {})
        graph = choose_graph(pdf, source_id, graphs, id_counts)
        profile_id = str(metadata.get("profile_id") or "").strip()
        if not profile_id and graph:
            profile_id = graph.profile_id
        if not profile_id and graph:
            graph_payload = json_read(graph.path, {})
            if isinstance(graph_payload, dict):
                profile_id = str(graph_payload.get("profile_id") or "").strip()
                if not profile_id and graph_payload.get("type") == "single_line_calc_diagram":
                    profile_id = "electrical_singleline"
        prepared_package = None
        if graph is None:
            prepared_package = build_standalone_package(
                pdf_path=pdf,
                discipline=discipline,
                gallery_block_id=gallery_id,
                source_block_id=source_id,
                title=source_title(pdf, source_id),
            )
            if prepared_package:
                profile_id = str(prepared_package.get("profile_id") or profile_id).strip()
        blocks.append(GalleryBlock(
            discipline=discipline,
            pdf_path=pdf,
            source_block_id=source_id,
            gallery_block_id=gallery_id,
            title=source_title(pdf, source_id),
            metadata=metadata,
            graph=graph,
            profile_id=profile_id or "unclassified",
            prepared_package=prepared_package,
        ))
    return blocks


def assign_profile_groups(blocks: list[GalleryBlock]) -> None:
    from backend.app.pipeline.stages.block_grounding.profiled_graph_localization import (
        PROFILE_LABELS,
    )

    ordered_profiles = []
    for block in blocks:
        if block.profile_id not in ordered_profiles:
            ordered_profiles.append(block.profile_id)
    page_by_profile = {profile: index for index, profile in enumerate(ordered_profiles, 1)}
    for block in blocks:
        block.page = page_by_profile[block.profile_id]
        block.page_label = PROFILE_LABELS.get(
            block.profile_id,
            "Требует отдельного профиля" if block.profile_id == "unclassified"
            else block.profile_id,
        )


def render_preview(
    source_pdf: Path,
    destination: Path,
    max_px: int,
    quality: int,
    *,
    dpi: int | None = None,
    force: bool = False,
) -> tuple[int, int]:
    """Отрендерить первую страницу в превью и вернуть фактический размер.

    Штатная витрина остаётся ограниченной ``max_px`` по длинной стороне. Для
    операторского просмотра можно задать точный ``dpi``; PNG используется для
    300 DPI, потому что WebP ограничен размером 16 383 px по каждой стороне, а
    отдельные сохранённые схемы корпуса шире этого предела.
    """
    if (
        not force
        and destination.is_file()
        and destination.stat().st_mtime_ns >= source_pdf.stat().st_mtime_ns
    ):
        with Image.open(destination) as existing:
            return existing.size
    document = fitz.open(source_pdf)
    try:
        if document.page_count < 1:
            raise RuntimeError(f"empty PDF: {source_pdf}")
        page = document[0]
        if dpi is not None:
            pixmap = page.get_pixmap(dpi=dpi, colorspace=fitz.csRGB, alpha=False)
        else:
            longest = max(float(page.rect.width), float(page.rect.height), 1.0)
            scale = max_px / longest
            pixmap = page.get_pixmap(
                matrix=fitz.Matrix(scale, scale), colorspace=fitz.csRGB, alpha=False
            )
        image = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_name(f"{destination.name}.gallery-{os.getpid()}.tmp")
        suffix = destination.suffix.lower()
        if suffix == ".png":
            image.save(temp, format="PNG", compress_level=6, dpi=(dpi or 72, dpi or 72))
        elif suffix in {".jpg", ".jpeg"}:
            image.save(
                temp,
                format="JPEG",
                quality=quality,
                subsampling=0,
                dpi=(dpi or 72, dpi or 72),
            )
        elif suffix == ".webp":
            image.save(temp, format="WEBP", quality=quality, method=4)
        else:
            raise ValueError(f"unsupported preview format: {destination.suffix}")
        os.replace(temp, destination)
        return image.size
    finally:
        document.close()


def rerender_existing_previews(
    discipline: str,
    *,
    dpi: int,
    preview_format: str,
    preview_quality: int,
    force: bool,
) -> dict[str, Any]:
    """Перерендерить только картинки опубликованной POS-витрины.

    Источники и имена берутся из уже опубликованного manifest/index. Поэтому
    этот режим не перестраивает CTX-пакеты, графы, профили и номера страниц.
    Индекс переключается на новые файлы атомарно и только после завершения всей
    дисциплины; при прерывании UI продолжит читать прежние WebP.
    """
    document_code = f"ВЕКТОГРАФ — {discipline}"
    output_dir = (
        POS_DOCUMENTS / document_code / "versions" / VERSION_ID
        / "03_analysis" / "latest"
    )
    blocks_dir = output_dir / "blocks_stage02_100"
    index_path = blocks_dir / "index.json"
    manifest_path = output_dir / "vector_graph_gallery.json"
    index_payload = json_read(index_path)
    manifest_payload = json_read(manifest_path)
    if not isinstance(index_payload, dict) or not isinstance(index_payload.get("blocks"), list):
        raise RuntimeError(f"invalid gallery index: {index_path}")
    if not isinstance(manifest_payload, dict) or not isinstance(manifest_payload.get("blocks"), list):
        raise RuntimeError(f"invalid gallery manifest: {manifest_path}")

    manifest_by_id = {
        str(item.get("block_id")): item
        for item in manifest_payload["blocks"]
        if isinstance(item, dict) and item.get("block_id")
    }
    extension = {"png": ".png", "jpeg": ".jpg", "webp": ".webp"}[preview_format]
    corpus_root = CORPUS_ROOT.resolve()
    rendered = []

    for position, record in enumerate(index_payload["blocks"], 1):
        if not isinstance(record, dict) or not record.get("block_id"):
            raise RuntimeError(f"invalid gallery block #{position}: {index_path}")
        block_id = str(record["block_id"])
        manifest_record = manifest_by_id.get(block_id)
        if manifest_record is None:
            raise RuntimeError(f"block {block_id} is absent from {manifest_path}")
        source_rel = str(manifest_record.get("source_pdf") or "")
        source_pdf = (ROOT / source_rel).resolve()
        try:
            source_pdf.relative_to(corpus_root)
        except ValueError as exc:
            raise RuntimeError(f"source escapes gallery corpus: {source_rel}") from exc
        if not source_pdf.is_file():
            raise RuntimeError(f"gallery source not found: {source_pdf}")

        old_file = str(record.get("file") or "")
        if not old_file or Path(old_file).name != old_file:
            raise RuntimeError(f"unsafe preview name for block {block_id}: {old_file!r}")
        image_name = Path(old_file).with_suffix(extension).name
        width, height = render_preview(
            source_pdf,
            blocks_dir / image_name,
            max_px=0,
            quality=preview_quality,
            dpi=dpi,
            force=force,
        )
        rendered.append((record, manifest_record, image_name, width, height))
        if position % 25 == 0 or position == len(index_payload["blocks"]):
            print(
                f"  {discipline}: {position}/{len(index_payload['blocks'])} @ {dpi} DPI",
                flush=True,
            )

    rendered_at = dt.datetime.now(dt.timezone.utc).isoformat()
    for record, manifest_record, image_name, width, height in rendered:
        record.update({
            "file": image_name,
            "dpi": dpi,
            "render_size": [width, height],
        })
        manifest_record.update({
            "file": image_name,
            "preview_dpi": dpi,
            "preview_render_size": [width, height],
        })
    index_payload.update({
        "dpi": dpi,
        "preview_format": preview_format,
        "preview_rendered_at": rendered_at,
    })
    manifest_payload["preview"] = {
        "dpi": dpi,
        "format": preview_format,
        "quality": "lossless" if preview_format == "png" else preview_quality,
        "rendered_at": rendered_at,
    }
    atomic_json(index_path, index_payload)
    atomic_json(manifest_path, manifest_payload)
    return {
        "document": document_code,
        "blocks": len(rendered),
        "dpi": dpi,
        "format": preview_format,
        "preview_dir": str(blocks_dir.relative_to(ROOT)),
    }


def create_catalog_pdf(path: Path, discipline: str, blocks: list[GalleryBlock]) -> None:
    document = fitz.open()
    page = document.new_page(width=842, height=595)
    graph_total = sum(block_has_graph(block) for block in blocks)
    profiles_total = len({block.profile_id for block in blocks})
    lines = [
        "VECTOGRAPH REVIEW GALLERY",
        f"ALIA / POS / {discipline}",
        f"Blocks: {len(blocks)}",
        f"Prepared graphs: {graph_total}",
        f"Profiles: {profiles_total}",
        "Open the Blocks view in the application to review every source crop.",
    ]
    y = 90
    for index, line in enumerate(lines):
        page.insert_text(
            (72, y),
            line,
            fontsize=24 if index == 0 else 16,
            fontname="helv",
            color=(0.1, 0.1, 0.1),
        )
        y += 55 if index == 0 else 34
    temp = path.with_name(f"{path.name}.gallery-{os.getpid()}.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    document.save(temp)
    document.close()
    os.replace(temp, path)


def graph_markdown(graph: GraphRecord, source_block_id: str) -> str:
    candidate = graph.path.with_name(graph.path.name.replace(".structure.json", ".structure.md"))
    try:
        text = candidate.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        text = ""
    return text or (
        f"# Векторный граф блока {source_block_id}\n\n"
        "Полное представление доступно в JSON графа."
    )


def create_graph_package(
    block: GalleryBlock,
    output_dir: Path,
    source_kind: str,
) -> str:
    from backend.app.pipeline.stages.block_grounding.block_profile_registry import (
        artifact_filename,
        make_package,
    )

    package_dir = output_dir / "block_vector_graphs"
    package_path = package_dir / artifact_filename(block.gallery_block_id)
    classification = {
        "profile_id": block.profile_id,
        "source": "reviewed_experiment_corpus",
        "confidence": "high" if block.profile_id != "unclassified" else "needs_profile",
        "block_title": block.title,
        "description": str(block.metadata.get("description") or block.title),
        "short_description": str(block.metadata.get("subtype") or ""),
        "source_block_id": block.source_block_id,
        "source_pdf": str(block.pdf_path.relative_to(ROOT)),
    }

    if block.graph is None and block.prepared_package is not None:
        package = copy.deepcopy(block.prepared_package)
        graph = package.get("graph")
        if not isinstance(graph, dict):
            block.prepared_package = None
            return create_graph_package(block, output_dir, source_kind)

        effective_kind = str(package.get("source_kind") or source_kind)
        package.update({
            "block_id": block.gallery_block_id,
            "page": block.page,
            "discipline": block.discipline,
            "profile_id": block.profile_id,
        })
        generated_classification = dict(package.get("classification") or {})
        generated_classification.update(classification)
        generated_classification["source"] = "standalone_pdf_ctx"
        package["classification"] = generated_classification

        sidecar_name = f"{artifact_filename(block.gallery_block_id)[:-5]}.graph.json"
        sidecar_path = package_dir / "_graphs" / sidecar_name
        atomic_json(sidecar_path, graph)
        package["graph"] = None
        package["graph_artifact"] = f"_graphs/{sidecar_name}"
        package["gallery"] = {
            "source_block_id": block.source_block_id,
            "source_pdf": block.pdf_path.name,
            "source_graph": None,
            "graph_origin": "generated_ctx",
        }
        atomic_json(package_path, package)
        return effective_kind

    if block.graph is None:
        package = make_package(
            block_id=block.gallery_block_id,
            page=block.page,
            source_kind="image_only",
            discipline=block.discipline,
            profile_id=block.profile_id,
            classification=classification,
            user_text=None,
            error="Для этого сохранённого варианта граф ещё не построен.",
        )
        package["gallery"] = {
            "source_block_id": block.source_block_id,
            "source_pdf": block.pdf_path.name,
        }
        atomic_json(package_path, package)
        return "image_only"

    graph = json_read(block.graph.path, {})
    if not isinstance(graph, dict):
        block.graph = None
        return create_graph_package(block, output_dir, source_kind)

    effective_kind = (
        "structured_singleline"
        if block.profile_id == "electrical_singleline"
        else source_kind
    )
    markdown = graph_markdown(block.graph, block.source_block_id)
    package = make_package(
        block_id=block.gallery_block_id,
        page=block.page,
        source_kind=effective_kind,
        discipline=block.discipline,
        profile_id=block.profile_id,
        classification=classification,
        graph=graph,
        gate={"use": True, "complete": bool((graph.get("readiness") or {}).get("complete", True))},
        markdown=markdown,
        user_text=f"# Блок {block.gallery_block_id}\n\n{markdown}\n\n{TASK}",
    )

    sidecar_name = f"{artifact_filename(block.gallery_block_id)[:-5]}.graph.json"
    sidecar_path = package_dir / "_graphs" / sidecar_name
    atomic_hardlink(block.graph.path, sidecar_path)
    package["graph"] = None
    package["graph_artifact"] = f"_graphs/{sidecar_name}"
    package["gallery"] = {
        "source_block_id": block.source_block_id,
        "source_pdf": block.pdf_path.name,
        "source_graph": str(block.graph.path.relative_to(ROOT)),
        "graph_origin": "saved_experiment_graph",
    }
    atomic_json(package_path, package)
    return effective_kind


def project_markdown(discipline: str, blocks: list[GalleryBlock]) -> str:
    counts = collections.Counter(block.page_label for block in blocks)
    lines = [
        f"# ВЕКТОГРАФ — {discipline}",
        "",
        f"Витрина содержит **{len(blocks)}** сохранённых графических блоков.",
        "Откройте раздел «Блоки» проекта и выбирайте тип графа в верхней навигации.",
        "",
        "## Типы блоков",
        "",
    ]
    lines.extend(f"- {label}: {count}" for label, count in counts.items())
    return "\n".join(lines) + "\n"


def build_document(
    discipline: str,
    blocks: list[GalleryBlock],
    *,
    execute: bool,
    max_preview_px: int,
    preview_quality: int,
) -> dict[str, Any]:
    assign_profile_groups(blocks)
    document_code = f"ВЕКТОГРАФ — {discipline}"
    doc_dir = POS_DOCUMENTS / document_code
    version_dir = doc_dir / "versions" / VERSION_ID
    output_dir = version_dir / "03_analysis" / "latest"
    blocks_dir = output_dir / "blocks_stage02_100"

    if not execute:
        return {
            "document": document_code,
            "blocks": len(blocks),
            "graphs": sum(block_has_graph(block) for block in blocks),
            "profiles": len({block.profile_id for block in blocks}),
        }

    for path in (
        version_dir / "01_input",
        version_dir / "02_work",
        output_dir,
        version_dir / "04_review",
        version_dir / "05_export",
        version_dir / "99_service",
        blocks_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)

    source_counts: collections.Counter = collections.Counter()
    index_blocks = []
    manifest_blocks = []
    result_pages: dict[int, list] = collections.defaultdict(list)
    document_pages: dict[int, list] = collections.defaultdict(list)

    for index, block in enumerate(blocks, 1):
        image_name = f"block_{block.gallery_block_id}.webp"
        render_preview(block.pdf_path, blocks_dir / image_name, max_preview_px, preview_quality)
        source_kind = create_graph_package(block, output_dir, DISCIPLINES[discipline])
        source_counts[source_kind] += 1
        record = {
            "block_id": block.gallery_block_id,
            "source_block_id": block.source_block_id,
            "block_type": "image",
            "page": block.page,
            "page_label": block.page_label,
            "file": image_name,
            "ocr_label": block.title,
            "profile_id": block.profile_id,
            "region_image_uses_crop": True,
        }
        index_blocks.append(record)
        result_block = {
            "id": block.gallery_block_id,
            "block_id": block.gallery_block_id,
            "block_type": "image",
            "page_index": 0,
            "coords_norm": [0.0, 0.0, 1.0, 1.0],
            "pdfplumber_text": "",
            "ocr_text": str(block.metadata.get("description") or ""),
        }
        result_pages[block.page].append(result_block)
        document_pages[block.page].append({
            "id": block.gallery_block_id,
            "block_id": block.gallery_block_id,
            "block_type": "image",
            "coords_norm": [0.0, 0.0, 1.0, 1.0],
            "profile_id": block.profile_id,
        })
        manifest_blocks.append({
            **record,
            "source_pdf": str(block.pdf_path.relative_to(ROOT)),
            "source_graph": str(block.graph.path.relative_to(ROOT)) if block.graph else None,
            "graph_origin": (
                "saved_experiment_graph" if block.graph
                else "generated_ctx" if block.prepared_package
                else None
            ),
            "source_kind": source_kind,
        })
        if index % 25 == 0 or index == len(blocks):
            print(f"  {discipline}: {index}/{len(blocks)}", flush=True)

    index_payload = {
        "schema_version": 1,
        "gallery": True,
        "total_blocks": len(index_blocks),
        "total_expected": len(index_blocks),
        "errors": 0,
        "failed_block_ids": [],
        "blocks": index_blocks,
    }
    atomic_json(blocks_dir / "index.json", index_payload)

    context_blocks = [
        {
            "block_id": item["block_id"],
            "page": item["page"],
            "source_kind": item["source_kind"],
            "coverage_status": (
                "ready" if item["source_kind"] != "image_only" else "missing_graph"
            ),
            "warnings": (
                [] if item["source_kind"] != "image_only" else ["граф ещё не построен"]
            ),
        }
        for item in manifest_blocks
    ]
    from backend.app.pipeline.stages.block_context.reference_catalog import catalog_runtime_info

    atomic_json(output_dir / "block_context_summary.json", {
        "schema_version": 2,
        "stage": "block_context",
        "pipeline_block": "block_vector_graph",
        "pipeline_block_title": "Векторные графы блоков",
        "status": "ok",
        "gallery": True,
        "reference_catalog": catalog_runtime_info(),
        "blocks_total": len(blocks),
        "blocks_ready": sum(item["source_kind"] != "image_only" for item in manifest_blocks),
        "blocks_failed": sum(item["source_kind"] == "image_only" for item in manifest_blocks),
        "source_counts": dict(source_counts),
        "blocks": context_blocks,
    })
    atomic_json(output_dir / "vector_graph_gallery.json", {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "discipline": discipline,
        "document_code": document_code,
        "source_root": str(CORPUS_ROOT.relative_to(ROOT)),
        "blocks_total": len(blocks),
        "graphs_total": sum(block_has_graph(block) for block in blocks),
        "profiles_total": len({block.profile_id for block in blocks}),
        "source_counts": dict(source_counts),
        "blocks": manifest_blocks,
    })

    result_json = {
        "schema_version": 1,
        "gallery": True,
        "pages": [
            {"page_number": page, "page_index": 0, "blocks": result_pages[page]}
            for page in sorted(result_pages)
        ],
    }
    document_graph = {
        "schema_version": 1,
        "gallery": True,
        "pages": [
            {
                "page": page,
                "page_number": page,
                "page_index": 0,
                "sheet_name": blocks_on_page[0]["profile_id"] if blocks_on_page else "",
                "text": "",
                "image_blocks": blocks_on_page,
            }
            for page, blocks_on_page in sorted(document_pages.items())
        ],
    }
    atomic_json(version_dir / "02_work" / "result.json", result_json)
    atomic_json(output_dir / "document_graph.json", document_graph)
    atomic_json(output_dir / "01_blocks_analysis.json", {"schema_version": 1, "block_analyses": []})
    atomic_json(output_dir / "02_text_analysis.json", {"schema_version": 1, "pages": []})
    atomic_json(output_dir / "03_findings.json", {"schema_version": 1, "findings": []})
    atomic_json(output_dir / "optimization.json", {"schema_version": 1, "optimizations": []})

    markdown = project_markdown(discipline, blocks)
    input_name = f"{document_code}_document.md"
    atomic_text(version_dir / "01_input" / input_name, markdown)
    atomic_text(version_dir / "02_work" / "document.md", markdown)
    pdf_name = f"{document_code}.pdf"
    input_pdf = version_dir / "01_input" / pdf_name
    create_catalog_pdf(input_pdf, discipline, blocks)
    atomic_hardlink(input_pdf, version_dir / "02_work" / "document.pdf")

    object_meta = json_read(ALIA_OBJECT / "object.json", {})
    object_id = str(object_meta.get("object_id") or "73a0e59a")
    project_info = {
        "project_id": document_code,
        "document_code": document_code,
        "name": document_code,
        "section": "POS",
        "description": f"Витрина профильных векторных графов {discipline}",
        "pdf_file": pdf_name,
        "pdf_files": [pdf_name],
        "md_file": input_name,
        "md_files": [input_name],
        "version_id": VERSION_ID,
        "version_label": "Корпус экспериментов",
        "version_source": "vector_graph_gallery",
    }
    atomic_json(version_dir / "01_input" / "project_info.json", project_info)
    atomic_json(version_dir / "01_input" / "input_manifest.json", {
        "schema_version": 1,
        "version_id": VERSION_ID,
        "source": "experiments/блоки разных дисциплин",
        "input_quad": [
            {"role": "pdf", "legacy_name": pdf_name, "present": True},
            {"role": "document_md", "legacy_name": input_name, "present": True},
        ],
        "missing_optional_files": ["ocr_html"],
    })
    atomic_json(version_dir / "version.json", {
        "schema_version": 1,
        "version_id": VERSION_ID,
        "version_no": 1,
        "label": "Корпус экспериментов",
        "source": "vector_graph_gallery",
        "status": "review_gallery",
        "analysis_status": "complete",
        "missing_analysis_files": [],
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "project_info": project_info,
    })
    atomic_json(doc_dir / "document.json", {
        "schema_version": 1,
        "document_code": document_code,
        "object_id": object_id,
        "discipline": "POS",
        "kind": "plain",
        "source": "vector_graph_gallery",
        "versions": [{
            "version_id": VERSION_ID,
            "version_no": 1,
            "label": "Корпус экспериментов",
            "source": "vector_graph_gallery",
            "status": "review_gallery",
        }],
        "version_ids": [VERSION_ID],
        "current_version": VERSION_ID,
    })
    atomic_text(doc_dir / "current_version.txt", VERSION_ID + "\n")

    return {
        "document": document_code,
        "blocks": len(blocks),
        "graphs": sum(block_has_graph(block) for block in blocks),
        "profiles": len({block.profile_id for block in blocks}),
        "preview_dir": str(blocks_dir.relative_to(ROOT)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="создать/обновить документы ALIA/POS")
    parser.add_argument(
        "--previews-only",
        action="store_true",
        help="перерендерить картинки уже опубликованных POS-витрин, не меняя графы",
    )
    parser.add_argument(
        "--disciplines",
        nargs="*",
        choices=tuple(DISCIPLINES),
        default=list(DISCIPLINES),
        help="ограничить набор дисциплин",
    )
    parser.add_argument("--max-preview-px", type=int, default=2200)
    parser.add_argument("--preview-quality", type=int, default=90)
    parser.add_argument("--preview-dpi", type=int, default=300)
    parser.add_argument(
        "--preview-format",
        choices=("png", "jpeg", "webp"),
        default="png",
        help="для точных 300 DPI рекомендуется png (у webp предел 16383 px)",
    )
    parser.add_argument(
        "--force-previews",
        action="store_true",
        help="перезаписать даже актуальные файлы превью",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    if not CORPUS_ROOT.is_dir():
        raise SystemExit(f"corpus not found: {CORPUS_ROOT}")
    if not (ALIA_OBJECT / "object.json").is_file():
        raise SystemExit(f"ALIA object not found: {ALIA_OBJECT}")

    if args.previews_only:
        if not args.execute:
            raise SystemExit("--previews-only requires --execute")
        if args.preview_dpi < 72 or args.preview_dpi > 600:
            raise SystemExit("--preview-dpi must be between 72 and 600")
        summaries = [
            rerender_existing_previews(
                discipline,
                dpi=args.preview_dpi,
                preview_format=args.preview_format,
                preview_quality=max(50, min(100, args.preview_quality)),
                force=args.force_previews,
            )
            for discipline in args.disciplines
        ]
        print(json.dumps({
            "mode": "previews_only",
            "documents": summaries,
            "totals": {
                "blocks": sum(item["blocks"] for item in summaries),
                "documents": len(summaries),
            },
        }, ensure_ascii=False, indent=2))
        return 0

    summaries = []
    for discipline in args.disciplines:
        blocks = discover_blocks(discipline)
        summaries.append(build_document(
            discipline,
            blocks,
            execute=args.execute,
            max_preview_px=max(600, args.max_preview_px),
            preview_quality=max(50, min(100, args.preview_quality)),
        ))

    print(json.dumps({
        "mode": "execute" if args.execute else "dry_run",
        "documents": summaries,
        "totals": {
            "blocks": sum(item["blocks"] for item in summaries),
            "graphs": sum(item["graphs"] for item in summaries),
            "documents": len(summaries),
        },
    }, ensure_ascii=False, indent=2))
    if not args.execute:
        print("\nДобавьте --execute, чтобы создать витрину.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
