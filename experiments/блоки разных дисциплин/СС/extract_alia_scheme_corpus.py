#!/usr/bin/env python3
"""Extract representative ALIA scheme blocks as vector PDFs.

The source page is embedded with ``show_pdf_page`` so text and CAD paths remain
vector.  Polygon blocks additionally receive a real PDF clipping path; using
only their bounding rectangle would leak neighbouring drawings into the crop.
"""

from __future__ import annotations

import json
from pathlib import Path

import fitz


ROOT = Path(__file__).resolve().parents[3]
ALIA_SS = ROOT / "projects/214. Alia (ASTERUS)/SS"
OUTPUT = Path(__file__).resolve().parent


# (block id, stable output name)
CORPUS = (
    ("9P7Y-Q3H3-36Q", "ALIA — 01 СОТ — структурная схема"),
    ("6GYN-H7Q7-GLT", "ALIA — 02 СОУЭ — структурная схема"),
    ("T3P6-ET3J-4Y7", "ALIA — 03 ВОК — структурная схема"),
    ("4R7T-AVPA-L9G", "ALIA — 04 АСКУВ — структурная схема"),
    ("6LJA-WMVK-6DK", "ALIA — 05 АСКУТ — структурная схема"),
    ("66AQ-KA4R-KFF", "ALIA — 06 АК — структурная схема управления"),
    ("QVKG-FHLJ-ALJ", "ALIA — 07 АК — структурная схема диспетчеризации"),
    ("43AF-KGTU-4RY", "ALIA — 08 АСУД — диспетчеризация лифтов"),
    ("4DN7-KLWV-UAF", "ALIA — 09 АСУД — переговорная связь МГН"),
    ("4W4C-L97P-PU7", "ALIA — 10 СОУЭ — коммутация шкафа"),
    ("4JGG-PA6Y-9NX", "ALIA — 11 СОУЭ — компоновка шкафа"),
    ("7MPY-GD9Y-6LG", "ALIA — 12 АК — функциональная схема"),
    ("6K7V-GUMJ-TV3", "ALIA — 13 АК — внешние подключения"),
    ("9XPY-CWMH-UEC", "ALIA — 14 СС-АК-ЭОМ — организация ниш"),
)


def _source_pdf(result_path: Path) -> Path:
    suffix = "_result.json"
    exact = result_path.with_name(result_path.name[: -len(suffix)] + ".pdf")
    if exact.exists():
        return exact
    pdfs = sorted(result_path.parent.glob("*.pdf"))
    if len(pdfs) != 1:
        raise RuntimeError(f"Cannot select source PDF for {result_path}")
    return pdfs[0]


def _find_blocks() -> dict[str, dict]:
    wanted = {block_id for block_id, _ in CORPUS}
    found: dict[str, dict] = {}
    for result_path in ALIA_SS.rglob("*_result.json"):
        data = json.loads(result_path.read_text(encoding="utf-8"))
        for page in data.get("pages", []):
            for block in page.get("blocks", []):
                block_id = block.get("id")
                if block_id not in wanted:
                    continue
                if block_id in found:
                    raise RuntimeError(f"Duplicate block id: {block_id}")
                found[block_id] = {
                    "block": block,
                    "page": page,
                    "result": result_path,
                    "pdf": _source_pdf(result_path),
                }
    missing = wanted - found.keys()
    if missing:
        raise RuntimeError(f"Missing ALIA blocks: {sorted(missing)}")
    return found


def _polygon_norm(block: dict, page_json: dict) -> list[list[float]] | None:
    polygon = block.get("polygon_points_norm")
    if polygon:
        return [[float(x), float(y)] for x, y in polygon]
    points = block.get("polygon_points")
    if not points:
        return None
    width = float(page_json["width"])
    height = float(page_json["height"])
    return [[float(x) / width, float(y) / height] for x, y in points]


def _install_polygon_clip(page: fitz.Page, pdf_points: list[tuple[float, float]]) -> None:
    """Wrap current page contents in a path expressed in PDF user space."""

    commands = [f"{pdf_points[0][0]:.5f} {pdf_points[0][1]:.5f} m"]
    commands.extend(f"{x:.5f} {y:.5f} l" for x, y in pdf_points[1:])
    clip = ("q\n" + "\n".join(commands) + "\nh W n\n").encode("ascii")

    doc = page.parent
    contents = page.get_contents()
    original = b"\n".join(doc.xref_stream(xref) for xref in contents)
    if not contents:
        raise RuntimeError("Output page has no content stream")
    first = contents[0]
    doc.update_stream(first, clip + original + b"\nQ\n")
    doc.xref_set_key(page.xref, "Contents", f"{first} 0 R")


def _extract(entry: dict, output_path: Path) -> dict:
    block = entry["block"]
    page_json = entry["page"]
    page_number = int(page_json["page_number"])
    coords = [float(value) for value in block["coords_norm"]]
    polygon = _polygon_norm(block, page_json)

    with fitz.open(entry["pdf"]) as source:
        source_page = source[page_number - 1]
        width, height = source_page.rect.width, source_page.rect.height
        crop = fitz.Rect(
            coords[0] * width,
            coords[1] * height,
            coords[2] * width,
            coords[3] * height,
        ) & source_page.rect
        if crop.is_empty or crop.width < 1 or crop.height < 1:
            raise RuntimeError(f"Empty crop for {block['id']}")

        # ``show_pdf_page`` does not preserve a source page's /Rotate semantics
        # together with a clip. Copying the actual page and changing CropBox
        # does, and also avoids wrapping all CAD geometry in a Form XObject.
        unrotated_crop = crop * source_page.derotation_matrix
        unrotated_crop.normalize()
        output = fitz.open()
        output.insert_pdf(source, from_page=page_number - 1, to_page=page_number - 1)
        target = output[0]

        if polygon:
            # result.json points use the rendered (rotated) top-left space.
            # Convert them first to the page's unrotated PyMuPDF space and then
            # to native PDF user space used by the copied content streams.
            inverse_pdf_matrix = ~source_page.transformation_matrix
            pdf_points = [
                tuple(
                    fitz.Point(x * width, y * height)
                    * source_page.derotation_matrix
                    * inverse_pdf_matrix
                )
                for x, y in polygon
            ]
            _install_polygon_clip(target, pdf_points)

        target.set_cropbox(unrotated_crop)

        metadata = source.metadata.copy()
        metadata["title"] = output_path.stem
        metadata["subject"] = (
            f"ALIA vector block {block['id']}; source page {page_number}; "
            f"shape {block.get('shape_type', 'rectangle')}"
        )
        output.set_metadata(metadata)
        output.save(output_path, garbage=4, deflate=True)
        output.close()

    with fitz.open(output_path) as check:
        page = check[0]
        return {
            "block_id": block["id"],
            "output": output_path.name,
            "source_pdf": str(entry["pdf"].relative_to(ROOT)),
            "source_page": page_number,
            "shape_type": block.get("shape_type", "rectangle"),
            "polygon_clip": bool(polygon),
            "page_width_pt": round(page.rect.width, 3),
            "page_height_pt": round(page.rect.height, 3),
            "text_characters": len(page.get_text().strip()),
            "drawing_paths": len(page.get_drawings()),
        }


def main() -> None:
    found = _find_blocks()
    manifest = []
    for block_id, name in CORPUS:
        output_path = OUTPUT / f"{name} — {block_id}.pdf"
        manifest.append(_extract(found[block_id], output_path))
        print(f"{block_id}: {output_path.name}")

    manifest_path = OUTPUT / "ALIA_SCHEME_CORPUS.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"manifest: {manifest_path}")


if __name__ == "__main__":
    main()
