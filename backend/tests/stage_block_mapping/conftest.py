"""Synthetic comparison world for the stage-2 block mapping read API.

A session/pair with upload artifacts (PDF bytes, blocks.json, Markdown) and one
frozen V3 run whose page.json rows are derived from those artifacts exactly like
source_prep does, plus Human Mapping ui_data built by hm_builder. Everything
lives in tmp_path; the live COMPARISON_ROOT, .env providers and models are never
touched.
"""
from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import run_storage as rs
from backend.app.services.project_change_v3 import source_prep
from backend.app.services.project_change_v3.hm_builder import build_human_mapping_ui_data

SID, PID, OID, RUN = "sess", "pair", "object", "run1"

# side → (pages meta, blocks, markdown bodies). Page 2 of OLD is rotated (270).
SOURCES = {
    "OLD": {
        "pages": [{"page_index": 0, "width_px": 2000, "height_px": 1000, "rotation": 0},
                  {"page_index": 1, "width_px": 2000, "height_px": 1414, "rotation": 270}],
        "blocks": [("o1", "text", 0, [0.1, 0.1, 0.4, 0.3]), ("o2", "image", 0, [0.5, 0.1, 0.9, 0.8]),
                   ("o3", "text", 1, [0.1, 0.1, 0.9, 0.5]), ("os", "stamp", 1, [0.7, 0.9, 0.99, 0.99])],
        "md": {"o1": "Текст OLD 1", "o2": "Схема OLD", "o3": "| А | Б |\n|---|---|\n| 1 | 2 |"},
    },
    "NEW": {
        "pages": [{"page_index": 0, "width_px": 1000, "height_px": 1414, "rotation": 0}],
        "blocks": [("n1", "text", 0, [0.1, 0.1, 0.5, 0.3]), ("n2", "image", 0, [0.1, 0.4, 0.9, 0.9]),
                   ("ns", "stamp", 0, [0.6, 0.92, 0.98, 0.99])],
        "md": {"n1": "Текст NEW 1", "n2": "Схема NEW"},
    },
}


def png_bytes(width: int, height: int) -> bytes:
    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
    raw = b"".join(b"\x00" + b"\xff" * width for _ in range(height))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b""))


def write_sources(root: Path) -> dict[str, dict[str, Path]]:
    documents = {}
    for side, spec in SOURCES.items():
        work = root / "uploads" / side.lower() / "02_work"
        work.mkdir(parents=True, exist_ok=True)
        (work / "document.pdf").write_bytes(f"%PDF synthetic {side}".encode())
        blocks = [{"block_id": bid, "block_type": kind, "page_index": page, "coords_norm": bbox}
                  for bid, kind, page, bbox in spec["blocks"]]
        (work / "blocks.json").write_text(json.dumps({"pages": spec["pages"], "blocks": blocks}), encoding="utf-8")
        md = "\n\n".join(f"### BLOCK #{i} [{'IMAGE' if k == 'image' else 'TEXT'}]: {bid}\n{spec['md'].get(bid, '')}"
                         for i, (bid, k, _p, _b) in enumerate(spec["blocks"], 1))
        (work / "document.md").write_text(md + "\n", encoding="utf-8")
        documents[side] = {"pdf": work / "document.pdf", "blocks": work / "blocks.json", "markdown": work / "document.md"}
    return documents


def region(rid, olds, news):
    ref = lambda side, bid: {"side": side, "physical_page": _page_of(side, bid), "block_id": bid,
                             "block_type": "TEXT", "relevance": "synthetic"}
    return {"region_id": rid, "old_pages": sorted({_page_of("OLD", b) for b in olds}) or [1],
            "new_pages": sorted({_page_of("NEW", b) for b in news}) or [1], "engineering_domain": f"Домен {rid}",
            "scope": "synthetic", "locations": [], "reason_for_correspondence": "synthetic",
            "important_text_blocks": [ref("OLD", b) for b in olds] + [ref("NEW", b) for b in news],
            "important_table_blocks": [], "important_graphic_blocks": [], "confidence": 0.6}


def _page_of(side, bid):
    return next(page for b, _k, page, _bbox in SOURCES[side]["blocks"] if b == bid) + 1


def build_run(documents, run_id=RUN, regions=None):
    directory = rs.create(SID, PID, run_id, OID)
    work = directory / "project_change_v3"
    structure = []
    for side in ("OLD", "NEW"):
        payload = json.loads(documents[side]["blocks"].read_text(encoding="utf-8"))
        md = source_prep.parse_md_blocks(documents[side]["markdown"])
        for meta in payload["pages"]:
            page_no = meta["page_index"] + 1
            page_dir = work / "source" / side.lower() / f"p{page_no:03d}"
            page_dir.mkdir(parents=True, exist_ok=True)
            w, h = meta["width_px"], meta["height_px"]  # display orientation, like the real rasters
            scale = 60 / max(w, h)
            (page_dir / "full_page.png").write_bytes(png_bytes(max(1, round(w * scale)), max(1, round(h * scale))))
            rows = []
            for block in [b for b in payload["blocks"] if b["page_index"] + 1 == page_no]:
                body = md.get(block["block_id"], "")
                tables = [m.group().strip() for m in source_prep._TABLE_RE.finditer(body)]
                modality = source_prep._modality(block["block_type"], tables)
                crop_ref = str(page_dir / f"{block['block_id']}.png") if modality == "GRAPHIC" else ""
                if crop_ref:
                    Path(crop_ref).write_bytes(png_bytes(4, 4))
                rows.append({"block_id": block["block_id"], "modality": modality, "source_block_type": block["block_type"],
                             "bbox": block["coords_norm"], "structured_md": body, "tables": tables,
                             "existing_description": body if modality == "GRAPHIC" else "",
                             "graphic_crop_ref": crop_ref, "graphic_crop_sha256": "0" * 64 if crop_ref else ""})
            (page_dir / "page.json").write_text(json.dumps({
                "side": side, "source_pdf": str(documents[side]["pdf"]), "source_pdf_sha256": rs.sha(documents[side]["pdf"]),
                "physical_page": page_no, "blocks": rows, "native_page_text": "", "full_page_ref": "/elsewhere",
                "full_page_sha256": ""}), encoding="utf-8")
            structure.append({"side": side, "physical_page": page_no, "blocks": [
                {"block_id": r["block_id"], "modality": r["modality"], "bbox": r["bbox"], "structured_md": r["structured_md"],
                 "tables": r["tables"], "graphic_crop_ref": r["graphic_crop_ref"]} for r in rows]})
    structure_file = work / "DOCUMENT_STRUCTURE.json"
    rs.atomic(structure_file, structure)
    from backend.app.services.project_change_v3.engine import _source_identity
    manifest = {"old_pdf_sha256": rs.sha(documents["OLD"]["pdf"]), "new_pdf_sha256": rs.sha(documents["NEW"]["pdf"]),
                "structure_sha256": rs.sha(structure_file)}
    semantic_map = {"pair": PID, "regions": regions or [region("R1", ["o1", "o2"], ["n1", "n2"]),
                                                         region("R2", ["o3"], ["n1"])],
                    "unmatched_old": [], "unmatched_new": [], "coverage_notes": []}
    hm = build_human_mapping_ui_data(pair_id=PID, object_id=OID, semantic_map=semantic_map, work_dir=work)
    hm.update(run_id=run_id, session_id=SID)
    rs.atomic(directory / "project_change_v3_result.json", {
        "run_id": run_id, "session_id": SID, "pair_id": PID, "object_id": OID, "source_manifest": manifest,
        "source_identity": _source_identity(documents), "projectchanges": [], "unresolved_hints": [], "provenance": {}})
    rs.atomic(directory / "project_change_v3_semantic_map.json", semantic_map)
    rs.atomic(directory / "human_mapping" / "ui_data.json", hm)
    (directory / "human_mapping" / "assets" / "old" / "p001").mkdir(parents=True, exist_ok=True)
    (directory / "human_mapping" / "assets" / "old" / "p001" / "o2.png").write_bytes(png_bytes(4, 4))
    state = {"run_id": run_id, "status": "COMPLETED", "reason_code": "v3_completed"}
    rs.atomic(directory / "state.json", state)
    rs.finalize(SID, PID, run_id, state)
    return directory


def hm_event(directory, kind, **fields):
    """Append a Human Mapping row exactly as the HM router would store it."""
    from backend.app.services.human_mapping_production import storage
    name = "reviews.jsonl" if kind == "review" else "human_block_link_edits.jsonl"
    path = directory / "human_mapping" / name
    rows = storage.read_jsonl(path)
    base = {"object_id": OID, "pair_key": PID, "comparison_id": PID, "run_id": directory.name,
            "timestamp": f"2026-09-25T10:00:{len(rows):02d}+00:00", "reviewer_source": "HUMAN"}
    base["review_id" if kind == "review" else "event_id"] = f"{kind[0]}{len(rows)}"
    storage.append_jsonl(path, {**base, **fields})


@pytest.fixture
def world(tmp_path, monkeypatch):
    root = tmp_path / "comparison"
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    monkeypatch.delenv("STAGE_BLOCK_MAPPING_WRITES", raising=False)
    from backend.app.services.common import object_service
    from backend.app.services.project_change_catalog import catalog
    from backend.app.services.project_change_v3 import provider, scope
    from backend.app.services.stage_block_mapping import service
    from backend.app.services.stage_comparison import paths
    monkeypatch.setattr(scope, "object_id_for_session", lambda sid: OID)
    monkeypatch.setattr(object_service, "_load_objects", lambda: {"objects": [{"id": OID}], "current_id": OID})
    monkeypatch.setattr(catalog, "cached_catalog", lambda **kw: {"entries": []})
    sentinel = provider.FakeProvider(handlers={})
    provider.set_test_provider(sentinel)
    service.clear_cache()
    documents = write_sources(tmp_path)
    rs.atomic(paths.session_json_path(SID), {"kind": "stage_comparison_shell", "schema_version": 1, "id": SID,
                                             "stage_a_path": str(tmp_path / "a"), "stage_b_path": str(tmp_path / "b")})
    doc = lambda side: {"pdf_path": str(documents[side]["pdf"]), "md_path": str(documents[side]["markdown"]),
                        "version_id": "v001", "document_code": side}
    rs.atomic(paths.pair_json_path(SID, PID), {"kind": "selected_pdf_pair", "id": PID, "left": doc("OLD"), "right": doc("NEW")})
    directory = build_run(documents)
    yield {"root": root, "tmp": tmp_path, "documents": documents, "run_dir": directory, "sentinel": sentinel}
    provider.reset_test_provider()
    service.clear_cache()
