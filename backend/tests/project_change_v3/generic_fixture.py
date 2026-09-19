"""Synthetic, clearly non-A/B comparison for zero-model V3 tests.

object_id  = obj_generic_test
pair_id    = generic_pair_not_A_or_B

Builds two versioned stage folders (the production ``comparison_stage_v1``
layout), a real stage-comparison session via ``store.create_session`` and a
pair with the fixed generic ID, plus deterministic FakeProvider handlers that
cite real block IDs/bboxes/crops of the prepared sources.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OBJECT_ID = "obj_generic_test"
PAIR_ID = "generic_pair_not_A_or_B"

PAGES = {
    "OLD": {
        1: [("o1_text", "text", [0.05, 0.05, 0.60, 0.20], "Расход приточной установки П1 — 1000 м3/ч."),
            ("o1_graphic", "image", [0.10, 0.30, 0.90, 0.80], "Схема установки П1 (описание)."),
            ("o1_extra", "text", [0.70, 0.05, 0.95, 0.20], "Примечание, не относится к изменению.")],
        2: [("o2_text", "text", [0.05, 0.05, 0.80, 0.30], "Уровень шума 45 дБА.")],
    },
    "NEW": {
        1: [("n1_table", "text", [0.05, 0.05, 0.60, 0.25], "| Установка | Расход |\n|---|---|\n| П1 | 1200 м3/ч |"),
            ("n1_graphic", "image", [0.10, 0.30, 0.90, 0.80], "Схема установки П1 (описание)."),
            ("n1_extra", "text", [0.70, 0.05, 0.95, 0.20], "Штамп листа.")],
        2: [("n2_text", "stamp", [0.05, 0.05, 0.80, 0.30], "Уровень шума 45 дБА.")],
    },
}


def _pdf(path: Path, side: str) -> None:
    import fitz

    doc = fitz.open()
    for page_no, blocks in PAGES[side].items():
        page = doc.new_page(width=600, height=400)
        for block_id, _kind, bbox, text in blocks:
            rect = fitz.Rect(bbox[0] * 600, bbox[1] * 400, bbox[2] * 600, bbox[3] * 400)
            page.draw_rect(rect, color=(0.1, 0.3, 0.8), width=1.5)
            page.insert_text((rect.x0 + 4, rect.y0 + 14), f"{side} p{page_no} {block_id}", fontsize=9)
    doc.save(path)
    doc.close()


def _markdown(path: Path, side: str) -> None:
    lines = ["# Document", ""]
    number = 0
    for page_no, blocks in PAGES[side].items():
        lines += [f"## Page {page_no}", ""]
        for block_id, kind, _bbox, text in blocks:
            number += 1
            label = "IMAGE" if kind == "image" else "TEXT"
            lines += [f"### BLOCK #{number} [{label}]: {block_id}", "", text, ""]
    path.write_text("\n".join(lines), encoding="utf-8")


def _blocks(path: Path, side: str) -> None:
    blocks = [
        {"block_id": block_id, "page_index": page_no - 1, "block_type": kind, "coords_norm": bbox}
        for page_no, rows in PAGES[side].items()
        for block_id, kind, bbox, _text in rows
    ]
    path.write_text(json.dumps({"schema_version": 1, "blocks": blocks}), encoding="utf-8")


def build_stage(root: Path, side: str, code: str) -> Path:
    stage = root / ("stage_1" if side == "OLD" else "stage_2")
    doc_dir = stage / "documents" / code
    work = doc_dir / "versions" / "v001" / "02_work"
    work.mkdir(parents=True)
    (stage / "stage.json").write_text(json.dumps({"storage_profile": "comparison_stage_v1"}), encoding="utf-8")
    (doc_dir / "document.json").write_text(
        json.dumps({"document_code": code, "current_version": "v001", "discipline": "OV"}), encoding="utf-8")
    info = doc_dir / "versions" / "v001" / "01_input"
    info.mkdir(parents=True)
    (info / "project_info.json").write_text(json.dumps({"pdf_file": f"{code}.pdf"}), encoding="utf-8")
    _pdf(work / "document.pdf", side)
    _markdown(work / "document.md", side)
    _blocks(work / "blocks.json", side)
    return stage


def build_comparison(tmp_path: Path) -> dict[str, Any]:
    """Real session + fixed generic pair under an isolated COMPARISON_ROOT."""
    from backend.app.services.stage_comparison import paths as paths_mod
    from backend.app.services.stage_comparison import store

    object_root = tmp_path / "projects_v2" / "objects" / OBJECT_ID / "comparison"
    stage_1 = build_stage(object_root, "OLD", "GEN-OV-OLD")
    stage_2 = build_stage(object_root, "NEW", "GEN-OV-NEW")
    session, _warnings = store.create_session(str(stage_1), str(stage_2))
    session_id = session["id"]
    left = session["documents"]["stage_1"][0]
    right = session["documents"]["stage_2"][0]
    pair = {
        "id": PAIR_ID,
        "kind": "selected_pdf_pair",
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "left": left,
        "right": right,
    }
    pair_path = paths_mod.pair_json_path(session_id, PAIR_ID)
    pair_path.parent.mkdir(parents=True, exist_ok=True)
    pair_path.write_text(json.dumps(pair, ensure_ascii=False), encoding="utf-8")
    meta_path = paths_mod.session_json_path(session_id)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["pair_order"] = [*meta.get("pair_order", []), PAIR_ID]
    meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    return {
        "session_id": session_id,
        "stage_1": str(stage_1.resolve()),
        "stage_2": str(stage_2.resolve()),
        "left": left,
        "right": right,
    }


def _block(pages: dict, side: str, page: int, block_id: str) -> dict[str, Any]:
    return next(b for b in pages[(side, page)]["blocks"] if b["block_id"] == block_id)


def _ref(pages: dict, side: str, page: int, block_id: str, relevance: str) -> dict[str, Any]:
    b = _block(pages, side, page, block_id)
    return {"side": side, "physical_page": page, "block_id": block_id,
            "block_type": b["modality"], "relevance": relevance}


def _evidence(pages: dict, side: str, page: int, block_id: str, role: str, fragment: str) -> dict[str, Any]:
    b = _block(pages, side, page, block_id)
    record = pages[(side, page)]
    return {"side": side, "source_pdf": record.get("source_pdf", ""), "physical_page": page,
            "block_id": block_id, "block_type": b["modality"], "bbox": b["bbox"],
            "crop_ref": b.get("graphic_crop_ref", ""), "relevant_fragment": fragment, "evidence_role": role}


def fake_handlers(pair_id: str = PAIR_ID) -> dict[str, Any]:
    """Mapper: R-001 (members on both sides) + R-002 (NEW side EMPTY)."""

    def mapping(**kwargs):
        pages = {(p["side"], p["physical_page"]): p for p in kwargs["data"]["pages"]}
        return {
            "pair": pair_id,
            "regions": [
                {"region_id": "R-001", "old_pages": [1], "new_pages": [1],
                 "engineering_domain": "Вентиляция", "scope": "Приточная установка П1",
                 "locations": ["Лист 1"], "reason_for_correspondence": "Одна установка П1",
                 "important_text_blocks": [_ref(pages, "OLD", 1, "o1_text", "Расход OLD")],
                 "important_table_blocks": [_ref(pages, "NEW", 1, "n1_table", "Расход NEW")],
                 "important_graphic_blocks": [_ref(pages, "OLD", 1, "o1_graphic", "Схема OLD"),
                                              _ref(pages, "NEW", 1, "n1_graphic", "Схема NEW")],
                 "confidence": 0.9},
                {"region_id": "R-002", "old_pages": [2], "new_pages": [2],
                 "engineering_domain": "Акустика", "scope": "Шум", "locations": ["Лист 2"],
                 "reason_for_correspondence": "Тот же раздел",
                 "important_text_blocks": [_ref(pages, "OLD", 2, "o2_text", "Шум OLD")],
                 "important_table_blocks": [], "important_graphic_blocks": [], "confidence": 0.5},
            ],
            "unmatched_old": [], "unmatched_new": [], "coverage_notes": [],
        }

    def mining(**kwargs):
        data = kwargs["data"]
        region = data["frozen_region"]
        pages = {(p["side"], p["physical_page"]): p for p in data["pages"]}
        if region["region_id"] == "R-002":
            return {"pair": pair_id, "region_id": "R-002", "projectchanges": [],
                    "unresolved_hints": [{
                        "hint_id": "H001", "kind": "UNRESOLVED_HINT", "engineering_subject": "Шум",
                        "suspected_change": "Возможно изменён уровень шума", "old_pages": [2], "new_pages": [2],
                        "evidence_items": [_evidence(pages, "OLD", 2, "o2_text", "OLD_STATE", "45 дБА")],
                        "missing_proof_or_conflict": "Нет доказательства NEW",
                    }], "coverage_notes": []}
        return {
            "pair": pair_id, "region_id": "R-001",
            "projectchanges": [{
                "projectchange_id": "PC-R-001-C001",
                "engineering_subject": "Приточная установка П1",
                "scope": "Система П1", "locations": ["Лист 1"],
                "change_summary": "Расход П1 увеличен с 1000 до 1200 м3/ч.",
                "old_state": "1000 м3/ч", "new_state": "1200 м3/ч",
                "changed_parameters": [{"name": "Расход", "old_value": "1000", "new_value": "1200",
                                        "unit": "м3/ч", "location": "П1"}],
                "old_pages": [1], "new_pages": [1],
                "evidence_items": [
                    _evidence(pages, "OLD", 1, "o1_text", "OLD_STATE", "1000 м3/ч"),
                    _evidence(pages, "OLD", 1, "o1_graphic", "OLD_STATE", "схема П1"),
                    _evidence(pages, "NEW", 1, "n1_table", "NEW_STATE", "1200 м3/ч"),
                    _evidence(pages, "NEW", 1, "n1_graphic", "NEW_STATE", "схема П1"),
                ],
                "modalities": ["TEXT", "TABLE", "GRAPHIC"], "confidence": 0.8,
                "why_one_event": "Один параметр одной установки",
            }],
            "unresolved_hints": [], "coverage_notes": [],
        }

    def dedupe(**kwargs):
        return {"pair": pair_id, "decisions": [
            {"decision": "KEEP_SEPARATE", "projectchange_ids": ["PC-R-001-C001"], "reason": "единственное"}],
            "notes": []}

    return {"MAPPING": mapping, "MINING": mining, "DEDUPE": dedupe}
