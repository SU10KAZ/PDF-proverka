"""Номер листа в замечаниях и сплошная нумерация F-ID.

Два прод-дефекта, найденные на AR/СТ26_01-14-АР2-6-1-РД_V1:

1. В столбце «Лист» вместо «Лист 2» выводилось НАЗВАНИЕ листа из штампа
   («Корпус 14.6. Маркировочные планы 1 этажа»). Корень — stage 02 читал граф
   по v1-ключу `sheet_no` (в v2-графе он всегда None) и падал в fallback на
   `sheet_name`; занятое поле потом блокировало детерминированный бэкфилл.
2. Нумерация шла «F-016, F-035, F-036, F-017»: atomicity_guard вставляет
   расщеплённые замечания в середину списка с хвостовыми номерами, а сплошная
   перенумерация выполнялась только как побочный эффект merge/dedup.
"""
from __future__ import annotations

import json

import backend.app.pipeline.stages.report.generate_excel_report as ger
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import sheet_for_page
from backend.app.pipeline.stages.findings_merge.runner import (
    backfill_text_evidence_in_findings,
    renumber_findings_sequentially,
)
from backend.app.pipeline.stages.prepare.graph_builder import looks_like_sheet_ref


V2_GRAPH = {
    "version": 2,
    "pages": [
        {"page": 3, "sheet_no": None, "sheet_no_raw": "1",
         "sheet_name": "Корпус 14.6. Маркировочные планы 1 этажа",
         "text_blocks": [{"id": "blk_text_p3"}]},
        {"page": 4, "sheet_no": None, "sheet_no_raw": "2",
         "sheet_name": "Корпус 14.6. Маркировочные планы 1 этажа",
         "text_blocks": [{"id": "blk_text_p4"}],
         "image_blocks": [{"block_id": "blk_img_p4"}]},
        {"page": 9, "sheet_no": None, "sheet_no_raw": None, "sheet_name": None},
    ],
}


# ─── looks_like_sheet_ref ────────────────────────────────────────────────────

def test_looks_like_sheet_ref_accepts_sheet_numbers():
    assert looks_like_sheet_ref("Лист 2")
    assert looks_like_sheet_ref("Листы 2, 4")
    assert looks_like_sheet_ref("Лист 31.11")
    assert looks_like_sheet_ref("2")
    assert looks_like_sheet_ref("13-14")
    assert looks_like_sheet_ref(7)


def test_looks_like_sheet_ref_rejects_stamp_titles():
    assert not looks_like_sheet_ref("Корпус 14.6. Маркировочные планы 1 этажа")
    assert not looks_like_sheet_ref("Схема ВРУ")
    assert not looks_like_sheet_ref("")
    assert not looks_like_sheet_ref(None)


# ─── stage 02: sheet_for_page на v2-графе ────────────────────────────────────

def test_sheet_for_page_reads_v2_sheet_no_raw():
    assert sheet_for_page(V2_GRAPH, 4) == "Лист 2"


def test_sheet_for_page_falls_back_to_name_only_without_number():
    assert sheet_for_page(V2_GRAPH, 9) is None
    v1_graph = {"pages": [{"page": 1, "sheet_no": "5", "sheet_name": "План"}]}
    assert sheet_for_page(v1_graph, 1) == "Лист 5"


# ─── findings_merge: бэкфилл перекрывает название листа ──────────────────────

def _write_case(tmp_path, findings):
    (tmp_path / "document_graph.json").write_text(
        json.dumps(V2_GRAPH, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "03_findings.json").write_text(
        json.dumps({"findings": findings}, ensure_ascii=False), encoding="utf-8")


def _read_findings(tmp_path):
    return json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))["findings"]


def test_backfill_overrides_sheet_title_with_number(tmp_path):
    _write_case(tmp_path, [
        {"id": "F-001", "page": 4,
         "sheet": "Корпус 14.6. Маркировочные планы 1 этажа"},
    ])
    backfill_text_evidence_in_findings("pid", output_dir=tmp_path)
    f = _read_findings(tmp_path)[0]
    assert f["sheet"] == "Лист 2"
    assert f["sheet_title"] == "Корпус 14.6. Маркировочные планы 1 этажа"


def test_backfill_keeps_title_when_page_not_in_map(tmp_path):
    _write_case(tmp_path, [
        {"id": "F-001", "page": 42, "sheet": "Корпус 14.6. Маркировочные планы"},
    ])
    backfill_text_evidence_in_findings("pid", output_dir=tmp_path)
    f = _read_findings(tmp_path)[0]
    # Номер не найден — название не теряем и не помечаем «лист недоступен».
    assert f["sheet"] == "Корпус 14.6. Маркировочные планы"
    assert not f.get("sheet_unavailable")


def test_backfill_does_not_touch_correct_sheet(tmp_path):
    _write_case(tmp_path, [{"id": "F-001", "page": 3, "sheet": "Листы 1, 2"}])
    backfill_text_evidence_in_findings("pid", output_dir=tmp_path)
    assert _read_findings(tmp_path)[0]["sheet"] == "Листы 1, 2"


def test_backfill_marks_unavailable_when_empty_and_unmapped(tmp_path):
    _write_case(tmp_path, [{"id": "F-001", "page": 42, "sheet": ""}])
    backfill_text_evidence_in_findings("pid", output_dir=tmp_path)
    f = _read_findings(tmp_path)[0]
    assert f["sheet_unavailable"] is True
    assert f["sheet_unavailable_reason"] == "page_not_in_map"


def test_backfill_resolves_sheet_from_blocks_when_no_page(tmp_path):
    """Замечания текстового этапа приходят без page — лист берём по блокам."""
    _write_case(tmp_path, [
        {"id": "F-001", "sheet": None, "page": None,
         "source_block_ids": ["blk_text_p4"]},
        {"id": "F-002", "sheet": None, "page": None,
         "evidence": [{"type": "text", "block_id": "blk_text_p3", "page": None}]},
        {"id": "F-003", "sheet": None, "page": None},
    ])
    backfill_text_evidence_in_findings("pid", output_dir=tmp_path)
    result = _read_findings(tmp_path)
    assert result[0]["sheet"] == "Лист 2"
    assert result[1]["sheet"] == "Лист 1"
    # ссылок на блоки нет — честно помечаем, что лист не выводится
    assert result[2]["sheet_unavailable"] is True
    assert result[2]["sheet_unavailable_reason"] == "no_page"


# ─── Excel: display-side восстановление номера ───────────────────────────────

def test_excel_normalize_findings_sheets(tmp_path):
    (tmp_path / "document_graph.json").write_text(
        json.dumps(V2_GRAPH, ensure_ascii=False), encoding="utf-8")
    findings = [
        {"id": "F-001", "page": 4, "sheet": "Корпус 14.6. Маркировочные планы 1 этажа"},
        {"id": "F-002", "page": 3, "sheet": "Лист 1"},
        {"id": "F-003", "page": 42, "sheet": "Неизвестный лист"},
        {"id": "F-004", "sheet": None, "page": None,
         "related_block_ids": ["blk_img_p4"]},
    ]
    fixed = ger.normalize_findings_sheets(
        findings, str(tmp_path / "03_findings.json"))
    assert fixed == 2
    assert findings[0]["sheet"] == "Лист 2"
    assert ger.f_sheet(findings[0], 1) == "Лист 2"
    assert findings[1]["sheet"] == "Лист 1"        # корректное не трогаем
    assert findings[2]["sheet"] == "Неизвестный лист"  # номера нет — как есть
    assert findings[3]["sheet"] == "Лист 2"        # выведен по блоку


# ─── сплошная нумерация F-ID ─────────────────────────────────────────────────

def test_renumber_fixes_atomicity_guard_tail_ids(tmp_path):
    findings = [
        {"id": "F-013"},
        {"id": "F-032", "atomicity_guard": {"split_from": "F-013"}},
        {"id": "F-014"},
        {"id": "F-016"},
        {"id": "F-035", "atomicity_guard": {"split_from": "F-016"}},
    ]
    (tmp_path / "03_findings.json").write_text(
        json.dumps({"findings": findings}, ensure_ascii=False), encoding="utf-8")

    report = renumber_findings_sequentially("pid", output_dir=tmp_path)
    result = _read_findings(tmp_path)

    assert [f["id"] for f in result] == ["F-001", "F-002", "F-003", "F-004", "F-005"]
    assert report["renumbered"] == 5
    # ссылка split_from переехала вместе с номерами
    assert result[1]["atomicity_guard"]["split_from"] == "F-001"
    assert result[4]["atomicity_guard"]["split_from"] == "F-004"


def test_renumber_is_noop_on_sequential_ids(tmp_path):
    (tmp_path / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001"}, {"id": "F-002"}]}, ensure_ascii=False),
        encoding="utf-8")
    before = (tmp_path / "03_findings.json").read_text(encoding="utf-8")
    report = renumber_findings_sequentially("pid", output_dir=tmp_path)
    assert report == {"renumbered": 0}
    assert (tmp_path / "03_findings.json").read_text(encoding="utf-8") == before


def test_renumber_missing_file_returns_none(tmp_path):
    assert renumber_findings_sequentially("pid", output_dir=tmp_path) is None
