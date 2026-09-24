"""V3 fake handlers for a production-shaped Consolidator E2E (synthetic, not DEV5).

Built on the generic V3 fixture sources (two pages per side).  Three Mapper
regions produce:

* the same airflow change of one unit twice — as text/table (R-001) and on the
  scheme (R-002), which the V3 Dedupe keeps separate (different locations);
* a documentary edit of the sheet stamp (R-003);
* two hints both numbered ``H001`` — a noise hint in R-002 and a conflict in
  R-003 that disputes the table value of the R-001 card.
"""
from __future__ import annotations

from typing import Any

from backend.tests.project_change_v3.generic_fixture import PAIR_ID, _evidence, _ref

FRAGMENT = "Расход приточной установки П1 составляет 1000 м3/ч по исходному проекту."


def v3_handlers(pair_id: str = PAIR_ID) -> dict[str, Any]:
    def mapping(**kwargs):
        pages = {(p["side"], p["physical_page"]): p for p in kwargs["data"]["pages"]}
        region = {"engineering_domain": "Вентиляция", "confidence": 0.9, "important_table_blocks": [],
                  "important_graphic_blocks": [], "reason_for_correspondence": "Одна установка П1"}
        return {"pair": pair_id, "regions": [
            {**region, "region_id": "R-001", "old_pages": [1], "new_pages": [1], "scope": "Расход П1",
             "locations": ["Лист 1"], "important_text_blocks": [_ref(pages, "OLD", 1, "o1_text", "Расход")]},
            {**region, "region_id": "R-002", "old_pages": [1, 2], "new_pages": [1, 2], "scope": "Схема П1",
             "locations": ["Схема установки"], "important_text_blocks": []},
            {**region, "region_id": "R-003", "old_pages": [1], "new_pages": [1], "scope": "Оформление",
             "engineering_domain": "Оформление листов", "locations": ["Штамп листа 1"],
             "important_text_blocks": []},
        ], "unmatched_old": [], "unmatched_new": [], "coverage_notes": []}

    def card(pages, cid, subject, evidence, param=True):
        return {"projectchange_id": cid, "engineering_subject": subject, "scope": "Система П1",
                "locations": ["Лист 1"], "change_summary": "Расход П1 увеличен с 1000 до 1200 м3/ч.",
                "old_state": "1000 м3/ч", "new_state": "1200 м3/ч",
                "changed_parameters": ([{"name": "Расход", "old_value": "1000", "new_value": "1200", "unit": "м3/ч",
                                         "location": "П1"}] if param else []),
                "old_pages": [1], "new_pages": [1], "evidence_items": evidence,
                "modalities": sorted({e["block_type"] for e in evidence}), "confidence": 0.8,
                "why_one_event": "Один параметр одной установки"}

    def mining(**kwargs):
        data = kwargs["data"]
        rid = data["frozen_region"]["region_id"]
        pages = {(p["side"], p["physical_page"]): p for p in data["pages"]}
        if rid == "R-001":
            return {"pair": pair_id, "region_id": rid, "unresolved_hints": [], "coverage_notes": [],
                    "projectchanges": [card(pages, "PC-R-001-C001", "Расход приточной установки П1", [
                        _evidence(pages, "OLD", 1, "o1_text", "OLD_STATE", FRAGMENT),
                        _evidence(pages, "NEW", 1, "n1_table", "NEW_STATE", "П1 1200 м3/ч")])]}
        if rid == "R-002":
            return {"pair": pair_id, "region_id": rid, "coverage_notes": [],
                    "projectchanges": [card(pages, "PC-R-002-C001", "Расход приточной установки П1 на схеме", [
                        _evidence(pages, "OLD", 1, "o1_graphic", "OLD_STATE", FRAGMENT),
                        _evidence(pages, "NEW", 1, "n1_graphic", "NEW_STATE", "На схеме П1 указано 1200 м3/ч")])],
                    "unresolved_hints": [{
                        "hint_id": "H001", "kind": "UNRESOLVED_HINT", "engineering_subject": "Шум",
                        "suspected_change": "Возможно изменён уровень шума", "old_pages": [2], "new_pages": [2],
                        "evidence_items": [_evidence(pages, "OLD", 2, "o2_text", "OLD_STATE", "45 дБА")],
                        "missing_proof_or_conflict": "Нет доказательства NEW"}]}
        return {"pair": pair_id, "region_id": rid, "coverage_notes": [],
                "projectchanges": [{
                    "projectchange_id": "PC-R-003-C001", "engineering_subject": "Штамп листа 1",
                    "scope": "Оформление", "locations": ["Штамп листа 1"],
                    "change_summary": "Изменено оформление штампа листа.", "old_state": "Примечание в штампе.",
                    "new_state": "Штамп листа без примечания.", "changed_parameters": [],
                    "old_pages": [1], "new_pages": [1],
                    "evidence_items": [_evidence(pages, "OLD", 1, "o1_extra", "OLD_STATE", "Примечание"),
                                       _evidence(pages, "NEW", 1, "n1_extra", "NEW_STATE", "Штамп листа")],
                    "modalities": ["TEXT"], "confidence": 0.6, "why_one_event": "Одна правка штампа"}],
                "unresolved_hints": [{
                    "hint_id": "H001", "kind": "SOURCE_CONFLICT", "engineering_subject": "Расход приточной установки П1",
                    "suspected_change": "Расход П1 в таблице 1200 м3/ч", "old_pages": [1], "new_pages": [1],
                    "evidence_items": [_evidence(pages, "NEW", 1, "n1_table", "NEW_STATE", "П1 1200 м3/ч")],
                    "missing_proof_or_conflict": "Таблица показывает 1200 м3/ч, расчёт расхода П1 не приведён."}]}

    def dedupe(**kwargs):
        ids = ["PC-R-001-C001", "PC-R-002-C001", "PC-R-003-C001"]
        return {"pair": pair_id, "notes": [], "decisions": [
            {"decision": "KEEP_SEPARATE", "projectchange_ids": [i], "reason": "разные места"} for i in ids]}

    return {"MAPPING": mapping, "MINING": mining, "DEDUPE": dedupe}
