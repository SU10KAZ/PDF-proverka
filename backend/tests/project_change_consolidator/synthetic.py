"""Synthetic ProjectChange cards for Consolidator tests (deliberately NOT DEV5 names/values)."""
from __future__ import annotations

from typing import Any


def evidence(side: str, page: int, block: str, fragment: str, block_type: str = "TEXT", role: str = "роль") -> dict[str, Any]:
    return {"side": side, "source_pdf": f"/synthetic/{side.lower()}.pdf", "physical_page": page, "block_id": block,
            "block_type": block_type, "bbox": [0.1, 0.1, 0.9, 0.9], "crop_ref": "", "relevant_fragment": fragment,
            "evidence_role": role}


def card(cid: str, *, subject: str, summary: str, old: str, new: str, params: list[tuple] | None = None,
         ev: list[dict[str, Any]] | None = None, locations: list[str] | None = None, scope: str = "LOCAL") -> dict[str, Any]:
    ev = ev or [evidence("OLD", 1, f"{cid}-o", "исходное решение " + old), evidence("NEW", 1, f"{cid}-n", "новое решение " + new)]
    return {
        "projectchange_id": cid, "engineering_subject": subject, "scope": scope, "locations": locations or [],
        "change_summary": summary, "old_state": old, "new_state": new,
        "changed_parameters": [{"name": n, "old_value": o, "new_value": nv, "unit": u, "location": loc}
                               for n, o, nv, u, loc in (params or [])],
        "old_pages": sorted({e["physical_page"] for e in ev if e["side"] == "OLD"}),
        "new_pages": sorted({e["physical_page"] for e in ev if e["side"] == "NEW"}),
        "evidence_items": ev, "modalities": sorted({e["block_type"] for e in ev}), "confidence": 0.9,
        "why_one_event": "одно решение", "dedupe_lineage": [cid], "dedupe_reason": "",
    }


NOTE = "Примечание 4: трубопроводы системы В5 от этажных распределителей прокладывать под потолком коридоров в изоляции"


def building_card(cid: str, building: int, page: int) -> dict[str, Any]:
    """One typical decision (same general note) repeated on the sheet of one building."""
    return card(cid, subject=f"Прокладка распределительных трубопроводов В5 здания {building}",
                summary="Трубопроводы В5 перенесены из стяжки под потолок коридоров.",
                old="Трубопроводы В5 прокладываются в стяжке пола коридоров.",
                new="Трубопроводы В5 прокладываются под потолком коридоров.",
                params=[("Способ прокладки трубопроводов В5", "в стяжке", "под потолком", "", f"Здание {building}")],
                ev=[evidence("OLD", page, f"o{page}-note", "Примечание 4: трубопроводы системы В5 от этажных распределителей прокладывать в стяжке пола в изоляции"),
                    evidence("NEW", page + 20, f"n{page}-note", NOTE)],
                locations=[f"Здание {building}", "Коридоры"], scope="SYSTEM_WIDE")


def pump_card(cid: str, system: str, page: int, old_q: str, new_q: str) -> dict[str, Any]:
    return card(cid, subject=f"Замена насосной установки системы {system}",
                summary=f"Насосная установка {system} заменена, подача {old_q} → {new_q} м3/ч.",
                old=f"Одна насосная установка {system} подачей {old_q} м3/ч.",
                new=f"Две зональные насосные установки {system} подачей {new_q} м3/ч.",
                params=[(f"Подача насосной установки {system}", old_q, new_q, "м3/ч", "Насосная")],
                ev=[evidence("OLD", page, f"o{page}-{system}", f"Насосная установка {system} подача {old_q} м3/ч напор 41,5 м", "TABLE"),
                    evidence("NEW", page + 20, f"n{page}-{system}", f"Установки {system} подача {new_q} м3/ч напор 44,5 м", "TABLE")],
                locations=["Насосная"], scope="LOCAL_ELEMENT")


def result_of(cards_with_regions: list[tuple[dict[str, Any], str]], hints_by_region: dict[str, list] | None = None,
              run_id: str = "synthetic_run", pair_id: str = "synthetic_pair") -> tuple[dict[str, Any], dict[str, Any]]:
    """(V3-shaped result, miner_results) for synthetic cards/hints."""
    hints_by_region = hints_by_region or {}
    regions = []
    for rid in dict.fromkeys([r for _, r in cards_with_regions] + list(hints_by_region)):
        regions.append({"region_id": rid, "projectchanges": [c for c, r in cards_with_regions if r == rid],
                        "unresolved_hints": list(hints_by_region.get(rid, []))})
    result = {"schema": "projectchange_v3_final/2", "run_id": run_id, "session_id": "synthetic_session",
              "pair_id": pair_id, "projectchanges": [c for c, _ in cards_with_regions],
              "projectchange_regions": {c["projectchange_id"]: r for c, r in cards_with_regions},
              "unresolved_hints": [h for r in regions for h in r["unresolved_hints"]], "dedupe": {}}
    return result, {"run_id": run_id, "regions": regions}


def hint(hid: str, subject: str, conflict: str, ev: list[dict[str, Any]], kind: str = "SOURCE_CONFLICT",
         old_pages=None, new_pages=None) -> dict[str, Any]:
    return {"hint_id": hid, "kind": kind, "engineering_subject": subject, "suspected_change": subject,
            "old_pages": old_pages or sorted({e["physical_page"] for e in ev if e["side"] == "OLD"}),
            "new_pages": new_pages or sorted({e["physical_page"] for e in ev if e["side"] == "NEW"}),
            "evidence_items": ev, "missing_proof_or_conflict": conflict}
