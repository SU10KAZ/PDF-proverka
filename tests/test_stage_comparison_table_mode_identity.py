"""Режим расчёта входит в upstream facet identity табличного атома."""
from __future__ import annotations

from backend.app.services.stage_comparison.unified_change_synthesizer.identity import (
    canonical_atomic_identity,
)
from backend.app.services.stage_comparison.unified_change_synthesizer.normalization import (
    load_table_diff_to_graphic_atoms,
)
from backend.app.services.stage_comparison.unified_change_synthesizer.synthesizer import (
    synthesize_unified_changes,
)


def _change(change_id: str, mode: str, before: float, after: float) -> dict:
    base = "demand_active_power_kw"
    return {
        "change_id": change_id,
        "match_id": "etm_1",
        "subject": "ВРУ1",
        "row_kind": "CONSUMER_TOTAL",
        "mode_label": mode,
        "mode_key": mode,
        "facet_ref": f"{base}@mode={mode}",
        "base_facet_ref": base,
        "facet_title": "Расчётная активная мощность",
        "unit": "кВт",
        "before_value": before,
        "after_value": after,
        "direction": "INCREASED",
        "match_method": "EXACT",
        "confidence": "HIGH",
        "notes": [],
        "evidence": {"LEFT": {}, "RIGHT": {}},
    }


def test_mode_is_included_in_fact_identity_without_changing_g245_g246():
    payload = {
        "contract_version": "electrical-table-diff.v1",
        "changes": [
            _change("etchg_work", "рабочий", 100.0, 110.0),
            _change("etchg_fire", "пожарный", 120.0, 130.0),
        ],
        "blocked": [],
        "unproven": [],
    }
    adapted = load_table_diff_to_graphic_atoms(payload, scope_ref="scope_1")
    identities = [canonical_atomic_identity(atom) for atom in adapted["atoms"]]
    assert {identity["facet_ref"] for identity in identities} == {
        "demand_active_power_kw@mode=рабочий",
        "demand_active_power_kw@mode=пожарный",
    }
    synthesis = synthesize_unified_changes(graphic_atoms=adapted["atoms"])
    assert len(synthesis["changes"]) == 2
    assert len({change["change_id"] for change in synthesis["changes"]}) == 2
    for atom in adapted["atoms"]:
        assert atom["provenance"]["base_facet_ref"] == "demand_active_power_kw"
