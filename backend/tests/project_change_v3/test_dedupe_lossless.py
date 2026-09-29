"""Lossless Dedupe merge (PROJECT_COMPARISON_V3_DEDUPE_LOSSLESS)."""
from __future__ import annotations

import copy

import pytest

from backend.app.services.project_change_v3.dedupe import (
    DEDUPE_LOSSLESS_ENV,
    apply_dedupe,
    dedupe_lossless_enabled,
    salient_tokens,
)


def _card(pid, subject, summary, new_state, params, evidence):
    return {
        "projectchange_id": pid, "engineering_subject": subject, "scope": "ВПВ жилой части",
        "change_summary": summary, "old_state": "Одна установка АЛЬФА на всю систему ВПВ.", "new_state": new_state,
        "locations": [f"loc-{pid}"], "modalities": ["TEXT"], "old_pages": [11], "new_pages": [20],
        "changed_parameters": params, "evidence_items": evidence, "why_one_event": "w", "confidence": 0.8,
    }


HEAD = {"name": "Требуемый напор", "old_value": "97,5", "new_value": "88,45", "unit": "м", "location": "ПЗ п. е)"}
PUMP = {"name": "Марка установки", "old_value": "АЛЬФА", "new_value": "Wilo CO 2 MVL 2005", "unit": "", "location": "NEW28"}
EV = {"side": "NEW", "physical_page": 20, "block_id": "b1", "relevant_fragment": "Hp=88,45"}


def _pair():
    narrow = _card("PCA-A-R003-C013", "Гидравлический расчёт требуемого напора ВПВ",
                   "Пересчитан требуемый напор ВПВ: 97,5 → 88,45 м.", "Требуемый напор 88,45 м.", [HEAD], [EV])
    rich = _card("PCA-A-R007-C001", "Насосная установка ВПВ: перерасчёт напора и замена оборудования",
                 "Установка АЛЬФА заменена двумя зональными Wilo CO 2 BL 40/110 и CO 2 MVL 2005; напор 88,45 м.",
                 "Wilo CO 2 BL 40/110 (1 зона), Wilo CO 2 MVL 2005 (2 зона), напор 88,45 м.", [HEAD, PUMP], [EV])
    raw = {"pair": "p", "decisions": [
        {"decision": "MERGE_DUPLICATES", "projectchange_ids": [narrow["projectchange_id"], rich["projectchange_id"]],
         "reason": "тот же пересчёт напора"}]}
    return [narrow, rich], raw


def test_legacy_merge_is_unchanged_without_flag(monkeypatch):
    monkeypatch.delenv(DEDUPE_LOSSLESS_ENV, raising=False)
    assert dedupe_lossless_enabled() is False
    changes, raw = _pair()
    [card] = apply_dedupe("p", copy.deepcopy(changes), raw)
    assert card["projectchange_id"] == "PCA-A-R003-C013"          # first id wins, as in 3.7.0
    assert card["change_summary"] == changes[0]["change_summary"]  # the replacement text is lost
    assert card["changed_parameters"] == [HEAD, HEAD, PUMP]        # plain concatenation
    assert "dedupe_members" not in card and "dedupe_residual" not in card


def test_lossless_picks_the_most_inclusive_member_and_keeps_every_text():
    changes, raw = _pair()
    [card] = apply_dedupe("p", copy.deepcopy(changes), raw, lossless=True)
    assert card["projectchange_id"] == "PCA-A-R007-C001"
    assert "Wilo" in card["change_summary"]
    assert card["dedupe_lineage"] == ["PCA-A-R007-C001", "PCA-A-R003-C013"]
    assert card["dedupe_canonical_reselected"] is True
    assert [m["projectchange_id"] for m in card["dedupe_members"]] == card["dedupe_lineage"]
    assert card["dedupe_members"][1]["change_summary"] == changes[0]["change_summary"]
    assert card["changed_parameters"] == [HEAD, PUMP]               # exact duplicate collapsed
    assert card["evidence_items"] == [EV]
    assert card["old_pages"] == [11] and card["new_pages"] == [20]


def test_lossless_records_what_the_representative_does_not_say():
    a = _card("A", "Счётчик ВСХНд-40 → ВСХНд-65", "Счётчик ВСХНд-40 заменён на ВСХНд-65.", "ВСХНд-65", [], [EV])
    b = _card("B", "Вставка", "Вставка Ду40 → Ду80.", "Ду80", [], [EV])
    raw = {"pair": "p", "decisions": [{"decision": "MERGE_DUPLICATES", "projectchange_ids": ["A", "B"], "reason": "r"}]}
    [card] = apply_dedupe("p", [a, b], raw, lossless=True)
    assert card["projectchange_id"] == "A"
    assert card["dedupe_canonical_reselected"] is False
    assert card["dedupe_residual"] == {"B": ["ду40", "ду80"]}  # A never mentions the insert sizes


def test_ties_keep_the_model_order():
    a = _card("A", "Ду200", "Ду200", "Ду200", [], [EV])
    b = _card("B", "Ду200", "Ду200", "Ду200", [], [EV])
    raw = {"pair": "p", "decisions": [{"decision": "MERGE_DUPLICATES", "projectchange_ids": ["A", "B"], "reason": "r"}]}
    [card] = apply_dedupe("p", [a, b], raw, lossless=True)
    assert card["projectchange_id"] == "A" and card["dedupe_residual"] == {}


def test_keep_separate_is_untouched_and_partition_still_enforced():
    changes, _ = _pair()
    keep = {"pair": "p", "decisions": [
        {"decision": "KEEP_SEPARATE", "projectchange_ids": [c["projectchange_id"]], "reason": "r"} for c in changes]}
    out = apply_dedupe("p", copy.deepcopy(changes), keep, lossless=True)
    assert [c["projectchange_id"] for c in out] == [c["projectchange_id"] for c in changes]
    assert all("dedupe_members" not in c for c in out)
    partial = {"pair": "p", "decisions": keep["decisions"][:1]}
    with pytest.raises(RuntimeError):
        apply_dedupe("p", copy.deepcopy(changes), partial, lossless=True)


def test_salient_tokens_are_designations_and_numbers():
    tokens = salient_tokens("Установка Wilo CO 2 MVL 2005, Ду200, 52,45 м, 3В1.1, ВПВ, насосная")
    assert {"wilo", "mvl", "2005", "ду200", "52.45", "3в1.1", "впв"} <= tokens
    assert "насосная" not in tokens and "установка" not in tokens


def test_flag_reads_exactly_one(monkeypatch):
    monkeypatch.setenv(DEDUPE_LOSSLESS_ENV, "1")
    assert dedupe_lossless_enabled() is True
    monkeypatch.setenv(DEDUPE_LOSSLESS_ENV, "true")
    assert dedupe_lossless_enabled() is False
