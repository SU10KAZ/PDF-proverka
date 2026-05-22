"""Regression tests for Phase A normative refresh.

Ensures that the 5 phantom/wrong-clause references identified by
`experiments/md_analysis_comparison/normative_checklist_research/norm_clause_verification_report.md`
do NOT reappear in `backend/app/data/discipline_checklists/*.md`, and that the
canonical replacements are present.

Each forbidden pattern below was verified against the live normative text
via `mcp__norms__find_paragraph` / `mcp__norms__semantic_search_json`. If
someone re-adds one of these phrases to a checklist by mistake (LLM
suggestion, copy-paste from old draft), this test fires.

No LLM, no pipeline, no runtime wiring — pure file reads.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.services.text_analysis.checklist_loader import (
    CHECKLIST_DIR,
    KNOWN_DISCIPLINES,
    load_checklist,
)


# Forbidden phrases: (substring, reason, affected_items).
# Use whole-substring matching — these phrases are specific enough that a
# bare substring search is robust against ordinary editorial rewrites.
FORBIDDEN_CLAUSE_PATTERNS = (
    (
        "СП 256.1325800.2016, п. 6.4",
        "EOM-05 wrong clause: п. 6.4 — про размещение ТП, не про "
        "перечень электроприёмников. Use 'раздел 7, п. 7.1–7.2'.",
    ),
    (
        "СП 256.1325800.2016, п. 8.5",
        "EOM-07 wrong clause: п. 8.5 — про ограничение 250 А, не про "
        "потери напряжения. Use 'п. 12.6 + ГОСТ Р 50571.5.52'.",
    ),
    (
        "8.1.46",
        "KJ-08 phantom clause: п. 8.1.46 в СП 63.13330.2018 не существует "
        "(нумерация скачет 8.1.45 → 8.1.47). Use 'п. 8.1.47–8.1.50 + 8.2'.",
    ),
    (
        "СП 63.13330.2018, п. 10.3.5",
        "KJ-16 wrong clause: п. 10.3.5 — про расстояния между стержнями, "
        "не про анкеровку. Use 'п. 10.3.21–10.3.28 + 10.3.29–10.3.30'.",
    ),
    (
        "СП 50.13330.2012",
        "AR-08 wrong norm: СП 50 — это «Тепловая защита», не инсоляция; "
        "редакция 2012 заменена на 2024. Use 'СанПиН 1.2.3685-21, раздел III'.",
    ),
    (
        "ГОСТ Р 21.1101-2013",
        "Outdated reference: ГОСТ Р 21.1101-2013 заменён на "
        "ГОСТ Р 21.101-2020. См. norm_clause_verification_report.md.",
    ),
)


# Required-replacement phrases: at least N occurrences across all checklist files.
REQUIRED_REPLACEMENTS = (
    (
        "СП 256.1325800.2016, раздел 7",
        1,
        "EOM-05 replacement basis for электроприёмники",
    ),
    (
        "п. 12.6",
        1,
        "EOM-07 replacement basis for потери напряжения",
    ),
    (
        "8.1.47–8.1.50",
        1,
        "KJ-08 replacement basis for продавливание",
    ),
    (
        "10.3.21–10.3.28",
        1,
        "KJ-16 replacement basis for анкеровка",
    ),
    (
        "10.3.29–10.3.30",
        1,
        "KJ-16 replacement basis for стыки внахлёст",
    ),
    (
        "СанПиН 1.2.3685-21",
        1,
        "AR-08 replacement basis for инсоляция",
    ),
    (
        "ГОСТ Р 21.101-2020",
        1,
        "Normative refresh: должно встречаться хотя бы раз "
        "(заменяет устаревший 21.1101-2013)",
    ),
)


@pytest.fixture(scope="module")
def all_checklists() -> dict[str, str]:
    return {disc: load_checklist(disc) for disc in sorted(KNOWN_DISCIPLINES)}


@pytest.fixture(scope="module")
def joined_checklists(all_checklists) -> str:
    return "\n".join(all_checklists.values())


# ---------------------------------------------------------------------------
# Forbidden patterns must not appear anywhere in any discipline file.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pattern, reason", FORBIDDEN_CLAUSE_PATTERNS)
def test_forbidden_clause_absent_from_all_disciplines(
    all_checklists, pattern, reason
):
    hits: list[str] = []
    for disc, text in all_checklists.items():
        if pattern in text:
            # Find the line(s) for a useful failure message.
            for ln, line in enumerate(text.splitlines(), start=1):
                if pattern in line:
                    hits.append(f"{disc}.md:{ln}: {line.strip()}")
    assert not hits, (
        f"\nFORBIDDEN clause/reference re-introduced: {pattern!r}\n"
        f"Reason: {reason}\n"
        f"Found in:\n  " + "\n  ".join(hits)
    )


# ---------------------------------------------------------------------------
# Required replacement phrases.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "phrase, min_count, reason", REQUIRED_REPLACEMENTS
)
def test_required_replacement_present(joined_checklists, phrase, min_count, reason):
    count = joined_checklists.count(phrase)
    assert count >= min_count, (
        f"Missing required replacement phrase {phrase!r} "
        f"(found {count}, need >= {min_count}).\nReason: {reason}"
    )


# ---------------------------------------------------------------------------
# Spot checks on specific items.
# ---------------------------------------------------------------------------


def test_eom_07_cites_voltage_drop_para(all_checklists):
    """EOM-07 explicitly cites СП 256 п. 12.6 (the voltage-drop paragraph)."""
    eom = all_checklists["EOM"]
    # The substring «потерь напряжения» must appear near «12.6» and
    # «ГОСТ Р 50571.5.52».
    assert "Расчёт потерь напряжения" in eom
    assert "12.6" in eom
    assert "ГОСТ Р 50571.5.52" in eom


def test_kj_08_cites_punching_paragraphs(all_checklists):
    """KJ-08 cites the actual punching-shear paragraphs."""
    kj = all_checklists["KJ"]
    assert "8.1.47" in kj  # «–8.1.50» continues from here
    assert "раздел 8.2" in kj or "8.2.1" in kj


def test_kj_16_cites_anchoring_paragraphs(all_checklists):
    """KJ-16 cites the actual anchoring + overlap paragraphs."""
    kj = all_checklists["KJ"]
    assert "10.3.21" in kj
    assert "10.3.29" in kj  # overlap


def test_ar_08_uses_sanpin_not_sp50(all_checklists):
    """AR-08 references СанПиН 1.2.3685-21 (not СП 50)."""
    ar = all_checklists["AR"]
    assert "Расчёт инсоляции" in ar
    assert "СанПиН 1.2.3685-21" in ar
    # And СП 50 must NOT be cited as the basis for insolation.
    assert "СП 50" not in ar.split("Расчёт инсоляции")[1].split("\n", 5)[0:5][0]


def test_vk_20_unchanged_left_shadow_only():
    """VK-20 must keep its references untouched in Phase A (shadow-only path)."""
    vk = load_checklist("VK")
    # СанПиН 2.1.4.1074-01 must remain referenced — engineer validation still pending.
    assert "СанПиН 2.1.4.1074-01" in vk
    # And the basis must still mention СП 30/73.
    assert "СП 30.13330" in vk
    assert "СП 73.13330" in vk


def test_all_discipline_files_still_load():
    """Sanity: edits did not break any file."""
    for disc in sorted(KNOWN_DISCIPLINES):
        text = load_checklist(disc)
        assert text.startswith("# Checklist")
        assert "## Mandatory required" in text
        assert "## Anti-patterns" in text
