"""Tests for backend/app/services/text_analysis/prompt_rules/*.md.

Validates that the P0 prompt rule blocks exist on disk, are non-empty, and
contain the key forbidden phrases / prohibitions from the research. These
files are NOT wired into the runner; they are a prepared safety layer.
"""
from __future__ import annotations

from pathlib import Path

import pytest

PROMPT_RULES_DIR = (
    Path(__file__).resolve().parents[2]
    / "backend"
    / "app"
    / "services"
    / "text_analysis"
    / "prompt_rules"
)

EXPECTED_FILES = (
    "README.md",
    "stage_gate_rules.md",
    "document_type_rules.md",
    "object_signal_rules.md",
    "cross_section_rules.md",
    "anti_hallucination_rules.md",
    "anti_phantom_clause_rules.md",
    "coordination_rules.md",
)


def test_prompt_rules_dir_exists():
    assert PROMPT_RULES_DIR.is_dir(), f"missing: {PROMPT_RULES_DIR}"


@pytest.mark.parametrize("name", EXPECTED_FILES)
def test_each_expected_file_exists(name):
    path = PROMPT_RULES_DIR / name
    assert path.is_file(), f"missing: {path}"


@pytest.mark.parametrize("name", EXPECTED_FILES)
def test_each_file_is_non_trivial(name):
    raw = (PROMPT_RULES_DIR / name).read_bytes()
    assert len(raw) >= 200, f"{name} is suspiciously small ({len(raw)} bytes)"
    text = raw.decode("utf-8")
    assert text.strip()


def test_stage_gate_block_mentions_pd_rd_kmd():
    text = (PROMPT_RULES_DIR / "stage_gate_rules.md").read_text(encoding="utf-8")
    assert "ПД" in text
    assert "РД" in text
    assert "КМД" in text


def test_document_type_block_mentions_all_four_types():
    text = (PROMPT_RULES_DIR / "document_type_rules.md").read_text(encoding="utf-8")
    for token in ("full_rd", "audit_comparison", "tz_vs_rd", "specification_only"):
        assert token in text, f"missing token {token!r}"


def test_cross_section_block_forbids_multi_05_13():
    text = (PROMPT_RULES_DIR / "cross_section_rules.md").read_text(encoding="utf-8")
    for n in range(5, 14):
        assert f"MULTI-{n:02d}" in text, f"missing MULTI-{n:02d} reference"


def test_coordination_block_lists_coordination_items():
    text = (PROMPT_RULES_DIR / "coordination_rules.md").read_text(encoding="utf-8")
    # A handful of representative coordination items from the research.
    for ref in ("AR-15", "EOM-16", "OV-17", "VK-16", "SS-16"):
        assert ref in text, f"missing coordination reference {ref!r}"


def test_anti_phantom_clause_block_mentions_replacement_norms():
    text = (PROMPT_RULES_DIR / "anti_phantom_clause_rules.md").read_text(
        encoding="utf-8"
    )
    assert "ГОСТ Р 21.1101-2013" in text
    assert "ГОСТ Р 21.101-2020" in text
    # ПУЭ-7 voluntary-application reminder.
    assert "ПУЭ-7" in text


def test_object_signal_block_lists_known_signals():
    text = (PROMPT_RULES_DIR / "object_signal_rules.md").read_text(encoding="utf-8")
    # Representative subset — must be present so future runners can map.
    for sig in (
        "residential_building",
        "high_rise",
        "fire_system_present",
        "lightning_protection_required",
        "category_1_power",
        "smoke_ventilation_required",
        "ventilation_system_present",
        "pumps_present",
    ):
        assert sig in text, f"missing signal name {sig!r}"


def test_anti_hallucination_forbids_speculative_phrases():
    text = (PROMPT_RULES_DIR / "anti_hallucination_rules.md").read_text(
        encoding="utf-8"
    )
    for phrase in ("Возможно", "Следует уточнить", "Похоже", "По-видимому"):
        assert phrase in text, f"missing forbidden phrase {phrase!r}"


def test_readme_lists_all_files():
    text = (PROMPT_RULES_DIR / "README.md").read_text(encoding="utf-8")
    for name in EXPECTED_FILES:
        assert name in text, f"README must reference {name!r}"


def test_no_runtime_imports_in_prompt_rules_dir():
    """Sanity: nobody put a .py file in prompt_rules/ — these are static
    markdown blocks, not Python modules."""
    py_files = list(PROMPT_RULES_DIR.glob("*.py"))
    assert not py_files, f"unexpected .py files in prompt_rules/: {py_files}"
