"""Tests for backend.app.services.text_analysis.prompt_loader.

Validates:
  - all 5 known Phase 1 prompt files exist in prompts/pipeline/ru/phase1/
  - each file is non-empty, valid UTF-8, above a sane minimum size
  - placeholder inventory per file matches the documented manifest
  - loader returns raw text on KNOWN_PROMPTS, raises specifically on
    unknown / missing / empty / wrong types
  - extract_placeholders returns sorted-unique names

No LLM. No pipeline. Reads files only.
"""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.prompt_loader import (
    KNOWN_PROMPTS,
    PHASE1_PROMPTS_DIR,
    PromptNotFoundError,
    available_prompts,
    extract_placeholders,
    load_prompt,
)


EXPECTED_PROMPTS = {
    "completeness_lens_production_prompt",
    "stage01_document_type_block",
    "stage01_few_shot_examples",
    "stage01_production_prompt",
    "stage01_severity_calibration",
}

# Per-prompt expected placeholder inventory, derived from the source files
# and the manifest in prompts/pipeline/ru/phase1/README.md.
EXPECTED_PLACEHOLDERS: dict[str, set[str]] = {
    "completeness_lens_production_prompt": {
        "DISCIPLINE", "DOCUMENT_TYPE", "CHECKLIST_CONTENT", "MD_CONTENT",
    },
    "stage01_document_type_block": {
        "DOCUMENT_TYPE",
    },
    "stage01_few_shot_examples": set(),
    "stage01_production_prompt": {
        "PROJECT_ID", "DISCIPLINE_ROLE", "DISCIPLINE_CHECKLIST",
        "DISCIPLINE_FINDING_CATEGORIES", "DISCIPLINE_NORMS_FILE",
        "DOCUMENT_TYPE", "MD_FILE_PATH", "OUTPUT_PATH",
    },
    "stage01_severity_calibration": set(),
}


# ---------------------------------------------------------------------------
# Module-level invariants.
# ---------------------------------------------------------------------------

def test_known_prompts_set_matches_manifest():
    assert KNOWN_PROMPTS == frozenset(EXPECTED_PROMPTS)


def test_phase1_prompts_dir_resolves_under_prompts_pipeline_ru():
    # Guards against someone repointing PROMPTS_DIR and silently breaking
    # the loader. The path must end with `prompts/pipeline/ru/phase1`.
    parts = PHASE1_PROMPTS_DIR.parts
    assert parts[-3:] == ("pipeline", "ru", "phase1"), (
        f"PHASE1_PROMPTS_DIR has unexpected tail: {parts[-4:]}"
    )
    assert PHASE1_PROMPTS_DIR.is_dir(), f"missing dir: {PHASE1_PROMPTS_DIR}"


# ---------------------------------------------------------------------------
# Per-file integrity.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(EXPECTED_PROMPTS))
def test_each_prompt_file_exists(name):
    path = PHASE1_PROMPTS_DIR / f"{name}.md"
    assert path.is_file(), f"missing: {path}"


@pytest.mark.parametrize("name", sorted(EXPECTED_PROMPTS))
def test_each_prompt_file_is_nonempty_and_utf8(name):
    path = PHASE1_PROMPTS_DIR / f"{name}.md"
    raw = path.read_bytes()
    # Lower bound: smallest research file at copy time was
    # stage01_document_type_block.md ≈ 8.3 kB. 4 kB sits safely below
    # any real prompt and above any stub.
    assert len(raw) >= 4000, f"{path.name} suspiciously small: {len(raw)} bytes"
    text = raw.decode("utf-8")
    assert text.strip(), f"{path.name} is whitespace-only"


@pytest.mark.parametrize("name", sorted(EXPECTED_PROMPTS))
def test_each_prompt_file_starts_with_markdown_heading(name):
    text = load_prompt(name)
    first_line = text.splitlines()[0] if text.splitlines() else ""
    assert first_line.startswith("#"), (
        f"{name}.md must start with a markdown heading; got {first_line!r}"
    )


@pytest.mark.parametrize("name", sorted(EXPECTED_PROMPTS))
def test_each_prompt_has_expected_placeholders(name):
    got = set(extract_placeholders(load_prompt(name)))
    expected = EXPECTED_PLACEHOLDERS[name]
    assert got == expected, (
        f"{name}.md placeholders mismatch: "
        f"got {sorted(got)} expected {sorted(expected)}"
    )


# ---------------------------------------------------------------------------
# Loader behaviour.
# ---------------------------------------------------------------------------

def test_load_prompt_returns_raw_text():
    text = load_prompt("completeness_lens_production_prompt")
    assert isinstance(text, str)
    assert text.startswith("# ")


def test_load_prompt_is_case_sensitive():
    # The existing prompts/pipeline/ru/*_task.md convention is lower_snake;
    # case matters because filenames carry meaning. Upper-case must fail.
    with pytest.raises(ValueError, match="unknown prompt"):
        load_prompt("Stage01_Production_Prompt")


def test_load_prompt_rejects_unknown():
    with pytest.raises(ValueError, match="unknown prompt"):
        load_prompt("completeness_v999")


def test_load_prompt_rejects_empty_string():
    with pytest.raises(ValueError):
        load_prompt("")


def test_load_prompt_rejects_whitespace_only():
    with pytest.raises(ValueError):
        load_prompt("   ")


def test_load_prompt_rejects_non_string():
    with pytest.raises(ValueError):
        load_prompt(None)  # type: ignore[arg-type]


def test_load_prompt_raises_specific_subclass_when_file_missing(tmp_path, monkeypatch):
    from backend.app.services.text_analysis import prompt_loader as pl

    monkeypatch.setattr(pl, "PHASE1_PROMPTS_DIR", tmp_path)
    with pytest.raises(PromptNotFoundError, match="prompt file missing"):
        pl.load_prompt("stage01_production_prompt")


def test_load_prompt_raises_when_file_is_empty(tmp_path, monkeypatch):
    from backend.app.services.text_analysis import prompt_loader as pl

    (tmp_path / "stage01_production_prompt.md").write_text("", encoding="utf-8")
    monkeypatch.setattr(pl, "PHASE1_PROMPTS_DIR", tmp_path)
    with pytest.raises(PromptNotFoundError, match="empty"):
        pl.load_prompt("stage01_production_prompt")


def test_available_prompts_returns_full_set():
    assert sorted(available_prompts()) == sorted(EXPECTED_PROMPTS)


def test_available_prompts_subset_of_known():
    assert set(available_prompts()) <= KNOWN_PROMPTS


# ---------------------------------------------------------------------------
# extract_placeholders semantics.
# ---------------------------------------------------------------------------

def test_extract_placeholders_finds_simple_names():
    assert extract_placeholders("hello {NAME} and {OTHER_NAME}") == [
        "NAME", "OTHER_NAME"
    ]


def test_extract_placeholders_dedups_and_sorts():
    assert extract_placeholders("{B} {A} {A} {B} {A}") == ["A", "B"]


def test_extract_placeholders_ignores_lowercase_and_curly_pairs():
    # Lower case = not a placeholder per convention.
    # `{{X}}` (double braces) is also not the convention.
    text = "{lower} {Mixed} {{DOUBLE}} {OK_NAME}"
    assert extract_placeholders(text) == ["OK_NAME"]


def test_extract_placeholders_empty_text_returns_empty_list():
    assert extract_placeholders("") == []


def test_extract_placeholders_rejects_non_string():
    with pytest.raises(ValueError):
        extract_placeholders(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Cross-module sanity: the document_type_detector ALLOWED set is referenced
# by the document-type routing block in the prompts. If anyone adds a new
# detector type without updating the prompts, this fails.
# ---------------------------------------------------------------------------

def test_prompt_routing_block_mentions_all_detector_types():
    from backend.app.services.text_analysis.document_type_detector import ALLOWED

    block = load_prompt("stage01_document_type_block")
    missing = [t for t in sorted(ALLOWED) if t not in block]
    assert not missing, (
        f"stage01_document_type_block.md does not mention these "
        f"document_type values from the detector ALLOWED set: {missing}"
    )
