"""Tests for backend.app.services.text_analysis.checklist_loader.

Validates:
  - all 8 known discipline files exist in backend/app/data/discipline_checklists/
  - each file is non-empty, valid UTF-8, has the expected `# Checklist — `
    header and the four canonical section headers (Mandatory / Recommended /
    Conditional / Anti-patterns), as required by checklist_rules.md
  - load_checklist() returns the raw text and is case-insensitive on input
  - load_checklist() raises on invalid input
  - available_disciplines() returns the full set

No LLM. No pipeline. Reads files only.
"""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.checklist_loader import (
    CHECKLIST_DIR,
    KNOWN_DISCIPLINES,
    ChecklistNotFoundError,
    available_disciplines,
    load_checklist,
)


EXPECTED_DISCIPLINES = {"AR", "EOM", "KJ", "KM", "MULTI", "OV", "SS", "VK"}

# Section headers that must appear in every discipline file, matching the
# verbatim strings checklist_rules.md says the runner parses for.
REQUIRED_TIER_HEADERS = (
    "## Mandatory required",
    "## Recommended items",
    "## Conditional items",
    "## Anti-patterns",
)


# ---------------------------------------------------------------------------
# Module-level invariants.
# ---------------------------------------------------------------------------

def test_known_disciplines_set_matches_design():
    assert KNOWN_DISCIPLINES == frozenset(EXPECTED_DISCIPLINES)


def test_checklist_dir_resolves_inside_app_data():
    # Must resolve to backend/app/data/discipline_checklists/ — guards against
    # someone repointing APP_DATA_DIR and accidentally leaking data path.
    assert CHECKLIST_DIR.name == "discipline_checklists"
    assert CHECKLIST_DIR.parent.name == "data"
    assert CHECKLIST_DIR.is_dir(), f"missing dir: {CHECKLIST_DIR}"


# ---------------------------------------------------------------------------
# Per-file integrity.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_discipline_file_exists(discipline):
    path = CHECKLIST_DIR / f"{discipline}.md"
    assert path.is_file(), f"missing: {path}"


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_discipline_file_is_nonempty_and_utf8(discipline):
    path = CHECKLIST_DIR / f"{discipline}.md"
    raw = path.read_bytes()
    # Lower bound chosen to catch accidentally-truncated copies (smallest
    # research file at copy time was AR.md ≈ 8.4 kB). 4 kB is comfortably below
    # that and above any plausible stub.
    assert len(raw) >= 4000, f"{path.name} suspiciously small: {len(raw)} bytes"
    text = raw.decode("utf-8")
    assert text.strip(), f"{path.name} is whitespace-only"


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_discipline_file_starts_with_checklist_header(discipline):
    text = load_checklist(discipline)
    first_line = text.splitlines()[0] if text.splitlines() else ""
    assert first_line.startswith("# Checklist"), (
        f"{discipline}.md must start with '# Checklist'; got {first_line!r}"
    )


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_discipline_file_has_required_tier_headers(discipline):
    text = load_checklist(discipline)
    missing = [h for h in REQUIRED_TIER_HEADERS if h not in text]
    assert not missing, f"{discipline}.md missing tier headers: {missing}"


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_discipline_file_has_problem_class_tags(discipline):
    # The completeness-lens contract requires every actionable bullet to carry
    # a `[problem_class=...` prefix. Each checklist file should have at least
    # a handful — guards against an editor accidentally stripping tags.
    text = load_checklist(discipline)
    assert text.count("[problem_class=") >= 3, (
        f"{discipline}.md should have multiple [problem_class=...] tags"
    )


# ---------------------------------------------------------------------------
# Loader behaviour.
# ---------------------------------------------------------------------------

def test_load_checklist_returns_raw_text():
    text = load_checklist("EOM")
    assert isinstance(text, str)
    assert text.startswith("# Checklist")


def test_load_checklist_is_case_insensitive_and_strips_whitespace():
    a = load_checklist("eom")
    b = load_checklist("  EOM  ")
    c = load_checklist("EOM")
    assert a == b == c


def test_load_checklist_rejects_unknown_discipline():
    with pytest.raises(ValueError, match="unknown discipline"):
        load_checklist("XYZ")


def test_load_checklist_rejects_empty_string():
    with pytest.raises(ValueError):
        load_checklist("")


def test_load_checklist_rejects_whitespace_only():
    with pytest.raises(ValueError):
        load_checklist("   ")


def test_load_checklist_rejects_non_string():
    with pytest.raises(ValueError):
        load_checklist(None)  # type: ignore[arg-type]


def test_load_checklist_raises_specific_subclass_when_file_missing(tmp_path, monkeypatch):
    # Point loader at a temp empty dir → all reads must raise the specific
    # ChecklistNotFoundError, not a bare FileNotFoundError that callers might
    # confuse with unrelated I/O issues.
    from backend.app.services.text_analysis import checklist_loader as cl

    monkeypatch.setattr(cl, "CHECKLIST_DIR", tmp_path)
    with pytest.raises(ChecklistNotFoundError, match="checklist file missing"):
        cl.load_checklist("EOM")


def test_load_checklist_raises_when_file_is_empty(tmp_path, monkeypatch):
    from backend.app.services.text_analysis import checklist_loader as cl

    (tmp_path / "EOM.md").write_text("", encoding="utf-8")
    monkeypatch.setattr(cl, "CHECKLIST_DIR", tmp_path)
    with pytest.raises(ChecklistNotFoundError, match="empty"):
        cl.load_checklist("EOM")


def test_available_disciplines_returns_full_set():
    got = available_disciplines()
    assert sorted(got) == sorted(EXPECTED_DISCIPLINES)


def test_available_disciplines_is_subset_of_known():
    got = set(available_disciplines())
    assert got <= KNOWN_DISCIPLINES
