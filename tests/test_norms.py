"""Тесты для norms — authoritative режим через Norms-main."""
import json
import pytest
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from norms import (  # noqa: E402
    generate_deterministic_checks,
    validate_norm_checks,
    build_missing_norms_queue,
    write_missing_norms_queue,
    merge_llm_norm_results,
)
from norms import external_provider  # noqa: E402


# ─── helpers ──────────────────────────────────────────────────────────────
def _stub_resolve(mapping: dict):
    """Возвращает stub для resolve_norm_status по словарю {query: payload}.

    Любой query, которого нет в mapping — считается not_in_index если
    семейство распознаётся, иначе unsupported.
    """
    def _impl(raw):
        key = (raw or "").strip()
        if key in mapping:
            return mapping[key]
        fam = external_provider._detect_family(key)
        if fam is None:
            return {
                "query": raw, "normalized_query": key, "found": False,
                "matched_code": None, "status": "unknown",
                "doc_status": None, "edition_status": None,
                "authoritative": False,
                "resolution_reason": "unsupported_family",
                "detected_family": None, "supported_family": False,
                "needs_manual_addition": False, "has_text": False,
                "replacement_doc": None, "current_version": None,
                "title": None, "file": None, "type": None, "year": None,
                "details": None, "source_url": None, "last_verified": None,
                "parse_confidence": None, "source": "not_found",
            }
        return {
            "query": raw, "normalized_query": key, "found": False,
            "matched_code": None, "status": "unknown",
            "doc_status": None, "edition_status": None,
            "authoritative": False,
            "resolution_reason": "not_in_index",
            "detected_family": fam, "supported_family": True,
            "needs_manual_addition": True, "has_text": False,
            "replacement_doc": None, "current_version": None,
            "title": None, "file": None, "type": None, "year": None,
            "details": None, "source_url": None, "last_verified": None,
            "parse_confidence": None, "source": "not_found",
        }
    return _impl


def _found_payload(code, status="active", edition_status=None,
                   replacement_doc=None, current_version=None,
                   has_text=True):
    doc_status = "active"
    if status == "replaced":
        doc_status = "replaced"
    elif status == "cancelled":
        doc_status = "cancelled"
    elif status == "outdated_edition":
        doc_status = "active"
        edition_status = "outdated"
    return {
        "query": code, "normalized_query": code, "found": True,
        "matched_code": code, "status": status,
        "doc_status": doc_status, "edition_status": edition_status,
        "authoritative": True, "resolution_reason": "exact",
        "detected_family": code.split()[0],
        "supported_family": True, "needs_manual_addition": False,
        "has_text": has_text,
        "replacement_doc": replacement_doc,
        "current_version": current_version or code,
        "title": "T", "file": f"{code}.md",
        "type": code.split()[0], "year": 2016,
        "details": None, "source_url": None, "last_verified": None,
        "parse_confidence": "high", "source": "vault",
    }


# ─── active ───────────────────────────────────────────────────────────────
def test_deterministic_checks_active_norm():
    norms_data = {"norms": {"СП 256.1325800.2016": {
        "cited_as": ["СП 256.1325800.2016"], "affected_findings": ["F-001"],
    }}}
    stub = _stub_resolve({
        "СП 256.1325800.2016": _found_payload("СП 256.1325800.2016", "active"),
    })
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["checks"]) == 1
    check = result["checks"][0]
    assert check["status"] == "active"
    assert check["verified_via"] == "norms_authoritative"
    assert check["needs_revision"] is False
    assert check["authoritative"] is True
    assert result["missing_norms"] == []
    assert result["unsupported_norms"] == []


# ─── replaced ─────────────────────────────────────────────────────────────
def test_deterministic_checks_replaced_norm():
    norms_data = {"norms": {"СП 31-110-2003": {
        "cited_as": ["СП 31-110-2003"], "affected_findings": ["F-002"],
    }}}
    stub = _stub_resolve({
        "СП 31-110-2003": _found_payload(
            "СП 31-110-2003", "replaced",
            replacement_doc="СП 256.1325800.2016",
        ),
    })
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["checks"]) == 1
    check = result["checks"][0]
    assert check["status"] == "replaced"
    assert check["needs_revision"] is True
    assert check["replacement_doc"] == "СП 256.1325800.2016"


# ─── outdated_edition ─────────────────────────────────────────────────────
def test_deterministic_checks_outdated_edition():
    norms_data = {"norms": {"СП 54.13330.2016": {
        "cited_as": ["СП 54.13330.2016"], "affected_findings": ["F-010"],
    }}}
    stub = _stub_resolve({
        "СП 54.13330.2016": _found_payload(
            "СП 54.13330.2016", "outdated_edition",
            current_version="СП 54.13330.2022",
        ),
    })
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    check = result["checks"][0]
    assert check["status"] == "outdated_edition"
    assert check["needs_revision"] is True
    assert check["current_version"] == "СП 54.13330.2022"


# ─── not_in_index → missing queue ────────────────────────────────────────
def test_deterministic_checks_not_in_index():
    norms_data = {"norms": {"ГОСТ 99999-2099": {
        "cited_as": ["ГОСТ 99999-2099"], "affected_findings": ["F-003"],
    }}}
    stub = _stub_resolve({})  # never found
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["missing_norms"]) == 1
    assert result["missing_norms"][0]["action"] == "add_document_to_vault"
    check = result["checks"][0]
    assert check["status"] == "not_found"
    assert check["verified_via"] == "norms_missing"
    # edition_status для not_found теперь "unknown", а не "not_found"
    assert check["edition_status"] == "unknown"
    # Цитаты НЕ уходят в LLM для missing — бессмысленно
    assert result["paragraphs_to_verify"] == []


# ─── unsupported_family ──────────────────────────────────────────────────
def test_deterministic_checks_unsupported_family():
    norms_data = {"norms": {"Какая-то абракадабра XYZ": {
        "cited_as": ["Какая-то абракадабра XYZ"],
        "affected_findings": ["F-004"],
    }}}
    stub = _stub_resolve({})
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["unsupported_norms"]) == 1
    assert result["unsupported_norms"][0]["action"] == "review_family_support"
    check = result["checks"][0]
    assert check["verified_via"] == "norms_unsupported"
    assert check["supported_family"] is False


# ─── missing_norms_queue ─────────────────────────────────────────────────
def test_missing_norms_queue_writes_three_files(tmp_path):
    norms_data = {"norms": {
        "ГОСТ 99999-2099": {
            "cited_as": ["ГОСТ 99999-2099"], "affected_findings": ["F-003"],
        },
        "XYZ абракадабра": {
            "cited_as": ["XYZ абракадабра"], "affected_findings": ["F-004"],
        },
    }}
    stub = _stub_resolve({})
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
        report = write_missing_norms_queue(tmp_path, result, project_id="test")

    assert (tmp_path / "missing_norms_queue.json").exists()
    assert (tmp_path / "missing_norms_report.json").exists()
    assert (tmp_path / "missing_norms_queue.md").exists()
    assert report["queue_size"] == 2
    assert report["missing"] == 1
    assert report["unsupported"] == 1

    queue_data = json.loads((tmp_path / "missing_norms_queue.json").read_text())
    assert queue_data["meta"]["source"] == "norms_main_status_index"
    assert len(queue_data["queue"]) == 2


# ─── merge_llm_norm_results: LLM не меняет статус ─────────────────────────
def test_merge_does_not_overwrite_status(tmp_path):
    det_path = tmp_path / "norm_checks.json"
    llm_path = tmp_path / "norm_checks_llm.json"

    det_data = {
        "meta": {"project_id": "test"},
        "checks": [{
            "norm_as_cited": "СП 256.1325800.2016",
            "doc_number": "СП 256.1325800.2016",
            "status": "active",
            "verified_via": "norms_authoritative",
            "authoritative": True,
            "needs_revision": False,
            "affected_findings": ["F-001"],
        }],
        "paragraph_checks": [],
    }
    llm_data = {
        "meta": {"project_id": "test"},
        # LLM пытается поменять статус — мы должны это проигнорировать.
        "checks": [{
            "doc_number": "СП 256.1325800.2016",
            "status": "cancelled",
            "verified_via": "llm_guessed",
        }],
        "paragraph_checks": [{
            "finding_id": "F-001",
            "norm": "СП 256.1325800.2016, п.14.9",
            "actual_quote": "Пункт 14.9 — текст",
            "claimed_quote": "Пункт 14.9 — текст",
            "paragraph_verified": True,
            "verified_via": "norms_mcp_paragraph",
        }],
    }
    det_path.write_text(json.dumps(det_data, ensure_ascii=False))
    llm_path.write_text(json.dumps(llm_data, ensure_ascii=False))

    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}, "meta": {}}), \
         patch("norms._core.save_norms_paragraphs"):
        stats = merge_llm_norm_results(det_path, llm_path)

    final = json.loads(det_path.read_text())
    assert final["checks"][0]["status"] == "active"  # статус НЕ поменялся
    assert final["checks"][0]["verified_via"] == "norms_authoritative"
    assert len(final["paragraph_checks"]) == 1
    assert stats["ignored_llm_status_attempts"] == 1
    assert stats["checks_updated_from_llm"] == 0
    assert final["meta"]["llm_may_change_status"] is False


# ─── paragraphs_to_verify: жёсткий фильтр ────────────────────────────────

def test_paragraphs_skipped_when_no_text_in_norms_main():
    """Если Norms-main говорит has_text=False — цитату не отправляем LLM."""
    norms_data = {"norms": {"СП 77.13330.2016": {
        "cited_as": ["СП 77.13330.2016"],
        "affected_findings": ["F-010"],
        "finding_norms": {"F-010": "СП 77.13330.2016, п. 5.1"},
    }}}
    stub = _stub_resolve({
        "СП 77.13330.2016": _found_payload(
            "СП 77.13330.2016", "active", has_text=False,
        ),
    })
    with patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert result["checks"][0]["has_text"] is False
    assert result["paragraphs_to_verify"] == []


def test_paragraphs_skipped_only_for_trusted_cache():
    """Trusted-запись в cache → skip. Legacy-запись → НЕ skip (пере-проверка)."""
    norms_data = {"norms": {"СП 256.1325800.2016": {
        "cited_as": ["СП 256.1325800.2016"],
        "affected_findings": ["F-020", "F-021"],
        "finding_norms": {
            "F-020": "СП 256.1325800.2016, п. 14.9",  # trusted
            "F-021": "СП 256.1325800.2016, п. 15.3",  # legacy
        },
    }}}
    stub = _stub_resolve({
        "СП 256.1325800.2016": _found_payload("СП 256.1325800.2016", "active"),
    })
    cache = {"paragraphs": {
        "СП 256.1325800.2016, п. 14.9": {
            "quote": "trusted text",
            "verified_via": "norms_mcp_paragraph",
            "source": "norms_main_mcp",
        },
        "СП 256.1325800.2016, п. 15.3": {
            "quote": "legacy text",
            "verified_via": "websearch",
            "source": "websearch+webfetch",
        },
    }}
    with patch("norms._core.load_norms_paragraphs", return_value=cache), \
         patch("norms.external_provider.resolve_norm_status", side_effect=stub):
        result = generate_deterministic_checks(norms_data, project_id="test")
    # F-020 (trusted) — пропущен; F-021 (legacy) — пере-проверяется
    assert len(result["paragraphs_to_verify"]) == 1
    assert result["paragraphs_to_verify"][0]["finding_id"] == "F-021"
    assert result["meta"]["paragraphs_trusted_skipped"] == 1
    assert result["meta"]["paragraphs_legacy_ignored"] == 1


def test_trusted_paragraph_entry_predicate():
    """Детерминированный contract для _is_trusted_paragraph_entry."""
    from norms._core import _is_trusted_paragraph_entry
    assert _is_trusted_paragraph_entry(
        {"verified_via": "norms_mcp_paragraph"}) is True
    assert _is_trusted_paragraph_entry(
        {"source": "norms_main_mcp"}) is True
    assert _is_trusted_paragraph_entry(
        {"verified_via": "websearch"}) is False
    assert _is_trusted_paragraph_entry(
        {"source": "websearch+webfetch"}) is False
    assert _is_trusted_paragraph_entry({}) is False
    assert _is_trusted_paragraph_entry(None) is False


# ─── validate_norm_checks нормализует legacy verified_via ────────────────
def test_validate_norm_checks(tmp_path):
    checks_data = {
        "meta": {},
        "checks": [
            # legacy verified_via='cache' → должен стать norms_authoritative
            {"doc_number": "СП 256", "status": "active",
             "verified_via": "cache", "needs_revision": False},
            # replaced без needs_revision — надо починить
            {"doc_number": "СП 31-110-2003", "status": "replaced",
             "verified_via": "norms_authoritative", "needs_revision": False},
        ],
    }
    checks_path = tmp_path / "norm_checks.json"
    checks_path.write_text(json.dumps(checks_data, ensure_ascii=False))
    result = validate_norm_checks(checks_path)
    assert result["total_checks"] == 2
    assert any("СП 31-110-2003" in f for f in result["fixes_applied"])
    assert any("legacy verified_via" in v for v in result["violations"])

    final = json.loads(checks_path.read_text())
    assert final["checks"][0]["verified_via"] == "norms_authoritative"
    assert final["checks"][1]["needs_revision"] is True
