"""The built status_index is the only runtime source of document status."""
from __future__ import annotations

import json

import pytest

from norms._core import generate_deterministic_checks


@pytest.fixture()
def provider(tmp_path, monkeypatch):
    from norms import external_provider as ep

    status_path = tmp_path / "status_index.json"
    status_path.write_text(json.dumps({
        "meta": {"source": "vault+status_overrides"},
        "norms": [
            {
                "code": "СП 256.1325800.2016",
                "aliases": ["СП 256.1325800.2016", "СП 256.132580.2016"],
                "type": "СП", "year": 2016, "title": "Электроустановки",
                "doc_status": "active", "edition_status": "current",
                "replacement_doc": None, "current_version": "СП 256.1325800.2016",
                "source": "vault", "authoritative": True, "has_text": True,
                "file": "sp.md",
            },
            {
                "code": "ВСН 59-88", "aliases": ["ВСН 59-88"], "type": "ВСН",
                "doc_status": "replaced", "edition_status": None,
                "replacement_doc": "СП 256.1325800.2016",
                "current_version": "СП 256.1325800.2016",
                "source": "override_only", "authoritative": True, "has_text": False,
                "file": None,
            },
            {
                "code": "ГОСТ Р 21.101-2020",
                "aliases": ["ГОСТ Р 21.101-2020", "ГОСТ Р 21_101-2020"],
                "type": "ГОСТ Р", "year": 2020,
                "doc_status": "replaced", "edition_status": None,
                "replacement_doc": "ГОСТ Р 21.101-2026",
                "current_version": "ГОСТ Р 21.101-2026",
                "effective_from": "2026-04-01",
                "source": "vault", "authoritative": True, "has_text": True,
                "file": "gost.md",
            },
        ],
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(ep, "NORMS_STATUS_INDEX_PATH", status_path)
    ep._reset_cache()
    yield ep
    ep._reset_cache()


def test_missing_index_is_safe_and_never_falls_back(tmp_path, monkeypatch):
    from norms import external_provider as ep

    # A tempting legacy database is deliberately present but has no runtime
    # path in the provider. It therefore cannot override the canonical index.
    (tmp_path / "norms_db.json").write_text(json.dumps({
        "norms": {"СП 256.1325800.2016": {"status": "active"}}
    }), encoding="utf-8")
    monkeypatch.setattr(ep, "NORMS_STATUS_INDEX_PATH", tmp_path / "missing.json")
    ep._reset_cache()

    assert ep.load_status_index()["norms"] == []
    result = ep.resolve_norm_status("СП 256.1325800.2016")
    assert result["found"] is False
    assert result["resolution_reason"] == "not_in_index"
    assert result["source"] == "not_found"


def test_vault_and_compiled_override_are_authoritative(provider):
    vault = provider.resolve_norm_status("СП 256.1325800.2016")
    override = provider.resolve_norm_status("ВСН 59-88")

    assert (vault["status"], vault["source"], vault["has_text"]) == (
        "active", "vault", True,
    )
    assert override["status"] == "replaced"
    assert override["source"] == "override_only"
    assert override["resolution_reason"] == "manual_override"
    assert override["replacement_doc"] == "СП 256.1325800.2016"


@pytest.mark.parametrize("raw", [
    "СП 256.1325800.2016",
    " СП 256.1325800.2016 ",
    "сп 256.1325800.2016",
    "СП  256.1325800.2016",
    "СП 256.1325800.2016 (ред. 29.01.2024)",
    "**СП 256.1325800.2016**",
    "СП 256.132580.2016",
])
def test_normalization_variants_resolve_to_same_index_entry(provider, raw):
    result = provider.resolve_norm_status(raw)
    assert result["matched_code"] == "СП 256.1325800.2016"
    assert result["authoritative"] is True


def test_missing_supported_and_unsupported_are_distinct(provider):
    missing = provider.resolve_norm_status("СП 999.13330.2099")
    unsupported = provider.resolve_norm_status("произвольный текст")

    assert missing["resolution_reason"] == "not_in_index"
    assert missing["supported_family"] is True
    assert missing["needs_manual_addition"] is True
    assert unsupported["resolution_reason"] == "unsupported_family"
    assert unsupported["supported_family"] is False


@pytest.mark.parametrize(("raw", "family"), [
    ("СП 1.13130.2020", "СП"),
    ("ГОСТ 12.1.004-91", "ГОСТ"),
    ("ГОСТ Р 50571.5.54-2013", "ГОСТ Р"),
    ("СНиП 2.04.01-85", "СНиП"),
    ("ВСН 59-88", "ВСН"),
    ("МДС 12-29.2006", "МДС"),
    ("РД 34.21.122-87", "РД"),
    ("ПУЭ-7", "ПУЭ"),
    ("ПП РФ №87", "ПП РФ"),
    ("ФЗ 123-ФЗ", "ФЗ"),
    ("СО 153-34.20.501-2003", "СО"),
])
def test_family_detection_for_missing_entries(provider, raw, family):
    result = provider.resolve_norm_status(raw)
    assert result["detected_family"] == family
    assert result["supported_family"] is True


def test_replacement_effective_date_is_exposed(provider):
    result = provider.resolve_norm_status("ГОСТ Р 21.101-2020")
    assert result["status"] == "replaced"
    assert result["replacement_doc"] == "ГОСТ Р 21.101-2026"
    assert result["effective_from"] == "2026-04-01"


def test_deterministic_checks_split_missing_from_unsupported(provider):
    data = {
        "norms": {
            "СП 256.1325800.2016": {
                "cited_as": ["СП 256.1325800.2016"],
                "affected_findings": ["F-1"], "contexts": [], "finding_norms": {},
            },
            "СП 999.13330.2099": {
                "cited_as": ["СП 999.13330.2099"],
                "affected_findings": ["F-2"], "contexts": [], "finding_norms": {},
            },
            "произвольный текст": {
                "cited_as": ["произвольный текст"],
                "affected_findings": ["F-3"], "contexts": [], "finding_norms": {},
            },
        }
    }
    result = generate_deterministic_checks(data)
    assert result["meta"]["authoritative"] == 1
    assert len(result["missing_norms"]) == 1
    assert len(result["unsupported_norms"]) == 1
