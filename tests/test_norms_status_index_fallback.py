"""
test_norms_status_index_fallback.py
-----------------------------------
Regression-тесты для классификационной верификации норм.

Покрытие:

1. **Mode A** — status_index.json отсутствует, vault отсутствует:
   - load_status_index() возвращает безопасный fallback или пустой каркас.
   - resolve_norm_status() не падает.
   - Известные из norms_db / missing_norms_vault → known_unverified.
   - Неизвестные с supported family → missing.
   - Произвольный текст без family → unsupported.

2. **Mode B** — корпус и индекс присутствуют:
   - vault-нормы → authoritative.
   - Override-only → authoritative.
   - Норма, известная только из norms_db (нет в vault и без override) →
     known_unverified, **не** authoritative.

3. Override приоритетнее всего:
   - active/replaced/cancelled из overrides отражаются корректно.

4. **Нормализация** — разные варианты записи дают одну и ту же норму.

5. Полные классификационные счётчики `_core.generate_deterministic_checks`
   разделяют known_unverified и missing.

Запуск:
    python3 -m pytest tests/test_norms_status_index_fallback.py -q
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from textwrap import dedent

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ---------------------------------------------------------------------------
# Утилиты setup
# ---------------------------------------------------------------------------

def _reset_module_cache(provider_mod):
    provider_mod._reset_cache()


def _set_paths(monkeypatch, provider_mod, *, status_index, norms_db,
               vault, missing_paths):
    monkeypatch.setattr(provider_mod, "NORMS_STATUS_INDEX_PATH", Path(status_index))
    monkeypatch.setattr(provider_mod, "NORMS_DB_PATH", Path(norms_db))
    monkeypatch.setattr(provider_mod, "NORMS_VAULT_PATH", Path(vault))
    monkeypatch.setattr(
        provider_mod, "MISSING_NORMS_VAULT_PATHS",
        tuple(Path(p) for p in missing_paths),
    )
    _reset_module_cache(provider_mod)


@pytest.fixture
def empty_sandbox(tmp_path, monkeypatch):
    """Полностью пустое окружение: ни индекса, ни vault, ни norms_db, ни missing."""
    from norms import external_provider as ep
    _set_paths(
        monkeypatch, ep,
        status_index=tmp_path / "no_status_index.json",
        norms_db=tmp_path / "no_norms_db.json",
        vault=tmp_path / "no_vault",
        missing_paths=[tmp_path / "no_missing.json"],
    )
    return ep


@pytest.fixture
def db_only_sandbox(tmp_path, monkeypatch):
    """Mode A: только norms_db, без vault и без status_index."""
    from norms import external_provider as ep
    db_path = tmp_path / "norms_db.json"
    db_path.write_text(
        json.dumps({
            "meta": {},
            "norms": {
                "СП 256.1325800.2016": {
                    "doc_number": "СП 256.1325800.2016",
                    "title": "Электроустановки жилых и общественных зданий",
                    "status": "active",
                    "edition_status": "ok",
                    "current_version": "СП 256.1325800.2016",
                },
                "ГОСТ 12.1.004-91": {
                    "doc_number": "ГОСТ 12.1.004-91",
                    "title": "Пожарная безопасность. Общие требования",
                    "status": "active",
                },
            },
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    _set_paths(
        monkeypatch, ep,
        status_index=tmp_path / "no_status_index.json",
        norms_db=db_path,
        vault=tmp_path / "no_vault",
        missing_paths=[tmp_path / "no_missing.json"],
    )
    return ep


@pytest.fixture
def overrides_only_sandbox(tmp_path, monkeypatch):
    """Mode A: только status_overrides.yaml без vault и без status_index."""
    from norms import external_provider as ep
    overrides_dir = tmp_path / "tools"
    overrides_dir.mkdir()
    (overrides_dir / "status_overrides.yaml").write_text(
        dedent("""
            overrides:
              СП 7.13130.2013:
                doc_status: active
                edition_status: current
              ВСН 59-88:
                doc_status: replaced
                replaced_by: СП 256.1325800.2016
              ГОСТ 9388-60:
                doc_status: cancelled
              ГОСТ 21.205-93:
                doc_status: unknown
        """).strip(),
        encoding="utf-8",
    )
    _set_paths(
        monkeypatch, ep,
        status_index=overrides_dir / "no_status_index.json",  # отсутствует
        norms_db=tmp_path / "no_norms_db.json",
        vault=tmp_path / "no_vault",
        missing_paths=[tmp_path / "no_missing.json"],
    )
    return ep


@pytest.fixture
def missing_only_sandbox(tmp_path, monkeypatch):
    """Mode A: только missing_norms_vault.json."""
    from norms import external_provider as ep
    missing_path = tmp_path / "missing_norms_vault.json"
    missing_path.write_text(
        json.dumps({
            "version": 1,
            "norms": {
                "СП 999.13330.2099": {
                    "doc_number": "СП 999.13330.2099",
                    "family": "СП",
                    "status": "pending",
                    "first_seen_at": "2026-04-01T00:00:00",
                },
            },
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    _set_paths(
        monkeypatch, ep,
        status_index=tmp_path / "no_status_index.json",
        norms_db=tmp_path / "no_norms_db.json",
        vault=tmp_path / "no_vault",
        missing_paths=[missing_path],
    )
    return ep


@pytest.fixture
def vault_index_sandbox(tmp_path, monkeypatch):
    """Mode B: подготовленный status_index.json. vault не нужен — индекс уже собран."""
    from norms import external_provider as ep
    status_path = tmp_path / "status_index.json"
    status_path.write_text(
        json.dumps({
            "meta": {"source": "vault"},
            "norms": [
                {
                    "code": "СП 256.1325800.2016",
                    "aliases": ["СП 256.1325800.2016"],
                    "type": "СП",
                    "year": 2016,
                    "title": "Электроустановки",
                    "doc_status": "active",
                    "edition_status": None,
                    "replacement_doc": None,
                    "source": "vault",
                    "authoritative": True,
                    "has_text": True,
                },
                {
                    "code": "ВСН 59-88",
                    "aliases": ["ВСН 59-88"],
                    "type": "ВСН",
                    "doc_status": "replaced",
                    "replacement_doc": "СП 256.1325800.2016",
                    "source": "override_only",
                    "authoritative": True,
                    "has_text": False,
                },
            ],
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    # vault: пустой каталог, чтобы файл vault.exists() = True (но без файлов).
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()

    db_path = tmp_path / "norms_db.json"
    db_path.write_text(
        json.dumps({
            "meta": {},
            "norms": {
                # ИЗВЕСТНА только в norms_db, нет в status_index
                "СП 50.13330.2024": {
                    "doc_number": "СП 50.13330.2024",
                    "title": "Тепловая защита",
                    "status": "active",
                },
            },
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    _set_paths(
        monkeypatch, ep,
        status_index=status_path,
        norms_db=db_path,
        vault=vault_dir,
        missing_paths=[tmp_path / "no_missing.json"],
    )
    return ep


# ---------------------------------------------------------------------------
# 1. Mode A — отсутствие всех источников
# ---------------------------------------------------------------------------

def test_empty_mode_returns_safe_fallback(empty_sandbox):
    """status_index отсутствует и нечего собрать → пустой каркас, без падений."""
    data = empty_sandbox.load_status_index()
    assert isinstance(data, dict)
    assert data.get("norms") == []


def test_empty_mode_diagnostics_reports_mode_A(empty_sandbox):
    diag = empty_sandbox.diagnostics()
    assert diag["mode"] in ("A_empty", "A_fallback_from_overrides")
    assert diag["status_index"]["exists"] is False
    assert diag["vault"]["exists"] is False


def test_empty_mode_supported_family_returns_missing(empty_sandbox):
    r = empty_sandbox.resolve_norm_status("СП 256.1325800.2016")
    assert r["classification"] == "missing"
    assert r["found"] is False
    assert r["authoritative"] is False


def test_empty_mode_unsupported_family_returns_unsupported(empty_sandbox):
    r = empty_sandbox.resolve_norm_status("какой-то произвольный текст")
    assert r["classification"] == "unsupported"
    assert r["supported_family"] is False


# ---------------------------------------------------------------------------
# 2. Mode A — норма известна только из norms_db / missing_norms_vault
# ---------------------------------------------------------------------------

def test_db_only_known_norm_is_known_unverified(db_only_sandbox):
    """Норма есть в norms_db, но не в status_index → known_unverified."""
    r = db_only_sandbox.resolve_norm_status("СП 256.1325800.2016")
    assert r["found"] is True
    assert r["classification"] == "known_unverified"
    assert r["authoritative"] is False
    assert r["source"] in {"norms_db", "missing_norms_vault"}


def test_db_only_unknown_norm_is_missing(db_only_sandbox):
    r = db_only_sandbox.resolve_norm_status("СП 99999.99999.9999")
    assert r["classification"] == "missing"
    assert r["found"] is False


def test_missing_vault_only_norm_is_known_unverified(missing_only_sandbox):
    """Норма есть только в missing_norms_vault.json → known_unverified."""
    r = missing_only_sandbox.resolve_norm_status("СП 999.13330.2099")
    assert r["classification"] == "known_unverified"
    assert r["found"] is True
    assert r["source"] == "missing_norms_vault"
    assert r["authoritative"] is False


# ---------------------------------------------------------------------------
# 3. Overrides всегда дают authoritative
# ---------------------------------------------------------------------------

def test_overrides_only_active(overrides_only_sandbox):
    r = overrides_only_sandbox.resolve_norm_status("СП 7.13130.2013")
    assert r["classification"] == "authoritative"
    assert r["status"] == "active"
    assert r["authoritative"] is True
    assert r["source"] == "override_only"


def test_overrides_only_replaced(overrides_only_sandbox):
    r = overrides_only_sandbox.resolve_norm_status("ВСН 59-88")
    assert r["classification"] == "authoritative"
    assert r["status"] == "replaced"
    assert r["replacement_doc"] == "СП 256.1325800.2016"


def test_overrides_only_cancelled(overrides_only_sandbox):
    r = overrides_only_sandbox.resolve_norm_status("ГОСТ 9388-60")
    assert r["classification"] == "authoritative"
    assert r["status"] == "cancelled"


def test_overrides_only_unknown(overrides_only_sandbox):
    r = overrides_only_sandbox.resolve_norm_status("ГОСТ 21.205-93")
    assert r["classification"] == "authoritative"
    assert r["doc_status"] == "unknown"


# ---------------------------------------------------------------------------
# 4. Mode B — норма из vault vs только из norms_db
# ---------------------------------------------------------------------------

def test_vault_norm_is_authoritative(vault_index_sandbox):
    r = vault_index_sandbox.resolve_norm_status("СП 256.1325800.2016")
    assert r["classification"] == "authoritative"
    assert r["authoritative"] is True
    assert r["status"] == "active"
    assert r["source"] == "vault"


def test_override_only_in_status_index_is_authoritative(vault_index_sandbox):
    r = vault_index_sandbox.resolve_norm_status("ВСН 59-88")
    assert r["classification"] == "authoritative"
    assert r["status"] == "replaced"
    assert r["source"] == "override_only"


def test_db_only_norm_not_authoritative_even_with_vault_present(vault_index_sandbox):
    """Норма есть в norms_db, но НЕТ в status_index → known_unverified.

    Это ключевой регресс-тест: нельзя называть authoritative то, что не
    подтверждено vault'ом или override'ом, даже если корпус частично есть.
    """
    r = vault_index_sandbox.resolve_norm_status("СП 50.13330.2024")
    assert r["classification"] == "known_unverified"
    assert r["authoritative"] is False
    assert r["source"] == "norms_db"


# ---------------------------------------------------------------------------
# 5. Нормализация — разные написания
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("variant", [
    "СП 256.1325800.2016",
    " СП 256.1325800.2016 ",
    "сп 256.1325800.2016",
    "СП  256.1325800.2016",        # двойные пробелы
    "СП 256.1325800.2016 (ред. 29.01.2024)",
    "СП 256.1325800.2016 с изменениями",
    "**СП 256.1325800.2016**",     # markdown
])
def test_normalization_variants_resolve_to_same_norm(variant, vault_index_sandbox):
    r = vault_index_sandbox.resolve_norm_status(variant)
    assert r["classification"] == "authoritative"
    assert r["matched_code"] == "СП 256.1325800.2016"


# ---------------------------------------------------------------------------
# 6. Семейство — каждое поддерживаемое определяется
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected_family", [
    ("СП 1.13130.2020", "СП"),
    ("ГОСТ 12.1.004-91", "ГОСТ"),
    ("ГОСТ Р 50571.5.54-2013", "ГОСТ Р"),
    ("СНиП 2.04.01-85", "СНиП"),
    ("ВСН 59-88", "ВСН"),
    ("МДС 12-29.2006", "МДС"),
    ("РД 34.21.122-87", "РД"),
    ("ПУЭ-7", "ПУЭ"),
    ("Постановление Правительства РФ от 28.05.2007 N 87", "ПП РФ"),
    ("Федеральный закон 123-ФЗ", "ФЗ"),
])
def test_family_detection(raw, expected_family, empty_sandbox):
    r = empty_sandbox.resolve_norm_status(raw)
    assert r["detected_family"] == expected_family


# ---------------------------------------------------------------------------
# 7. _core.generate_deterministic_checks — распределение по 4-м категориям
# ---------------------------------------------------------------------------

def test_generate_deterministic_checks_splits_known_unverified_from_missing(
    vault_index_sandbox,
):
    """Известная только в norms_db и неизвестная нигде должны попадать в
    РАЗНЫЕ ведра: known_unverified и missing соответственно."""
    from norms import _core
    norms_data = {
        "norms": {
            # vault → authoritative
            "СП 256.1325800.2016": {
                "cited_as": ["СП 256.1325800.2016"],
                "affected_findings": ["F-1"],
                "finding_norms": {"F-1": "СП 256.1325800.2016"},
            },
            # только norms_db → known_unverified
            "СП 50.13330.2024": {
                "cited_as": ["СП 50.13330.2024"],
                "affected_findings": ["F-2"],
                "finding_norms": {"F-2": "СП 50.13330.2024"},
            },
            # нигде → missing
            "СП 99999.99999.9999": {
                "cited_as": ["СП 99999.99999.9999"],
                "affected_findings": ["F-3"],
                "finding_norms": {"F-3": "СП 99999.99999.9999"},
            },
            # без распознанного семейства → unsupported
            "ничего нет": {
                "cited_as": ["ничего нет"],
                "affected_findings": ["F-4"],
                "finding_norms": {"F-4": "ничего нет"},
            },
        }
    }
    result = _core.generate_deterministic_checks(norms_data)
    meta = result["meta"]
    assert meta["authoritative"] == 1
    assert meta["known_unverified"] == 1
    assert meta["missing"] == 1
    assert meta["unsupported"] == 1
    # known_unverified ≠ missing
    assert any(
        c["verified_via"] == "norms_known_unverified"
        for c in result["checks"]
    )
    assert "known_unverified_norms" in result
    assert len(result["known_unverified_norms"]) == 1


def test_fallback_status_index_does_not_promote_norms_db_to_authoritative(
    db_only_sandbox,
):
    """Если статус-индекс пуст и собирается из norms_db, нормы оттуда НЕ должны
    стать authoritative — должны оставаться known_unverified."""
    r = db_only_sandbox.resolve_norm_status("СП 256.1325800.2016")
    assert r["authoritative"] is False
    assert r["classification"] == "known_unverified"
