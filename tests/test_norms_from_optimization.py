"""Нормы предложений по оптимизации попадают в контур верификации (этап 04).

Промпт оптимизации требует поле `norm` и соответствие ДЕЙСТВУЮЩИМ нормам, но
справочника модели не даёт, а этап 04 исторически читал только 03_findings.json.
Ссылки предложений не проверял никто — модель уходила гуглить их сама, без следа
и без сверки. Здесь проверяется сбор норм из optimization.json и слияние карт.
"""
import json

import pytest

from norms import (
    extract_norms_from_findings,
    extract_norms_from_optimization,
    format_optimizations_to_fix,
    merge_norms_maps,
)


def _write(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


@pytest.fixture
def optimization_file(tmp_path):
    return _write(tmp_path / "optimization.json", {
        "meta": {"project_id": "TEST", "total_items": 3},
        "items": [
            {
                "id": "OPT-001",
                "norm": "СП 30.13330.2020, п. 9.14",
                "proposed": "Заменить на аналог по СП 73.13330.2016",
                "current": "Текущее решение",
                "risks": "минимальные",
            },
            {
                "id": "OPT-002",
                "norm": "СП 30.13330.2020",
                "proposed": "Унифицировать типоразмеры",
                "current": "",
                "risks": "",
            },
            {"id": "OPT-003", "norm": "", "proposed": "Без нормы", "current": "", "risks": ""},
        ],
    })


def test_extracts_norms_from_all_text_fields(optimization_file):
    data = extract_norms_from_optimization(optimization_file)

    assert data["total_optimizations"] == 3
    # СП 73.13330.2016 упомянута только в `proposed` — её тоже надо поймать.
    assert set(data["norms"]) == {"СП 30.13330.2020", "СП 73.13330.2016"}
    assert data["norms"]["СП 30.13330.2020"]["affected_optimizations"] == ["OPT-001", "OPT-002"]
    assert data["norms"]["СП 73.13330.2016"]["affected_optimizations"] == ["OPT-001"]


def test_optimization_ids_never_leak_into_affected_findings(optimization_file):
    """affected_findings читают как F-ID (missing_norms_service, format_findings_to_fix).

    OPT-ID там сломал бы их, поэтому принадлежность живёт в отдельном списке.
    """
    data = extract_norms_from_optimization(optimization_file)
    for info in data["norms"].values():
        assert info["affected_findings"] == []


def test_merge_keeps_both_sides_for_shared_norm(tmp_path, optimization_file):
    findings = _write(tmp_path / "03_findings.json", {
        "findings": [
            {"id": "F-001", "norm": "СП 30.13330.2020, п. 5.1", "problem": "Проблема"},
            {"id": "F-002", "norm": "ГОСТ 12.2.063-2015", "problem": "Другая"},
        ],
    })

    merged = merge_norms_maps(
        extract_norms_from_findings(findings),
        extract_norms_from_optimization(optimization_file),
    )

    shared = merged["norms"]["СП 30.13330.2020"]
    assert shared["affected_findings"] == ["F-001"]
    assert shared["affected_optimizations"] == ["OPT-001", "OPT-002"]
    # Норма только из замечаний и норма только из предложений — обе на месте.
    assert merged["norms"]["ГОСТ 12.2.063-2015"]["affected_optimizations"] == []
    assert merged["norms"]["СП 73.13330.2016"]["affected_findings"] == []
    assert merged["total_findings"] == 2
    assert merged["total_optimizations"] == 3


def test_merge_of_findings_only_is_unchanged(tmp_path):
    """Слияние одной карты не должно менять поведение легаси-пути."""
    findings = _write(tmp_path / "03_findings.json", {
        "findings": [{"id": "F-001", "norm": "СП 30.13330.2020", "problem": "Проблема"}],
    })
    data = extract_norms_from_findings(findings)
    merged = merge_norms_maps(data)

    assert set(merged["norms"]) == set(data["norms"])
    assert merged["norms"]["СП 30.13330.2020"]["affected_findings"] == ["F-001"]
    assert merged["total_findings"] == 1


def test_format_optimizations_to_fix_lists_only_bad_norms(tmp_path, optimization_file):
    checks = _write(tmp_path / "norm_checks.json", {
        "checks": [
            {
                "norm_as_cited": "СП 30.13330.2020",
                "status": "replaced",
                "details": "Заменён новым документом",
                "current_version": "СП 30.13330.2025",
                "replacement_doc": "СП 30.13330.2025",
                "needs_revision": True,
                "affected_findings": [],
                "affected_optimizations": ["OPT-001"],
            },
            {
                "norm_as_cited": "СП 73.13330.2016",
                "status": "active",
                "needs_revision": False,
                "affected_optimizations": ["OPT-001"],
            },
        ],
    })

    out = format_optimizations_to_fix(checks, optimization_file)

    assert "OPT-001" in out
    assert "СП 30.13330.2025" in out          # автору сообщают, чем заменена
    assert "OPT-002" not in out               # его норма не помечена needs_revision
    assert "OPT-003" not in out               # у него норм нет вовсе


def test_format_optimizations_to_fix_silent_when_all_good(tmp_path, optimization_file):
    checks = _write(tmp_path / "norm_checks.json", {
        "checks": [{
            "norm_as_cited": "СП 30.13330.2020",
            "status": "active",
            "needs_revision": False,
            "affected_optimizations": ["OPT-001"],
        }],
    })

    assert "Пересмотр не требуется" in format_optimizations_to_fix(checks, optimization_file)


def test_missing_optimization_file_raises_not_silently_empty(tmp_path):
    """Раннер сам решает, есть ли файл; молча возвращать пустоту функция не должна."""
    with pytest.raises(OSError):
        extract_norms_from_optimization(tmp_path / "нет-такого.json")
