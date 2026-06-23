"""Тесты PROTOTYPE v2-primary write path (Шаг 5/10).

Всё пишется/читается в tmp_path; production projects_v2 не трогается.
Production-чокпоинты к prototype НЕ подключены — это проверка механизма.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.storage import storage_write_facade as swf
from backend.app.services.storage.storage_write_facade import StorageWriteFacade, V2Target
from backend.app.services.storage import v2_primary_prototype as proto

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"


@pytest.fixture
def target():
    return V2Target(
        object_folder="214_Alia_ASTERUS",
        discipline="KJ",
        document_code="DOC-PROTO",
        version_id="v002",
    )


def _fake_output(dirpath: Path, *, findings_n: int = 7) -> Path:
    """Создать fake legacy _output с поздними артефактами аудита."""
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": f"F-{i:03d}", "category": "Критическое"}
                                 for i in range(findings_n)]}), encoding="utf-8")
    (dirpath / "03_findings_review.json").write_text(json.dumps({"reviews": []}), encoding="utf-8")
    (dirpath / "norm_checks.json").write_text(json.dumps({"checks": []}), encoding="utf-8")
    (dirpath / "optimization.json").write_text(json.dumps({"items": []}), encoding="utf-8")
    (dirpath / "optimization_review.json").write_text(json.dumps({"reviews": []}), encoding="utf-8")
    (dirpath / "pipeline_log.json").write_text(json.dumps({"stages": ["done"]}), encoding="utf-8")
    return dirpath


# ── Test 1: get_write_mode распознаёт v2 primary ────────────────────────────

def test_get_write_mode_detects_v2_primary(monkeypatch):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    assert swf.get_write_mode() == swf.WRITE_MODE_V2_PRIMARY
    assert swf.v2_is_primary() is True
    assert swf.v2_writes_enabled() is True


def test_unknown_write_mode_falls_back_to_legacy(monkeypatch):
    monkeypatch.setenv(_WMODE, "garbage")
    assert swf.get_write_mode() == swf.WRITE_MODE_LEGACY
    assert swf.v2_is_primary() is False


# ── Test 2: v2 primary save project info ────────────────────────────────────

def test_v2_primary_save_project_info(monkeypatch, tmp_path, target):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    v2 = tmp_path / "projects_v2"
    facade = StorageWriteFacade(v2_root=v2)

    res = proto.write_project_metadata_v2(facade, target, {"name": "DOC-PROTO", "section": "KJ"})
    assert res.v2_ok is True
    assert res.legacy_authoritative is False  # v2 primary → legacy не авторитетна

    # читается из v2 без legacy
    snap = proto.read_project_v2(v2, target)
    assert snap["found"] is True
    vj = json.loads((target.version_dir(v2) / "version.json").read_text(encoding="utf-8"))
    assert vj["project_info"]["name"] == "DOC-PROTO"


# ── Test 3: completed audit artifacts → v2 primary locations ────────────────

def test_completed_audit_artifacts_written_to_v2(monkeypatch, tmp_path, target):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    v2 = tmp_path / "projects_v2"
    facade = StorageWriteFacade(v2_root=v2)
    out = _fake_output(tmp_path / "legacy_output", findings_n=7)

    results = proto.write_completed_audit_artifacts_v2(facade, target, out, run_id="run_proto_1")
    assert set(results) == set(proto.LATE_AUDIT_ARTIFACTS)
    assert all(r.v2_ok for r in results.values())

    latest = target.version_dir(v2) / "03_analysis" / "latest"
    for name in ("03_findings.json", "norm_checks.json", "optimization.json"):
        assert (latest / name).is_file()

    snap = proto.read_project_v2(v2, target)
    assert snap["findings_count"] == 7
    assert snap["has_pipeline_log"] is True  # run_id → runs/<id>/pipeline_log.json


# ── Test 4: v2 read без legacy ──────────────────────────────────────────────

def test_v2_read_without_legacy(monkeypatch, tmp_path, target):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    v2 = tmp_path / "projects_v2"
    facade = StorageWriteFacade(v2_root=v2)
    proto.write_project_metadata_v2(facade, target, {"name": "DOC-PROTO"})
    proto.write_completed_audit_artifacts_v2(
        facade, target, _fake_output(tmp_path / "out", findings_n=5), run_id="r1")

    # legacy-каталога вообще нет
    assert not (tmp_path / "projects").exists()

    snap = proto.read_project_v2(v2, target)
    assert snap["legacy_used"] is False
    assert snap["found"] is True
    assert snap["findings_count"] == 5
    assert snap["analysis_files"]["has_03_findings"] is True


# ── Test 5: неполный v2 + нет legacy → НЕ фабриковать данные ─────────────────

def test_incomplete_v2_no_legacy_no_fake_data(monkeypatch, tmp_path, target):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    v2 = tmp_path / "projects_v2"
    facade = StorageWriteFacade(v2_root=v2)
    # пишем ТОЛЬКО метаданные, без analysis-артефактов
    proto.write_project_metadata_v2(facade, target, {"name": "DOC-PROTO"})

    snap = proto.read_project_v2(v2, target)
    assert snap["found"] is True
    assert snap["findings_count"] == 0          # честный ноль, не выдуманные данные
    assert snap["findings"] is None
    assert snap["analysis_files"]["has_03_findings"] is False
    assert snap["has_pipeline_log"] is False


# ── Test 6: v2 primary не мутирует пути вне tmp_path ─────────────────────────

def test_v2_primary_does_not_write_outside_tmp(monkeypatch, tmp_path, target):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    v2 = tmp_path / "projects_v2"
    facade = StorageWriteFacade(v2_root=v2)

    r1 = proto.write_project_metadata_v2(facade, target, {"name": "DOC-PROTO"})
    r2 = proto.write_input_bundle_v2(facade, target, [("doc.pdf", b"%PDF-1.4 fake")])
    r3map = proto.write_completed_audit_artifacts_v2(
        facade, target, _fake_output(tmp_path / "out"), run_id="r1")

    all_paths = list(r1.v2_paths) + list(r2.v2_paths)
    for r in r3map.values():
        all_paths += list(r.v2_paths)
    assert all_paths, "ожидались записанные v2-пути"
    # каждый записанный путь — строго под tmp_path
    for p in all_paths:
        assert Path(p).resolve().is_relative_to(tmp_path.resolve()), p
    # никаких legacy_write не передавалось → legacy не трогали
    assert r1.legacy_ok is None and r2.legacy_ok is None


# ── Test 7: export/read smoke — найти source PDF из v2 без legacy ────────────

def test_export_read_smoke_locates_pdf_from_v2(monkeypatch, tmp_path, target):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    v2 = tmp_path / "projects_v2"
    facade = StorageWriteFacade(v2_root=v2)
    proto.write_input_bundle_v2(facade, target, [
        ("DOC-PROTO.pdf", b"%PDF-1.4 fake pdf bytes"),
        ("DOC-PROTO_document.md", b"# md"),
    ])
    proto.write_completed_audit_artifacts_v2(
        facade, target, _fake_output(tmp_path / "out", findings_n=3), run_id="r1")

    assert not (tmp_path / "projects").exists()  # legacy отсутствует
    snap = proto.read_project_v2(v2, target)
    # экспорт/чтение может локализовать исходный PDF из v2
    assert "DOC-PROTO.pdf" in snap["input_files"]
    assert snap["findings_count"] == 3
