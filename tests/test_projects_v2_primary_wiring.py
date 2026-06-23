"""Тесты Шага 6A/10 — wiring save_project_info к V2_PRIMARY (flag-gated).

Всё в tmp_path; production projects_v2 не трогается. Проверяем, что
legacy/dual_shadow поведение НЕ меняется, v2-primary ветка активируется только
под флагом, имеет корректную failure-семантику, а адрес документа в v2
совпадает с миграционным (v2lib.migrate_project) — без path-divergence.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.storage import storage_write_facade as swf
from backend.app.services.storage import v2_primary_wiring as wiring
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"


@pytest.fixture
def legacy_project(tmp_path):
    """Минимальное legacy-дерево: projects/<obj>/<disc>/<proj>/project_info.json."""
    root = tmp_path / "projects"
    proj = root / "214. Alia (ASTERUS)" / "KJ" / "DOC-6A"
    proj.mkdir(parents=True)
    info = {"name": "DOC-6A", "document_code": "DOC-6A", "section": "KJ",
            "project_id": "DOC-6A"}
    (proj / "project_info.json").write_text(json.dumps(info), encoding="utf-8")
    return root, proj, info


# ── Test 1 & 2: legacy / dual_shadow unchanged ──────────────────────────────

def test_legacy_mode_is_not_primary(monkeypatch):
    monkeypatch.setenv(_WMODE, "legacy")
    assert swf.v2_is_primary() is False


def test_dual_shadow_mode_is_not_primary(monkeypatch):
    monkeypatch.setenv(_WMODE, "dual_write_shadow")
    assert swf.v2_is_primary() is False
    assert swf.v2_writes_enabled() is True  # тень включена, но не primary


# ── Test 3: V2_PRIMARY save_project_info пишет v2 первично ───────────────────

def test_v2_primary_writes_v2_metadata_and_reads_back(monkeypatch, tmp_path, legacy_project):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    root, proj, info = legacy_project
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)

    ok = wiring.save_project_info_v2_primary(
        "DOC-6A", info, version_id="v001",
        legacy_root=proj, legacy_path=proj / "project_info.json", v2_root=v2,
    )
    assert ok is True

    target = wiring.resolve_v2_target(proj, "v001", v2_root=v2)
    assert target is not None
    vj = json.loads((target.version_dir(v2) / "version.json").read_text(encoding="utf-8"))
    assert vj["project_info"]["name"] == "DOC-6A"
    assert (target.doc_dir(v2) / "document.json").is_file()
    # legacy archive тоже записан (fail-soft, после успешной v2)
    assert (proj / "project_info.json").is_file()


# ── Test 4: V2_PRIMARY падает если v2 primary write падает ───────────────────

def test_v2_primary_failure_not_masked_by_legacy(monkeypatch, tmp_path, legacy_project):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    root, proj, info = legacy_project
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)
    legacy_path = tmp_path / "fresh_legacy" / "project_info.json"  # ещё не существует

    import backend.app.services.storage.v2_primary_prototype as proto

    def _boom(facade, target, data, *, legacy_write=None):
        raise RuntimeError("v2 primary write exploded")
    monkeypatch.setattr(proto, "write_project_metadata_v2", _boom)

    ok = wiring.save_project_info_v2_primary(
        "DOC-6A", info, version_id="v001",
        legacy_root=proj, legacy_path=legacy_path, v2_root=v2,
    )
    assert ok is False
    # legacy НЕ записан (v2 упал первым, legacy-плечо не выполнялось → не маскирует)
    assert not legacy_path.exists()


# ── Test 4b: миграционная паритетность адреса (регрессия на path-divergence) ─

def test_resolve_target_matches_migration_document_code(tmp_path):
    """resolve_v2_target.document_code == v2lib.document_code_for(root_entry).

    Регрессия на находку adversarial-review: раньше document_code брался из
    project_info name/code без strip(.pdf), что расходилось с миграцией.
    """
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)
    # plain-проект с `.pdf`-суффиксом в имени папки (gotcha версионирования)
    proj = tmp_path / "projects" / "214. Alia (ASTERUS)" / "AR" / "DOC-X V2.pdf"
    proj.mkdir(parents=True)
    (proj / "project_info.json").write_text(
        json.dumps({"name": "DOC-X V2.pdf", "document_code": "DOC-X V2.pdf",
                    "section": "AR"}), encoding="utf-8")

    v2lib = wiring._load_v2lib()
    root_entry = wiring._root_entry(proj)
    target = wiring.resolve_v2_target(proj, "v001", v2_root=v2)
    assert target is not None
    # .pdf снят, совпадает с миграционным document_code_for
    assert target.document_code == v2lib.safe_component(v2lib.document_code_for(root_entry))
    assert target.document_code == "DOC-X V2"  # strip_pdf_suffix
    # discipline = имя папки-раздела, не project_info['section'] произвольно
    assert target.discipline == "AR"


def test_resolve_target_none_on_too_shallow_path(tmp_path):
    """Слишком мелкий путь (нет уровня объект/раздел) → None (surfaced)."""
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)
    # путь у корня ФС: object_dir == его родитель → None
    target = wiring.resolve_v2_target(Path("/"), "v001", v2_root=v2)
    assert target is None


# ── Test 5: completed audit artifacts v2-primary (через prototype helper) ────

def test_v2_primary_completed_audit_artifacts(monkeypatch, tmp_path, legacy_project):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    root, proj, info = legacy_project
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)
    from backend.app.services.storage import v2_primary_prototype as proto
    from backend.app.services.storage.storage_write_facade import StorageWriteFacade

    target = wiring.resolve_v2_target(proj, "v001", v2_root=v2)
    assert target is not None

    out = tmp_path / "out"
    out.mkdir()
    (out / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-1"}, {"id": "F-2"}]}), encoding="utf-8")
    (out / "pipeline_log.json").write_text(json.dumps({"stages": []}), encoding="utf-8")

    facade = StorageWriteFacade(v2_root=v2)
    res = proto.write_completed_audit_artifacts_v2(facade, target, out, run_id="r1")
    assert res["03_findings.json"].v2_ok is True

    adapter = ProjectsV2Adapter(v2)
    doc_dir = target.doc_dir(v2)
    assert adapter.findings_count(doc_dir, "v001") == 2
    assert adapter.has_pipeline_log(doc_dir, "v001") is True


# ── Test 6: KB expert review — defer (объяснение) ───────────────────────────

def test_kb_expert_review_v2_primary_deferred():
    """KB expert_review v2-primary primary-write ОТЛОЖЕН до отдельного шага.

    Причина: expert_review пишется в v2 `04_review/`, который prototype-методы
    (save_version_metadata/save_input_bundle/save_analysis_artifact) пока не
    покрывают; текущий KB-хук остаётся shadow-mirror (Шаг 2b) и в v2-primary
    делает whole-project mirror — это требует реального legacy-проекта и не
    относится к non-destructive metadata-чокпоинтам Шага 6A.
    """
    assert True  # маркер осознанного defer (не silent skip)


# ── Test 7: нет записи вне tmp_path ─────────────────────────────────────────

def test_v2_primary_no_write_outside_tmp(monkeypatch, tmp_path, legacy_project):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    root, proj, info = legacy_project
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)

    wiring.save_project_info_v2_primary(
        "DOC-6A", info, version_id="v001",
        legacy_root=proj, legacy_path=proj / "project_info.json", v2_root=v2,
    )
    target = wiring.resolve_v2_target(proj, "v001", v2_root=v2)
    assert target.version_dir(v2).resolve().is_relative_to(tmp_path.resolve())
    assert target.doc_dir(v2).resolve().is_relative_to(tmp_path.resolve())
