"""
Тесты StorageWriteFacade — подготовительный фасад записи в projects_v2 (Step 8/10).

Ключевые инварианты (см. docs/projects_v2_storage_standard.md):
  * production default = `legacy` → в projects_v2 НИЧЕГО не пишется;
  * dual_write_shadow → legacy ПЕРВОЙ (авторитетна), потом v2-тень;
  * сбой v2-записи в shadow НЕ ломает legacy (fail-soft + diagnostic);
  * projects_v2_primary → v2 primary, legacy архив (не authoritative);
  * деструктив в v2 заблокирован;
  * без явного режима фасад не создаёт ни одного файла в v2.

Изоляция: каждый тест пишет ТОЛЬКО в tmp_path; режим выставляется через
monkeypatch.setenv (default-поведение проверяется БЕЗ env).
"""

from __future__ import annotations

import json

import pytest

from backend.app.services.storage import storage_write_facade as swf
from backend.app.services.storage.storage_write_facade import (
    StorageWriteFacade,
    V2Target,
    DestructiveWriteBlocked,
    StorageWriteError,
    WRITE_MODE_LEGACY,
    WRITE_MODE_DUAL_SHADOW,
    WRITE_MODE_V2_PRIMARY,
)

ENV = "AUDIT_PROJECTS_V2_WRITE_MODE"


@pytest.fixture
def target() -> V2Target:
    return V2Target(
        object_folder="214_Alia_ASTERUS",
        discipline="EOM",
        document_code="13АВ-РД-ЭО-К3",
        version_id="v1",
    )


def _latest(tmp_path, target: V2Target, name: str):
    return (tmp_path / "objects" / target.object_folder / "disciplines"
            / target.discipline / "documents" / target.document_code
            / "versions" / "v001" / "03_analysis" / "latest" / name)


# --------------------------------------------------------------------------
# режимы по умолчанию
# --------------------------------------------------------------------------

def test_default_mode_is_legacy(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert swf.get_write_mode() == WRITE_MODE_LEGACY
    assert swf.v2_writes_enabled() is False
    assert swf.v2_is_primary() is False


def test_unknown_mode_falls_back_to_legacy(monkeypatch):
    monkeypatch.setenv(ENV, "garbage_mode")
    assert swf.get_write_mode() == WRITE_MODE_LEGACY
    assert swf.v2_writes_enabled() is False


def test_dual_shadow_not_on_by_default(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert swf.get_write_mode() != WRITE_MODE_DUAL_SHADOW


def test_vid_normalization():
    assert swf.normalize_vid_for_disk("v1") == "v001"
    assert swf.normalize_vid_for_disk("V2") == "v002"
    assert swf.normalize_vid_for_disk("v003") == "v003"
    assert swf.normalize_vid_for_disk("v012") == "v012"


# --------------------------------------------------------------------------
# legacy: v2 не трогается
# --------------------------------------------------------------------------

def test_legacy_mode_does_not_write_v2(monkeypatch, tmp_path, target):
    monkeypatch.delenv(ENV, raising=False)  # default legacy
    facade = StorageWriteFacade(v2_root=tmp_path)
    calls = []
    res = facade.save_analysis_artifact(
        target, "03_findings.json", {"findings": []},
        legacy_write=lambda: calls.append("legacy"),
    )
    assert res.mode == WRITE_MODE_LEGACY
    assert res.legacy_ok is True
    assert res.v2_attempted is False
    assert res.v2_ok is None
    assert calls == ["legacy"]
    # ни одного файла в v2
    assert not (tmp_path / "objects").exists()


def test_legacy_mode_without_legacy_writer_is_noop(monkeypatch, tmp_path, target):
    monkeypatch.delenv(ENV, raising=False)
    facade = StorageWriteFacade(v2_root=tmp_path)
    res = facade.save_version_metadata(target, {"version_no": 1})
    assert res.legacy_ok is None  # writer не передан
    assert res.v2_attempted is False
    assert not (tmp_path / "objects").exists()


# --------------------------------------------------------------------------
# dual_write_shadow: legacy первой, затем v2
# --------------------------------------------------------------------------

def test_dual_shadow_writes_legacy_first_then_v2(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    order = []
    res = facade.save_analysis_artifact(
        target, "03_findings.json",
        {"findings": [{"id": "F-001"}]},
        legacy_write=lambda: order.append("legacy"),
    )
    assert res.legacy_ok is True
    assert res.v2_ok is True
    assert res.legacy_authoritative is True
    assert order == ["legacy"]
    p = _latest(tmp_path, target, "03_findings.json")
    assert p.exists()
    assert json.loads(p.read_text(encoding="utf-8"))["findings"][0]["id"] == "F-001"


def test_save_input_bundle_writes_01_input(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    res = facade.save_input_bundle(
        target,
        [("document.pdf", b"%PDF-1.4"), ("document_document.md", b"# md")],
        legacy_write=lambda: None,
    )
    assert res.v2_ok is True
    base = (tmp_path / "objects" / target.object_folder / "disciplines" / target.discipline
            / "documents" / target.document_code / "versions" / "v001" / "01_input")
    assert (base / "document.pdf").read_bytes() == b"%PDF-1.4"
    assert (base / "document_document.md").read_bytes() == b"# md"


def test_save_analysis_artifact_writes_latest_and_run(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    res = facade.save_analysis_artifact(
        target, "01_blocks_analysis.json", {"blocks": []},
        run_id="run_20260616", legacy_write=lambda: None,
    )
    assert res.v2_ok is True
    assert _latest(tmp_path, target, "01_blocks_analysis.json").exists()
    run = (tmp_path / "objects" / target.object_folder / "disciplines" / target.discipline
           / "documents" / target.document_code / "versions" / "v001"
           / "03_analysis" / "runs" / "run_20260616" / "01_blocks_analysis.json")
    assert run.exists()


@pytest.mark.parametrize("status", ["source_only", "legacy_partial", "complete"])
def test_version_metadata_status_preserved(monkeypatch, tmp_path, target, status):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    res = facade.save_version_metadata(
        target, {"version_no": 1, "label": "V1", "analysis_status": status},
        legacy_write=lambda: None,
    )
    assert res.v2_ok is True
    vpath = (tmp_path / "objects" / target.object_folder / "disciplines" / target.discipline
             / "documents" / target.document_code / "versions" / "v001" / "version.json")
    vj = json.loads(vpath.read_text(encoding="utf-8"))
    assert vj["analysis_status"] == status
    assert vj["version_id"] == "v001"
    # каркас документа создан
    doc_dir = vpath.parents[2]
    assert (doc_dir / "document.json").exists()
    assert (doc_dir / "current_version.txt").read_text(encoding="utf-8") == "v001"


# --------------------------------------------------------------------------
# shadow: сбой v2 НЕ ломает legacy
# --------------------------------------------------------------------------

def test_shadow_v2_failure_does_not_break_legacy(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    # принудительно ломаем резолв v2-root
    facade.v2_root = lambda: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore
    order = []
    res = facade.save_version_metadata(
        target, {"version_no": 1}, legacy_write=lambda: order.append("legacy"),
    )
    assert order == ["legacy"]
    assert res.legacy_ok is True       # legacy НЕ пострадала
    assert res.v2_ok is False          # v2 упала
    assert res.v2_error and "RuntimeError" in res.v2_error
    assert not (tmp_path / "objects").exists()


# --------------------------------------------------------------------------
# projects_v2_primary
# --------------------------------------------------------------------------

def test_v2_primary_writes_v2_and_archives_legacy(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_V2_PRIMARY)
    facade = StorageWriteFacade(v2_root=tmp_path)
    archived = []
    res = facade.save_analysis_artifact(
        target, "03_findings.json", {"findings": []},
        legacy_write=lambda: archived.append("archive"),
    )
    assert res.v2_ok is True
    assert res.legacy_ok is True
    assert res.legacy_authoritative is False
    assert archived == ["archive"]
    assert _latest(tmp_path, target, "03_findings.json").exists()


# --------------------------------------------------------------------------
# деструктив + безопасность имён
# --------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [WRITE_MODE_LEGACY, WRITE_MODE_DUAL_SHADOW, WRITE_MODE_V2_PRIMARY])
def test_destructive_blocked_in_all_modes(monkeypatch, tmp_path, mode):
    monkeypatch.setenv(ENV, mode)
    facade = StorageWriteFacade(v2_root=tmp_path)
    with pytest.raises(DestructiveWriteBlocked):
        facade.block_destructive("clean_project_data")


def test_unsafe_input_filename_rejected(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    # путь-обход должен превратиться в basename и (для пустого) — отказ
    res = facade.save_input_bundle(target, [("../../etc/passwd", b"x")], legacy_write=lambda: None)
    # basename('../../etc/passwd') == 'passwd' → пишется внутрь 01_input, не наружу
    inp = (tmp_path / "objects" / target.object_folder / "disciplines" / target.discipline
           / "documents" / target.document_code / "versions" / "v001" / "01_input")
    assert (inp / "passwd").exists()
    assert not (tmp_path / "etc").exists()
    assert res.v2_ok is True


def test_empty_input_filename_raises_in_shadow_but_keeps_legacy(monkeypatch, tmp_path, target):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    facade = StorageWriteFacade(v2_root=tmp_path)
    order = []
    res = facade.save_input_bundle(target, [("   ", b"x")], legacy_write=lambda: order.append("legacy"))
    # legacy прошла, v2 упала fail-soft (StorageWriteError внутри _v2)
    assert order == ["legacy"]
    assert res.legacy_ok is True
    assert res.v2_ok is False
    assert "StorageWriteError" in (res.v2_error or "")


# ==========================================================================
# Step 9/10 — shadow_mirror_project (production mirror через миграцию)
# ==========================================================================

def _make_legacy_project(data_root):
    """Создать синтетический legacy-проект + objects.json в temp DATA root."""
    import json as _json
    obj_name = "214. Alia (ASTERUS)"
    proj = data_root / "projects" / obj_name / "EOM" / "13АВ-РД-ЭО-К3"
    proj.mkdir(parents=True)
    (data_root / "backend" / "app" / "data").mkdir(parents=True)
    (data_root / "backend" / "app" / "data" / "objects.json").write_text(
        _json.dumps({"objects": [
            {"id": "aliaobj01", "name": obj_name,
             "projects_dir": str(data_root / "projects" / obj_name)}
        ]}, ensure_ascii=False), encoding="utf-8")
    (proj / "document.pdf").write_bytes(b"%PDF-1.4 canary")
    (proj / "13АВ-РД-ЭО-К3_document.md").write_text("# md", encoding="utf-8")
    (proj / "project_info.json").write_text(
        _json.dumps({"project_id": "13АВ-РД-ЭО-К3", "name": "13АВ-РД-ЭО-К3",
                     "section": "EOM", "pdf_file": "document.pdf"}, ensure_ascii=False),
        encoding="utf-8")
    (proj / "_output").mkdir()
    return proj


def test_shadow_mirror_path_legacy_noop(monkeypatch, tmp_path):
    proj = _make_legacy_project(tmp_path)
    v2root = tmp_path / "projects_v2"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2root))
    monkeypatch.delenv(ENV, raising=False)            # legacy
    swf._default_facade = None                         # reset singleton → подхватит env
    res = swf.shadow_mirror_project_path_safe(proj)
    assert res is None
    assert not v2root.exists()                         # v2 не создан в legacy


def test_shadow_mirror_path_shadow_writes_v2(monkeypatch, tmp_path):
    proj = _make_legacy_project(tmp_path)
    v2root = tmp_path / "projects_v2"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2root))
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    swf._default_facade = None
    res = swf.shadow_mirror_project_path_safe(proj)
    assert res is not None and res.v2_ok is True
    docdir = (v2root / "objects" / "214_Alia_ASTERUS" / "disciplines" / "EOM"
              / "documents" / "13АВ-РД-ЭО-К3")
    assert (docdir / "document.json").exists()
    v001 = docdir / "versions" / "v001"
    names = sorted(p.name for p in (v001 / "01_input").glob("*"))
    assert any(n.endswith(".pdf") for n in names)
    assert any(n.endswith(".md") for n in names)
    # old_to_new_map обновлён
    mp = json.loads((v2root / "_system" / "old_to_new_map.json").read_text(encoding="utf-8"))
    assert any(m["document_code"] == "13АВ-РД-ЭО-К3" for m in mp["migrations"])


def test_shadow_mirror_idempotent_single_map_entry(monkeypatch, tmp_path):
    proj = _make_legacy_project(tmp_path)
    v2root = tmp_path / "projects_v2"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2root))
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    swf._default_facade = None
    swf.shadow_mirror_project_path_safe(proj)
    swf.shadow_mirror_project_path_safe(proj)          # повтор
    mp = json.loads((v2root / "_system" / "old_to_new_map.json").read_text(encoding="utf-8"))
    hits = [m for m in mp["migrations"] if m["document_code"] == "13АВ-РД-ЭО-К3"]
    assert len(hits) == 1


def test_shadow_mirror_v2_failure_is_fail_soft(monkeypatch, tmp_path):
    proj = _make_legacy_project(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", "/proc/nonexistent_xyz/projects_v2")
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    swf._default_facade = None
    # не бросает наружу; результат — None или WriteResult с v2_ok False
    res = swf.shadow_mirror_project_path_safe(proj)
    assert res is None or res.v2_ok is False


def test_shadow_mirror_path_safe_never_raises_on_garbage(monkeypatch, tmp_path):
    monkeypatch.setenv(ENV, WRITE_MODE_DUAL_SHADOW)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(tmp_path / "v2"))
    swf._default_facade = None
    # Главная гарантия safe-обёртки: НИКОГДА не бросает наружу (fail-soft).
    # На «мусорном» пути миграция может отработать вхолостую — допустимо; важно
    # лишь, что вызов вернулся без исключения.
    res = swf.shadow_mirror_project_path_safe(tmp_path / "does_not_exist")
    assert res is None or isinstance(res, swf.WriteResult)
