"""
Тесты readiness-классификации projects_v2 (этап подготовки batch-миграции).

Гермётичны: чистая `classify_readiness` проверяется на сконструированных
сигналах; сборщики (`build_signal`/`build_readiness`/`detect_document_code_conflicts`)
— на синтетическом legacy-дереве в tmp_path. Реальные projects/ не трогаются.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import readiness  # noqa: E402
import v2lib       # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _full_signal(**over) -> dict:
    base = dict(
        has_pdf=True, has_document_md=True, has_ocr_html=True, has_result_json=True,
        has_project_info=True, has_output=True, has_analysis=True,
        kind="plain", document_code="X", object_id="o1", discipline="EOM",
    )
    base.update(over)
    return base


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def make_plain(disc_dir: Path, base: str, *, quad=("pdf", "md", "ocr", "result"),
               project_info=True, analysis=True) -> Path:
    proj = disc_dir / base
    if "pdf" in quad:
        _write(proj / f"{base}.pdf", "%PDF " + base)
    if "md" in quad:
        _write(proj / f"{base}_document.md", "# " + base)
    if "ocr" in quad:
        _write(proj / f"{base}_ocr.html", "<html>")
    if "result" in quad:
        _write(proj / f"{base}_result.json", "{}")
    if project_info:
        _write(proj / "project_info.json", json.dumps({"project_id": base}))
    if analysis:
        _write(proj / "_output" / "03_findings.json", "{}")
        _write(proj / "_output" / "02_text_analysis.json", "{}")
    return proj


def make_container_pdf_v2(disc_dir: Path, base: str) -> Path:
    container = disc_dir / f"{base}(main)"
    v1 = container / base
    v2 = container / f"{base} V2.pdf"
    for vdir in (v1, v2):
        _write(vdir / f"{vdir.name}.pdf", "%PDF")
        _write(vdir / f"{vdir.name}_document.md", "# md")
        _write(vdir / f"{vdir.name}_ocr.html", "<html>")
        _write(vdir / f"{vdir.name}_result.json", "{}")
        _write(vdir / "project_info.json", "{}")
    _write(v1 / "_output" / "03_findings.json", "{}")
    _write(container / "version_group.json", json.dumps({
        "logical_project_id": base, "primary_version_id": "v1", "latest_version_id": "v2",
        "versions": [
            {"version_id": "v1", "version_no": 1, "label": "V1", "folder": base},
            {"version_id": "v2", "version_no": 2, "label": "V2", "folder": f"{base} V2.pdf"},
        ],
    }))
    return container


# ---------------------------------------------------------------------------
# чистая классификация
# ---------------------------------------------------------------------------


def test_classify_auto_safe():
    v = readiness.classify_readiness(_full_signal())
    assert v["group"] == readiness.AUTO_SAFE
    assert v["warnings"] == [] and v["blockers"] == []


def test_classify_warnings_pdf_named_version_folder():
    v = readiness.classify_readiness(_full_signal(
        kind="container", has_version_group=True, pdf_named_version_folder=True))
    assert v["group"] == readiness.CAN_MIGRATE_WITH_WARNINGS
    assert "pdf_in_version_folder_name" in v["warnings"]
    assert v["blockers"] == []


def test_classify_warnings_no_analysis_and_missing_ocr():
    v = readiness.classify_readiness(_full_signal(has_analysis=False, has_ocr_html=False))
    assert v["group"] == readiness.CAN_MIGRATE_WITH_WARNINGS
    assert "no_analysis" in v["warnings"]
    assert "missing_ocr_html" in v["warnings"]


def test_classify_manual_incomplete_quad():
    v = readiness.classify_readiness(_full_signal(has_result_json=False))
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED
    assert "incomplete_input_quad" in v["blockers"]


def test_classify_manual_multiple_pdf():
    v = readiness.classify_readiness(_full_signal(multiple_pdf=True))
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED
    assert "multiple_pdf" in v["blockers"]


def test_classify_manual_missing_project_info():
    v = readiness.classify_readiness(_full_signal(has_project_info=False))
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED
    assert "missing_project_info" in v["blockers"]


def test_classify_manual_document_code_conflict():
    v = readiness.classify_readiness(_full_signal(document_code_conflict=True))
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED
    assert "document_code_conflict" in v["blockers"]


def test_classify_manual_container_without_version_group():
    v = readiness.classify_readiness(_full_signal(kind="container", has_version_group=False))
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED
    assert "container_without_version_group" in v["blockers"]


def test_classify_skip_empty():
    v = readiness.classify_readiness({
        "has_pdf": False, "has_document_md": False, "has_result_json": False,
        "has_output": False, "has_project_info": False,
    })
    assert v["group"] == readiness.SKIP_EMPTY_OR_INVALID


def test_blocker_precedence_over_warning():
    # есть и warning (no_analysis), и blocker (incomplete) -> MANUAL
    v = readiness.classify_readiness(_full_signal(has_analysis=False, has_pdf=False))
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED


# ---------------------------------------------------------------------------
# сбор сигналов + конфликты на синтетическом дереве
# ---------------------------------------------------------------------------


def test_build_signal_auto_safe(tmp_path):
    root = tmp_path / "projects"
    disc = root / "OBJ" / "EOM"
    proj = make_plain(disc, "ALPHA")
    # объект распознаётся в реестре -> нет warning object_id_not_in_registry
    objects_map = {"by_name": {"OBJ": "o1"}, "by_path": {}, "by_id": {}}
    s = readiness.build_signal(disc.parent, "EOM", proj, objects_map)
    v = readiness.classify_readiness(s)
    assert s["has_pdf"] and s["has_document_md"] and s["has_ocr_html"] and s["has_result_json"]
    assert s["has_analysis"]
    assert s["object_resolved"] is True
    assert v["group"] == readiness.AUTO_SAFE


def test_build_signal_incomplete_quad(tmp_path):
    root = tmp_path / "projects"
    disc = root / "OBJ" / "EOM"
    proj = make_plain(disc, "BETA", quad=("pdf",), analysis=False)  # only pdf
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    s = readiness.build_signal(disc.parent, "EOM", proj, objects_map)
    v = readiness.classify_readiness(s)
    assert s["has_pdf"] and not s["has_document_md"] and not s["has_result_json"]
    assert v["group"] == readiness.MANUAL_REVIEW_REQUIRED
    assert "incomplete_input_quad" in v["blockers"]


def test_build_signal_container_pdf_v2_is_warning(tmp_path):
    root = tmp_path / "projects"
    disc = root / "OBJ" / "SS"
    cont = make_container_pdf_v2(disc, "GAMMA")
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    s = readiness.build_signal(disc.parent, "SS", cont, objects_map)
    assert s["kind"] == "container"
    assert s["version_count"] == 2
    assert s["pdf_named_version_folder"] is True
    assert s["has_version_group"] is True
    v = readiness.classify_readiness(s)
    assert v["group"] == readiness.CAN_MIGRATE_WITH_WARNINGS
    assert "pdf_in_version_folder_name" in v["warnings"]


def test_detect_document_code_conflict(tmp_path):
    root = tmp_path / "projects"
    disc = root / "OBJ" / "EOM"
    # plain "DELTA" и контейнер "DELTA(main)" -> один document_code DELTA
    p1 = make_plain(disc, "DELTA")
    p2 = make_container_pdf_v2(disc, "DELTA")
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    rows = readiness.build_readiness(root, objects_map)
    delta = [r for r in rows if r["document_code"] == "DELTA"]
    assert len(delta) == 2
    assert all(r["document_code_conflict"] for r in delta)
    assert all(r["group"] == readiness.MANUAL_REVIEW_REQUIRED for r in delta)


def test_no_legacy_entry_returns_empty(tmp_path):
    # пустой projects_root -> нет записей
    empty_root = tmp_path / "projects_empty"
    empty_root.mkdir()
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    rows = readiness.build_readiness(empty_root, objects_map)
    assert rows == []
    # несуществующий root тоже не падает
    assert readiness.build_readiness(tmp_path / "nope", objects_map) == []


def test_v2_already_migrated_flag(tmp_path):
    root = tmp_path / "projects"
    disc = root / "OBJ" / "EOM"
    proj = make_plain(disc, "EPS")
    v2_root = tmp_path / "projects_v2"
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    # до миграции — не migrated
    s0 = readiness.build_signal(disc.parent, "EOM", proj, objects_map, v2_root=v2_root)
    assert s0["v2_already_migrated"] is False
    assert s0["recorded_in_map"] is False
    # мигрируем
    v2lib.migrate_project(proj, v2_root, objects_map=objects_map, run_id="run_test")

    # v2 есть, но карты (old_to_new_map) ещё нет -> несогласованность, НЕ ALREADY_MIGRATED
    s1 = readiness.build_signal(disc.parent, "EOM", proj, objects_map, v2_root=v2_root)
    assert s1["v2_already_migrated"] is True
    assert s1["recorded_in_map"] is False
    v1 = readiness.classify_readiness(s1)
    assert v1["group"] == readiness.CAN_MIGRATE_WITH_WARNINGS
    assert "v2_present_not_in_map" in v1["warnings"]

    # запись в карте присутствует -> ALREADY_MIGRATED
    keys = {(s1["object_id"], s1["document_code"])}
    s2 = readiness.build_signal(disc.parent, "EOM", proj, objects_map,
                                v2_root=v2_root, migrated_keys=keys)
    assert s2["recorded_in_map"] is True
    assert readiness.classify_readiness(s2)["group"] == readiness.ALREADY_MIGRATED
