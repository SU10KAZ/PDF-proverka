"""
Тесты projects_v2 (этап 1 подготовки новой структуры хранения).

Все тесты гермётичны: строят синтетическое legacy-дерево в tmp_path и
работают только с ним + параллельным projects_v2. Реальные projects/ и
comparison/ не трогаются.

Покрытие:
  * layout / schema projects_v2;
  * inventory legacy (plain + (main)-контейнер);
  * миграция одного проекта;
  * сохранение checksum;
  * запрет изменения legacy-папки;
  * обработка (main)-контейнера и V2-папки с .pdf в имени.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import v2lib  # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def make_legacy_plain(disc_dir: Path, base: str, *, with_output: bool = True) -> Path:
    """Создаёт обычный legacy-проект с полным входным комплектом."""
    proj = disc_dir / base
    _write(proj / f"{base}.pdf", "%PDF-1.4 fake " + base)
    _write(proj / f"{base}_document.md", f"# {base}\nmd content")
    _write(proj / f"{base}_ocr.html", f"<html>{base}</html>")
    _write(proj / f"{base}_result.json", json.dumps({"blocks": [base]}))
    _write(proj / "project_info.json", json.dumps({"project_id": base, "section": disc_dir.name}))
    if with_output:
        out = proj / "_output"
        _write(out / "02_text_analysis.json", json.dumps({"t": base}))
        _write(out / "01_blocks_analysis.json", json.dumps({"b": base}))
        _write(out / "03_findings.json", json.dumps({"findings": [base]}))
        _write(out / "03_findings_review.json", json.dumps({"verdicts": []}))
        _write(out / "pipeline_log.json", json.dumps({"stages": []}))
        _write(out / "document_graph.json", json.dumps({"pages": []}))
        _write(out / "norm_checks.json", json.dumps({"norms": []}))
        _write(out / "block_batch_001.json", json.dumps({"batch": 1}))
        _write(out / "blocks" / "index.json", json.dumps({"blocks": []}))
        _write(out / "blocks" / "block_001.png", "PNGDATA")
    return proj


def make_legacy_container(disc_dir: Path, base: str) -> Path:
    """Контейнер `<base>(main)` с V1 и V2-папкой, имя которой кончается на .pdf."""
    container = disc_dir / f"{base}(main)"
    v1 = container / base
    v2 = container / f"{base} V2.pdf"  # gotcha: имя папки кончается на .pdf
    for vdir, tag in ((v1, "v1"), (v2, "v2")):
        _write(vdir / f"{base} ({tag}).pdf", f"%PDF {base} {tag}")
        _write(vdir / f"{base} ({tag})_document.md", f"# {base} {tag}")
        _write(vdir / f"{base} ({tag})_ocr.html", f"<html>{tag}</html>")
        _write(vdir / f"{base} ({tag})_result.json", json.dumps({"v": tag}))
        _write(vdir / "project_info.json", json.dumps({"project_id": base}))
        _write(vdir / "_output" / "03_findings.json", json.dumps({"v": tag}))
        _write(vdir / "_output" / "02_text_analysis.json", json.dumps({"v": tag}))
        _write(vdir / "_output" / "01_blocks_analysis.json", json.dumps({"v": tag}))
    _write(container / "version_group.json", json.dumps({
        "schema_version": 1,
        "logical_project_id": base,
        "container": f"{base}(main)",
        "primary_version_id": "v1",
        "latest_version_id": "v2",
        "versions": [
            {"version_id": "v1", "version_no": 1, "label": "V1", "folder": base},
            {"version_id": "v2", "version_no": 2, "label": "V2", "folder": f"{base} V2.pdf"},
        ],
    }, ensure_ascii=False))
    return container


def snapshot_tree(root: Path) -> dict:
    """sha256 + размер всех файлов под root (для проверки неизменности)."""
    snap = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            snap[str(p.relative_to(root))] = (v2lib.sha256_file(p), p.stat().st_size)
    return snap


@pytest.fixture
def legacy_root(tmp_path) -> Path:
    root = tmp_path / "projects"
    disc = root / "OBJ-1" / "EOM"
    disc.mkdir(parents=True)
    make_legacy_plain(disc, "PROJ-ALPHA")
    make_legacy_container(disc, "PROJ-BETA")
    return root


# ---------------------------------------------------------------------------
# 1. layout / schema
# ---------------------------------------------------------------------------


def test_skeleton_and_schema(tmp_path):
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    schema = json.loads((v2 / "_system" / "schema.json").read_text(encoding="utf-8"))
    assert schema["layout_version"] == v2lib.LAYOUT_VERSION
    assert "01_input" in schema["input_quad"] or "pdf" in schema["input_quad"]
    assert (v2 / "objects").is_dir()
    # все фиксированные подпапки версии описаны в стандарте
    assert v2lib.VERSION_SUBDIRS == ("01_input", "02_work", "03_analysis",
                                     "04_review", "05_export", "99_service")


# ---------------------------------------------------------------------------
# 2. inventory
# ---------------------------------------------------------------------------


def test_inventory_plain_and_container(legacy_root):
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    rows = v2lib.build_inventory(legacy_root, objects_map)
    by_name = {r["project_name"]: r for r in rows}

    assert "PROJ-ALPHA" in by_name
    alpha = by_name["PROJ-ALPHA"]
    assert alpha["kind"] == "plain"
    assert alpha["version_count"] == 1
    assert alpha["has_pdf"] and alpha["has_document_md"]
    assert alpha["has_ocr_html"] and alpha["has_result_json"]
    assert alpha["has_03_findings"] and alpha["has_blocks"]
    assert alpha["warnings"] == ""

    assert "PROJ-BETA(main)" in by_name
    beta = by_name["PROJ-BETA(main)"]
    assert beta["kind"] == "container"
    assert beta["version_count"] == 2
    assert beta["has_version_group"]
    assert "v002" in beta["versions"]
    assert "version-folder-name-ends-with-.pdf:v002" in beta["warnings"]


def test_inventory_write(tmp_path, legacy_root):
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    rows = v2lib.build_inventory(legacy_root, objects_map)
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    jpath = v2 / "_system" / "migration_inventory.json"
    cpath = v2 / "_system" / "migration_inventory.csv"
    v2lib.write_inventory(rows, jpath, cpath)
    assert jpath.exists() and cpath.exists()
    data = json.loads(jpath.read_text(encoding="utf-8"))
    assert data["count"] == len(rows)
    # csv header содержит все поля
    header = cpath.read_text(encoding="utf-8").splitlines()[0]
    for field in v2lib.INVENTORY_FIELDS:
        assert field in header


# ---------------------------------------------------------------------------
# 3. миграция одного проекта + checksum
# ---------------------------------------------------------------------------


def test_migrate_plain_project(tmp_path, legacy_root):
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    proj = legacy_root / "OBJ-1" / "EOM" / "PROJ-ALPHA"

    result = v2lib.migrate_project(proj, v2, objects_map=objects_map, run_id="run_test")
    doc_dir = Path(result["v2_document_dir"])
    vroot = doc_dir / "versions" / "v001"

    # фиксированный скелет
    for sub in v2lib.VERSION_SUBDIRS:
        assert (vroot / sub).is_dir(), sub

    # 01_input — входной комплект + манифест, оригинальные имена
    assert (vroot / "01_input" / "PROJ-ALPHA.pdf").exists()
    assert (vroot / "01_input" / "PROJ-ALPHA_document.md").exists()
    assert (vroot / "01_input" / "PROJ-ALPHA_ocr.html").exists()
    assert (vroot / "01_input" / "PROJ-ALPHA_result.json").exists()
    assert (vroot / "01_input" / "input_manifest.json").exists()
    assert (vroot / "01_input" / "project_info.json").exists()

    # 02_work — нормализованные имена
    assert (vroot / "02_work" / "document.md").exists()
    assert (vroot / "02_work" / "ocr.html").exists()
    assert (vroot / "02_work" / "result.json").exists()
    assert (vroot / "02_work" / "document.pdf").exists()

    # 03_analysis — verbatim run + latest с критичными артефактами
    assert (vroot / "03_analysis" / "runs" / "run_test" / "03_findings.json").exists()
    assert (vroot / "03_analysis" / "runs" / "run_test" / "blocks" / "block_001.png").exists()
    for crit in v2lib.CRITICAL_ANALYSIS_FILES:
        assert (vroot / "03_analysis" / "latest" / crit).exists(), crit

    # классификация review / service
    assert (vroot / "04_review" / "03_findings_review.json").exists()
    assert (vroot / "99_service" / "pipeline_log.json").exists()
    assert (vroot / "99_service" / "block_batch_001.json").exists()

    # version.json + document.json + current_version.txt
    vj = json.loads((vroot / "version.json").read_text(encoding="utf-8"))
    assert vj["version_id"] == "v001"
    assert vj["legacy_folder_name"] == "PROJ-ALPHA"
    dj = json.loads((doc_dir / "document.json").read_text(encoding="utf-8"))
    assert dj["kind"] == "plain"
    assert dj["current_version"] == "v001"
    assert (doc_dir / "current_version.txt").read_text(encoding="utf-8").strip() == "v001"

    # object.json создан
    assert (doc_dir.parents[3] / "object.json").exists()


def test_checksum_preserved(tmp_path, legacy_root):
    v2 = tmp_path / "projects_v2"
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    proj = legacy_root / "OBJ-1" / "EOM" / "PROJ-ALPHA"
    result = v2lib.migrate_project(proj, v2, objects_map=objects_map, run_id="run_test")

    # checksum исходника == checksum копии в 01_input
    src = proj / "PROJ-ALPHA_result.json"
    dst = Path(result["v2_document_dir"]) / "versions" / "v001" / "01_input" / "PROJ-ALPHA_result.json"
    assert v2lib.sha256_file(src) == v2lib.sha256_file(dst)

    # все записанные в files[] sha совпадают с реальными копиями
    for vrec in result["versions"]:
        for f in vrec["files"]:
            if f["sha256"] is None:
                continue
            assert v2lib.sha256_file(Path(f["new_path"])) == f["sha256"]


# ---------------------------------------------------------------------------
# 5. legacy не изменяется
# ---------------------------------------------------------------------------


def test_legacy_not_modified(tmp_path, legacy_root):
    before = snapshot_tree(legacy_root)
    before_count = len(before)

    v2 = tmp_path / "projects_v2"
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    v2lib.build_inventory(legacy_root, objects_map)
    for proj in ("PROJ-ALPHA", "PROJ-BETA(main)"):
        v2lib.migrate_project(legacy_root / "OBJ-1" / "EOM" / proj, v2,
                              objects_map=objects_map, run_id="run_test")

    after = snapshot_tree(legacy_root)
    assert after == before, "legacy projects/ tree must be byte-identical after migration"
    assert len(after) == before_count


# ---------------------------------------------------------------------------
# 6. контейнер (main) + V2-папка с .pdf в имени
# ---------------------------------------------------------------------------


def test_migrate_container_with_pdf_version_folder(tmp_path, legacy_root):
    v2 = tmp_path / "projects_v2"
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    container = legacy_root / "OBJ-1" / "EOM" / "PROJ-BETA(main)"

    result = v2lib.migrate_project(container, v2, objects_map=objects_map, run_id="run_test")

    # document_code = logical id (без (main)), kind=container
    assert result["document_code"] == "PROJ-BETA"
    assert result["kind"] == "container"

    vids = sorted(v["version_id"] for v in result["versions"])
    assert vids == ["v001", "v002"]

    doc_dir = Path(result["v2_document_dir"])
    # папки версий — строгие индексы, НЕ ".pdf"
    assert (doc_dir / "versions" / "v001").is_dir()
    assert (doc_dir / "versions" / "v002").is_dir()
    assert not (doc_dir / "versions").joinpath("PROJ-BETA V2.pdf").exists()

    # оригинальное имя V2-папки сохранено в version.json
    v2j = json.loads((doc_dir / "versions" / "v002" / "version.json").read_text(encoding="utf-8"))
    assert v2j["legacy_folder_name"] == "PROJ-BETA V2.pdf"
    assert v2j["version_no"] == 2

    # current_version из latest_version_id манифеста (v2 -> v002)
    assert (doc_dir / "current_version.txt").read_text(encoding="utf-8").strip() == "v002"

    # input_manifest хранит legacy-имя комплекта
    im = json.loads((doc_dir / "versions" / "v002" / "01_input" / "input_manifest.json").read_text(encoding="utf-8"))
    assert im["legacy_folder_name"] == "PROJ-BETA V2.pdf"


# ---------------------------------------------------------------------------
# 4 (validate) + old_to_new_map
# ---------------------------------------------------------------------------


def test_validate_migration_passes(tmp_path, legacy_root):
    import importlib
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
    validate_migration = importlib.import_module("validate_migration")

    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}

    map_path = v2 / "_system" / "old_to_new_map.json"
    map_obj = v2lib.load_old_to_new_map(map_path)
    for proj in ("PROJ-ALPHA", "PROJ-BETA(main)"):
        result = v2lib.migrate_project(legacy_root / "OBJ-1" / "EOM" / proj, v2,
                                       objects_map=objects_map, run_id="run_test")
        for vrec in result["versions"]:
            v2lib.upsert_migration(map_obj, {
                "object_id": result["object_id"],
                "object_name": result["object_name"],
                "discipline": result["discipline"],
                "document_code": result["document_code"],
                "kind": result["kind"],
                "version_id": vrec["version_id"],
                "version_no": vrec["version_no"],
                "legacy_folder_name": vrec["legacy_folder_name"],
                "legacy_folder_path": vrec["legacy_folder_path"],
                "analysis_run_id": vrec["analysis_run_id"],
                "v2_document_dir": result["v2_document_dir"],
                "files": vrec["files"],
            })
    v2lib.save_old_to_new_map(map_obj, map_path)
    assert map_path.exists()

    errors, notes = validate_migration.validate_map(map_obj)
    assert errors == [], errors
    assert notes


def test_validate_detects_legacy_change(tmp_path, legacy_root):
    import importlib
    validate_migration = importlib.import_module("validate_migration")

    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    objects_map = {"by_name": {}, "by_path": {}, "by_id": {}}
    result = v2lib.migrate_project(legacy_root / "OBJ-1" / "EOM" / "PROJ-ALPHA", v2,
                                   objects_map=objects_map, run_id="run_test")
    map_obj = {"schema_version": 1, "migrations": []}
    for vrec in result["versions"]:
        v2lib.upsert_migration(map_obj, {
            "object_id": result["object_id"], "object_name": result["object_name"],
            "discipline": result["discipline"], "document_code": result["document_code"],
            "kind": result["kind"], "version_id": vrec["version_id"],
            "version_no": vrec["version_no"], "legacy_folder_name": vrec["legacy_folder_name"],
            "legacy_folder_path": vrec["legacy_folder_path"],
            "analysis_run_id": vrec["analysis_run_id"],
            "v2_document_dir": result["v2_document_dir"], "files": vrec["files"],
        })
    # имитируем изменение legacy-файла после миграции
    (legacy_root / "OBJ-1" / "EOM" / "PROJ-ALPHA" / "PROJ-ALPHA_document.md").write_text(
        "TAMPERED", encoding="utf-8")
    errors, _ = validate_migration.validate_map(map_obj)
    assert any("LEGACY CHANGED" in e for e in errors)
