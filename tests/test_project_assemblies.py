from __future__ import annotations

import json
import hashlib
from pathlib import Path

import fitz
import pytest

from backend.app.services.project_assemblies import service


def _source(object_dir: Path, code: str, version: str, *, pages: int, legacy: bool = False) -> Path:
    version_dir = object_dir / "disciplines" / "OV" / "documents" / code / "versions" / version
    work = version_dir / "02_work"
    work.mkdir(parents=True)
    document = version_dir.parents[1]
    (document / "document.json").write_text(json.dumps({
        "document_code": code,
        "object_id": "obj-test",
        "discipline": "OV",
        "current_version": version,
        "versions": [{"version_id": version}],
    }), encoding="utf-8")

    pdf = fitz.open()
    blocks = []
    page_rows = []
    md = [f"# Document: {code}.pdf", f"Path: OV / {code}", ""]
    html_links = []
    ordinal = 0
    for page_no in range(1, pages + 1):
        page = pdf.new_page(width=600, height=800)
        page.insert_text((50, 80), f"{code} page {page_no}")
        if page_no == 1:
            annotation = page.add_rect_annot(fitz.Rect(45, 45, 180, 95))
            annotation.set_info(content=f"annotation {code}")
            annotation.update()
        page.set_rotation(90 if page_no % 2 == 0 else 0)
        block_id = "blk_" + (f"{code}-{page_no}".encode().hex() + "0" * 32)[:32]
        ordinal += 1
        blocks.append({
            "block_id": block_id,
            "ordinal": ordinal,
            "page_index": page_no - 1,
            "block_type": "text",
            "coords_norm": [0.05, 0.05, 0.8, 0.2],
        })
        page_rows.append({
            "page_index": page_no - 1,
            "page_label": page_no,
            "width_px": 600,
            "height_px": 800,
            "rotation": page.rotation,
        })
        if legacy:
            md += [f"## СТРАНИЦА {page_no}", f"### BLOCK [TEXT]: {block_id}", f"Текст {code} {page_no}", ""]
        else:
            md += [f"## Page {page_no}", f"### BLOCK #{ordinal} [TEXT]: {block_id}",
                   "> **Crop:** [Crop](https://vibe.invalid/api/crops/dead)",
                   f"> **Stamp:** Code: {code} | Stage: Р | Sheet: {page_no} | Object: Test | Name: Лист {page_no} | Organization: Org | Revisions: ",
                   f"Текст {code} {page_no}", ""]
        html_links.append(f'<a href="#page-{page_no - 1}">Sheet {page_no} — Лист {page_no}</a>')
    pdf.save(work / "document.pdf", no_new_id=True)
    pdf.close()
    (work / "document.md").write_text("\n".join(md), encoding="utf-8")
    (work / "blocks.json").write_text(json.dumps({
        "schema_version": 1,
        "document_id": code,
        "pages": page_rows,
        "blocks": blocks,
    }), encoding="utf-8")
    (work / "ocr.html").write_text("<html><body>" + "".join(html_links) + "</body></html>", encoding="utf-8")
    return version_dir


@pytest.fixture
def assembly_env(tmp_path, monkeypatch):
    object_dir = tmp_path / "projects_v2" / "objects" / "obj-folder"
    comparison = object_dir / "comparison"
    comparison.mkdir(parents=True)
    (object_dir / "object.json").write_text(json.dumps({
        "object_id": "obj-test", "display_name": "Test", "folder_name": "obj-folder",
    }), encoding="utf-8")
    monkeypatch.setattr(service, "_comparison_dir", lambda object_id, create=False: comparison)
    monkeypatch.setattr(service, "MIN_FREE_RESERVE", 0)
    return object_dir


def test_builds_deterministic_package_and_origin(assembly_env):
    _source(assembly_env, "OLD-A", "v001", pages=2)
    _source(assembly_env, "OLD-B", "v002", pages=1)
    catalog = service.list_sources("obj-test")
    refs = [row["source_ref"] for row in catalog["items"]]

    preview = service.preview("obj-test", refs)
    assert preview["totals"]["page_count"] == 3
    assert [(row["page_start"], row["page_end"]) for row in preview["sources"]] == [(1, 2), (3, 3)]

    first = service.create_assembly(
        object_id="obj-test", name="Новая версия", section="OV", source_refs=refs,
        author="Engineer", idempotency_key="request-1",
    )
    repeated = service.create_assembly(
        object_id="obj-test", name="Новая версия", section="OV", source_refs=refs,
        author="Engineer", idempotency_key="request-1",
    )
    assert repeated["assembly_id"] == first["assembly_id"]
    assert repeated["version_id"] == first["version_id"]

    work = service._root("obj-test") / first["assembly_id"] / "versions" / "v001" / "02_work"
    with fitz.open(work / "document.pdf") as pdf:
        assert pdf.page_count == 3
        assert [page.rotation for page in pdf] == [0, 90, 0]
        assert "OLD-A page 1" in pdf[0].get_text()
        assert "OLD-B page 1" in pdf[2].get_text()
        assert next(pdf[0].annots()).info["content"] == "annotation OLD-A"
    blocks = json.loads((work / "blocks.json").read_text(encoding="utf-8"))
    assert [row["page_index"] for row in blocks["pages"]] == [0, 1, 2]
    assert len({row["block_id"] for row in blocks["blocks"]}) == 3
    assert all(row["block_id"].startswith("blk_") and len(row["block_id"]) == 36 for row in blocks["blocks"])
    markdown = (work / "document.md").read_text(encoding="utf-8")
    assert markdown.count("# Document:") == 1
    assert "## Page 3" in markdown
    assert "vibe.invalid" not in markdown
    origin = service.origin_for_page("obj-test", first["assembly_id"], "v001", 3)
    assert origin["source"]["document_code"] == "OLD-B"
    assert origin["page"]["source_page"] == 1


def test_same_sources_produce_byte_identical_artifacts(assembly_env):
    _source(assembly_env, "A", "v001", pages=2)
    [source] = service.list_sources("obj-test")["items"]
    one = service.create_assembly(object_id="obj-test", name="One", section="OV", source_refs=[source["source_ref"]])
    two = service.create_assembly(object_id="obj-test", name="One", section="OV", source_refs=[source["source_ref"]])
    assert one["manifest_sha256"] == two["manifest_sha256"]
    for artifact in ("document.pdf", "document.md", "blocks.json", "ocr.html", "assembly_origin.json"):
        assert one["artifacts"][artifact] == two["artifacts"][artifact]


def test_rejects_legacy_markdown(assembly_env):
    _source(assembly_env, "LEGACY", "v001", pages=1, legacy=True)
    [source] = service.list_sources("obj-test")["items"]
    with pytest.raises(service.AssemblyError) as caught:
        service.preview("obj-test", [source["source_ref"]])
    assert caught.value.code == "LEGACY_OCR_FORMAT"


def test_feature_flag_defaults_to_off(monkeypatch):
    monkeypatch.delenv("PROJECT_ASSEMBLIES_ENABLED", raising=False)
    assert service.enabled() is False


def test_ordinary_stage_source_signature_keeps_historical_bytes():
    from backend.app.services.stage_comparison import store

    left = [{"pdf_path": "/stage_1/a.pdf", "html_path": "/stage_1/a.html", "version_id": "v001"}]
    right = [{"pdf_path": "/stage_2/a.pdf", "html_path": "/stage_2/a.html", "version_id": "v002"}]
    historical = {
        "stage_a_path": str(Path("/stage_1").resolve()),
        "stage_b_path": str(Path("/stage_2").resolve()),
        "stage_1": [("/stage_1/a.pdf", "/stage_1/a.html", "v001")],
        "stage_2": [("/stage_2/a.pdf", "/stage_2/a.html", "v002")],
    }
    expected = hashlib.sha256(
        json.dumps(historical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    assert store._source_signature("/stage_1", "/stage_2", left, right) == expected


def test_attachment_is_discovered_in_normal_stage_session(assembly_env, tmp_path, monkeypatch):
    _source(assembly_env, "NEW-A", "v001", pages=1)
    [source] = service.list_sources("obj-test")["items"]
    built = service.create_assembly(
        object_id="obj-test", name="Сборный NEW", section="OV", source_refs=[source["source_ref"]],
        composition_completeness="INCOMPLETE",
    )
    service.attach("obj-test", built["assembly_id"], built["version_id"], "stage_2")
    comparison = assembly_env / "comparison"
    stage_1, stage_2 = comparison / "stage_1", comparison / "stage_2"
    stage_1.mkdir()
    stage_2.mkdir()
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "1")
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "sessions"))

    from backend.app.services.stage_comparison import store

    session, _warnings = store.create_session(str(stage_1), str(stage_2))
    assert session["stage_a_path"] == str(stage_1.resolve())
    assert session["stage_b_path"] == str(stage_2.resolve())
    assert session["documents"]["stage_1"] == []
    [document] = session["documents"]["stage_2"]
    assert document["assembly_ref"]["assembly_id"] == built["assembly_id"]
    assert document["assembly_ref"]["composition_completeness"] == "INCOMPLETE"
    assert Path(document["pdf_path"]).is_file()


def test_sheet_suffixed_page_headers_are_accepted_and_kept(assembly_env):
    """Выгрузки портала с «## Page N — Sheet M — Название» тоже склеиваются."""
    first = _source(assembly_env, "SUF-A", "v001", pages=2)
    _source(assembly_env, "SUF-B", "v001", pages=2)
    md_path = first / "02_work" / "document.md"
    md_path.write_text(
        md_path.read_text(encoding="utf-8").replace("## Page 2", "## Page 2 — Sheet 1 — Содержание тома"),
        encoding="utf-8",
    )
    refs = [row["source_ref"] for row in service.list_sources("obj-test")["items"]]

    built = service.create_assembly(object_id="obj-test", name="Суффиксы", section="OV", source_refs=refs)

    assert built["status"] == "READY"
    work = assembly_env / "comparison" / "assemblies" / built["assembly_id"] / "versions" / built["version_id"] / "02_work"
    md = (work / "document.md").read_text(encoding="utf-8")
    assert "## Page 2 — Sheet 1 — Содержание тома" in md
    assert "## Page 4" in md
