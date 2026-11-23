"""Одинаковые листы в сборке проектов: поиск, согласование, сборка без копий.

Правило (инженер, 22.11.2026): дубль — лист того же формата с той же графикой
и текстом на поле листа; штамп, плашка «В ПРОИЗВОДСТВО РАБОТ» и QR-код не
учитываются; любое другое отличие оставляет лист в сборке.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import fitz
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.project_assemblies import duplicates
from backend.app.services.project_assemblies import service
from backend.app.services.stage_comparison import document_groups

W, H = 842.0, 595.0  # A3 альбомный
STAMP = (0.70, 0.86, 0.99, 0.99)
_FONT = fitz.Font("cjk")


def _text(page: fitz.Page, point, text: str, size: float = 9) -> None:
    if "F0" not in [font[4] for font in page.get_fonts()]:
        page.insert_font(fontname="F0", fontbuffer=_FONT.buffer)
    page.insert_text(point, text, fontname="F0", fontsize=size)


def _drawing(page: fitz.Page, kind: str) -> None:
    """Содержание поля листа; ``X+`` — тот же узел с одной лишней окружностью."""
    shape = page.new_shape()
    shape.draw_rect(fitz.Rect(20, 10, W - 10, H - 10))
    if kind.startswith("X"):
        shape.draw_line((80, 80), (500, 80))
        shape.draw_line((80, 80), (80, 300))
        shape.draw_circle((300, 200), 60)
    else:
        shape.draw_line((100, 400), (600, 120))
        shape.draw_rect(fitz.Rect(150, 150, 400, 260))
    shape.finish(color=(0, 0, 0), width=1)
    if kind == "X+":
        shape.draw_circle((420, 260), 6)
        shape.finish(color=(0, 0, 0), width=1)
    shape.commit()
    _text(page, (90, 340), "Узел 1. Проход кабеля через перекрытие" if kind.startswith("X") else "План сетей")


def _stamp(page: fitz.Page, code: str, sheet: int, revision: str) -> None:
    x0, y0, x1, y1 = STAMP
    rect = fitz.Rect(x0 * W, y0 * H, x1 * W, y1 * H)
    page.draw_rect(rect, color=(0, 0, 0), width=1)
    _text(page, (rect.x0 + 6, rect.y0 + 20), f"{code} Лист {sheet} {revision}")


def _approval(page: fitz.Page, date: str, seed: int, origin=(470.0, 380.0)) -> None:
    """Отметка выпуска: синяя рамка, текст с датой и QR на белой подложке."""
    x, y = origin
    blue = (0.1098, 0.5333, 0.749)
    shape = page.new_shape()
    for rect in (fitz.Rect(x, y, x + 216, y + 2), fitz.Rect(x, y + 81, x + 216, y + 83),
                 fitz.Rect(x, y, x + 2, y + 83), fitz.Rect(x + 214, y, x + 216, y + 83)):
        shape.draw_rect(rect)
    shape.finish(fill=blue, color=None)
    shape.draw_rect(fitz.Rect(x + 216, y - 12, x + 322, y + 94))
    shape.finish(fill=(1, 1, 1), color=None)
    rng = random.Random(seed)
    cell = 83 / 25
    for row in range(25):
        for col in range(25):
            if rng.random() < 0.5 or (row in (0, 24) or col in (0, 24)):
                shape.draw_rect(fitz.Rect(x + 227 + col * cell, y + row * cell, x + 227 + (col + 1) * cell,
                                          y + (row + 1) * cell))
    shape.finish(fill=(0, 0, 0), color=None)
    shape.commit()
    _text(page, (x + 8, y + 20), "В ПРОИЗВОДСТВО РАБОТ", 8)
    _text(page, (x + 8, y + 70), f"Иванов И.И. {date}", 8)


def _document(root: Path, code: str, pages: list[dict]) -> dict[str, str]:
    """PDF + blocks.json + results.md + ocr.html одного проекта."""
    root.mkdir(parents=True, exist_ok=True)
    pdf = fitz.open()
    blocks, page_rows = [], []
    md = [f"# Document: {code}.pdf", f"Path: OV / {code}", ""]
    links = []
    for number, spec in enumerate(pages, start=1):
        page = pdf.new_page(width=W, height=H)
        _drawing(page, spec["drawing"])
        _stamp(page, code, spec.get("sheet", number), spec.get("revision", ""))
        if spec.get("approval"):
            _approval(page, spec["approval"], seed=hash((code, number)) & 0xFFFF)
        if spec.get("rotation"):
            page.set_rotation(spec["rotation"])
        rotated = page.rotation in (90, 270)
        width_px, height_px = (int(H), int(W)) if rotated else (int(W), int(H))
        text_id = "blk_" + (f"{code}-{number}-t".encode().hex() + "0" * 32)[:32]
        stamp_id = "blk_" + (f"{code}-{number}-s".encode().hex() + "0" * 32)[:32]
        stamp_coords = list(STAMP)
        if page.rotation == 90:
            # Рамка штампа после поворота: правый нижний угол → левый нижний.
            stamp_coords = [1 - STAMP[3], STAMP[0], 1 - STAMP[1], STAMP[2]]
        blocks += [
            {"block_id": text_id, "ordinal": number, "page_index": number - 1, "block_type": "text",
             "coords_norm": [0.1, 0.1, 0.6, 0.6]},
            {"block_id": stamp_id, "page_index": number - 1, "block_type": "stamp", "coords_norm": stamp_coords},
        ]
        page_rows.append({"page_index": number - 1, "page_label": number, "width_px": width_px,
                          "height_px": height_px, "rotation": page.rotation})
        md += [f"## Page {number}", f"### BLOCK #{number} [TEXT]: {text_id}",
               f"> **Stamp:** Code: {code} | Stage: Р | Sheet: {number} | Name: Лист {number}",
               f"Текст {code} {number}", ""]
        links.append(f'<a href="#page-{number - 1}">Sheet {spec.get("sheet", number)} — Лист {number}</a>')
    paths = {
        "pdf_path": str(root / "document.pdf"),
        "blocks_path": str(root / "blocks.json"),
        "markdown_path": str(root / "document.md"),
        "html_path": str(root / "ocr.html"),
    }
    pdf.save(paths["pdf_path"], no_new_id=True)
    pdf.close()
    Path(paths["blocks_path"]).write_text(json.dumps({"schema_version": 1, "document_id": code,
                                                      "pages": page_rows, "blocks": blocks}), encoding="utf-8")
    Path(paths["markdown_path"]).write_text("\n".join(md), encoding="utf-8")
    Path(paths["html_path"]).write_text("<html><body>" + "".join(links) + "</body></html>", encoding="utf-8")
    return paths


def _scan(tmp_path: Path, documents: dict[str, list[dict]]) -> dict:
    rows = []
    for code, pages in documents.items():
        paths = _document(tmp_path / code, code, pages)
        rows.append({**paths, "document_code": code, "pdf_sha256": code})
    return duplicates.scan(rows)


def _pairs(found: dict) -> list[tuple]:
    return [
        (pair["original"]["document_code"], pair["original"]["source_page"],
         pair["copy"]["document_code"], pair["copy"]["source_page"])
        for pair in found["pairs"]
    ]


# ─── Детектор ────────────────────────────────────────────────────────────────

def test_copy_with_other_stamp_approval_and_qr_is_a_duplicate(tmp_path):
    found = _scan(tmp_path, {
        "EM-K1": [{"drawing": "X", "approval": "18.09.2025"}, {"drawing": "Y", "approval": "18.09.2025"}],
        # Другой шифр, номер листа, отметка изменения, дата выпуска и QR.
        "EM-K2": [{"drawing": "X", "sheet": 7, "revision": "1 Зам. 06.2025", "approval": "03.10.2025"}],
    })

    assert _pairs(found) == [("EM-K1", 1, "EM-K2", 1)]
    pair = found["pairs"][0]
    assert pair["original"]["combined_page"] == 1 and pair["copy"]["combined_page"] == 3
    assert pair["pair_key"] == duplicates.pair_key(pair["copy"], pair["original"])
    # Исключённые зоны: штамп и отметка выпуска (рамка + QR одной зоной).
    assert len(pair["ignored_zones"]["copy"]) == 2


def test_small_drawing_change_keeps_the_sheet(tmp_path):
    found = _scan(tmp_path, {
        "EM-K1": [{"drawing": "X", "approval": "18.09.2025"}],
        "EM-K2": [{"drawing": "X+", "approval": "18.09.2025"}],
    })

    assert found["pairs"] == []


def test_different_text_on_sheet_field_keeps_the_sheet(tmp_path):
    rows = []
    for code, caption in (("A", "Узел 1"), ("B", "Узел 2")):
        paths = _document(tmp_path / code, code, [{"drawing": "Y"}])
        with fitz.open(paths["pdf_path"]) as pdf:
            _text(pdf[0], (90, 360), caption)
            pdf.saveIncr()
        rows.append({**paths, "document_code": code, "pdf_sha256": code})

    assert duplicates.scan(rows)["pairs"] == []


def _whole_page_stamp_rows(tmp_path: Path) -> list[dict]:
    """Два разных листа, у которых распознавание отметило штампом весь лист."""
    rows = []
    for code, drawing in (("EN2", "X"), ("GP37", "Y")):
        paths = _document(tmp_path / code, code, [{"drawing": drawing, "approval": "18.09.2025"}])
        blocks = json.loads(Path(paths["blocks_path"]).read_text(encoding="utf-8"))
        for block in blocks["blocks"]:
            if block["block_type"] == "stamp":
                block["coords_norm"] = [0.0, 0.0, 1.0, 1.0]
        Path(paths["blocks_path"]).write_text(json.dumps(blocks), encoding="utf-8")
        rows.append({**paths, "document_code": code, "pdf_sha256": code})
    return rows


def test_whole_page_stamp_is_not_trusted(tmp_path):
    # 23.11.2026: ЭН2 стр. 82 (ПромЛед) «совпала» с прожектором Platek из ГП3.7 —
    # штамп на весь лист исключал из сравнения всё.
    assert duplicates.scan(_whole_page_stamp_rows(tmp_path))["pairs"] == []


def test_zones_covering_most_of_the_sheet_never_make_a_duplicate(tmp_path, monkeypatch):
    monkeypatch.setattr(duplicates, "_STAMP_MAX_AREA", 1.01)  # без первой защиты держит вторая

    assert duplicates.scan(_whole_page_stamp_rows(tmp_path))["pairs"] == []


def test_oversized_sheet_renders_at_fractional_dpi(tmp_path):
    # Лист больше 4000 px при 100 dpi сравнивается при дробном dpi — раньше падало
    # TypeError в get_pixmap (сборка АПС, 23.11.2026).
    pdf_path = tmp_path / "big.pdf"
    pdf = fitz.open()
    for _ in range(2):
        page = pdf.new_page(width=4200, height=1190)
        page.draw_rect(fitz.Rect(200, 200, 1800, 900), color=(0, 0, 0), width=2)
        page.insert_text((300, 400), "План расположения оборудования", fontsize=40)
    pdf.save(pdf_path)
    pdf.close()

    found = duplicates.scan([{"pdf_path": str(pdf_path), "document_code": "BIG", "pdf_sha256": "big"}])

    assert _pairs(found) == [("BIG", 1, "BIG", 2)]


def test_repeated_sheet_inside_one_project_and_three_copies(tmp_path):
    found = _scan(tmp_path, {
        "K1": [{"drawing": "X"}, {"drawing": "X", "sheet": 5}],
        "K2": [{"drawing": "Y"}, {"drawing": "X", "approval": "01.01.2025"}],
    })

    # Каждая копия — пара с первым вхождением листа.
    assert _pairs(found) == [("K1", 1, "K1", 2), ("K1", 1, "K2", 2)]


def test_rotated_pages_mask_zones_in_rotated_coordinates(tmp_path):
    found = _scan(tmp_path, {
        "R1": [{"drawing": "X", "approval": "18.09.2025", "rotation": 90}],
        "R2": [{"drawing": "X", "sheet": 3, "approval": "03.10.2025", "rotation": 90}],
    })

    assert _pairs(found) == [("R1", 1, "R2", 1)]


# ─── Сборка с согласованием ──────────────────────────────────────────────────

def _source_version(object_dir: Path, code: str, pages: list[dict]) -> None:
    version_dir = object_dir / "disciplines" / "OV" / "documents" / code / "versions" / "v001"
    _document(version_dir / "02_work", code, pages)
    (version_dir.parents[1] / "document.json").write_text(json.dumps({
        "document_code": code, "object_id": "obj-test", "discipline": "OV",
        "current_version": "v001", "versions": [{"version_id": "v001"}],
    }), encoding="utf-8")


@pytest.fixture
def dedup_env(tmp_path, monkeypatch):
    object_dir = tmp_path / "projects_v2" / "objects" / "obj-folder"
    comparison = object_dir / "comparison"
    comparison.mkdir(parents=True)
    (object_dir / "object.json").write_text(json.dumps({
        "object_id": "obj-test", "display_name": "Test", "folder_name": "obj-folder",
    }), encoding="utf-8")
    monkeypatch.setattr(service, "_comparison_dir", lambda object_id, create=False: comparison)
    monkeypatch.setattr(service, "MIN_FREE_RESERVE", 0)
    # Воркер выполняем в том же процессе: фоновый процесс не видит подмен теста.
    monkeypatch.setattr(service, "launch_version", lambda o, a, v: service.run_version(o, a, v)
                        or service.get_assembly(o, a, v))
    _source_version(object_dir, "EM-K1", [{"drawing": "X", "approval": "18.09.2025"}, {"drawing": "Y"}])
    _source_version(object_dir, "EM-K2", [{"drawing": "Y", "sheet": 9}, {"drawing": "X", "sheet": 4,
                                                                          "approval": "03.10.2025"}])
    return object_dir


def _refs() -> list[str]:
    items = sorted(service.list_sources("obj-test")["items"], key=lambda row: row["document_code"])
    return [row["source_ref"] for row in items]


def _work(result: dict) -> Path:
    return service._root("obj-test") / result["assembly_id"] / "versions" / result["version_id"] / "02_work"


def test_assembly_waits_for_review_then_builds_without_copies(dedup_env):
    created = service.create_assembly(object_id="obj-test", name="ЭМ · сборка из 2", section="OV",
                                      source_refs=_refs(), build=False, dedup=True)
    assert created["status"] == "REVIEW"
    assert created["duplicates"] == {"pairs": 2, "undecided": 2}

    review = service.duplicate_review("obj-test", created["assembly_id"], created["version_id"])
    assert [(p["original"]["source_page"], p["copy"]["document_code"], p["copy"]["source_page"])
            for p in review["pairs"]] == [(2, "EM-K2", 1), (1, "EM-K2", 2)]
    assert {p["proposed"] for p in review["pairs"]} == {"remove"}
    assert all(p["decision"] is None for p in review["pairs"])

    built = service.submit_duplicate_review("obj-test", created["assembly_id"], created["version_id"],
                                            keep_pair_ids=[], author="Инженер")
    assert built["status"] == "READY"
    assert built["dedup"]["removed_pages"] == 2 and built["dedup"]["kept_pairs"] == 0
    assert built["totals"]["page_count"] == 2 and built["totals"]["source_page_count"] == 4

    work = _work(built)
    with fitz.open(work / "document.pdf") as pdf:
        assert pdf.page_count == 2
    origin = json.loads((work / "assembly_origin.json").read_text(encoding="utf-8"))
    assert [(row["assembly_page"], row["source_index"], row["source_page"]) for row in origin["pages"]] == [
        (1, 0, 1), (2, 0, 2),
    ]
    assert [(row["source_index"], row["source_page"], row["duplicate_of"]["assembly_page"])
            for row in origin["removed_pages"]] == [(1, 1, 2), (1, 2, 1)]
    # У проекта, все листы которого оказались копиями, нет диапазона в сборке.
    assert [(row["page_start"], row["page_end"]) for row in origin["sources"]] == [(1, 2), (None, None)]
    assert origin["sources"][1]["removed_pages"] == 2
    blocks = json.loads((work / "blocks.json").read_text(encoding="utf-8"))
    assert [row["page_index"] for row in blocks["pages"]] == [0, 1]
    assert {row["page_index"] for row in blocks["blocks"]} == {0, 1}
    md = (work / "document.md").read_text(encoding="utf-8")
    assert "## Page 3" not in md and "EM-K2" not in md
    # Решения версии видны и после сборки.
    after = service.duplicate_review("obj-test", built["assembly_id"], built["version_id"])
    assert after["status"] == "READY" and {p["decision"] for p in after["pairs"]} == {"remove"}
    assert after["reviewed_by"] == "Инженер"


def test_returned_pair_keeps_copy_and_decisions_are_remembered(dedup_env):
    created = service.create_assembly(object_id="obj-test", name="ЭМ", section="OV",
                                      source_refs=_refs(), build=False, dedup=True)
    review = service.duplicate_review("obj-test", created["assembly_id"], created["version_id"])
    returned = review["pairs"][0]["pair_id"]
    built = service.submit_duplicate_review("obj-test", created["assembly_id"], created["version_id"],
                                            keep_pair_ids=[returned])
    assert built["dedup"] == {**built["dedup"], "removed_pages": 1, "kept_pairs": 1}
    with fitz.open(_work(built) / "document.pdf") as pdf:
        assert pdf.page_count == 3

    # Пересборка находит те же пары с готовыми решениями и не ждёт инженера.
    again = service.create_assembly(object_id="obj-test", name="ЭМ", section="OV", source_refs=_refs(),
                                    build=False, dedup=True, assembly_id=created["assembly_id"])
    assert again["version_id"] != created["version_id"]
    assert again["status"] == "READY"
    assert again["dedup"]["removed_pages"] == 1
    sources = {p["pair_id"]: p["decision_source"]
               for p in service.duplicate_review("obj-test", again["assembly_id"], again["version_id"])["pairs"]}
    assert set(sources.values()) == {"remembered"}


def test_submit_outside_review_is_refused(dedup_env):
    created = service.create_assembly(object_id="obj-test", name="ЭМ", section="OV",
                                      source_refs=_refs(), build=False, dedup=True)
    with pytest.raises(service.AssemblyError) as unknown:
        service.submit_duplicate_review("obj-test", created["assembly_id"], created["version_id"], ["dup_nope"])
    assert unknown.value.code == "UNKNOWN_DUPLICATE_PAIR"
    service.submit_duplicate_review("obj-test", created["assembly_id"], created["version_id"], [])
    with pytest.raises(service.AssemblyError) as twice:
        service.submit_duplicate_review("obj-test", created["assembly_id"], created["version_id"], [])
    assert twice.value.code == "NOT_AWAITING_REVIEW"


def test_without_dedup_assembly_is_unchanged(dedup_env):
    built = service.create_assembly(object_id="obj-test", name="ЭМ", section="OV", source_refs=_refs())

    assert "dedup" not in built
    assert built["totals"]["page_count"] == 4
    origin = json.loads((_work(built) / "assembly_origin.json").read_text(encoding="utf-8"))
    assert "removed_pages" not in origin
    assert all("removed_pages" not in row for row in origin["sources"])
    assert not list((service._root("obj-test") / built["assembly_id"] / "pending").glob("*.duplicates.json"))


def test_source_page_image_for_review_window(dedup_env):
    created = service.create_assembly(object_id="obj-test", name="ЭМ", section="OV",
                                      source_refs=_refs(), build=False, dedup=True)
    image = service.render_source_page("obj-test", created["assembly_id"], created["version_id"], 1, 2, 600)
    assert image["media_type"] == "image/jpeg" and image["body"][:2] == b"\xff\xd8"
    with pytest.raises(service.AssemblyError) as missing:
        service.render_source_page("obj-test", created["assembly_id"], created["version_id"], 1, 9, 600)
    assert missing.value.code == "PAGE_NOT_FOUND"


# ─── Группа документов и API ─────────────────────────────────────────────────

def _stage_document(object_dir: Path, stage: str, code: str, pages: list[dict]) -> None:
    doc = object_dir / "comparison" / stage / "documents" / code
    _document(doc / "versions" / "v001" / "02_work", code, pages)
    (doc / "document.json").write_text(json.dumps({
        "document_code": code, "object_id": "obj-test", "discipline": "OV",
        "current_version": "v001", "versions": [{"version_id": "v001"}],
    }), encoding="utf-8")


@pytest.fixture
def group_env(tmp_path, monkeypatch):
    object_dir = tmp_path / "projects_v2" / "objects" / "obj-folder"
    comparison = object_dir / "comparison"
    comparison.mkdir(parents=True)
    (object_dir / "object.json").write_text(json.dumps({
        "object_id": "obj-test", "display_name": "Test", "folder_name": "obj-folder",
    }), encoding="utf-8")
    monkeypatch.setenv("PROJECT_ASSEMBLIES_ENABLED", "1")
    monkeypatch.setenv("PROJECT_ASSEMBLY_DEDUP_ENABLED", "1")
    monkeypatch.setattr(service, "_comparison_dir", lambda object_id, create=False: comparison)
    monkeypatch.setattr(service, "MIN_FREE_RESERVE", 0)
    monkeypatch.setattr(service, "launch_version", lambda o, a, v: service.run_version(o, a, v)
                        or service.get_assembly(o, a, v))
    monkeypatch.setattr(document_groups, "_comparison_dir", lambda object_id: comparison)
    _stage_document(object_dir, "stage_1", "P-EM", [{"drawing": "Y"}])
    _stage_document(object_dir, "stage_2", "EM-K1", [{"drawing": "X", "approval": "18.09.2025"}, {"drawing": "Y"}])
    _stage_document(object_dir, "stage_2", "EM-K2", [{"drawing": "X", "sheet": 4, "approval": "03.10.2025"}])
    return object_dir


def test_group_build_waits_for_review_and_reopens_after_build(group_env):
    group = document_groups.add_members("obj-test", "P-EM", ["EM-K1", "EM-K2"])["group"]
    assert group["status"] == "REVIEW"
    assert group["right"]["duplicates"] == {"pairs": 1, "undecided": 1}
    assert group["error"] is None

    review = document_groups.duplicate_review("obj-test", group["group_id"], "right")
    assert len(review["pairs"]) == 1 and review["side"] == "right"

    done = document_groups.submit_duplicate_review("obj-test", group["group_id"], [], "right", author="И")["group"]
    assert done["right"]["status"] == "READY"
    assert done["right"]["duplicates"] == {"pairs": 1, "removed_pages": 1, "kept_pairs": 0}
    assert done["page_count"] == 2

    # Повторное открытие собранной версии: вернуть копию → пересборка без ожидания.
    pair_id = review["pairs"][0]["pair_id"]
    again = document_groups.submit_duplicate_review("obj-test", group["group_id"], [pair_id], "right")["group"]
    assert again["right"]["assembly_version_id"] != done["right"]["assembly_version_id"]
    assert again["right"]["status"] == "READY"
    assert again["right"]["duplicates"] == {"pairs": 1, "removed_pages": 0, "kept_pairs": 1}
    assert again["page_count"] == 3


def test_duplicates_api(group_env):
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    group = document_groups.add_members("obj-test", "P-EM", ["EM-K1", "EM-K2"])["group"]
    base = f"/api/stage-comparison/objects/obj-test/document-groups/{group['group_id']}/duplicates"

    listed = client.get(base, params={"side": "right"})
    assert listed.status_code == 200
    pair = listed.json()["pairs"][0]

    image = client.get(base + "/page", params={"side": "right", "source_index": pair["copy"]["source_index"],
                                               "page": pair["copy"]["source_page"], "width": 480})
    assert image.status_code == 200 and image.headers["content-type"] == "image/jpeg"
    cached = client.get(base + "/page", headers={"if-none-match": image.headers["etag"]},
                        params={"side": "right", "source_index": pair["copy"]["source_index"],
                                "page": pair["copy"]["source_page"], "width": 480})
    assert cached.status_code == 304

    bad = client.post(base, json={"side": "right", "keep_pair_ids": ["dup_unknown"]})
    assert bad.status_code == 400 and bad.json()["detail"]["code"] == "UNKNOWN_DUPLICATE_PAIR"
    ok = client.post(base, json={"side": "right", "keep_pair_ids": []})
    assert ok.status_code == 200 and ok.json()["group"]["right"]["status"] == "READY"
    repeat = client.post(base, json={"side": "right", "keep_pair_ids": []})
    assert repeat.status_code == 200  # собранная версия: решения обновлены, сторона пересобрана
