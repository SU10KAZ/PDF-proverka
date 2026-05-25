"""Tests for pair config template persistence (links + page_alignment).

Покрывает:
1. template_key стабилен и зависит только от полных путей PDF (после resolve)
2. find_template возвращает None, если файла нет
3. save_template записывает links + alignment в comparison/templates/<key>.json
4. apply_template перезаписывает links + alignment в пару и метит pair.json
5. clear_applied_template снимает только пометку (links/alignment не трогает)
6. create_session() авто-применяет template для пар с совпадающими PDF путями
7. template_status даёт UI-frame для баннера
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


def _utc():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_session_with_pair(session_id: str, pair_id: str,
                            left_pdf: Path, right_pdf: Path) -> dict:
    """Создаёт минимальные session.json + pair.json в новом формате."""
    from backend.app.services.stage_comparison import paths as paths_mod

    paths_mod.session_json_path(session_id).write_text(
        json.dumps({"id": session_id, "pair_order": [pair_id],
                    "warnings": [], "created_at": _utc()}),
        encoding="utf-8",
    )
    pair = {
        "id": pair_id, "status": "matched",
        "left":  {"filename": left_pdf.name, "pdf_path": str(left_pdf), "md_path": None},
        "right": {"filename": right_pdf.name, "pdf_path": str(right_pdf), "md_path": None},
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8",
    )
    # Минимальные links + alignment
    paths_mod.links_path(session_id, pair_id).write_text("[]", encoding="utf-8")
    paths_mod.page_alignment_path(session_id, pair_id).write_text(
        json.dumps({"items": [], "left_page_count": 1, "right_page_count": 1}),
        encoding="utf-8",
    )
    return pair


def _make_fake_pdf(p: Path, name: str) -> Path:
    f = p / name
    f.write_bytes(b"%PDF-1.4\n")  # достаточно для resolve()
    return f


# ─── 1. template_key ───────────────────────────────────────────────────


def test_template_key_stable_and_deterministic(tmp_path):
    from backend.app.services.stage_comparison import pair_template
    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")

    k1 = pair_template.template_key(str(a), str(b))
    k2 = pair_template.template_key(str(a), str(b))
    assert k1 == k2
    assert len(k1) == 40  # sha1 hex
    # Перестановка → другой ключ
    k_swap = pair_template.template_key(str(b), str(a))
    assert k_swap != k1


def test_template_key_resolves_symlinks_and_normalization(tmp_path):
    from backend.app.services.stage_comparison import pair_template
    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")

    # Тот же файл, но путь с двойными слешами
    weird_a = str(a).replace("/", "//", 1)
    k1 = pair_template.template_key(str(a), str(b))
    k2 = pair_template.template_key(weird_a, str(b))
    assert k1 == k2


# ─── 2. find_template: no file → None ──────────────────────────────────


def test_find_template_returns_none_when_missing(tmp_path):
    from backend.app.services.stage_comparison import pair_template
    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")
    assert pair_template.find_template(str(a), str(b)) is None


# ─── 3. save_template ───────────────────────────────────────────────────


def test_save_template_writes_links_and_alignment(tmp_path):
    from backend.app.services.stage_comparison import pair_template, paths as paths_mod
    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")
    _make_session_with_pair("sess_t1", "p1", a, b)

    # Подсунем какие-то links и alignment
    links = [{"left_block_id": "L1", "right_block_id": "R1",
              "method": "manual", "alignment_slot": 1, "page": 1}]
    paths_mod.links_path("sess_t1", "p1").write_text(
        json.dumps(links, ensure_ascii=False), encoding="utf-8",
    )
    alignment = {"items": [{"slot": 1, "left_page": 1, "right_page": 1}],
                 "left_page_count": 5, "right_page_count": 4}
    paths_mod.page_alignment_path("sess_t1", "p1").write_text(
        json.dumps(alignment), encoding="utf-8",
    )

    payload = pair_template.save_template("sess_t1", "p1")
    assert payload["links_count"] == 1
    assert payload["links"] == links
    assert payload["page_alignment"]["left_page_count"] == 5
    assert payload["key"]
    # Проверим, что файл реально на диске под этим ключом
    p = paths_mod.pair_template_path(payload["key"])
    assert p.exists()
    on_disk = json.loads(p.read_text(encoding="utf-8"))
    assert on_disk["links_count"] == 1


# ─── 4. apply_template ──────────────────────────────────────────────────


def test_apply_template_overwrites_links_and_alignment_and_marks_pair(tmp_path):
    from backend.app.services.stage_comparison import pair_template, paths as paths_mod

    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")

    # Сессия 1: настроили links, сохранили шаблон
    _make_session_with_pair("sess_src", "p1", a, b)
    links_src = [{"left_block_id": "X", "right_block_id": "Y",
                  "method": "manual", "alignment_slot": 1, "page": 1}]
    paths_mod.links_path("sess_src", "p1").write_text(
        json.dumps(links_src), encoding="utf-8",
    )
    paths_mod.page_alignment_path("sess_src", "p1").write_text(
        json.dumps({"items": [{"slot": 1, "left_page": 1, "right_page": 1}],
                    "left_page_count": 3, "right_page_count": 3}), encoding="utf-8",
    )
    pair_template.save_template("sess_src", "p1")

    # Сессия 2: те же PDF, но пустая пара
    _make_session_with_pair("sess_dst", "p1", a, b)
    # До apply — links пустые
    assert json.loads(paths_mod.links_path("sess_dst", "p1").read_text(encoding="utf-8")) == []

    result = pair_template.apply_template("sess_dst", "p1")
    assert result["applied"] is True
    assert result["links_applied"] == 1
    assert result["alignment_applied"] is True

    applied_links = json.loads(paths_mod.links_path("sess_dst", "p1").read_text(encoding="utf-8"))
    assert applied_links == links_src

    # Маркер в pair.json
    pj = json.loads(paths_mod.pair_json_path("sess_dst", "p1").read_text(encoding="utf-8"))
    assert pj["template_applied"] is True
    assert pj["template_applied_at"]
    assert pj["template_key"] == result["template_key"]
    assert pj["template_source_session_id"] == "sess_src"


def test_apply_template_returns_no_template_when_missing(tmp_path):
    from backend.app.services.stage_comparison import pair_template

    a = _make_fake_pdf(tmp_path, "noop_a.pdf")
    b = _make_fake_pdf(tmp_path, "noop_b.pdf")
    _make_session_with_pair("sess_dst2", "p1", a, b)
    result = pair_template.apply_template("sess_dst2", "p1")
    assert result["applied"] is False
    assert result["reason"] == "no_template"


# ─── 5. clear_applied_template ─────────────────────────────────────────


def test_clear_applied_template_only_drops_marker(tmp_path):
    from backend.app.services.stage_comparison import pair_template, paths as paths_mod

    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")
    _make_session_with_pair("sess_src5", "p1", a, b)
    links = [{"left_block_id": "X", "right_block_id": "Y",
              "method": "manual", "alignment_slot": 1, "page": 1}]
    paths_mod.links_path("sess_src5", "p1").write_text(
        json.dumps(links), encoding="utf-8",
    )
    pair_template.save_template("sess_src5", "p1")

    _make_session_with_pair("sess_dst5", "p1", a, b)
    pair_template.apply_template("sess_dst5", "p1")
    pj_before = json.loads(paths_mod.pair_json_path("sess_dst5", "p1").read_text(encoding="utf-8"))
    assert pj_before["template_applied"] is True

    pair_template.clear_applied_template("sess_dst5", "p1")
    pj_after = json.loads(paths_mod.pair_json_path("sess_dst5", "p1").read_text(encoding="utf-8"))
    assert "template_applied" not in pj_after
    # links сохранились — clear не должен их трогать
    saved_links = json.loads(paths_mod.links_path("sess_dst5", "p1").read_text(encoding="utf-8"))
    assert saved_links == links


# ─── 6. create_session auto-applies template ───────────────────────────


def test_create_session_auto_applies_template_for_matching_pdfs(tmp_path, monkeypatch):
    """Полный сценарий: настроили в сессии 1, удалили её, создали сессию 2
    через store.create_session() — шаблон применяется автоматически."""
    from backend.app.services.stage_comparison import (
        pair_template, paths as paths_mod, store as store_mod,
    )

    # Готовим папки stage_a / stage_b с одинаковыми PDF, чтобы scanner_mod.match_pdfs
    # нашёл пару. Для теста создаём фейковые PDF.
    stage_a = tmp_path / "stage_a"
    stage_b = tmp_path / "stage_b"
    stage_a.mkdir(); stage_b.mkdir()
    a_pdf = _make_fake_pdf(stage_a, "doc.pdf")
    b_pdf = _make_fake_pdf(stage_b, "doc.pdf")

    # Эмулируем scanner_mod и alignment_mod, чтобы не требовать настоящих PDF
    fake_pair_dict = {
        "status": "matched",
        "match_score": 0.95,
        "left":  {"filename": "doc.pdf", "pdf_path": str(a_pdf), "md_path": None,
                  "result_json_path": None, "relative": "doc.pdf", "stem": "doc",
                  "has_md": False, "has_result_json": False},
        "right": {"filename": "doc.pdf", "pdf_path": str(b_pdf), "md_path": None,
                  "result_json_path": None, "relative": "doc.pdf", "stem": "doc",
                  "has_md": False, "has_result_json": False},
    }

    class _FakePair:
        def to_dict(self):
            return dict(fake_pair_dict)

    monkeypatch.setattr(
        store_mod.scanner_mod, "scan_stage_folder",
        lambda path: ([], []),
    )
    monkeypatch.setattr(
        store_mod.scanner_mod, "match_pdfs",
        lambda left, right: [_FakePair()],
    )
    monkeypatch.setattr(store_mod, "_pdf_page_count", lambda path: 5)

    # Сессия 1: настроили links, сохранили шаблон
    sess1, _ = store_mod.create_session(str(stage_a), str(stage_b))
    sid1 = sess1["id"]
    pid1 = (sess1.get("pairs") or [])[0]["id"]
    # Пишем links вручную
    links = [{"left_block_id": "L", "right_block_id": "R",
              "method": "manual", "alignment_slot": 1, "page": 1}]
    paths_mod.links_path(sid1, pid1).write_text(
        json.dumps(links), encoding="utf-8",
    )
    pair_template.save_template(sid1, pid1)

    # Сессия 2: создаём заново; ожидаем, что links уже применены из шаблона
    sess2, _ = store_mod.create_session(str(stage_a), str(stage_b))
    sid2 = sess2["id"]
    pid2 = (sess2.get("pairs") or [])[0]["id"]
    assert sid2 != sid1  # новая сессия

    applied_links = json.loads(paths_mod.links_path(sid2, pid2).read_text(encoding="utf-8"))
    assert applied_links == links

    # И на pair.json есть пометка template_applied
    pj = json.loads(paths_mod.pair_json_path(sid2, pid2).read_text(encoding="utf-8"))
    assert pj["template_applied"] is True
    assert pj["template_source_session_id"] == sid1


# ─── 7. template_status UI helper ──────────────────────────────────────


def test_template_status_reports_correct_state(tmp_path):
    from backend.app.services.stage_comparison import pair_template, paths as paths_mod

    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")

    _make_session_with_pair("sess_st1", "p1", a, b)
    # No template yet
    status = pair_template.template_status("sess_st1", "p1")
    assert status["has_template"] is False
    assert status["applied"] is False
    assert status["template"] is None

    # Создадим шаблон через save_template
    paths_mod.links_path("sess_st1", "p1").write_text(
        json.dumps([{"left_block_id": "L", "right_block_id": "R",
                     "method": "manual", "alignment_slot": 1, "page": 1}]),
        encoding="utf-8",
    )
    pair_template.save_template("sess_st1", "p1")

    # Make new session with same PDFs
    _make_session_with_pair("sess_st2", "p1", a, b)
    status2 = pair_template.template_status("sess_st2", "p1")
    assert status2["has_template"] is True
    assert status2["template"]["links_count"] == 1
    # Apply → applied=True
    pair_template.apply_template("sess_st2", "p1")
    status3 = pair_template.template_status("sess_st2", "p1")
    assert status3["applied"] is True
    assert status3["template"]["source_session_id"] == "sess_st1"


# ─── 8. API endpoints smoke ─────────────────────────────────────────────


def test_template_endpoints_via_testclient(tmp_path):
    from fastapi.testclient import TestClient
    from backend.app.main import app
    from backend.app.services.stage_comparison import paths as paths_mod

    a = _make_fake_pdf(tmp_path, "a.pdf")
    b = _make_fake_pdf(tmp_path, "b.pdf")
    _make_session_with_pair("sess_api1", "p1", a, b)
    paths_mod.links_path("sess_api1", "p1").write_text(
        json.dumps([{"left_block_id": "L", "right_block_id": "R",
                     "method": "manual", "alignment_slot": 1, "page": 1}]),
        encoding="utf-8",
    )

    client = TestClient(app)
    # Status: пусто
    r = client.get("/api/stage-comparison/sessions/sess_api1/pairs/p1/template-status")
    assert r.status_code == 200, r.text
    assert r.json()["has_template"] is False

    # Save
    r = client.post("/api/stage-comparison/sessions/sess_api1/pairs/p1/save-template")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["links_count"] == 1

    # Status: есть, не applied (т.к. сохранили в этой же сессии)
    r = client.get("/api/stage-comparison/sessions/sess_api1/pairs/p1/template-status")
    assert r.status_code == 200
    body = r.json()
    assert body["has_template"] is True

    # Apply
    r = client.post("/api/stage-comparison/sessions/sess_api1/pairs/p1/apply-template")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["applied"] is True
    assert body["links_applied"] == 1

    # Clear marker
    r = client.post("/api/stage-comparison/sessions/sess_api1/pairs/p1/clear-template")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
