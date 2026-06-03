"""Тесты V2-режима вкладки «Расхождения» — pair-scoped верификация инженера.

Ключевые инварианты:
  • V2 показывает расхождения ТОЛЬКО текущей PDF-пары (не всю сессию).
  • Ручные статусы хранятся отдельно в `pairs/<pid>/v2_review_status.json`.
  • `comparison_result.json` НИКОГДА не мутируется ручной разметкой.
  • Read-only: ни Qwen, ни Opus, ни unified-analysis не запускаются.
"""
from __future__ import annotations

import io
import json
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from openpyxl import load_workbook


# ─── Фикстуры: tmp comparison root + сессия с двумя парами ────────────────


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_v2_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


def _paths():
    from backend.app.services.stage_comparison import paths as paths_mod
    return paths_mod


def _write_session(session_id: str, pair_ids: list[str]):
    paths_mod = _paths()
    session = {
        "id": session_id,
        "pair_order": list(pair_ids),
        "warnings": [],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(session, ensure_ascii=False), encoding="utf-8")


def _write_pair(session_id: str, pair_id: str, left_name: str, right_name: str):
    paths_mod = _paths()
    pair = {
        "id": pair_id,
        "status": "matched",
        "left": {"filename": left_name, "pdf_path": f"/dev/null/{left_name}"},
        "right": {"filename": right_name, "pdf_path": f"/dev/null/{right_name}"},
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8")


def _write_comparison_result(session_id: str, pair_id: str, changes: list[dict]):
    paths_mod = _paths()
    p = paths_mod.enriched_comparison_result_path(session_id, pair_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "status": "done", "changes": changes}
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return p


def _change(cid, *, title, sev="medium", source="text", old="", new="",
            rhr=False, conf=0.9, cost="unknown"):
    return {
        "id": cid,
        "source": source,
        "type": "changed",
        "category": "general",
        "severity": sev,
        "title": title,
        "summary": f"summary {title}",
        "old_value": old,
        "new_value": new,
        "construction_impact": "влияние",
        "cost_impact": cost,
        "requires_human_review": rhr,
        "confidence": conf,
        "evidence_left": {"quote": old or "L", "section": "X", "approx_location": "стр. 1"},
        "evidence_right": {"quote": new or "R", "section": "X", "approx_location": "стр. 1"},
    }


@pytest.fixture
def session_with_two_pairs():
    sid = "sess_v2"
    p1, p2 = "pAAA", "pBBB"
    _write_session(sid, [p1, p2])
    _write_pair(sid, p1, "left1.pdf", "right1.pdf")
    _write_pair(sid, p2, "left2.pdf", "right2.pdf")
    # Пара 1 — три изменения (одно high, одно нужна ручная проверка, одно cost).
    _write_comparison_result(sid, p1, [
        _change("chg_p1_a", title="P1 высокая", sev="high", old="A1", new="A2"),
        _change("chg_p1_b", title="P1 ручная", sev="low", rhr=True, old="B1", new="B2"),
        _change("chg_p1_c", title="P1 стоимость", sev="medium", cost="likely", old="C1", new="C2"),
    ])
    # Пара 2 — два изменения с ДРУГИМИ id и заголовками.
    _write_comparison_result(sid, p2, [
        _change("chg_p2_x", title="P2 первое", sev="medium", old="X1", new="X2"),
        _change("chg_p2_y", title="P2 второе", sev="high", old="Y1", new="Y2"),
    ])
    return {"sid": sid, "p1": p1, "p2": p2}


def _client():
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def _v2_base(sid, pid):
    return f"/api/stage-comparison/sessions/{sid}/pairs/{pid}/v2"


# ─── 1/2. Pair-scoping ───────────────────────────────────────────────────


def test_v2_changes_returns_only_current_pair(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes")
    assert r.status_code == 200
    data = r.json()
    assert data["pair_id"] == s["p1"]
    titles = {it["title"] for it in data["items"]}
    assert titles == {"P1 высокая", "P1 ручная", "P1 стоимость"}
    assert all(it["pair_id"] == s["p1"] for it in data["items"])


def test_v2_changes_does_not_leak_other_pairs(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes")
    titles = {it["title"] for it in r.json()["items"]}
    # Заголовки из пары 2 не должны попасть в выдачу пары 1.
    assert "P2 первое" not in titles
    assert "P2 второе" not in titles


def test_v2_changes_pair2_independent(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], s['p2'])}/changes")
    titles = {it["title"] for it in r.json()["items"]}
    assert titles == {"P2 первое", "P2 второе"}


# ─── 3. Summary только по текущей паре ───────────────────────────────────


def test_v2_summary_scoped_to_pair(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], s['p1'])}/summary")
    assert r.status_code == 200
    summ = r.json()["summary"]
    assert summ["total"] == 3       # только 3 изменения пары 1, не 5
    assert summ["high"] == 1
    assert summ["medium"] == 1
    assert summ["low"] == 1
    assert summ["needs_human_review"] == 1
    assert summ["not_reviewed"] == 3
    assert summ["confirmed"] == 0


def test_v2_summary_embedded_in_changes(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], s['p2'])}/changes")
    assert r.json()["summary"]["total"] == 2


# ─── 4. PATCH одного изменения ───────────────────────────────────────────


def test_v2_patch_saves_review_status(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    changes = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"]
    cid = changes[0]["id"]
    r = client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{cid}",
                     json={"review_status": "confirmed", "review_comment": "Проверено инженером"})
    assert r.status_code == 200
    # Перечитываем — статус сохранился.
    again = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()
    target = next(it for it in again["items"] if it["id"] == cid)
    assert target["review_status"] == "confirmed"
    assert target["review_comment"] == "Проверено инженером"
    assert target["reviewed_at"]
    assert again["summary"]["confirmed"] == 1
    assert again["summary"]["not_reviewed"] == 2


def test_v2_patch_rejects_invalid_status(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    cid = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"][0]["id"]
    r = client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{cid}",
                     json={"review_status": "totally_bogus"})
    assert r.status_code == 400


def test_v2_patch_unknown_change_404(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/v2_doesnotexist",
                     json={"review_status": "confirmed"})
    assert r.status_code == 404


def test_v2_patch_change_of_other_pair_404(session_with_two_pairs):
    """id из пары 2 нельзя пропатчить через endpoint пары 1."""
    s = session_with_two_pairs
    client = _client()
    p2_id = client.get(f"{_v2_base(s['sid'], s['p2'])}/changes").json()["items"][0]["id"]
    r = client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{p2_id}",
                     json={"review_status": "confirmed"})
    assert r.status_code == 404


# ─── 5. Bulk PATCH в рамках пары ─────────────────────────────────────────


def test_v2_bulk_patch_scoped_to_pair(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    p1_items = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"]
    p2_items = client.get(f"{_v2_base(s['sid'], s['p2'])}/changes").json()["items"]
    p1_ids = [it["id"] for it in p1_items]
    foreign_id = p2_items[0]["id"]
    r = client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes",
                     json={"ids": p1_ids + [foreign_id],
                           "patch": {"review_status": "rejected",
                                     "review_comment": "Пакетно отклонено"}})
    assert r.status_code == 200
    body = r.json()
    assert set(body["updated"]) == set(p1_ids)
    assert foreign_id in body["skipped"]      # чужой id не применился
    again = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()
    assert again["summary"]["rejected"] == 3
    # Пара 2 осталась нетронутой.
    p2_again = client.get(f"{_v2_base(s['sid'], s['p2'])}/changes").json()
    assert p2_again["summary"]["rejected"] == 0


# ─── 6. Хранение в pairs/<pid>/v2_review_status.json ─────────────────────


def test_v2_status_stored_in_pair_file(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    cid = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"][0]["id"]
    client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{cid}",
                 json={"review_status": "confirmed"})
    status_path = _paths().v2_review_status_path(s["sid"], s["p1"])
    assert status_path.exists()
    assert status_path.name == "v2_review_status.json"
    data = json.loads(status_path.read_text(encoding="utf-8"))
    assert cid in data["items"]
    assert data["items"][cid]["review_status"] == "confirmed"
    # Файл пары 2 не создан (мы её не размечали).
    assert not _paths().v2_review_status_path(s["sid"], s["p2"]).exists()


# ─── 7. comparison_result.json не мутируется ─────────────────────────────


def test_v2_does_not_mutate_comparison_result(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    cr_path = _paths().enriched_comparison_result_path(s["sid"], s["p1"])
    before = cr_path.read_bytes()
    cid = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"][0]["id"]
    client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{cid}",
                 json={"review_status": "confirmed", "review_comment": "x"})
    client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes",
                 json={"ids": [cid], "patch": {"review_status": "rejected"}})
    after = cr_path.read_bytes()
    assert before == after, "comparison_result.json не должен меняться при ручной разметке"


# ─── 8. Export XLSX только текущей пары ──────────────────────────────────


def test_v2_export_only_current_pair(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], s['p1'])}/export.xlsx")
    assert r.status_code == 200
    assert "spreadsheetml" in r.headers["content-type"]
    wb = load_workbook(io.BytesIO(r.content))
    assert wb.sheetnames == [
        "Summary", "All V2 changes", "Confirmed", "Needs clarification",
        "Rejected", "Cost impact", "Not reviewed",
    ]
    ws = wb["All V2 changes"]
    # Заголовок + 3 строки (только пара 1).
    titles = [ws.cell(row=ri, column=7).value for ri in range(2, ws.max_row + 1)]
    assert set(titles) == {"P1 высокая", "P1 ручная", "P1 стоимость"}
    assert "P2 первое" not in titles


def test_v2_export_confirmed_sheet_reflects_status(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    cid = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"][0]["id"]
    client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{cid}",
                 json={"review_status": "confirmed"})
    r = client.get(f"{_v2_base(s['sid'], s['p1'])}/export.xlsx")
    wb = load_workbook(io.BytesIO(r.content))
    confirmed = wb["Confirmed"]
    assert confirmed.max_row == 2     # header + 1 confirmed row


# ─── id-стабильность ─────────────────────────────────────────────────────


def test_v2_ids_are_stable_across_rebuilds(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    a = {it["title"]: it["id"] for it in client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"]}
    b = {it["title"]: it["id"] for it in client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"]}
    assert a == b
    assert all(v.startswith("v2_") for v in a.values())


# ─── 404 для несуществующей пары/сессии ──────────────────────────────────


def test_v2_unknown_pair_404(session_with_two_pairs):
    s = session_with_two_pairs
    client = _client()
    r = client.get(f"{_v2_base(s['sid'], 'pNOPE')}/changes")
    assert r.status_code == 404


# ─── 11. Никаких Qwen/Opus вызовов ───────────────────────────────────────


def test_v2_does_not_invoke_llm_providers(session_with_two_pairs, monkeypatch):
    """V2 read-only: ни локальный Qwen, ни Opus-provider не дёргаются."""
    s = session_with_two_pairs

    from backend.app.services.stage_comparison import enriched_comparison as ec
    from backend.app.services.stage_comparison import graphic_llm_local as gl

    def _boom(*a, **k):
        raise AssertionError("LLM provider must NOT be called by V2 read flow")

    # Любая попытка реально сравнить/описать через модель = провал теста.
    monkeypatch.setattr(ec, "run_enriched_comparison", _boom, raising=False)
    monkeypatch.setattr(gl, "describe_image_local", _boom, raising=False)

    client = _client()
    assert client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").status_code == 200
    cid = client.get(f"{_v2_base(s['sid'], s['p1'])}/changes").json()["items"][0]["id"]
    assert client.patch(f"{_v2_base(s['sid'], s['p1'])}/changes/{cid}",
                        json={"review_status": "confirmed"}).status_code == 200
    assert client.get(f"{_v2_base(s['sid'], s['p1'])}/export.xlsx").status_code == 200


# ─── UI parity (кнопка V2 + старая вкладка цела) ─────────────────────────

_ROOT = Path(__file__).resolve().parent.parent


def test_ui_has_v2_button():
    """index.html должен содержать переключатель V2 во вкладке «Расхождения»."""
    html = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    assert "scSetV2View" in html
    assert "scV2View" in html


def test_ui_old_discrepancies_tab_intact():
    """Старый unified-режим вкладки «Расхождения» не должен быть удалён."""
    html = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    assert "scUnifiedItemsSorted" in html        # старая таблица расхождений
    assert "scDiffSubtab==='unified'" in html


def test_ui_app_js_exposes_v2_methods():
    app_js = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    assert "scLoadV2Changes" in app_js
    assert "scV2View" in app_js
