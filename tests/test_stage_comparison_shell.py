from __future__ import annotations

import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fitz
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import store


def _pdf_pages_bytes(*labels: str) -> bytes:
    document = fitz.open()
    for label in labels:
        page = document.new_page(width=240, height=160)
        page.insert_text((24, 48), label)
    payload = document.tobytes()
    document.close()
    return payload


def _pdf_bytes(label: str) -> bytes:
    return _pdf_pages_bytes(label)


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router_mod.router)
    return app


def test_shell_imports_and_object_list_opens(monkeypatch):
    expected = {"roots": ["/safe"], "items": [{"id": "o1", "name": "Object"}], "count": 1}
    monkeypatch.setattr(router_mod.objects_mod, "list_objects", lambda: expected)

    response = TestClient(_app()).get("/api/stage-comparison/objects")

    assert response.status_code == 200
    assert response.json() == expected
    assert store.SHELL_KIND == "stage_comparison_shell"


def test_pair_get_reads_ready_stage4_artifacts_without_model_call(monkeypatch):
    pair = {
        "id": "pair",
        "left": {"pdf_path": "/tmp/left.pdf", "filename": "left.pdf"},
        "right": {"pdf_path": "/tmp/right.pdf", "filename": "right.pdf"},
    }
    final = {
        "version": 1,
        "kind": "stage_comparison_text_final_comparison",
        "pair_id": "pair",
        "review_status": "completed",
        "sheet_groups": [],
        "summary": {"same": 314, "moved": 16, "uncertain": 91},
    }
    calls = []

    async def forbidden_model_call(*_args, **_kwargs):
        calls.append("model")
        raise AssertionError("GET pair must never run the AI reviewer")

    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"id": "session"})
    monkeypatch.setattr(store, "_load_pair", lambda *_: pair)
    monkeypatch.setattr(store, "_page_count", lambda *_: 1)
    monkeypatch.setattr(store, "get_sheet_matching_state", lambda *_: None)
    monkeypatch.setattr(store, "get_text_comparison_state", lambda *_: None)
    monkeypatch.setattr(store, "get_text_differences_state", lambda *_: None)
    monkeypatch.setattr(store, "get_text_ai_review_state", lambda *_: {"status": "completed"})
    monkeypatch.setattr(store, "get_text_final_comparison_state", lambda *_: final)
    monkeypatch.setattr(store, "run_text_ai_review", forbidden_model_call)

    response = TestClient(_app()).get("/api/stage-comparison/sessions/session/pairs/pair")

    assert response.status_code == 200
    assert response.json()["text_final_comparison"] == final
    assert calls == []


def test_text_exclusion_endpoint_and_downstream_gate(monkeypatch):
    current = {
        "version": 1,
        "kind": "stage_comparison_text_exclusions",
        "pair_id": "pair",
        "stale": False,
        "valid": True,
        "policy": {"required_before_downstream_comparison": True},
    }
    monkeypatch.setattr(store, "get_text_exclusions_state", lambda *_: current)
    response = TestClient(_app()).get(
        "/api/stage-comparison/sessions/session/pairs/pair/text-exclusions"
    )
    assert response.status_code == 200
    assert response.json() == current
    assert store.require_text_exclusions_for_downstream("session", "pair") == current

    monkeypatch.setattr(
        store, "get_text_exclusions_state", lambda *_: {**current, "stale": True}
    )
    try:
        store.require_text_exclusions_for_downstream("session", "pair")
    except ValueError as exc:
        assert str(exc) == "text_exclusions_stale"
    else:
        raise AssertionError("stale exclusions must block every downstream stage")

    monkeypatch.setattr(
        store, "get_text_exclusions_state", lambda *_: {**current, "valid": False}
    )
    try:
        store.require_text_exclusions_for_downstream("session", "pair")
    except ValueError as exc:
        assert str(exc) == "text_exclusions_invalid"
    else:
        raise AssertionError("invalid exclusions must block every downstream stage")


def test_pdf_list_pair_and_raster_page(tmp_path, monkeypatch):
    comparison_root = tmp_path / "comparison-runtime"
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    left_pdf = stage_1 / "project.pdf"
    right_pdf = stage_2 / "working.pdf"
    left_pdf.write_bytes(_pdf_bytes("stage 1"))
    right_pdf.write_bytes(_pdf_bytes("stage 2"))
    # The matcher reads only the ready-made Page/Sheet index from results HTML.
    html_index = '<ol><li><a href="#page-0">Sheet 7 — Корпус 4. План 1 этажа</a></li></ol>'
    (stage_1 / "project_results.html").write_text(html_index, encoding="utf-8")
    (stage_2 / "working_results.html").write_text(
        html_index.replace("Sheet 7", "Sheet 99"), encoding="utf-8"
    )
    (stage_2 / "blocks.json").write_text("not-json-on-purpose", encoding="utf-8")
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))

    client = TestClient(_app())
    created = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    )
    assert created.status_code == 200
    session = created.json()
    assert [item["filename"] for item in session["documents"]["stage_1"]] == ["project.pdf"]
    assert [item["filename"] for item in session["documents"]["stage_2"]] == ["working.pdf"]
    assert session["documents"]["stage_1"][0]["html_path"] == str(stage_1 / "project_results.html")

    pair_response = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/pairs",
        json={"left_pdf": str(left_pdf), "right_pdf": str(right_pdf)},
    )
    assert pair_response.status_code == 200
    pair_view = pair_response.json()
    assert pair_view["left_page_count"] == 1
    assert pair_view["right_page_count"] == 1

    pair_id = pair_view["pair"]["id"]
    initial_session = client.get(f"/api/stage-comparison/sessions/{session['id']}").json()
    assert initial_session["pairs"][0]["sheet_matching_ready"] is False
    suggested_pairing = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/document-pairing/suggest"
    )
    assert suggested_pairing.status_code == 200
    assert suggested_pairing.json()["matched_count"] == 0
    assert suggested_pairing.json()["confirmed_pairs"] == []
    pairing_response = client.put(
        f"/api/stage-comparison/sessions/{session['id']}/document-pairing",
        json={
            "left_order": [str(left_pdf)],
            "right_order": [str(right_pdf)],
            "confirmed_pairs": [{"left_pdf": str(left_pdf), "right_pdf": str(right_pdf)}],
        },
    )
    assert pairing_response.status_code == 200
    assert pairing_response.json()["confirmed_pairs"] == [
        {"left_pdf": str(left_pdf), "right_pdf": str(right_pdf)}
    ]
    restored = client.get(f"/api/stage-comparison/sessions/{session['id']}")
    assert restored.status_code == 200
    assert restored.json()["document_pairing"]["left_order"] == [str(left_pdf)]
    reopened = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    )
    assert reopened.json()["id"] == session["id"]
    assert reopened.json()["document_pairing"] == pairing_response.json()

    preview = client.get(
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/page-preview",
        params={"side": "left", "page": 1, "width": 1400},
    )
    assert preview.status_code == 200
    assert preview.headers["content-type"].startswith("image/png")
    assert preview.content.startswith(b"\x89PNG")

    processed = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/sheet-match-suggestions"
    )
    assert processed.status_code == 200
    assert processed.json()["suggestions"]["suggestions"][0]["primary_right_page"] == 1
    restored_after_processing = client.get(
        f"/api/stage-comparison/sessions/{session['id']}"
    ).json()
    assert restored_after_processing["pairs"][0]["sheet_matching_ready"] is True

    saved = client.put(
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/sheet-links",
        json={
            "links": [{
                "left_pages": [1], "right_pages": [1], "source": "manual",
                "confidence": "manual", "reason": ["user_corrected"],
            }],
            "unlinked_left_pages": [],
        },
    )
    assert saved.status_code == 200
    assert saved.json()["links"]["links"][0]["source"] == "manual"

    removed_paths = [
        f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair_id}/page-image?side=left&page=1",
        f"/api/stage-comparison/sessions/{session['id']}/comparison-statuses",
        "/api/stage-comparison/change-regions-cleanup-pilot",
        "/api/stage-comparison/change-groups-pilot",
        "/api/stage-comparison/semantic-diff-pilot",
        "/api/stage-comparison/semantic-diff-v6a1-pilot",
        "/api/stage-comparison/semantic-diff-v6a2-mass",
        "/api/stage-comparison/pipeline-v2/run",
    ]
    for path in removed_paths:
        assert client.get(path).status_code == 404, path

    stored = list(comparison_root.rglob("*.json"))
    assert stored
    assert any(path.name == "sheet_match_suggestions.json" for path in stored)
    assert any(path.name == "sheet_links.json" for path in stored)
    assert not any(token in path.as_posix() for path in stored for token in ("findings", "diagnostic"))


def test_document_pairing_rejects_documents_outside_session(tmp_path, monkeypatch):
    stage_1 = tmp_path / "stage_1"
    stage_2 = tmp_path / "stage_2"
    stage_1.mkdir()
    stage_2.mkdir()
    left_pdf = stage_1 / "left.pdf"
    right_pdf = stage_2 / "right.pdf"
    left_pdf.write_bytes(_pdf_bytes("left"))
    right_pdf.write_bytes(_pdf_bytes("right"))
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))
    client = TestClient(_app())
    session = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    ).json()

    response = client.put(
        f"/api/stage-comparison/sessions/{session['id']}/document-pairing",
        json={
            "left_order": [str(tmp_path / "foreign.pdf")],
            "right_order": [str(right_pdf)],
            "confirmed_pairs": [],
        },
    )

    assert response.status_code == 400
    assert "left_order_must_contain_all_session_documents" in response.json()["detail"]


def _viewer_pair(tmp_path, monkeypatch) -> tuple[TestClient, str, str]:
    """Сессия с парой PDF, готовая к запросам векторной страницы."""
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    (stage_1 / "project.pdf").write_bytes(_pdf_bytes("stage 1"))
    (stage_2 / "working.pdf").write_bytes(_pdf_bytes("stage 2"))
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison-runtime"))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))
    store._svg_cache.clear()

    client = TestClient(_app())
    session = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    ).json()
    pair = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/pairs",
        json={"left_pdf": str(stage_1 / "project.pdf"), "right_pdf": str(stage_2 / "working.pdf")},
    ).json()
    return client, session["id"], pair["pair"]["id"]


def test_pdf_text_search_is_case_insensitive_and_isolated_per_side(tmp_path, monkeypatch):
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    left_pdf = stage_1 / "project.pdf"
    right_pdf = stage_2 / "working.pdf"
    left_pdf.write_bytes(_pdf_pages_bytes("Pump room", "Corridor", "PUMP schedule pump"))
    right_pdf.write_bytes(_pdf_pages_bytes("Pipe room", "Working drawing"))
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison-runtime"))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))
    with store._pdf_text_cache_lock:
        store._pdf_text_cache.clear()

    client = TestClient(_app())
    session = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    ).json()
    pair = client.post(
        f"/api/stage-comparison/sessions/{session['id']}/pairs",
        json={"left_pdf": str(left_pdf), "right_pdf": str(right_pdf)},
    ).json()["pair"]
    url = f"/api/stage-comparison/sessions/{session['id']}/pairs/{pair['id']}/text-search"

    left = client.get(url, params={"side": "left", "query": "  pump  "})
    assert left.status_code == 200
    payload = left.json()
    assert payload | {"pages": []} == {
        "query": "pump",
        "pages": [],
        "matched_pages": 2,
        "total_matches": 3,
        "page_count": 3,
        "has_text_layer": True,
    }
    assert [
        (item["page"], item["matches"], len(item["highlights"]))
        for item in payload["pages"]
    ] == [(1, 1, 1), (3, 2, 2)]
    for item in payload["pages"]:
        for highlight in item["highlights"]:
            assert set(highlight) == {"match_index", "x", "y", "width", "height"}
            assert 0 <= highlight["x"] < 1
            assert 0 <= highlight["y"] < 1
            assert 0 < highlight["width"] <= 1
            assert 0 < highlight["height"] <= 1
            assert highlight["x"] + highlight["width"] <= 1.000001
            assert highlight["y"] + highlight["height"] <= 1.000001
    assert [item["match_index"] for item in payload["pages"][1]["highlights"]] == [0, 1]

    phrase = client.get(url, params={"side": "left", "query": "PUMP SCHEDULE"})
    assert phrase.status_code == 200
    assert [
        (item["page"], item["matches"], len(item["highlights"]))
        for item in phrase.json()["pages"]
    ] == [(3, 1, 2)]
    assert {item["match_index"] for item in phrase.json()["pages"][0]["highlights"]} == {0}

    right = client.get(url, params={"side": "right", "query": "pump"})
    assert right.status_code == 200
    assert right.json()["pages"] == []
    assert right.json()["has_text_layer"] is True


def test_pdf_text_search_rejects_bad_input(tmp_path, monkeypatch):
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    url = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/text-search"

    assert client.get(url, params={"side": "middle", "query": "stage"}).status_code == 422
    assert client.get(url, params={"side": "left", "query": ""}).status_code == 422
    assert client.get(url, params={"side": "left", "query": " "}).status_code == 400
    assert client.get(url, params={"side": "left", "query": "x" * 201}).status_code == 422


def test_page_svg_namespaces_ids_per_side(tmp_path, monkeypatch):
    """Обе страницы пары живут в ОДНОМ документе фронтенда.

    MuPDF нумерует clip/font-идентификаторы с нуля на каждой странице, поэтому
    без префикса `url(#clip_1)` правой панели разрешился бы в clip-path левой.
    """
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)

    left = client.get(
        f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-svg",
        params={"side": "left", "page": 1},
    )
    right = client.get(
        f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-svg",
        params={"side": "right", "page": 1},
    )

    assert left.status_code == 200 and right.status_code == 200
    assert b"<svg" in left.content and b"<svg" in right.content
    assert b'id="scr_' not in left.content
    assert b'id="scl_' not in right.content
    for payload, prefix in ((left.content, b"scl_"), (right.content, b"scr_")):
        for anchor in (b'id="', b"url(#", b'href="#'):
            for position in _positions(payload, anchor):
                assert payload[position + len(anchor):position + len(anchor) + len(prefix)] == prefix


def _positions(payload: bytes, anchor: bytes) -> list[int]:
    found, start = [], payload.find(anchor)
    while start != -1:
        found.append(start)
        start = payload.find(anchor, start + 1)
    return found


def test_page_svg_is_pre_gzipped_and_revalidates(tmp_path, monkeypatch):
    """Лист A1 после text_as_path весит ~6 МБ.

    Сжимаем его в threadpool и кэшируем, а не отдаём общему GZipMiddleware —
    тот жмёт уровнем 9 прямо в event loop. Повторный заход снимается ETag'ом.
    """
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    url = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-svg"

    first = client.get(url, params={"side": "left", "page": 1})
    assert first.status_code == 200
    assert first.headers["content-encoding"] == "gzip"
    assert first.headers["etag"]

    cached = client.get(
        url, params={"side": "left", "page": 1}, headers={"If-None-Match": first.headers["etag"]}
    )
    assert cached.status_code == 304
    assert not cached.content

    plain = client.get(url, params={"side": "left", "page": 1}, headers={"Accept-Encoding": "identity"})
    assert plain.status_code == 200
    assert "content-encoding" not in plain.headers
    assert plain.content == first.content


def test_page_svg_cache_follows_the_pdf(tmp_path, monkeypatch):
    """Перезалитый PDF обязан вытеснить свою запись кэша."""
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    url = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-svg"

    before = client.get(url, params={"side": "left", "page": 1})
    (tmp_path / "object" / "comparison" / "stage_1" / "project.pdf").write_bytes(
        _pdf_bytes("stage 1 corrected")
    )
    after = client.get(url, params={"side": "left", "page": 1})

    assert after.status_code == 200
    assert after.headers["etag"] != before.headers["etag"]


def test_page_svg_rejects_out_of_range_page(tmp_path, monkeypatch):
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)

    response = client.get(
        f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-svg",
        params={"side": "left", "page": 99},
    )

    assert response.status_code == 400


def test_page_svg_strips_active_content():
    """Страница вставляется инлайновым SVG, а не через <img>.

    В <img> скрипт внутри SVG инертен, инлайн — выполнится в контексте портала.
    PDF приносит пользователь, поэтому активные узлы снимаются до отдачи.
    """
    harden = store._harden_svg

    assert "<script" not in harden('<svg><script>alert(1)</script><path/></svg>')
    assert "<script" not in harden('<svg><script src="x.js"/></svg>')
    assert "onload" not in harden('<svg><g onload="alert(1)"><path/></g></svg>')
    assert "onclick" not in harden("<svg><g onclick='alert(1)'><path/></g></svg>")
    assert "javascript:" not in harden('<svg><a xlink:href="javascript:alert(1)"/></svg>')
    assert "foreignObject" not in harden("<svg><foreignObject><body/></foreignObject></svg>")

    # обычный чертёж обязан пройти насквозь байт в байт
    drawing = '<svg><path d="M0 0" clip-path="url(#scl_clip_1)"/><use href="#scl_font_1_2"/></svg>'
    assert harden(drawing) == drawing


def test_document_pairing_drops_rows_empty_on_both_sides(tmp_path, monkeypatch):
    """Строка без документа с ОБЕИХ сторон — мусор, а не «пара».

    Дырка на ОДНОЙ стороне, наоборот, осмысленна: это документ, которому пока
    не нашли пары, и он обязан стоять напротив пустого места.
    """
    stage_1 = tmp_path / "object" / "comparison" / "stage_1"
    stage_2 = tmp_path / "object" / "comparison" / "stage_2"
    stage_1.mkdir(parents=True)
    stage_2.mkdir(parents=True)
    left_pdf = stage_1 / "project.pdf"
    right_pdf = stage_2 / "working.pdf"
    left_pdf.write_bytes(_pdf_bytes("stage 1"))
    right_pdf.write_bytes(_pdf_bytes("stage 2"))
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison-runtime"))
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(tmp_path))

    client = TestClient(_app())
    session = client.post(
        "/api/stage-comparison/sessions",
        json={"stage_a_path": str(stage_1), "stage_b_path": str(stage_2)},
    ).json()

    saved = client.put(
        f"/api/stage-comparison/sessions/{session['id']}/document-pairing",
        json={
            # строка 1 — только П, строка 2 — только РД, строка 3 — пустая с обеих
            "left_order": [str(left_pdf), None, None],
            "right_order": [None, str(right_pdf), None],
            "confirmed_pairs": [],
        },
    )

    assert saved.status_code == 200
    payload = saved.json()
    assert payload["left_order"] == [str(left_pdf), None]
    assert payload["right_order"] == [None, str(right_pdf)]

    # и после перезагрузки сессии мусорной строки тоже нет
    reopened = client.get(f"/api/stage-comparison/sessions/{session['id']}").json()
    pairing = reopened["document_pairing"]
    assert not [
        index for index, (left, right) in enumerate(
            zip(pairing["left_order"], pairing["right_order"])
        ) if not left and not right
    ]


def test_page_thumbnail_is_cached_raster(tmp_path, monkeypatch):
    """Миниатюры для полосы навигации: PNG, ETag и долгий кэш.

    Насыщенный лист рисуется ~120 мс, а полоса прокручивается туда-обратно —
    без кэша каждый проход стоил бы десятки перерисовок.
    """
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    store._thumb_cache.clear()
    url = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-thumb"

    first = client.get(url, params={"side": "left", "page": 1, "width": 160})
    assert first.status_code == 200
    assert first.headers["content-type"] == "image/png"
    assert first.content.startswith(b"\x89PNG")
    assert "max-age=86400" in first.headers["cache-control"]

    cached = client.get(
        url,
        params={"side": "left", "page": 1, "width": 160},
        headers={"If-None-Match": first.headers["etag"]},
    )
    assert cached.status_code == 304

    # ширина участвует в ключе кэша — иначе панель получила бы чужой размер
    wider = client.get(url, params={"side": "left", "page": 1, "width": 320})
    assert wider.status_code == 200
    assert wider.headers["etag"] != first.headers["etag"]


def test_page_thumbnail_rejects_bad_input(tmp_path, monkeypatch):
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    url = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-thumb"

    assert client.get(url, params={"side": "left", "page": 99}).status_code == 400
    assert client.get(url, params={"side": "middle", "page": 1}).status_code == 422
    assert client.get(url, params={"side": "left", "page": 1, "width": 4000}).status_code == 422


def test_page_info_preview_and_tile_contract(tmp_path, monkeypatch):
    """Основной viewer получает геометрию, preview и только нужные тайлы."""
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    base = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}"
    with store._page_display_cache_lock:
        stale_contexts = list(store._page_display_cache.values())
        store._page_display_cache.clear()
        store._page_display_build_locks.clear()
    for context in stale_contexts:
        store._close_page_display_context(context)
    with store._page_raster_cache_lock:
        store._page_raster_cache.clear()
        store._page_raster_cache_bytes = 0

    info = client.get(f"{base}/page-info", params={"side": "left", "page": 1})
    assert info.status_code == 200
    assert info.json()["width"] > 0
    assert info.json()["height"] > 0
    assert info.json()["signature"]
    assert info.json()["tile_size"] == 512
    assert info.json()["max_level"] == 6

    preview = client.get(
        f"{base}/page-preview", params={"side": "left", "page": 1, "width": 1400}
    )
    assert preview.status_code == 200
    assert preview.content.startswith(b"\x89PNG")
    assert "max-age=86400" in preview.headers["cache-control"]
    assert preview.headers["etag"]
    with store._page_display_cache_lock:
        assert len(store._page_display_cache) == 1
        display_list = next(iter(store._page_display_cache.values()))["display_list"]
    assert client.get(
        f"{base}/page-preview",
        params={"side": "left", "page": 1, "width": 1400},
        headers={"If-None-Match": preview.headers["etag"]},
    ).status_code == 304

    tile = client.get(
        f"{base}/page-tile",
        params={"side": "left", "page": 1, "level": 0, "x": 0, "y": 0},
    )
    assert tile.status_code == 200
    assert tile.content.startswith(b"\x89PNG")
    assert tile.headers["etag"]
    with store._page_display_cache_lock:
        assert next(iter(store._page_display_cache.values()))["display_list"] is display_list
    assert client.get(
        f"{base}/page-tile",
        params={"side": "left", "page": 1, "level": 0, "x": 999, "y": 0},
    ).status_code == 400
    assert client.get(
        f"{base}/page-tile",
        params={"side": "left", "page": 1, "level": 7, "x": 0, "y": 0},
    ).status_code == 422


def test_page_tiles_render_concurrently_from_one_display_list(tmp_path, monkeypatch):
    """Пачка видимых тайлов не разбирает одну PDF-страницу несколько раз."""
    client, session_id, pair_id = _viewer_pair(tmp_path, monkeypatch)
    url = f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-tile"
    coordinates = [(0, 0), (1, 0), (0, 1), (1, 1)]

    def load_tile(position):
        x, y = position
        return client.get(
            url,
            params={"side": "left", "page": 1, "level": 2, "x": x, "y": y},
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(load_tile, coordinates))

    assert all(response.status_code == 200 for response in responses)
    assert all(response.content.startswith(b"\x89PNG") for response in responses)
    pdf_path = str(tmp_path / "object" / "comparison" / "stage_1" / "project.pdf")
    with store._page_display_cache_lock:
        matching = [
            entry for key, entry in store._page_display_cache.items() if key.startswith(pdf_path + "|")
        ]
        assert len(matching) == 1
        assert matching[0]["users"] == 0
