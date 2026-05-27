"""Тесты для «сохранённой конфигурации» Stage Comparison.

Покрытие:
  1. service: load/save/clear, atomic write через temp+rename;
  2. service: пустые пути → ValueError;
  3. service: битый JSON / отсутствие файла → load возвращает None;
  4. endpoint GET /saved-config — saved=false когда конфига нет;
  5. endpoint PUT /saved-config — сохраняет, возвращает saved=true + данные;
  6. endpoint PUT /saved-config — пустые пути → 400;
  7. endpoint PUT /saved-config — allowlist enforcement (403 на пути вне root'а);
  8. endpoint DELETE /saved-config — удаляет файл, повторный GET возвращает saved=false.

Никаких внешних API. Никаких реальных HTTP. Файлы — в tmp_path.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def _saved_config_tmp(tmp_path, monkeypatch):
    """Изолировать saved_config файл во временной директории."""
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    yield cfg_path


@pytest.fixture
def _allowlist_root(tmp_path, monkeypatch):
    """Разрешённый root для allowlist (для endpoint-теста)."""
    root = tmp_path / "allowlisted"
    root.mkdir()
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(root))
    return root


# ─── service: load / save / clear ─────────────────────────────────────────


def test_load_returns_none_when_file_missing(_saved_config_tmp):
    from backend.app.services.stage_comparison import saved_config as sc
    assert sc.load_saved_config() is None


def test_save_then_load_roundtrip(_saved_config_tmp):
    from backend.app.services.stage_comparison import saved_config as sc
    saved = sc.save_saved_config(
        stage_a_path="/tmp/stage_1",
        stage_b_path="/tmp/stage_2",
        object_label="Test Object",
        stage_a_label="stage_1",
        stage_b_label="stage_2",
        note="canonical",
    )
    assert saved["stage_a_path"] == "/tmp/stage_1"
    assert saved["stage_b_path"] == "/tmp/stage_2"
    assert saved["object_label"] == "Test Object"
    assert saved["saved_at"]  # ISO timestamp set
    assert saved["note"] == "canonical"

    loaded = sc.load_saved_config()
    assert loaded is not None
    assert loaded["stage_a_path"] == "/tmp/stage_1"
    assert loaded["stage_b_path"] == "/tmp/stage_2"


def test_save_uses_atomic_write_temp_rename(_saved_config_tmp, monkeypatch):
    """save должен писать через temp+rename, иначе при сбое получим
    битый файл. Тест: посреди write_text fail → исходного файла нет."""
    from backend.app.services.stage_comparison import saved_config as sc

    # Cначала сохраним валидный config
    sc.save_saved_config(stage_a_path="/a", stage_b_path="/b")
    assert _saved_config_tmp.exists()
    original_bytes = _saved_config_tmp.read_bytes()

    # Теперь имитируем сбой в tmp.replace через monkeypatch Path.replace
    original_replace = Path.replace
    raised = {"v": False}

    def boom_replace(self, *args, **kwargs):
        if str(self).endswith(".tmp"):
            raised["v"] = True
            raise OSError("simulated rename fail")
        return original_replace(self, *args, **kwargs)

    monkeypatch.setattr(Path, "replace", boom_replace)
    with pytest.raises(OSError):
        sc.save_saved_config(stage_a_path="/x", stage_b_path="/y")
    assert raised["v"]
    # Исходный файл цел (не повреждён частичной записью)
    assert _saved_config_tmp.read_bytes() == original_bytes


def test_save_empty_paths_raises_value_error(_saved_config_tmp):
    from backend.app.services.stage_comparison import saved_config as sc
    with pytest.raises(ValueError):
        sc.save_saved_config(stage_a_path="", stage_b_path="/b")
    with pytest.raises(ValueError):
        sc.save_saved_config(stage_a_path="/a", stage_b_path="   ")


def test_load_returns_none_on_broken_json(_saved_config_tmp):
    from backend.app.services.stage_comparison import saved_config as sc
    _saved_config_tmp.parent.mkdir(parents=True, exist_ok=True)
    _saved_config_tmp.write_text("{not: valid json", encoding="utf-8")
    assert sc.load_saved_config() is None


def test_load_returns_none_when_required_fields_empty(_saved_config_tmp):
    """Файл существует, но stage_a_path/stage_b_path пустые — config не валиден."""
    from backend.app.services.stage_comparison import saved_config as sc
    _saved_config_tmp.parent.mkdir(parents=True, exist_ok=True)
    _saved_config_tmp.write_text(json.dumps({"stage_a_path": "", "stage_b_path": ""}), encoding="utf-8")
    assert sc.load_saved_config() is None


def test_clear_removes_file(_saved_config_tmp):
    from backend.app.services.stage_comparison import saved_config as sc
    sc.save_saved_config(stage_a_path="/a", stage_b_path="/b")
    assert _saved_config_tmp.exists()
    assert sc.clear_saved_config() is True
    assert not _saved_config_tmp.exists()
    # Повторный clear — no-op
    assert sc.clear_saved_config() is False


# ─── HTTP endpoints ───────────────────────────────────────────────────────


def _build_app(_saved_config_tmp, _allowlist_root):
    from fastapi import FastAPI
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return app


def test_get_saved_config_returns_unsaved_when_missing(_saved_config_tmp):
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    r = client.get("/api/stage-comparison/saved-config")
    assert r.status_code == 200
    assert r.json() == {"saved": False}


def test_put_saved_config_saves_and_get_returns_it(_saved_config_tmp, _allowlist_root):
    from fastapi.testclient import TestClient
    app = _build_app(_saved_config_tmp, _allowlist_root)
    client = TestClient(app)

    sa = _allowlist_root / "stage_1"
    sb = _allowlist_root / "stage_2"
    sa.mkdir(); sb.mkdir()

    r = client.put("/api/stage-comparison/saved-config", json={
        "stage_a_path": str(sa),
        "stage_b_path": str(sb),
        "object_label": "Test",
        "stage_a_label": "stage_1",
        "stage_b_label": "stage_2",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["saved"] is True
    assert body["stage_a_path"] == str(sa)
    assert body["stage_b_path"] == str(sb)
    assert body["object_label"] == "Test"

    r2 = client.get("/api/stage-comparison/saved-config")
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["saved"] is True
    assert body2["stage_a_path"] == str(sa)


def test_put_saved_config_empty_paths_returns_400(_saved_config_tmp, _allowlist_root):
    from fastapi.testclient import TestClient
    app = _build_app(_saved_config_tmp, _allowlist_root)
    client = TestClient(app)
    # Pydantic пропускает строки, но service raise ValueError → 400
    r = client.put("/api/stage-comparison/saved-config", json={
        "stage_a_path": "   ",
        "stage_b_path": str(_allowlist_root / "stage_2"),
    })
    # allowlist отбивается ДО ValueError, потому что "   " не in allowlist root
    # → 403; либо если allowlist разрешит — то 400.
    assert r.status_code in (400, 403)


def test_put_saved_config_blocked_by_allowlist(_saved_config_tmp, _allowlist_root):
    """Путь вне разрешённого root → 403."""
    from fastapi.testclient import TestClient
    app = _build_app(_saved_config_tmp, _allowlist_root)
    client = TestClient(app)
    r = client.put("/api/stage-comparison/saved-config", json={
        "stage_a_path": "/etc",  # вне allowlist
        "stage_b_path": str(_allowlist_root / "stage_2"),
    })
    assert r.status_code == 403


def test_delete_saved_config_removes_it(_saved_config_tmp, _allowlist_root):
    from fastapi.testclient import TestClient
    app = _build_app(_saved_config_tmp, _allowlist_root)
    client = TestClient(app)

    sa = _allowlist_root / "stage_1"
    sb = _allowlist_root / "stage_2"
    sa.mkdir(); sb.mkdir()
    client.put("/api/stage-comparison/saved-config", json={
        "stage_a_path": str(sa),
        "stage_b_path": str(sb),
    })
    r = client.delete("/api/stage-comparison/saved-config")
    assert r.status_code == 200
    assert r.json()["deleted"] is True

    r2 = client.get("/api/stage-comparison/saved-config")
    assert r2.json() == {"saved": False}


# ─── Pair order (drag-and-drop reorder) ───────────────────────────────────


def _setup_session_with_pairs(tmp_path, monkeypatch, pair_ids: list[str]) -> str:
    """Создать минимальную сессию с указанными pair_id'ами в pair_order."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_root"))
    from backend.app.services.stage_comparison import store, paths as paths_mod
    # Кладём session.json вручную
    sid = "sess_reorder_test"
    sdir = paths_mod.session_dir(sid)
    sess_json = paths_mod.session_json_path(sid)
    import json as _json
    sess_json.write_text(_json.dumps({
        "id": sid,
        "created_at": "2026-05-26T00:00:00Z",
        "stage_a_path": str(tmp_path / "stage_a"),
        "stage_b_path": str(tmp_path / "stage_b"),
        "pair_order": list(pair_ids),
        "warnings": [],
    }), encoding="utf-8")
    # Минимальные pair.json для каждого pid
    for pid in pair_ids:
        pdir = paths_mod.pair_dir(sid, pid)
        (pdir / "pair.json").write_text(_json.dumps({
            "id": pid, "status": "matched", "left": {}, "right": {},
        }), encoding="utf-8")
    return sid


def test_set_pair_order_persists_new_order(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import store, paths as paths_mod
    sid = _setup_session_with_pairs(tmp_path, monkeypatch, ["p1", "p2", "p3", "p4"])

    new = store.set_pair_order(sid, ["p3", "p1", "p4", "p2"])
    assert new == ["p3", "p1", "p4", "p2"]

    import json as _json
    saved = _json.loads(paths_mod.session_json_path(sid).read_text())
    assert saved["pair_order"] == ["p3", "p1", "p4", "p2"]


def test_set_pair_order_ignores_unknown_ids(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import store
    sid = _setup_session_with_pairs(tmp_path, monkeypatch, ["p1", "p2", "p3"])

    new = store.set_pair_order(sid, ["pZZZ", "p2", "p1", "phantom"])
    # pZZZ и phantom не существуют — отброшены; p3 не упомянут — в конец
    assert new == ["p2", "p1", "p3"]


def test_set_pair_order_appends_missing_to_end(tmp_path, monkeypatch):
    """UI может передать неполный список (например после фильтра).
    Потерянные пары должны добавиться в конец, а не пропасть."""
    from backend.app.services.stage_comparison import store
    sid = _setup_session_with_pairs(tmp_path, monkeypatch, ["p1", "p2", "p3", "p4"])

    new = store.set_pair_order(sid, ["p4", "p2"])
    assert new == ["p4", "p2", "p1", "p3"]


def test_set_pair_order_noop_when_unchanged(tmp_path, monkeypatch):
    """Если новый порядок совпадает с текущим — не должно быть лишней записи."""
    from backend.app.services.stage_comparison import store, paths as paths_mod
    sid = _setup_session_with_pairs(tmp_path, monkeypatch, ["p1", "p2", "p3"])

    p = paths_mod.session_json_path(sid)
    before_mtime = p.stat().st_mtime_ns
    # Микропауза не нужна — set_pair_order проверит equality и не вызовет write
    new = store.set_pair_order(sid, ["p1", "p2", "p3"])
    assert new == ["p1", "p2", "p3"]
    after_mtime = p.stat().st_mtime_ns
    assert before_mtime == after_mtime, "session.json was rewritten despite no change"


def test_set_pair_order_session_not_found_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_root"))
    from backend.app.services.stage_comparison import store
    with pytest.raises(KeyError):
        store.set_pair_order("no_such_session", ["p1"])


def test_set_pair_order_endpoint_returns_normalized_order(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    sid = _setup_session_with_pairs(tmp_path, monkeypatch, ["p1", "p2", "p3"])

    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)

    r = client.put(f"/api/stage-comparison/sessions/{sid}/pair-order",
                   json={"pair_ids": ["p3", "p1"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    # p2 не упомянут — добавлен в конец
    assert body["pair_order"] == ["p3", "p1", "p2"]


def test_set_pair_order_endpoint_404_on_unknown_session(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_root"))
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    r = client.put("/api/stage-comparison/sessions/missing/pair-order",
                   json={"pair_ids": ["p1"]})
    assert r.status_code == 404


# ─── Canonical configuration (v2: session-aware, one per object) ──────────


def _setup_full_session(tmp_path, monkeypatch, sid: str, pair_specs: list[dict]) -> str:
    """Создать сессию с pair.json'ами под allowlist root.

    pair_specs = [{"id": "p1", "status": "matched", "analysis_mode": "block_links",
                   "left_filename": "a.pdf", "right_filename": "b.pdf"}, ...]
    """
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_root"))
    allow_root = tmp_path / "allowlisted"
    allow_root.mkdir(exist_ok=True)
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(allow_root))
    stage_a = allow_root / "stage_1"
    stage_b = allow_root / "stage_2"
    stage_a.mkdir(exist_ok=True)
    stage_b.mkdir(exist_ok=True)
    from backend.app.services.stage_comparison import paths as paths_mod
    import json as _json
    sess_json = paths_mod.session_json_path(sid)
    sess_json.write_text(_json.dumps({
        "id": sid,
        "created_at": "2026-05-27T00:00:00Z",
        "stage_a_path": str(stage_a),
        "stage_b_path": str(stage_b),
        "pair_order": [p["id"] for p in pair_specs],
        "warnings": [],
    }), encoding="utf-8")
    for spec in pair_specs:
        pid = spec["id"]
        pdir = paths_mod.pair_dir(sid, pid)
        pair_payload = {
            "id": pid,
            "status": spec.get("status", "matched"),
            "match_score": spec.get("match_score", 1.0),
            "left": {"filename": spec.get("left_filename", "a.pdf"),
                     "pdf_path": str(stage_a / spec.get("left_filename", "a.pdf"))},
            "right": {"filename": spec.get("right_filename", "b.pdf"),
                      "pdf_path": str(stage_b / spec.get("right_filename", "b.pdf"))},
        }
        if spec.get("analysis_mode"):
            pair_payload["analysis_mode"] = spec["analysis_mode"]
        (pdir / "pair.json").write_text(_json.dumps(pair_payload), encoding="utf-8")
    return sid


def _build_app_full():
    from fastapi import FastAPI
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return app


def test_canonical_config_missing_returns_unsaved(tmp_path, monkeypatch):
    """GET /canonical-config когда конфига нет → saved=false."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    app = _build_app_full()
    r = TestClient(app).get("/api/stage-comparison/canonical-config")
    assert r.status_code == 200
    assert r.json() == {"saved": False}


def test_save_canonical_persists_pairs_and_session_id(tmp_path, monkeypatch):
    """POST /sessions/{sid}/save-canonical сохраняет pairs/режимы/session_id."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    sid = _setup_full_session(tmp_path, monkeypatch, "sess_canon_save", [
        {"id": "p1", "analysis_mode": "block_links",
         "left_filename": "a1.pdf", "right_filename": "b1.pdf"},
        {"id": "p2", "analysis_mode": "concept_no_block_links",
         "left_filename": "a2.pdf", "right_filename": "b2.pdf"},
    ])
    app = _build_app_full()
    client = TestClient(app)
    r = client.post(
        f"/api/stage-comparison/sessions/{sid}/save-canonical",
        json={"object_label": "Test Object", "stage_a_label": "stage_1", "stage_b_label": "stage_2"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["saved"] is True
    assert body["canonical_session_id"] == sid
    assert body["object_label"] == "Test Object"
    assert body["config_version"] == 2
    assert body["config_hash"]  # sha256 hash present
    pairs = body["pairs"]
    assert len(pairs) == 2
    pair_ids = {p["pair_id"] for p in pairs}
    assert pair_ids == {"p1", "p2"}
    p1 = next(p for p in pairs if p["pair_id"] == "p1")
    assert p1["analysis_mode"] == "block_links"
    p2 = next(p for p in pairs if p["pair_id"] == "p2")
    assert p2["analysis_mode"] == "concept_no_block_links"


def test_save_canonical_overwrites_previous(tmp_path, monkeypatch):
    """Повторный save перезаписывает предыдущий канон (не создаёт историю)."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    sid1 = _setup_full_session(tmp_path, monkeypatch, "sess_old", [
        {"id": "p1", "left_filename": "a.pdf", "right_filename": "b.pdf"},
    ])
    app = _build_app_full()
    client = TestClient(app)
    client.post(f"/api/stage-comparison/sessions/{sid1}/save-canonical", json={})
    sid2 = _setup_full_session(tmp_path, monkeypatch, "sess_new", [
        {"id": "x1", "left_filename": "c.pdf", "right_filename": "d.pdf"},
        {"id": "x2", "left_filename": "e.pdf", "right_filename": "f.pdf"},
    ])
    r = client.post(f"/api/stage-comparison/sessions/{sid2}/save-canonical", json={})
    assert r.status_code == 200
    g = client.get("/api/stage-comparison/canonical-config")
    body = g.json()
    assert body["canonical_session_id"] == sid2
    assert {p["pair_id"] for p in body["pairs"]} == {"x1", "x2"}


def test_save_canonical_404_on_missing_session(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_root"))
    allow_root = tmp_path / "allowlisted"
    allow_root.mkdir()
    monkeypatch.setenv("AUDIT_STAGE_COMPARISON_ROOTS", str(allow_root))
    app = _build_app_full()
    r = TestClient(app).post(
        "/api/stage-comparison/sessions/missing_sid/save-canonical", json={},
    )
    assert r.status_code == 404


def test_canonical_open_returns_session_when_available(tmp_path, monkeypatch):
    """GET /canonical-config/open подгружает полный объект сессии."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    sid = _setup_full_session(tmp_path, monkeypatch, "sess_open", [
        {"id": "p1", "analysis_mode": "block_links"},
    ])
    app = _build_app_full()
    client = TestClient(app)
    client.post(f"/api/stage-comparison/sessions/{sid}/save-canonical", json={})

    r = client.get("/api/stage-comparison/canonical-config/open")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["saved"] is True
    assert body["canonical_session_id"] == sid
    assert body["canonical_session_available"] is True
    assert body["session"]["id"] == sid
    assert {p["id"] for p in body["session"]["pairs"]} == {"p1"}
    # config_hash_current совпадает с saved → не stale
    assert body["config_stale"] is False


def test_canonical_open_marks_session_unavailable_when_missing(tmp_path, monkeypatch):
    """canonical_session_id есть в конфиге, но сессии нет на диске → available=false."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    sid = _setup_full_session(tmp_path, monkeypatch, "sess_will_die", [
        {"id": "p1"},
    ])
    app = _build_app_full()
    client = TestClient(app)
    client.post(f"/api/stage-comparison/sessions/{sid}/save-canonical", json={})

    # Удалим session.json — сессия становится недоступна
    from backend.app.services.stage_comparison import paths as paths_mod
    paths_mod.session_json_path(sid).unlink()

    r = client.get("/api/stage-comparison/canonical-config/open")
    body = r.json()
    assert body["saved"] is True
    assert body["canonical_session_available"] is False
    assert "session" not in body or body.get("session") is None


def test_canonical_config_stale_when_pairs_changed(tmp_path, monkeypatch):
    """Если конфигурацию сохранили, а потом сессия изменилась — config_stale=true."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    sid = _setup_full_session(tmp_path, monkeypatch, "sess_stale", [
        {"id": "p1", "analysis_mode": "block_links"},
    ])
    app = _build_app_full()
    client = TestClient(app)
    client.post(f"/api/stage-comparison/sessions/{sid}/save-canonical", json={})

    # Меняем pair.json — config_hash сессии расходится с сохранённым
    from backend.app.services.stage_comparison import paths as paths_mod
    import json as _json
    pair_json = paths_mod.pair_json_path(sid, "p1")
    pdata = _json.loads(pair_json.read_text())
    pdata["analysis_mode"] = "concept_no_block_links"
    pair_json.write_text(_json.dumps(pdata), encoding="utf-8")

    r = client.get("/api/stage-comparison/canonical-config/open")
    body = r.json()
    assert body["saved"] is True
    assert body["canonical_session_id"] == sid
    assert body["config_stale"] is True
    assert body["config_hash_saved"] != body["config_hash_current"]


def test_save_canonical_does_not_touch_old_sessions(tmp_path, monkeypatch):
    """Старые sessions остаются на диске — мы их не удаляем."""
    from fastapi.testclient import TestClient
    cfg_path = tmp_path / "saved_config.json"
    monkeypatch.setenv("STAGE_COMPARISON_SAVED_CONFIG_PATH", str(cfg_path))
    sid_old = _setup_full_session(tmp_path, monkeypatch, "old_sess", [{"id": "po1"}])
    sid_new = _setup_full_session(tmp_path, monkeypatch, "new_sess", [{"id": "pn1"}])
    app = _build_app_full()
    client = TestClient(app)
    client.post(f"/api/stage-comparison/sessions/{sid_new}/save-canonical", json={})

    # session.json старой сессии — на месте
    from backend.app.services.stage_comparison import paths as paths_mod
    assert paths_mod.session_json_path(sid_old).exists()
    assert paths_mod.session_json_path(sid_new).exists()


def test_canonical_config_hash_stable_for_same_pairs(tmp_path, monkeypatch):
    """Идемпотентность: одна и та же конфигурация → один и тот же config_hash."""
    from backend.app.services.stage_comparison import saved_config as sc
    pairs_a = [
        {"pair_id": "p1", "left_filename": "a.pdf", "right_filename": "b.pdf",
         "disabled": False, "analysis_mode": "block_links", "order": 1,
         "manual_links_count": 0, "status": "matched"},
    ]
    pairs_b = [dict(p) for p in pairs_a]  # shallow copy
    assert sc._compute_config_hash(pairs_a) == sc._compute_config_hash(pairs_b)
