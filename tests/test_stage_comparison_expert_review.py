"""Тесты pair-scoped экспертных решений в «Сравнение стадий».

Ключевой инвариант: решение по расхождению скоупится парой
(`<pair_id>::<raw_id>`), поэтому одинаковые штамповые id (chg_customer,
chg_stamp_org …) в разных парах НЕ делят один вердикт. Плюс — миграция
v1→v2 голых raw-ключей.
"""
import json

import pytest


@pytest.fixture
def er_module(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    # Сбросить закешированные пути модулей не нужно — paths читает env лениво.
    from backend.app.services.stage_comparison import expert_review as er
    return er


def _write_raw(er, sid, decisions):
    path = er.paths_mod.expert_review_path(sid)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(decisions, ensure_ascii=False), encoding="utf-8")


def test_make_key_composite(er_module):
    assert er_module.make_key("p1", "chg_x") == "p1::chg_x"


def test_decision_scoped_per_pair(er_module):
    sid = "sess_scope"
    p1, p2 = "pA", "pB"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key(p1, "chg_customer"), "decision": "rejected"},
    ])
    data = er_module.load(sid)
    # Решение видно только под своей парой, не под чужой.
    assert er_module.make_key(p1, "chg_customer") in data["decisions"]
    assert er_module.make_key(p2, "chg_customer") not in data["decisions"]


def test_per_pair_status_uses_composite_key(er_module, monkeypatch):
    sid = "sess_pp"
    p1, p2 = "pA", "pB"
    # Один и тот же raw id присутствует в обеих парах (штамповое изменение).
    changes = [(p1, "chg_customer"), (p2, "chg_customer"), (p1, "chg_floors")]
    monkeypatch.setattr(er_module, "_iter_session_pair_changes", lambda _sid: list(changes))

    # Эксперт разметил chg_customer ТОЛЬКО в паре p1.
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key(p1, "chg_customer"), "decision": "rejected"},
    ])
    out = er_module.get_with_summary(sid, include_pairs=True)
    pp = out["per_pair"]
    # p1: 2 расхождения, 1 размечено. p2: 1 расхождение, 0 размечено
    # (решение из p1 НЕ протекает).
    assert pp[p1]["decided"] == 1
    assert pp[p2]["decided"] == 0


def test_migration_unique_id_binds_to_its_pair(er_module, monkeypatch):
    sid = "sess_mig_unique"
    # legacy v1 файл: голый raw id без пары.
    _write_raw(er_module, sid, {
        "version": 1,
        "updated_at": "2026-01-01T00:00:00Z",
        "decisions": {
            "chg_floors": {"decision": "accepted", "rejection_reason": ""},
        },
    })
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [("pA", "chg_floors")])

    data = er_module.load(sid)
    assert data["version"] == 2
    assert data.get("migrated_pair_scoped") is True
    assert "chg_floors" not in data["decisions"]          # голого ключа больше нет
    assert er_module.make_key("pA", "chg_floors") in data["decisions"]


def test_migration_shared_id_is_dropped(er_module, monkeypatch):
    sid = "sess_mig_shared"
    _write_raw(er_module, sid, {
        "version": 1,
        "decisions": {
            "chg_customer": {"decision": "rejected", "rejection_reason": ""},
            "chg_floors": {"decision": "accepted", "rejection_reason": ""},
        },
    })
    # chg_customer — в двух парах (нельзя достоверно привязать) → drop.
    changes = [("pA", "chg_customer"), ("pB", "chg_customer"), ("pA", "chg_floors")]
    monkeypatch.setattr(er_module, "_iter_session_pair_changes", lambda _sid: list(changes))

    data = er_module.load(sid)
    keys = set(data["decisions"])
    assert keys == {er_module.make_key("pA", "chg_floors")}


def test_migration_idempotent(er_module, monkeypatch):
    sid = "sess_mig_idem"
    _write_raw(er_module, sid, {
        "version": 1,
        "decisions": {"chg_floors": {"decision": "accepted", "rejection_reason": ""}},
    })
    calls = {"n": 0}

    def _counting(_sid):
        calls["n"] += 1
        return [("pA", "chg_floors")]

    monkeypatch.setattr(er_module, "_iter_session_pair_changes", _counting)

    er_module.load(sid)
    first = calls["n"]
    er_module.load(sid)        # второй раз — уже всё составное, миграция не нужна
    assert calls["n"] == first  # источник changes не перечитывался повторно


# ── Prune orphan-решений (исчезнувшие raw_id после регенерации сравнения) ──


def test_prune_orphans_removes_stale_raw_ids(er_module, monkeypatch):
    sid = "sess_prune"
    pid = "pA"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key(pid, "chg_keep"), "decision": "accepted"},
        {"item_id": er_module.make_key(pid, "chg_gone"), "decision": "rejected"},
    ])
    # Текущее сравнение содержит только chg_keep (chg_gone исчез при регенерации).
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [(pid, "chg_keep")])
    report = er_module.prune_orphans(sid)
    assert report["removed_count"] == 1
    assert er_module.make_key(pid, "chg_gone") in report["removed_keys"]
    data = er_module.load(sid)
    assert er_module.make_key(pid, "chg_keep") in data["decisions"]
    assert er_module.make_key(pid, "chg_gone") not in data["decisions"]


def test_prune_keeps_decisions_of_unenumerated_pair(er_module, monkeypatch):
    sid = "sess_prune_keep"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key("pA", "chg_x"), "decision": "accepted"},
        {"item_id": er_module.make_key("pB", "chg_y"), "decision": "rejected"},
    ])
    # Перечислима только pA; pB не done / перезапускается → её решения не трогаем.
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [("pA", "chg_x")])
    er_module.prune_orphans(sid)
    data = er_module.load(sid)
    assert er_module.make_key("pB", "chg_y") in data["decisions"]
    assert er_module.make_key("pA", "chg_x") in data["decisions"]


def test_prune_dry_run_does_not_write(er_module, monkeypatch):
    sid = "sess_prune_dry"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key("pA", "chg_keep"), "decision": "accepted"},
        {"item_id": er_module.make_key("pA", "chg_gone"), "decision": "accepted"},
    ])
    # Частичное совпадение: chg_keep валиден, chg_gone — orphan.
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [("pA", "chg_keep")])
    report = er_module.prune_orphans(sid, dry_run=True)
    assert report["removed_count"] == 1
    assert report["dry_run"] is True
    data = er_module.load(sid)
    assert er_module.make_key("pA", "chg_gone") in data["decisions"]  # не удалён


def test_prune_zero_overlap_keeps_all(er_module, monkeypatch):
    """Guard инцидента 2026-06-04: если НИ ОДИН сохранённый id пары не совпал с
    текущими changes (пара регенерируется / id переgenerированы) — НЕ стираем
    пару целиком."""
    sid = "sess_prune_zero_overlap"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key("pA", "chg_old1"), "decision": "accepted"},
        {"item_id": er_module.make_key("pA", "chg_old2"), "decision": "rejected"},
    ])
    # Текущие changes пары — совсем другие id (полная регенерация / race).
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [("pA", "chg_new1"), ("pA", "chg_new2")])
    report = er_module.prune_orphans(sid)
    assert report["removed_count"] == 0          # ничего не стёрто
    data = er_module.load(sid)
    assert er_module.make_key("pA", "chg_old1") in data["decisions"]
    assert er_module.make_key("pA", "chg_old2") in data["decisions"]


def test_get_with_summary_is_non_destructive(er_module, monkeypatch):
    """Чтение не мутирует хранилище — orphan'ы остаются на диске (их убирает
    только явный prune_orphans / скоупинг на фронте)."""
    sid = "sess_summary_nondestructive"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key("pA", "chg_keep"), "decision": "accepted"},
        {"item_id": er_module.make_key("pA", "chg_gone"), "decision": "accepted"},
    ])
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [("pA", "chg_keep")])
    out = er_module.get_with_summary(sid)
    assert er_module.make_key("pA", "chg_gone") in out["decisions"]
    data = er_module.load(sid)
    assert er_module.make_key("pA", "chg_gone") in data["decisions"]


def test_prune_orphans_endpoint_dry_run_then_real(er_module, monkeypatch):
    """Endpoint POST .../expert-review/prune-orphans: dry_run считает без записи,
    реальный прогон удаляет orphan. (router-level, проверяет регистрацию роута.)"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod

    sid = "sess_prune_endpoint"
    er_module.apply_batch(sid, decisions=[
        {"item_id": er_module.make_key("pA", "chg_keep"), "decision": "accepted"},
        {"item_id": er_module.make_key("pA", "chg_gone"), "decision": "accepted"},
    ])
    monkeypatch.setattr(er_module, "_iter_session_pair_changes",
                        lambda _sid: [("pA", "chg_keep")])
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    base = f"/api/stage-comparison/sessions/{sid}/expert-review/prune-orphans"

    r = client.post(base + "?dry_run=true")
    assert r.status_code == 200, r.text
    assert r.json()["dry_run"] is True
    assert r.json()["removed_count"] == 1
    assert er_module.make_key("pA", "chg_gone") in er_module.load(sid)["decisions"]

    r2 = client.post(base)
    assert r2.status_code == 200, r2.text
    assert r2.json()["removed_count"] == 1
    assert er_module.make_key("pA", "chg_gone") not in er_module.load(sid)["decisions"]
