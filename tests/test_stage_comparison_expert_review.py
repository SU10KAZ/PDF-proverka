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
