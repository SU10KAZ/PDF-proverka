"""Service-level тесты движка V2-ведомости (без router/frontend).

Покрывают чистый engine-слой `v2_review`:
* `make_v2_id` — стабильный id изменения;
* `classify_impact` — инженерное остаётся, админ/оформление/косметика
  исключаются, high/стоимость → ручная проверка (не авто-скрытие);
* `build_pair_v2_changes` — исключение по умолчанию + `include_excluded`,
  запись `v2_excluded_changes.json` (derived), без мутации `comparison_result`;
* `derive_quality_label` — disputed/requires_human_review/evidence_verified.

Эти тесты НЕ поднимают FastAPI router и не читают frontend — только сервис.
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from backend.app.services.stage_comparison import v2_review


# ─── tmp comparison root + минимальная синтетическая сессия ──────────────────

@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_v2_engine_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


def _paths():
    from backend.app.services.stage_comparison import paths as paths_mod
    return paths_mod


def _write_session(session_id: str, pair_ids: list[str]):
    p = _paths()
    p.session_json_path(session_id).write_text(json.dumps({
        "id": session_id, "pair_order": list(pair_ids), "warnings": [],
        "created_at": datetime(2026, 6, 6).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }, ensure_ascii=False), encoding="utf-8")


def _write_pair(session_id: str, pair_id: str):
    p = _paths()
    p.pair_json_path(session_id, pair_id).write_text(json.dumps({
        "id": pair_id, "status": "matched",
        "left": {"filename": "old.pdf", "pdf_path": "/dev/null/old.pdf"},
        "right": {"filename": "new.pdf", "pdf_path": "/dev/null/new.pdf"},
    }, ensure_ascii=False), encoding="utf-8")


def _write_comparison_result(session_id: str, pair_id: str, changes: list[dict]):
    p = _paths().enriched_comparison_result_path(session_id, pair_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": 1, "status": "done", "changes": changes},
                            ensure_ascii=False), encoding="utf-8")
    return p


def _change(cid, *, title, sev="medium", old="", new="", rhr=False,
            cost="unknown", **extra):
    base = {
        "id": cid, "source": "text", "type": "changed", "category": "general",
        "severity": sev, "title": title, "summary": f"summary {title}",
        "old_value": old, "new_value": new, "construction_impact": "влияние",
        "cost_impact": cost, "requires_human_review": rhr, "confidence": 0.9,
        "evidence_left": {"quote": old or "L", "section": "X", "approx_location": "стр. 1"},
        "evidence_right": {"quote": new or "R", "section": "X", "approx_location": "стр. 1"},
    }
    base.update(extra)
    return base


# Три инженерных + три исключаемых изменения (как в production-приёмке).
def _mixed_changes() -> list[dict]:
    return [
        _change("chg_eng_beton", title="Класс бетона B25 на B30", sev="medium", old="B25", new="B30"),
        _change("chg_eng_rebar", title="Армирование плиты пересмотрено", sev="low", old="A400", new="A500"),
        _change("chg_eng_thick", title="Толщина монолитной плиты", sev="low", old="200мм", new="250мм"),
        _change("chg_admin_org", title="Сменилась проектная организация и ГИП, подпись", sev="low", old="o1", new="o2"),
        _change("chg_doc_shifr", title="Изменён шифр, номер листа и дата выпуска", sev="low", old="d1", new="d2"),
        _change("chg_cosm", title="Переформулировка: значение не изменилось", sev="low", old="c1", new="c2"),
    ]


def _session_mixed():
    sid, pid = "sess_eng", "pENG"
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    _write_comparison_result(sid, pid, _mixed_changes())
    return sid, pid


# ─── make_v2_id ──────────────────────────────────────────────────────────────

def test_make_v2_id_stable_and_prefixed():
    item = _change("chg_a", title="T", old="1", new="2")
    a = v2_review.make_v2_id("pX", item)
    b = v2_review.make_v2_id("pX", dict(item))
    assert a == b                      # детерминирован
    assert a.startswith("v2_")


def test_make_v2_id_distinguishes_content_and_scopes_by_pair():
    i1 = _change("chg_a", title="T1", old="1", new="2")
    i2 = _change("chg_b", title="T2", old="3", new="4")
    assert v2_review.make_v2_id("pX", i1) != v2_review.make_v2_id("pX", i2)
    # один и тот же chg_… id в разных парах → разные v2-id (скоуп пары)
    assert v2_review.make_v2_id("pX", i1) != v2_review.make_v2_id("pY", i1)


# ─── classify_impact ─────────────────────────────────────────────────────────

def test_classify_impact_engineering_is_kept():
    cls, reason = v2_review.classify_impact(
        _change("c", title="Класс бетона B25 на B30", old="B25", new="B30"))
    assert reason is None
    assert cls not in v2_review.EXCLUDED_IMPACT_CLASSES


@pytest.mark.parametrize("title,expected", [
    ("Сменилась проектная организация и ГИП, подпись", "admin_only"),
    ("Изменён шифр, номер листа и дата выпуска", "documentation_only"),
    ("Переформулировка: значение не изменилось", "cosmetic_or_noise"),
])
def test_classify_impact_excludes_admin_doc_cosmetic(title, expected):
    cls, reason = v2_review.classify_impact(_change("c", title=title, sev="low"))
    assert cls == expected
    assert cls in v2_review.EXCLUDED_IMPACT_CLASSES
    assert reason                       # причина исключения заполнена


def test_classify_impact_cost_impact_keeps_on_manual():
    # админ/документационный текст, но влияние на стоимость (possible/likely) →
    # НЕ прячем автоматически, оставляем на ручную проверку.
    cls, reason = v2_review.classify_impact(
        _change("c", title="Изменён шифр, номер листа и дата выпуска",
                sev="low", cost="likely"))
    assert cls not in v2_review.EXCLUDED_IMPACT_CLASSES
    assert reason is None


# ─── build_pair_v2_changes: исключение + include_excluded + derived-файл ──────

def test_build_excludes_admin_doc_cosmetic_by_default():
    sid, pid = _session_mixed()
    res = v2_review.build_pair_v2_changes(sid, pid)
    titles = [it.get("title") for it in res["items"]]
    assert len(res["items"]) == 3                       # только инженерные
    assert all("организация" not in t and "шифр" not in t
               and "Переформулировка" not in t for t in titles)
    assert res["summary"]["excluded_total"] == 3


def test_build_include_excluded_returns_all_with_flags():
    sid, pid = _session_mixed()
    res = v2_review.build_pair_v2_changes(sid, pid, include_excluded=True)
    assert len(res["items"]) == 6
    for it in res["items"]:
        assert "impact_class" in it
        assert "excluded_from_main" in it
    excluded = [it for it in res["items"] if it.get("excluded_from_main")]
    assert len(excluded) == 3
    assert all(it["impact_class"] in v2_review.EXCLUDED_IMPACT_CLASSES for it in excluded)


def test_excluded_changes_file_written():
    sid, pid = _session_mixed()
    v2_review.build_pair_v2_changes(sid, pid)
    path = _paths().v2_excluded_changes_path(sid, pid)
    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    blob = json.dumps(data, ensure_ascii=False)
    # три исключённых изменения зафиксированы для аудита
    assert "организация" in blob and "шифр" in blob and "Переформулировка" in blob


def test_build_does_not_mutate_comparison_result():
    sid, pid = _session_mixed()
    cr_path = _paths().enriched_comparison_result_path(sid, pid)
    before = cr_path.read_text(encoding="utf-8")
    v2_review.build_pair_v2_changes(sid, pid)
    v2_review.build_pair_v2_changes(sid, pid, include_excluded=True)
    assert cr_path.read_text(encoding="utf-8") == before


# ─── derive_quality_label ────────────────────────────────────────────────────

def test_derive_quality_label_flags():
    assert v2_review.derive_quality_label({"requires_human_review": True}) == "needs_human_review"
    assert v2_review.derive_quality_label({"disputed": True}) == "questionable"
    assert v2_review.derive_quality_label({"evidence_verified": False}) == "questionable"
    # requires_human_review приоритетнее disputed
    assert v2_review.derive_quality_label(
        {"requires_human_review": True, "disputed": True}) == "needs_human_review"
