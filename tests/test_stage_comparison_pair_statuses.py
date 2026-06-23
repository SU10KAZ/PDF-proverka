"""Тесты персистентных статусов сравнения по парам сессии.

`get_session_comparison_statuses` — источник истины для колонки «Сравнение»
в UI. Читает только `comparison_result.json` каждой пары и НЕ зависит от
того, какой unified-job сейчас «активен». Это чинит баг, когда одно-парный
fallback/retry-job затенял полный результат сессии и пары показывались как
«—» (не запускалось), хотя сравнение было выполнено.
"""
import json

import pytest


@pytest.fixture
def ec_module(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    from backend.app.services.stage_comparison import enriched_comparison as ec
    return ec


def _write_result(ec, sid, pid, payload):
    p = ec.paths_mod.enriched_comparison_result_path(sid, pid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_statuses_read_persistent_results(ec_module, monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    sid = "sess_cmp"
    pairs = [
        {"id": "pdone"},
        {"id": "ptoolarge"},
        {"id": "pfb"},
        {"id": "pnone"},   # без сохранённого результата
    ]
    monkeypatch.setattr(store_mod, "get_session", lambda _sid: {"id": sid, "pairs": pairs})

    _write_result(ec_module, sid, "pdone", {"status": "done", "changes": [{"id": "a"}, {"id": "b"}]})
    _write_result(ec_module, sid, "ptoolarge", {"status": "too_large", "changes": []})
    _write_result(ec_module, sid, "pfb", {
        "status": "done", "strategy": "evidence_first_s2_fallback",
        "fallback": True, "changes": [{"id": "x"}],
    })
    # pnone — файла нет

    st = ec_module.get_session_comparison_statuses(sid)

    # Пара без результата НЕ попадает в map → UI трактует как «не запускалось».
    assert set(st.keys()) == {"pdone", "ptoolarge", "pfb"}
    # core-поля (равенство заменено на проверку полей — схема дополнена mode/counts).
    assert st["pdone"]["status"] == "done"
    assert st["pdone"]["changes_count"] == 2
    assert st["pdone"]["strategy"] is None
    assert st["pdone"]["via_fallback"] is False
    assert st["pdone"]["mode"] == "normal"
    assert st["ptoolarge"]["status"] == "too_large"
    assert st["ptoolarge"]["changes_count"] == 0
    assert st["ptoolarge"]["mode"] == "fallback"  # кликабельный fallback-бейдж
    # fallback-результат сохраняет признак стратегии, via_fallback и mode.
    assert st["pfb"]["status"] == "done"
    assert st["pfb"]["via_fallback"] is True
    assert st["pfb"]["strategy"] == "evidence_first_s2_fallback"
    assert st["pfb"]["changes_count"] == 1
    assert st["pfb"]["mode"] == "fallback"


def _write_enriched_md(ec, sid, pid, side, text):
    p = ec.paths_mod.text_enrichment_md_path(sid, pid, side)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def test_too_large_without_result_is_synthesized(ec_module, monkeypatch):
    """Пара без comparison_result.json, но с готовыми enriched MD сверх лимита,
    должна получить синтетический status=too_large — чтобы UI показал
    кликабельный fallback-бейдж (раньше такие пары были «—» без кнопки).
    """
    from backend.app.services.stage_comparison import store as store_mod
    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS", "100")
    sid = "sess_big"
    pairs = [
        {"id": "pbig"},        # enriched MD сверх лимита, без результата
        {"id": "psmall"},      # enriched MD есть, но в пределах лимита, без результата
        {"id": "phalf"},       # только одна сторона enriched — не «ready»
    ]
    monkeypatch.setattr(store_mod, "get_session", lambda _sid: {"id": sid, "pairs": pairs})

    _write_enriched_md(ec_module, sid, "pbig", "left", "x" * 80)
    _write_enriched_md(ec_module, sid, "pbig", "right", "y" * 80)   # total 160 > 100
    _write_enriched_md(ec_module, sid, "psmall", "left", "x" * 20)
    _write_enriched_md(ec_module, sid, "psmall", "right", "y" * 20)  # total 40 < 100
    _write_enriched_md(ec_module, sid, "phalf", "left", "x" * 200)   # только left

    st = ec_module.get_session_comparison_statuses(sid)

    # Большая пара → too_large (кликабельный fallback в UI).
    assert st["pbig"]["status"] == "too_large"
    assert st["pbig"]["total_chars"] == 160
    assert st["pbig"]["limit_chars"] == 100
    # Маленькая (в пределах лимита) и неполная (одна сторона) → не в map → «—».
    assert "psmall" not in st
    assert "phalf" not in st


def test_statuses_empty_for_unknown_session(ec_module, monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    monkeypatch.setattr(store_mod, "get_session", lambda _sid: None)
    assert ec_module.get_session_comparison_statuses("nope") == {}


# ─── enrichment: mode / present_one_side / requires_human_review / created_at ──

def test_mode_and_counts_from_changes(ec_module, monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    sid = "sess_enr"
    monkeypatch.setattr(store_mod, "get_session",
                        lambda _sid: {"id": sid, "pairs": [{"id": "p1"}]})
    _write_result(ec_module, sid, "p1", {
        "status": "done", "updated_at": "2026-06-09T09:16:00Z",
        "changes": [
            {"type": "present_one_side", "requires_human_review": True},
            {"type": "changed", "requires_human_review": True},
            {"type": "changed"},
        ],
    })
    e = ec_module.get_session_comparison_statuses(sid)["p1"]
    assert e["status"] == "done"
    assert e["changes_count"] == 3
    assert e["present_one_side_count"] == 1
    assert e["requires_human_review_count"] == 2
    assert e["mode"] == "normal"               # обычное сравнение
    assert e["created_at"] == "2026-06-09T09:16:00Z"


def test_done_with_review_pending_is_still_compared(ec_module, monkeypatch):
    """changes>0 и «review 0/N» — пара всё равно considered compared (status=done).
    Колонка «Сравнение» не должна зависеть от экспертной проверки."""
    from backend.app.services.stage_comparison import store as store_mod
    sid = "sess_rev"
    monkeypatch.setattr(store_mod, "get_session",
                        lambda _sid: {"id": sid, "pairs": [{"id": "p1"}]})
    # comparison_result есть и done; никакого expert_review/v2 не пишем вообще.
    _write_result(ec_module, sid, "p1", {
        "status": "done", "changes": [{"type": "changed", "requires_human_review": True}] * 23,
    })
    e = ec_module.get_session_comparison_statuses(sid)["p1"]
    assert e["status"] == "done"               # сравнено, несмотря на «проверено 0/23»
    assert e["requires_human_review_count"] == 23


def test_evidence_first_strategy_implies_fallback_mode(ec_module, monkeypatch):
    """strategy=evidence_first_s2_fallback без явного fallback-флага → mode=fallback."""
    from backend.app.services.stage_comparison import store as store_mod
    sid = "sess_ef"
    monkeypatch.setattr(store_mod, "get_session",
                        lambda _sid: {"id": sid, "pairs": [{"id": "p1"}]})
    _write_result(ec_module, sid, "p1", {
        "status": "done", "strategy": "evidence_first_s2_fallback", "changes": [{"id": "x"}],
    })
    e = ec_module.get_session_comparison_statuses(sid)["p1"]
    assert e["via_fallback"] is True
    assert e["mode"] == "fallback"


def test_failed_status_preserved(ec_module, monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    sid = "sess_fail"
    monkeypatch.setattr(store_mod, "get_session",
                        lambda _sid: {"id": sid, "pairs": [{"id": "p1"}]})
    _write_result(ec_module, sid, "p1", {"status": "failed", "changes": []})
    e = ec_module.get_session_comparison_statuses(sid)["p1"]
    assert e["status"] == "failed"
    assert e["mode"] == "normal"
