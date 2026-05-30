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
    assert st["pdone"] == {"status": "done", "changes_count": 2, "strategy": None, "via_fallback": False}
    assert st["ptoolarge"]["status"] == "too_large"
    assert st["ptoolarge"]["changes_count"] == 0
    # fallback-результат сохраняет признак стратегии и via_fallback.
    assert st["pfb"]["status"] == "done"
    assert st["pfb"]["via_fallback"] is True
    assert st["pfb"]["strategy"] == "evidence_first_s2_fallback"
    assert st["pfb"]["changes_count"] == 1


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
