"""Тесты Haiku-семантического доматчинга листов поверх штамп-матчинга.

Покрывает:
  * build_llm_match_prompt — строки листов, пропуск безымянных, cap;
  * parse_llm_match_pairs — чистый JSON / claude-обёртка / fence / фильтр
    confidence / дедуп page;
  * llm_match_sheets — мок-provider (done / error / empty side) + диагностика;
  * make_llm_match_fn → match_sheet_indexes(llm_match_fn=…): инъекция пары в
    остаток, инварианты (page ≤ 1 раза), тип llm_semantic, счётчик;
  * store.suggest_alignment_by_stamp(use_llm=True) с мок-provider.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from backend.app.services.stage_comparison import stamp_matching as sm
from backend.app.services.stage_comparison import stamp_llm_match as slm


# ─── helpers ────────────────────────────────────────────────────────────────

def _md(pages: list[tuple[int, object, object]]) -> str:
    out: list[str] = []
    for pn, sno, snm in pages:
        out.append(f"## СТРАНИЦА {pn}")
        if sno is not None:
            out.append(f"**Лист:** {sno}")
        if snm is not None:
            out.append(f"**Наименование листа:** {snm}")
        out.append(f"текст страницы {pn}")
        out.append("")
    return "\n".join(out)


@dataclass
class _FakeProviderResult:
    status: str
    raw_response: str = ""
    error: str | None = None
    duration_sec: float = 0.1
    model: str = "haiku"


class _FakeProvider:
    """Мок ClaudeCodeProvider: отдаёт заранее заданный ответ, без subprocess."""

    def __init__(self, result: _FakeProviderResult):
        self._result = result
        self.calls: list[dict] = []

    def check_availability(self):
        return True, None

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        self.calls.append({"system": system_prompt, "user": user_prompt,
                           "model": model, "timeout": timeout_sec})
        return self._result


# ─── build_llm_match_prompt ─────────────────────────────────────────────────

def test_build_prompt_includes_names_skips_nameless():
    left = sm.build_sheet_index(_md([(1, "1", "Однолинейная расчетная схема ГРЩ"),
                                     (2, "2", None)]))  # 2 — безымянная
    right = sm.build_sheet_index(_md([(1, "1", "Однолинейная схема ГРЩ")]))
    system, user, meta = slm.build_llm_match_prompt(left, right)
    assert "Однолинейная расчетная схема ГРЩ" in user
    assert "Однолинейная схема ГРЩ" in user
    # безымянная страница без sheet_no не попадает (meta учитывает только именованные)
    assert meta["left_lines"] == 1
    assert meta["right_lines"] == 1
    assert "page 1" in user


def test_build_prompt_respects_cap():
    left = sm.build_sheet_index(_md([(i, str(i), f"Лист номер {i}") for i in range(1, 11)]))
    right = sm.build_sheet_index(_md([(1, "1", "Лист номер 1")]))
    _, _, meta = slm.build_llm_match_prompt(left, right, max_sheets=3)
    assert meta["left_lines"] == 3


# ─── parse_llm_match_pairs ──────────────────────────────────────────────────

def test_parse_plain_json():
    raw = json.dumps({"pairs": [
        {"old_page": 51, "new_page": 32, "confidence": 0.9, "reason": "ГРЩ"},
    ]})
    pairs = slm.parse_llm_match_pairs(raw, min_confidence=0.6)
    assert pairs == [{"old_page": 51, "new_page": 32, "confidence": 0.9, "reason": "ГРЩ"}]


def test_parse_claude_wrapper_and_fence():
    inner = "```json\n" + json.dumps({"pairs": [
        {"old_page": 2, "new_page": 3, "confidence": 0.8, "reason": "x"}]}) + "\n```"
    raw = json.dumps({"result": inner})
    pairs = slm.parse_llm_match_pairs(raw, min_confidence=0.6)
    assert pairs == [{"old_page": 2, "new_page": 3, "confidence": 0.8, "reason": "x"}]


def test_parse_filters_low_confidence_and_dedup():
    raw = json.dumps({"pairs": [
        {"old_page": 1, "new_page": 1, "confidence": 0.4},   # ниже порога
        {"old_page": 2, "new_page": 2, "confidence": 0.95},
        {"old_page": 2, "new_page": 5, "confidence": 0.9},   # дубль old_page=2 → отброшен
        {"old_page": 7, "new_page": 2, "confidence": 0.9},   # дубль new_page=2 → отброшен
    ]})
    pairs = slm.parse_llm_match_pairs(raw, min_confidence=0.6)
    assert pairs == [{"old_page": 2, "new_page": 2, "confidence": 0.95, "reason": ""}]


def test_parse_garbage_returns_empty():
    assert slm.parse_llm_match_pairs("не json вообще") == []
    assert slm.parse_llm_match_pairs("") == []
    assert slm.parse_llm_match_pairs(json.dumps({"pairs": "не список"})) == []


# ─── llm_match_sheets (мок-provider) ────────────────────────────────────────

def test_llm_match_sheets_done():
    left = sm.build_sheet_index(_md([(51, "51", "Однолинейная расчетная схема ГРЩ")]))
    right = sm.build_sheet_index(_md([(32, "32", "Однолинейная схема ГРЩ")]))
    provider = _FakeProvider(_FakeProviderResult(
        status="done",
        raw_response=json.dumps({"result": json.dumps({"pairs": [
            {"old_page": 51, "new_page": 32, "confidence": 0.88, "reason": "ГРЩ"}]})}),
    ))
    rep = slm.llm_match_sheets(left, right, provider=provider, min_confidence=0.6)
    assert rep["status"] == "ok"
    assert rep["pairs"] == [{"old_page": 51, "new_page": 32,
                             "confidence": 0.88, "reason": "ГРЩ"}]
    assert provider.calls and provider.calls[0]["model"] == "haiku"


def test_llm_match_sheets_provider_error_failsoft():
    left = sm.build_sheet_index(_md([(1, "1", "A")]))
    right = sm.build_sheet_index(_md([(1, "1", "B")]))
    provider = _FakeProvider(_FakeProviderResult(status="error", error="rc=1"))
    rep = slm.llm_match_sheets(left, right, provider=provider)
    assert rep["status"] == "error"
    assert rep["pairs"] == []


def test_llm_match_sheets_empty_side_skips_call():
    left = sm.build_sheet_index(_md([(1, "1", "A")]))
    right = sm.build_sheet_index("")  # нет именованных листов
    provider = _FakeProvider(_FakeProviderResult(status="done", raw_response="{}"))
    rep = slm.llm_match_sheets(left, right, provider=provider)
    assert rep["status"] == "no_unmatched"
    assert provider.calls == []  # provider не дёргался


# ─── make_llm_match_fn + match_sheet_indexes инъекция ───────────────────────

def test_match_sheet_indexes_injects_llm_pair():
    # Имена слегка разные → детерминированный матчер может не свести.
    left = sm.build_sheet_index(_md([(1, "1", "Содержание тома"),
                                     (2, "2", "Однолинейная расчетная схема ГРЩ")]))
    right = sm.build_sheet_index(_md([(1, "1", "Содержание тома"),
                                      (2, "2", "Однолинейная схема ГРЩ Корпус 5 раздел")]))

    provider = _FakeProvider(_FakeProviderResult(
        status="done",
        raw_response=json.dumps({"pairs": [
            {"old_page": 2, "new_page": 2, "confidence": 0.86, "reason": "ГРЩ"}]}),
    ))
    diag: dict = {}
    fn = slm.make_llm_match_fn(provider, diagnostics=diag)
    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)

    matched = {(it["left_page"], it["right_page"], it["match_type"])
               for it in res["suggested_items"] if it["match"]}
    assert (1, 1, "exact_name") in matched
    assert (2, 2, "llm_semantic") in matched
    assert res["llm_match_count"] == 1
    assert diag["pairs_added"] == 1
    # llm-пара помечена needs_review
    llm_item = next(it for it in res["suggested_items"]
                    if it.get("match_type") == "llm_semantic")
    assert llm_item["needs_review"] is True


def test_llm_cannot_override_deterministic_match():
    # Точное совпадение уже свело (2↔2). LLM попытается увести 2→3 — игнор.
    left = sm.build_sheet_index(_md([(2, "2", "Схема ГРЩ")]))
    right = sm.build_sheet_index(_md([(2, "2", "Схема ГРЩ"), (3, "3", "Схема ГРЩ-2")]))

    def fn(rem_left, rem_right):
        return [(2, 3, 0.9, "llm_semantic")]  # 2 уже занят детерминированно

    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)
    matched = {(it["left_page"], it["right_page"]) for it in res["suggested_items"]
               if it["match"]}
    assert (2, 2) in matched
    assert (2, 3) not in matched
    assert res["llm_match_count"] == 0


def test_llm_fn_exception_is_failsoft():
    left = sm.build_sheet_index(_md([(1, "1", "A одно"), (2, "2", "B два")]))
    right = sm.build_sheet_index(_md([(1, "1", "C три"), (2, "2", "D четыре")]))

    def boom(rem_left, rem_right):
        raise RuntimeError("LLM упал")

    res = sm.match_sheet_indexes(left, right, llm_match_fn=boom)
    assert res["llm_match_count"] == 0  # не упало, просто ноль доматчей


# ─── store.suggest_alignment_by_stamp(use_llm=True) ─────────────────────────

def test_store_suggest_by_stamp_use_llm(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison import text_llm_provider

    left_md = tmp_path / "left.md"
    right_md = tmp_path / "right.md"
    left_md.write_text(_md([(1, "1", "Содержание тома"),
                            (2, "2", "Однолинейная расчетная схема ГРЩ")]),
                       encoding="utf-8")
    right_md.write_text(_md([(1, "1", "Содержание тома"),
                             (2, "2", "Однолинейная схема ГРЩ ввод корпус")]),
                        encoding="utf-8")

    fake_pair = {
        "id": "pX",
        "left": {"md_path": str(left_md), "pdf_path": None, "result_json_path": None},
        "right": {"md_path": str(right_md), "pdf_path": None, "result_json_path": None},
    }
    monkeypatch.setattr(store, "_find_pair_meta", lambda s, p: fake_pair)

    provider = _FakeProvider(_FakeProviderResult(
        status="done",
        raw_response=json.dumps({"pairs": [
            {"old_page": 2, "new_page": 2, "confidence": 0.85, "reason": "ГРЩ"}]}),
    ))
    monkeypatch.setattr(text_llm_provider, "ClaudeCodeProvider", lambda: provider)
    monkeypatch.setenv("STAGE_COMPARISON_STAMP_LLM_ENABLED", "true")

    res = store.suggest_alignment_by_stamp("sid", "pX", use_llm=True)
    assert res["llm_requested"] is True
    matched = {(it["left_page"], it["right_page"], it["match_type"])
               for it in res["suggested_items"] if it["match"]}
    assert (1, 1, "exact_name") in matched
    assert (2, 2, "llm_semantic") in matched
    assert res["llm"]["pairs_added"] == 1


def test_store_suggest_by_stamp_use_llm_provider_unavailable(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison import text_llm_provider

    left_md = tmp_path / "left.md"
    right_md = tmp_path / "right.md"
    left_md.write_text(_md([(1, "1", "Содержание тома")]), encoding="utf-8")
    right_md.write_text(_md([(1, "1", "Содержание тома")]), encoding="utf-8")
    fake_pair = {"id": "pX",
                 "left": {"md_path": str(left_md), "pdf_path": None, "result_json_path": None},
                 "right": {"md_path": str(right_md), "pdf_path": None, "result_json_path": None}}
    monkeypatch.setattr(store, "_find_pair_meta", lambda s, p: fake_pair)

    class _Unavailable:
        def check_availability(self):
            return False, "claude_cli_not_found"

    monkeypatch.setattr(text_llm_provider, "ClaudeCodeProvider", lambda: _Unavailable())
    monkeypatch.setenv("STAGE_COMPARISON_STAMP_LLM_ENABLED", "true")

    # Должен деградировать до детерминированного результата без падения.
    res = store.suggest_alignment_by_stamp("sid", "pX", use_llm=True)
    assert res["method"] == "stamp"
    assert res["llm"]["status"] == "provider_not_available"
