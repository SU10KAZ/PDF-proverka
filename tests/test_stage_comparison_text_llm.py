"""Тесты для семантического LLM-анализа текстовых расхождений (Claude Sonnet).

Покрывает (см. task spec):
  1.  missing_md  → status="missing_md", LLM не вызывается
  2.  disabled    → status="disabled" (STAGE_COMPARISON_TEXT_LLM_ENABLED=false)
  3.  provider_not_available → prompt сохраняется в pair-папку
  4.  too_large   → status="too_large", LLM не вызывается
  5.  valid JSON  → text_llm_diff.json сохраняется со status="done" и changes[]
  6.  invalid JSON → status="error", warnings содержит причину
  7.  rebuild_findings создаёт text findings из text_llm_diff.json
  8.  rebuild_findings НЕ строит difflib findings, если text_llm_diff.json нет
  9.  user_note/status сохраняются при повторном rebuild
  10. warnings text_llm_not_run / missing_md / provider_unavailable выпускаются
  11. batch text job без confirm=true → status="rejected_no_confirm", LLM не вызывается
  12. cancel batch text job → status="cancelled", queued items → cancelled

Тесты используют mock-провайдеров; реальный Claude не вызывается.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest


# ─── env: изолируем COMPARISON_ROOT ─────────────────────────────────────


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    # По умолчанию отключаем LLM, чтобы тесты не пытались ходить в Claude
    monkeypatch.delenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", raising=False)
    yield root


# ─── helpers ─────────────────────────────────────────────────────────────


def _make_pair(session_id: str, pair_id: str, *, left_md: Path | None, right_md: Path | None) -> dict:
    """Записать минимальные session.json + pair.json и вернуть pair-структуру."""
    from backend.app.services.stage_comparison import paths as paths_mod

    pairs_meta = {"id": session_id, "pair_order": [pair_id], "warnings": [],
                  "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(pairs_meta, ensure_ascii=False), encoding="utf-8",
    )
    pair = {
        "id": pair_id,
        "status": "matched",
        "left":  {"filename": "left.pdf",  "pdf_path": "/dev/null/left.pdf",  "md_path": (str(left_md) if left_md else None)},
        "right": {"filename": "right.pdf", "pdf_path": "/dev/null/right.pdf", "md_path": (str(right_md) if right_md else None)},
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8",
    )
    return pair


def _write_md(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


# ─── 1. Missing MD ───────────────────────────────────────────────────────


def test_missing_md_returns_missing_md_status(tmp_path):
    from backend.app.services.stage_comparison import text_llm

    _make_pair("sess1", "p1", left_md=None, right_md=None)
    result = text_llm.run_text_comparison("sess1", "p1")
    assert result["status"] == "missing_md"
    assert result["changes"] == []
    assert "warnings" in result and result["warnings"]


def test_one_side_md_missing_still_returns_missing_md(tmp_path):
    from backend.app.services.stage_comparison import text_llm

    left = _write_md(tmp_path, "left.md", "# Stage 1\nContent")
    _make_pair("sess2", "p1", left_md=left, right_md=None)
    result = text_llm.run_text_comparison("sess2", "p1")
    assert result["status"] == "missing_md"


# ─── 2. Disabled provider ───────────────────────────────────────────────


def test_disabled_via_env(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm

    monkeypatch.delenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", raising=False)
    left = _write_md(tmp_path, "a.md", "old")
    right = _write_md(tmp_path, "b.md", "new")
    _make_pair("sess3", "p1", left_md=left, right_md=right)
    result = text_llm.run_text_comparison("sess3", "p1")
    assert result["status"] == "disabled"
    # LLM не вызывается → raw_response_excerpt пуст
    assert result.get("raw_response_excerpt", "") == ""


# ─── 3. Provider not available ──────────────────────────────────────────


class _UnavailableProvider:
    name = "mock_unavailable"
    def check_availability(self):
        return False, "binary_not_found"
    def invoke(self, **kwargs):
        raise AssertionError("should not be called")


def test_provider_not_available_saves_prompt(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider, paths as paths_mod

    left = _write_md(tmp_path, "a.md", "old content")
    right = _write_md(tmp_path, "b.md", "new content")
    _make_pair("sess4", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet",
        timeout_sec=60, max_chars=10_000,
    )
    result = text_llm.run_text_comparison(
        "sess4", "p1", provider=_UnavailableProvider(), config=cfg,
    )
    assert result["status"] == "provider_not_available"
    assert "claude" in (result["warnings"][0].lower()) or "provider" in result["warnings"][0].lower()
    assert result.get("error") == "binary_not_found"
    # Prompt сохранён для ручного запуска
    prompt_p = paths_mod.text_llm_prompt_path("sess4", "p1")
    assert prompt_p.exists()
    assert "<OLD_STAGE_MD>" in prompt_p.read_text(encoding="utf-8")
    assert "<NEW_STAGE_MD>" in prompt_p.read_text(encoding="utf-8")


# ─── 4. Too large ───────────────────────────────────────────────────────


class _MockOkProvider:
    name = "mock_ok"
    last_call = None
    def __init__(self, response_text: str):
        self.response_text = response_text
    def check_availability(self):
        return True, None
    def invoke(self, **kwargs):
        from backend.app.services.stage_comparison.text_llm_provider import ProviderResult
        _MockOkProvider.last_call = kwargs
        return ProviderResult(
            status="done", raw_response=self.response_text,
            duration_sec=0.01, provider=self.name, model=kwargs.get("model") or "sonnet",
        )


def test_too_large_does_not_invoke_provider(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider

    big = "x" * 5000
    left = _write_md(tmp_path, "a.md", big)
    right = _write_md(tmp_path, "b.md", big)
    _make_pair("sess5", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet",
        timeout_sec=60, max_chars=5_000,   # суммарно 10000 > 5000
    )
    invoked = []
    class _NoCallProvider(_MockOkProvider):
        def invoke(self, **kw):
            invoked.append(kw)
            return super().invoke(**kw)
    result = text_llm.run_text_comparison(
        "sess5", "p1", provider=_NoCallProvider("{}"), config=cfg,
    )
    assert result["status"] == "too_large"
    assert invoked == [], "provider must not be invoked for too_large"
    stats = result["input_stats"]
    assert stats["total_chars"] >= 10_000
    assert stats["limit_chars"] == 5_000


# ─── 5. Valid JSON → status=done ───────────────────────────────────────


VALID_RESPONSE = json.dumps({
    "type": "result", "subtype": "success",
    "result": json.dumps({
        "summary": "Изменено оборудование и нагрузки.",
        "designer_declared_changes": [
            {"title": "Ведомость изменений", "summary": "Заменили насос П-1",
             "source_stage": "right", "importance": "high"},
        ],
        "changes": [
            {
                "id": "txtchg_001",
                "type": "equipment_changed",
                "category": "equipment",
                "severity": "high",
                "confidence": 0.9,
                "title": "Замена насоса П-1",
                "summary": "Старый Grundfos заменён на Wilo той же производительности.",
                "old_value": "Grundfos UPS 50-120",
                "new_value": "Wilo Yonos MAXO 50/0,5-12",
                "construction_impact": "Изменение монтажных размеров и подключения.",
                "cost_impact": "possible",
                "requires_human_review": True,
                "evidence_left":  {"quote": "Насос Grundfos UPS 50-120",
                                    "section": "Спецификация",
                                    "approx_location": "стр. 12"},
                "evidence_right": {"quote": "Насос Wilo Yonos MAXO 50/0,5-12",
                                    "section": "Спецификация",
                                    "approx_location": "стр. 12"},
            },
        ],
    }),
})


def test_valid_json_stored_as_done(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider, paths as paths_mod

    left = _write_md(tmp_path, "a.md", "# Old\nGrundfos")
    right = _write_md(tmp_path, "b.md", "# New\nWilo")
    _make_pair("sess6", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet",
        timeout_sec=60, max_chars=100_000,
    )
    result = text_llm.run_text_comparison(
        "sess6", "p1", provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    assert result["status"] == "done"
    assert result["summary"].startswith("Изменено оборудование")
    assert len(result["changes"]) == 1
    ch = result["changes"][0]
    assert ch["type"] == "equipment_changed"
    assert ch["category"] == "equipment"
    assert ch["old_value"].startswith("Grundfos")
    assert ch["new_value"].startswith("Wilo")
    # Файл сохранён
    p = paths_mod.text_llm_diff_path("sess6", "p1")
    assert p.exists()
    loaded = json.loads(p.read_text(encoding="utf-8"))
    assert loaded["status"] == "done"


def test_done_cached_when_not_force(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("sess7", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet", timeout_sec=60, max_chars=100_000,
    )
    prov = _MockOkProvider(VALID_RESPONSE)
    text_llm.run_text_comparison("sess7", "p1", provider=prov, config=cfg)
    # Сохранён cached → повторный вызов без force НЕ должен дёргать provider
    class _FailProvider:
        name = "fail"
        def check_availability(self): return True, None
        def invoke(self, **kw): raise AssertionError("must not be called when cached")
    result2 = text_llm.run_text_comparison(
        "sess7", "p1", provider=_FailProvider(), config=cfg, force=False,
    )
    assert result2["status"] == "done"


def test_force_reinvokes_provider(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("sess8", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet", timeout_sec=60, max_chars=100_000,
    )
    prov = _MockOkProvider(VALID_RESPONSE)
    text_llm.run_text_comparison("sess8", "p1", provider=prov, config=cfg)
    calls = [0]
    class _CountingProvider(_MockOkProvider):
        def invoke(self, **kw):
            calls[0] += 1
            return super().invoke(**kw)
    text_llm.run_text_comparison(
        "sess8", "p1", provider=_CountingProvider(VALID_RESPONSE), config=cfg, force=True,
    )
    assert calls[0] == 1


# ─── 6. Invalid JSON ─────────────────────────────────────────────────────


def test_invalid_json_marks_error(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("sess9", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet", timeout_sec=60, max_chars=100_000,
    )
    # Claude-wrapper JSON корректный, но внутри result — невалидный JSON
    bad_response = json.dumps({
        "type": "result", "result": "<<<not_json_at_all>>>",
    })
    result = text_llm.run_text_comparison(
        "sess9", "p1", provider=_MockOkProvider(bad_response), config=cfg,
    )
    assert result["status"] == "error"
    assert result["changes"] == []
    assert any("invalid" in w.lower() or "json" in w.lower() for w in result["warnings"])


# ─── 7. rebuild_findings consumes text_llm_diff.json ─────────────────────


def test_rebuild_findings_uses_text_llm_diff(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider, findings, paths as paths_mod
    # findings reads via store_mod.get_session — нужно session.json + pair.json
    left = _write_md(tmp_path, "a.md", "old")
    right = _write_md(tmp_path, "b.md", "new")
    _make_pair("sess10", "p1", left_md=left, right_md=right)
    # Дополнительно session-meta: pairs нужно прокинуть store_mod через _make_pair
    # ✓ store_mod.get_session читает session.json + pair.json (см. pair_order)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet", timeout_sec=60, max_chars=100_000,
    )
    text_llm.run_text_comparison(
        "sess10", "p1", provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    # Создадим пустой findings.json
    paths_mod.findings_path("sess10").write_text(
        json.dumps({"version": 1, "items": []}), encoding="utf-8",
    )
    findings.rebuild_findings("sess10")
    all_findings = json.loads(paths_mod.findings_path("sess10").read_text(encoding="utf-8"))
    text_findings = [f for f in (all_findings.get("items") or []) if f.get("category") == "text"]
    assert len(text_findings) >= 1, f"expected ≥1 text finding; got: {all_findings.get('items')}"
    assert any(f["type"] == "text_equipment_changed" for f in text_findings)
    f0 = next(f for f in text_findings if f["type"] == "text_equipment_changed")
    assert f0["title"].startswith("Замена насоса")
    assert "construction_impact" in (f0.get("source") or {})
    assert (f0.get("source") or {}).get("text_llm_change_id") == "txtchg_001"


def test_rebuild_findings_skips_text_when_no_text_llm_diff(tmp_path):
    """Без text_llm_diff.json — НЕТ text findings (difflib не используется)."""
    from backend.app.services.stage_comparison import findings, paths as paths_mod

    left = _write_md(tmp_path, "a.md", "Line A\nLine B")
    right = _write_md(tmp_path, "b.md", "Line A\nLine X")  # есть отличие, difflib бы нашёл
    _make_pair("sess11", "p1", left_md=left, right_md=right)
    paths_mod.findings_path("sess11").write_text(
        json.dumps({"version": 1, "items": []}), encoding="utf-8",
    )
    findings.rebuild_findings("sess11")
    all_findings = json.loads(paths_mod.findings_path("sess11").read_text(encoding="utf-8"))
    text_findings = [f for f in (all_findings.get("items") or []) if f.get("category") == "text"]
    assert text_findings == [], f"difflib must NOT create text findings; got: {text_findings}"


# ─── 9. user_note сохраняется при повторном rebuild ─────────────────────


def test_user_note_preserved_across_rebuilds(tmp_path):
    from backend.app.services.stage_comparison import text_llm, text_llm_provider, findings, paths as paths_mod

    left = _write_md(tmp_path, "a.md", "old")
    right = _write_md(tmp_path, "b.md", "new")
    _make_pair("sess12", "p1", left_md=left, right_md=right)
    cfg = text_llm_provider.ProviderConfig(
        enabled=True, provider="mock", model="sonnet", timeout_sec=60, max_chars=100_000,
    )
    text_llm.run_text_comparison("sess12", "p1", provider=_MockOkProvider(VALID_RESPONSE), config=cfg)
    paths_mod.findings_path("sess12").write_text(
        json.dumps({"version": 1, "items": []}), encoding="utf-8",
    )
    findings.rebuild_findings("sess12")
    fp = paths_mod.findings_path("sess12")
    payload = json.loads(fp.read_text(encoding="utf-8"))
    target = next(f for f in payload["items"] if f["type"] == "text_equipment_changed")
    # Имитируем юзер-правку
    for it in payload["items"]:
        if it["id"] == target["id"]:
            it["user_note"] = "Подтверждено инженером"
            it["status"] = "accepted"
            it["severity"] = "high"
    fp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    # Повторный rebuild — те же changes, должен сохранить user_note
    findings.rebuild_findings("sess12")
    payload2 = json.loads(fp.read_text(encoding="utf-8"))
    same = next(f for f in payload2["items"] if f["type"] == "text_equipment_changed")
    assert same["user_note"] == "Подтверждено инженером"
    assert same["status"] == "accepted"
    assert same["severity"] == "high"


# ─── 10. Warnings ───────────────────────────────────────────────────────


def test_warning_text_llm_not_run(tmp_path):
    from backend.app.services.stage_comparison import warnings as warnings_mod

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("sess_w1", "p1", left_md=left, right_md=right)
    result = warnings_mod.compute_warnings("sess_w1")
    types = {it["type"] for it in result["items"]}
    assert "text_llm_not_run" in types


def test_warning_text_llm_missing_md(tmp_path):
    from backend.app.services.stage_comparison import warnings as warnings_mod

    left = _write_md(tmp_path, "a.md", "x")
    _make_pair("sess_w2", "p1", left_md=left, right_md=None)
    result = warnings_mod.compute_warnings("sess_w2")
    types = {it["type"] for it in result["items"]}
    # Должен быть text_llm_missing_md (а старый missing_md тоже остаётся)
    assert "text_llm_missing_md" in types


# ─── 11. Batch job без confirm=true → rejected ──────────────────────────


def test_batch_job_without_confirm_is_rejected(tmp_path):
    from backend.app.services.stage_comparison import text_llm_jobs

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("sess_j1", "p1", left_md=left, right_md=right)
    job = text_llm_jobs.create_text_llm_job(
        "sess_j1", scope="pair", pair_id="p1", confirm=False,
    )
    assert job["status"] == "rejected_no_confirm"
    assert job["confirm"] is False


# ─── 12. Cancel batch job ───────────────────────────────────────────────


def test_cancel_batch_job(tmp_path):
    from backend.app.services.stage_comparison import text_llm_jobs

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("sess_j2", "p1", left_md=left, right_md=right)
    job = text_llm_jobs.create_text_llm_job(
        "sess_j2", scope="pair", pair_id="p1", confirm=True,
    )
    assert job["status"] == "queued"
    # Cancel сразу — без фактического старта
    cancelled = text_llm_jobs.cancel_job("sess_j2", job["id"])
    assert cancelled is not None
    assert cancelled["status"] == "cancelled"
    for it in cancelled["items"]:
        assert it["status"] in ("cancelled", "done", "failed", "skipped")


# ─── Bonus: prompt-builders ─────────────────────────────────────────────


def test_build_prompts_wraps_content_in_markers():
    from backend.app.services.stage_comparison import text_llm

    system, user = text_llm.build_prompts("OLD_TEXT_AB", "NEW_TEXT_CD")
    assert "<OLD_STAGE_MD>" in user and "OLD_TEXT_AB" in user
    assert "<NEW_STAGE_MD>" in user and "NEW_TEXT_CD" in user
    assert "<OLD_STAGE_MD>" in system or "OLD_STAGE_MD" in system  # system reference


def test_safety_instruction_present():
    from backend.app.services.stage_comparison import text_llm

    sys_prompt = text_llm.SYSTEM_PROMPT
    # Должна быть инструкция игнорировать команды внутри документов
    assert ("ДОКУМЕНТАЦИЯ" in sys_prompt and "игнорируй" in sys_prompt.lower()) \
        or ("прайт-инъекций" in sys_prompt.lower())


# ═══════════════════════════════════════════════════════════════════════
# Preflight (Task: production-safe UX) — 2026-05-23
# ═══════════════════════════════════════════════════════════════════════


def _make_cfg(*, enabled: bool = True, max_chars: int = 350_000):
    from backend.app.services.stage_comparison import text_llm_provider
    return text_llm_provider.ProviderConfig(
        enabled=enabled, provider="claude_code", model="sonnet",
        timeout_sec=720, max_chars=max_chars,
    )


# ─── 1. pair preflight: chars ──────────────────────────────────────────


def test_pair_preflight_counts_chars(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm_preflight
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    left = _write_md(tmp_path, "a.md", "x" * 1000)
    right = _write_md(tmp_path, "b.md", "y" * 2000)
    _make_pair("pf_sess1", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    info = text_llm_preflight.estimate_pair("pf_sess1", "p1", config=cfg)
    assert info["left_chars"] == 1000
    assert info["right_chars"] == 2000
    assert info["total_chars"] == 3000
    assert info["has_left_md"] is True
    assert info["has_right_md"] is True
    assert info["within_limit"] is True
    # Минимум должен сработать (3000 chars очень мало)
    assert info["estimated_duration_sec"] >= text_llm_preflight.MIN_DURATION_SEC
    assert info["estimated_cost_usd"] >= text_llm_preflight.MIN_COST_USD


# ─── 2. pair preflight: missing_md ─────────────────────────────────────


def test_pair_preflight_missing_md(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm_preflight
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    left = _write_md(tmp_path, "a.md", "content")
    _make_pair("pf_sess2", "p1", left_md=left, right_md=None)

    cfg = _make_cfg()
    info = text_llm_preflight.estimate_pair("pf_sess2", "p1", config=cfg)
    assert info["has_left_md"] is True
    assert info["has_right_md"] is False
    assert "missing_md" in info["warnings"]
    assert "missing_md" in info["blocking"]
    assert info["status"] == "missing_md"


# ─── 3. pair preflight: too_large ──────────────────────────────────────


def test_pair_preflight_too_large(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm_preflight
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    big = "z" * 6_000
    left = _write_md(tmp_path, "a.md", big)
    right = _write_md(tmp_path, "b.md", big)
    _make_pair("pf_sess3", "p1", left_md=left, right_md=right)

    cfg = _make_cfg(max_chars=5_000)  # 12000 > 5000
    info = text_llm_preflight.estimate_pair("pf_sess3", "p1", config=cfg)
    assert info["within_limit"] is False
    assert "too_large" in info["warnings"]
    assert "too_large" in info["blocking"]
    assert info["status"] == "too_large"


# ─── 4. pair preflight: provider disabled / unavailable ────────────────


def test_pair_preflight_provider_disabled(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm_preflight
    # ENABLED=false → reason=disabled_via_env
    monkeypatch.delenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", raising=False)

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("pf_sess4", "p1", left_md=left, right_md=right)

    # Не передаём config — пусть load_config() сработает с env
    info = text_llm_preflight.estimate_pair("pf_sess4", "p1")
    assert info["provider_enabled"] is False
    assert "disabled" in info["blocking"]
    assert info["status"] == "provider_unavailable"


def test_pair_preflight_provider_unavailable(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm_preflight, text_llm_provider
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")
    # Скрываем claude CLI напрямую — env-стирания PATH недостаточно, потому что
    # ClaudeCodeProvider также проверяет несколько hard-coded путей.
    monkeypatch.setattr(text_llm_provider.ClaudeCodeProvider, "_find_cli", lambda self: None)

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("pf_sess4b", "p1", left_md=left, right_md=right)

    info = text_llm_preflight.estimate_pair("pf_sess4b", "p1")
    assert info["provider_enabled"] is True
    assert info["provider_available"] is False
    assert "provider_unavailable" in info["blocking"]


# ─── 5. session preflight: multiple pairs ──────────────────────────────


def test_session_preflight_aggregates_multiple_pairs(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import text_llm_preflight, paths as paths_mod
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    # Make multiple pairs in one session manually.
    sid = "pf_sess5"
    paths_mod.session_json_path(sid).write_text(
        json.dumps({"id": sid, "pair_order": ["p1", "p2", "p3"], "warnings": []}),
        encoding="utf-8",
    )
    for i, (l, r) in enumerate([("a1", "b1"), ("a2", "b2"), ("a3", "b3")], 1):
        lp = _write_md(tmp_path, f"l{i}.md", l * 1500)
        rp = _write_md(tmp_path, f"r{i}.md", r * 1500)
        pair = {
            "id": f"p{i}", "status": "matched",
            "left":  {"filename": f"L{i}.pdf", "pdf_path": "/x", "md_path": str(lp)},
            "right": {"filename": f"R{i}.pdf", "pdf_path": "/x", "md_path": str(rp)},
        }
        paths_mod.pair_json_path(sid, f"p{i}").write_text(
            json.dumps(pair, ensure_ascii=False), encoding="utf-8",
        )

    cfg = _make_cfg()
    summary = text_llm_preflight.estimate_session(sid, scope="session", force=False, config=cfg)
    assert summary["total_pairs"] == 3
    # Ни одна не cached → все runnable, кроме если provider unavailable
    # В CI претензии к claude CLI могут не быть, но пары всё равно по объёму OK.
    # Допустим, что либо все runnable, либо все skipped как provider_unavailable.
    if summary["runnable_pairs"] > 0:
        assert summary["runnable_pairs"] == 3
        assert summary["estimated_duration_sec"] >= 3 * text_llm_preflight.MIN_DURATION_SEC
        assert summary["estimated_cost_usd"] >= 3 * text_llm_preflight.MIN_COST_USD
    else:
        assert summary["skipped_pairs"] == 3


# ─── 6. session preflight: skipped cached when force=false ─────────────


def test_session_preflight_skips_cached_when_not_force(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import (
        text_llm_preflight, text_llm, paths as paths_mod,
    )
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    left = _write_md(tmp_path, "a.md", "old")
    right = _write_md(tmp_path, "b.md", "new")
    _make_pair("pf_sess6", "p1", left_md=left, right_md=right)

    # Запишем "уже посчитанный" text_llm_diff.json
    cfg = _make_cfg()
    text_llm.run_text_comparison(
        "pf_sess6", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    # И теперь preflight без force должен пометить как cached
    summary = text_llm_preflight.estimate_session(
        "pf_sess6", scope="session", force=False, config=cfg,
    )
    assert summary["total_pairs"] == 1
    assert summary["runnable_pairs"] == 0
    assert summary["skipped_pairs"] == 1
    assert summary["skipped_reasons"].get("cached", 0) == 1
    assert summary["items"][0]["status"] == "cached"


# ─── 7. session preflight: force=true → cached included ────────────────


def test_session_preflight_force_includes_cached(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import (
        text_llm_preflight, text_llm,
    )
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    left = _write_md(tmp_path, "a.md", "old")
    right = _write_md(tmp_path, "b.md", "new")
    _make_pair("pf_sess7", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    text_llm.run_text_comparison(
        "pf_sess7", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    summary = text_llm_preflight.estimate_session(
        "pf_sess7", scope="session", force=True, config=cfg,
    )
    assert summary["runnable_pairs"] == 1
    assert summary["skipped_pairs"] == 0


# ─── 8. cost/time estimate vs baseline ─────────────────────────────────


def test_estimate_matches_reference_baseline():
    from backend.app.services.stage_comparison import text_llm_preflight as p

    # Ровно на baseline должен дать ~481.5 * 1.2 ≈ 578 c и ~0.61 * 1.2 ≈ $0.73
    dur = p.estimate_duration_sec(p.REFERENCE_CHARS)
    cost = p.estimate_cost_usd(p.REFERENCE_CHARS)
    assert dur == 578
    assert abs(cost - 0.73) < 0.01

    # Удвоенный объём → ровно в два раза больше (линейная зависимость)
    dur2 = p.estimate_duration_sec(p.REFERENCE_CHARS * 2)
    cost2 = p.estimate_cost_usd(p.REFERENCE_CHARS * 2)
    assert dur2 == 2 * dur
    assert abs(cost2 - 2 * cost) < 0.02

    # Минимум для крохотных входов
    assert p.estimate_duration_sec(10) == p.MIN_DURATION_SEC
    assert p.estimate_cost_usd(10) == p.MIN_COST_USD


# ─── 9. hard batch limit blocks job creation ──────────────────────────


def test_hard_batch_limit_blocks_session_run(tmp_path, monkeypatch):
    """Если оценка batch превышает hard limit — endpoint должен вернуть 422."""
    from fastapi.testclient import TestClient
    from backend.app.main import app
    from backend.app.services.stage_comparison import paths as paths_mod

    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_HARD_COST_USD", "0.01")  # очень низкий hard
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_HARD_DURATION_SEC", "10")  # очень низкий hard

    sid = "pf_sess9"
    paths_mod.session_json_path(sid).write_text(
        json.dumps({"id": sid, "pair_order": ["p1"], "warnings": []}),
        encoding="utf-8",
    )
    left = _write_md(tmp_path, "a.md", "x" * 2000)
    right = _write_md(tmp_path, "b.md", "y" * 2000)
    pair = {
        "id": "p1", "status": "matched",
        "left":  {"filename": "L.pdf", "pdf_path": "/x", "md_path": str(left)},
        "right": {"filename": "R.pdf", "pdf_path": "/x", "md_path": str(right)},
    }
    paths_mod.pair_json_path(sid, "p1").write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8",
    )

    client = TestClient(app)
    r = client.post(
        f"/api/stage-comparison/sessions/{sid}/text-llm-diff-jobs",
        json={"scope": "session", "confirm": True, "force": False},
    )
    assert r.status_code == 422, r.text
    body = r.json()
    detail = body.get("detail") or {}
    assert detail.get("code") == "batch_limit_exceeded"


# ─── 10. text_llm_diff.json stores preflight estimate ──────────────────


def test_text_llm_diff_stores_preflight_estimate(tmp_path):
    from backend.app.services.stage_comparison import (
        text_llm, paths as paths_mod,
    )

    left = _write_md(tmp_path, "a.md", "old" * 100)
    right = _write_md(tmp_path, "b.md", "new" * 100)
    _make_pair("pf_sess10", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    result = text_llm.run_text_comparison(
        "pf_sess10", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    assert result["status"] == "done"
    # Поля сохраняются
    assert "preflight" in result
    assert "total_chars" in result["preflight"]
    assert "estimated_duration_sec" in result["preflight"]
    assert "estimated_cost_usd" in result["preflight"]
    assert "cost_estimate_usd" in result
    assert result["cost_estimate_usd"] == result["preflight"]["estimated_cost_usd"]
    # actual_cost_usd должно быть None (mock не вернул total_cost_usd)
    assert result["actual_cost_usd"] is None

    # И в файле — тоже
    on_disk = json.loads(paths_mod.text_llm_diff_path("pf_sess10", "p1").read_text(encoding="utf-8"))
    assert on_disk["preflight"]["total_chars"] == result["preflight"]["total_chars"]


def test_text_llm_diff_extracts_actual_cost_from_claude_wrapper(tmp_path):
    """Если claude CLI вернул total_cost_usd в обёртке — сохраняем как actual."""
    from backend.app.services.stage_comparison import text_llm

    # Эмулируем stdout формата claude -p --output-format json (с total_cost_usd)
    inner_json = json.dumps({
        "summary": "x",
        "designer_declared_changes": [],
        "changes": [],
    })
    response_with_cost = json.dumps({
        "type": "result", "subtype": "success",
        "result": inner_json,
        "total_cost_usd": 0.6081,
    })

    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("pf_sess10b", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    result = text_llm.run_text_comparison(
        "pf_sess10b", "p1",
        provider=_MockOkProvider(response_with_cost), config=cfg,
    )
    assert result["status"] == "done"
    assert result["actual_cost_usd"] == 0.6081


# ─── Sanity: preflight endpoints via TestClient ───────────────────────


def test_pair_preflight_endpoint_removed(tmp_path, monkeypatch):
    """Pair-preflight GET endpoint больше не часть публичного API.

    После рефакторинга session-only flow роут удалён. Внутренняя
    функция `estimate_pair()` остаётся (нужна как helper для
    `estimate_session()`), но HTTP-доступ к ней снят.
    """
    from fastapi.testclient import TestClient
    from backend.app.main import app

    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")
    left = _write_md(tmp_path, "a.md", "x" * 500)
    right = _write_md(tmp_path, "b.md", "y" * 500)
    _make_pair("pf_api1", "p1", left_md=left, right_md=right)

    client = TestClient(app)
    r = client.get("/api/stage-comparison/sessions/pf_api1/pairs/p1/text-llm-preflight")
    # FastAPI отдаёт 404 (no route matched) или 405 (method not allowed для другого
    # глагола). Главное — это не 200.
    assert r.status_code in (404, 405), r.text


def test_pair_sync_run_endpoint_removed(tmp_path, monkeypatch):
    """Pair-sync POST endpoint больше не существует.

    Единственный путь запуска — session-batch через
    POST .../text-llm-diff-jobs.
    """
    from fastapi.testclient import TestClient
    from backend.app.main import app

    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")
    left = _write_md(tmp_path, "a.md", "x" * 500)
    right = _write_md(tmp_path, "b.md", "y" * 500)
    _make_pair("pf_api1b", "p1", left_md=left, right_md=right)

    client = TestClient(app)
    r = client.post(
        "/api/stage-comparison/sessions/pf_api1b/pairs/p1/text-llm-diff",
        json={"run": True, "force": False},
    )
    assert r.status_code in (404, 405), r.text


def test_pair_get_endpoint_still_works(tmp_path, monkeypatch):
    """Read-only GET pair endpoint остаётся (debug + blocks-view дёргают его)."""
    from fastapi.testclient import TestClient
    from backend.app.main import app

    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")
    left = _write_md(tmp_path, "a.md", "x" * 50)
    right = _write_md(tmp_path, "b.md", "y" * 50)
    _make_pair("pf_api1c", "p1", left_md=left, right_md=right)

    client = TestClient(app)
    r = client.get("/api/stage-comparison/sessions/pf_api1c/pairs/p1/text-llm-diff")
    assert r.status_code == 200, r.text
    # Запускали — нет; ожидаем `not_run`.
    assert r.json().get("status") == "not_run"


def test_preflight_endpoint_session(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from backend.app.main import app

    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")
    left = _write_md(tmp_path, "a.md", "x")
    right = _write_md(tmp_path, "b.md", "y")
    _make_pair("pf_api2", "p1", left_md=left, right_md=right)

    client = TestClient(app)
    r = client.post(
        "/api/stage-comparison/sessions/pf_api2/text-llm-preflight",
        json={"scope": "session", "force": False},
    )
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["total_pairs"] == 1
    assert "items" in d
    assert "warnings" in d


# ═══════════════════════════════════════════════════════════════════════
# Image/imagine filtering (2026-05-23)
# Графические блоки сравниваются отдельным визуальным модулем, поэтому
# text-LLM получает MD, очищенный от image/imagine-блоков.
# ═══════════════════════════════════════════════════════════════════════


def test_filter_removes_chandra_image_blocks():
    """`### BLOCK [IMAGE]:` Chandra-стиль до следующего ### BLOCK [TEXT] —
    удаляется целиком; соседний text-block сохраняется."""
    from backend.app.services.stage_comparison.text_llm_input import (
        prepare_text_only_markdown,
    )
    md = (
        "## СТРАНИЦА 1\n"
        "**Лист:** 1 (из 2)\n\n"
        "### BLOCK [TEXT]: AAA-111\n"
        "Текст спецификации: насос Grundfos UPS 50-120, расход 3.5 л/с.\n\n"
        "### BLOCK [IMAGE]: BBB-222\n"
        "**[ИЗОБРАЖЕНИЕ]** | Тип: План этажа\n"
        "**Описание:** На чертеже изображен план первого этажа со штриховкой стен.\n"
        "**Текст на чертеже:** План 1 этажа\n\n"
        "### BLOCK [TEXT]: CCC-333\n"
        "Требование: класс энергоэффективности — A.\n"
    )
    res = prepare_text_only_markdown(md)
    out = res["text"]
    assert "BLOCK [IMAGE]" not in out
    assert "ИЗОБРАЖЕНИЕ" not in out
    assert "штриховкой стен" not in out
    # Текстовые блоки остаются
    assert "Grundfos UPS 50-120" in out
    assert "класс энергоэффективности — A" in out
    assert res["stats"]["removed_image_blocks"] >= 1
    assert res["stats"]["filtered_chars"] < res["stats"]["original_chars"]


def test_filter_removes_fenced_image_block():
    """Fenced ```image ... ``` блок удаляется целиком."""
    from backend.app.services.stage_comparison.text_llm_input import (
        prepare_text_only_markdown,
    )
    md = (
        "# Раздел 1\n"
        "Текст требования.\n\n"
        "```image\n"
        "ASCII-схема изображения, которая не нужна Claude\n"
        "```\n\n"
        "Текст требования 2.\n"
    )
    res = prepare_text_only_markdown(md)
    out = res["text"]
    assert "ASCII-схема изображения" not in out
    assert "```image" not in out
    assert "Текст требования 2" in out
    assert res["stats"]["removed_image_blocks"] >= 1


def test_filter_removes_type_image_marker():
    """Standalone YAML/JSON-маркер `type: image` удаляется."""
    from backend.app.services.stage_comparison.text_llm_input import (
        prepare_text_only_markdown,
    )
    md = (
        "title: Раздел\n"
        "type: image\n"
        "block_type: imagine\n"
        "kind: image\n"
        "Текст после маркеров — должен остаться.\n"
    )
    res = prepare_text_only_markdown(md)
    out = res["text"]
    assert "type: image" not in out
    assert "block_type: imagine" not in out
    assert "kind: image" not in out
    assert "Текст после маркеров" in out
    assert res["stats"]["removed_image_blocks"] >= 3


def test_filter_removes_markdown_image_lines():
    """`![alt](url)` строки удаляются, но соседняя проза сохраняется."""
    from backend.app.services.stage_comparison.text_llm_input import (
        prepare_text_only_markdown,
    )
    md = (
        "## План\n"
        "Перед картинкой важный текст.\n"
        "![чертёж](plan.png)\n"
        "После картинки тоже важный текст.\n"
    )
    res = prepare_text_only_markdown(md)
    out = res["text"]
    assert "plan.png" not in out
    assert "Перед картинкой важный текст" in out
    assert "После картинки тоже важный текст" in out


def test_filter_preserves_word_inside_prose():
    """Слово «изображение» внутри обычной фразы НЕ должно вызывать удаление."""
    from backend.app.services.stage_comparison.text_llm_input import (
        prepare_text_only_markdown,
    )
    md = (
        "# Раздел\n"
        "В пояснительной записке упоминается изображение типового узла,\n"
        "но это полноценный текстовый параграф со ссылкой на чертёж.\n"
    )
    res = prepare_text_only_markdown(md)
    out = res["text"]
    assert "В пояснительной записке упоминается изображение" in out
    # Сам блок не помечен как image — не должен быть удалён
    assert res["stats"]["filtered_chars"] >= res["stats"]["original_chars"] - 10


def test_filter_removes_image_tag_block():
    """Теги <image>...</image> и [IMAGE]...[/IMAGE] удаляются."""
    from backend.app.services.stage_comparison.text_llm_input import (
        prepare_text_only_markdown,
    )
    md = (
        "Текст до.\n"
        "<image>\nописание картинки\n</image>\n"
        "Текст между.\n"
        "[IMAGE]\nещё одно описание\n[/IMAGE]\n"
        "Текст после.\n"
    )
    res = prepare_text_only_markdown(md)
    out = res["text"]
    assert "описание картинки" not in out
    assert "ещё одно описание" not in out
    assert "Текст до" in out and "Текст между" in out and "Текст после" in out


def test_pair_preflight_reports_filtered_stats(tmp_path, monkeypatch):
    """Preflight должен возвращать original/filtered/removed-поля для UI."""
    from backend.app.services.stage_comparison import text_llm_preflight
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    md_with_image = (
        "## СТРАНИЦА 1\n"
        "### BLOCK [TEXT]: A1\nПолезный текст спецификации.\n\n"
        "### BLOCK [IMAGE]: B1\n**[ИЗОБРАЖЕНИЕ]** | Описание чертежа.\n"
        "Подробный визуальный анализ с подписями и габаритами.\n\n"
        "### BLOCK [TEXT]: A2\nЕщё текст требования.\n"
    )
    left = _write_md(tmp_path, "a.md", md_with_image)
    right = _write_md(tmp_path, "b.md", md_with_image)
    _make_pair("pf_filter1", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    info = text_llm_preflight.estimate_pair("pf_filter1", "p1", config=cfg)

    assert info["total_original_chars"] > info["total_filtered_chars"]
    assert info["removed_image_blocks_total"] >= 2  # один image на сторону
    assert info["removed_image_chars_total"] > 0
    # legacy-поле total_chars = filtered
    assert info["total_chars"] == info["total_filtered_chars"]


def test_preflight_uses_filtered_for_cost_and_time(tmp_path, monkeypatch):
    """Оценка стоимости/времени считается по filtered_chars, не по оригиналу."""
    from backend.app.services.stage_comparison import text_llm_preflight
    monkeypatch.setenv("STAGE_COMPARISON_TEXT_LLM_ENABLED", "true")

    # Два MD одинакового исходного размера, но один — почти весь image
    plain = "X" * 30_000
    bloated = (
        "### BLOCK [IMAGE]: I1\n"
        + ("a" * 27_000) + "\n"
        + "### BLOCK [TEXT]: T1\n"
        + ("Y" * 3_000)
    )

    left = _write_md(tmp_path, "a.md", plain)
    right = _write_md(tmp_path, "b.md", plain)
    _make_pair("pf_cost1", "p1", left_md=left, right_md=right)

    left2 = _write_md(tmp_path, "a2.md", bloated)
    right2 = _write_md(tmp_path, "b2.md", bloated)
    _make_pair("pf_cost2", "p2", left_md=left2, right_md=right2)

    cfg = _make_cfg()
    info_plain = text_llm_preflight.estimate_pair("pf_cost1", "p1", config=cfg)
    info_bloat = text_llm_preflight.estimate_pair("pf_cost2", "p2", config=cfg)

    # Раздутый image-контентом MD должен стоить дешевле, потому что filtered
    # меньше, хотя исходный размер сопоставим.
    assert info_bloat["estimated_cost_usd"] < info_plain["estimated_cost_usd"]
    assert info_bloat["estimated_duration_sec"] <= info_plain["estimated_duration_sec"]


def test_text_llm_diff_stores_extended_input_stats(tmp_path):
    """text_llm_diff.json должен содержать original/filtered/removed-поля."""
    from backend.app.services.stage_comparison import text_llm

    md = (
        "### BLOCK [TEXT]: T1\nТекстовый блок про оборудование.\n\n"
        "### BLOCK [IMAGE]: I1\n"
        "**[ИЗОБРАЖЕНИЕ]** | Тип: Схема\n"
        "Длинное описание изображения, которое не должно попасть в LLM.\n"
    )
    left = _write_md(tmp_path, "a.md", md)
    right = _write_md(tmp_path, "b.md", md)
    _make_pair("filt_diff1", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    result = text_llm.run_text_comparison(
        "filt_diff1", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    stats = result["input_stats"]
    expected_keys = {
        "left_original_chars", "right_original_chars",
        "left_filtered_chars", "right_filtered_chars",
        "total_original_chars", "total_filtered_chars",
        "removed_image_blocks_left", "removed_image_blocks_right",
        "removed_image_chars_left", "removed_image_chars_right",
        "limit_chars",
    }
    assert expected_keys.issubset(set(stats.keys())), stats.keys()
    assert stats["total_original_chars"] > stats["total_filtered_chars"]
    assert stats["removed_image_blocks_left"] >= 1
    assert stats["removed_image_blocks_right"] >= 1
    # Legacy alias = filtered
    assert stats["total_chars"] == stats["total_filtered_chars"]


def test_prompt_to_provider_has_no_image_blocks(tmp_path):
    """В prompt, который реально уходит в provider, не должно быть image-блоков."""
    from backend.app.services.stage_comparison import text_llm

    md = (
        "### BLOCK [TEXT]: T1\nТребование: класс А.\n\n"
        "### BLOCK [IMAGE]: I1\n"
        "**[ИЗОБРАЖЕНИЕ]** | Тип: План\n"
        "Длинное описание чертежа с упоминанием штриховки.\n"
    )
    left = _write_md(tmp_path, "a.md", md)
    right = _write_md(tmp_path, "b.md", md)
    _make_pair("filt_prompt1", "p1", left_md=left, right_md=right)

    captured = {}
    class _CapturingProvider(_MockOkProvider):
        def invoke(self, **kw):
            captured["system"] = kw.get("system_prompt") or ""
            captured["user"] = kw.get("user_prompt") or ""
            return super().invoke(**kw)

    cfg = _make_cfg()
    text_llm.run_text_comparison(
        "filt_prompt1", "p1",
        provider=_CapturingProvider(VALID_RESPONSE), config=cfg,
    )

    user_prompt = captured["user"]
    assert "BLOCK [IMAGE]" not in user_prompt
    assert "ИЗОБРАЖЕНИЕ" not in user_prompt
    assert "штриховки" not in user_prompt
    # Полезный текст сохранён
    assert "Требование: класс А" in user_prompt


def test_filter_warning_when_filtered_text_too_short(tmp_path):
    """Если после фильтрации остаётся <1000 символов — warning в результате."""
    from backend.app.services.stage_comparison import text_llm

    # Большой image-блок, маленький text-блок
    md = (
        "### BLOCK [TEXT]: T1\nКороткий текст.\n\n"
        "### BLOCK [IMAGE]: I1\n**[ИЗОБРАЖЕНИЕ]**\n"
        + ("Описание изображения " * 100) + "\n"
    )
    left = _write_md(tmp_path, "a.md", md)
    right = _write_md(tmp_path, "b.md", md)
    _make_pair("filt_warn1", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    result = text_llm.run_text_comparison(
        "filt_warn1", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    short_warn = [w for w in (result.get("warnings") or [])
                  if "мало текста" in w.lower()]
    assert short_warn, result.get("warnings")


def test_system_prompt_mentions_image_exclusion():
    """Системный промпт должен явно запрещать анализ image/imagine-блоков."""
    from backend.app.services.stage_comparison import text_llm
    sp = text_llm.SYSTEM_PROMPT
    assert "image/imagine" in sp.lower() or "image/imagine-блок" in sp.lower()
    # Жёсткое указание игнорировать оставшиеся image-маркеры
    assert "игнорируй" in sp.lower()
    assert "графич" in sp.lower()


def test_text_only_md_debug_files_written(tmp_path):
    """Очищенный MD сохраняется в pair-папке для отладки."""
    from backend.app.services.stage_comparison import text_llm, paths as paths_mod

    md = (
        "### BLOCK [TEXT]: T1\nТекстовая часть.\n\n"
        "### BLOCK [IMAGE]: I1\n**[ИЗОБРАЖЕНИЕ]** | Описание.\n"
    )
    left = _write_md(tmp_path, "a.md", md)
    right = _write_md(tmp_path, "b.md", md)
    _make_pair("filt_dbg1", "p1", left_md=left, right_md=right)

    cfg = _make_cfg()
    text_llm.run_text_comparison(
        "filt_dbg1", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    lp = paths_mod.text_llm_text_only_md_path("filt_dbg1", "p1", "left")
    rp = paths_mod.text_llm_text_only_md_path("filt_dbg1", "p1", "right")
    assert lp.exists() and rp.exists()
    lp_text = lp.read_text(encoding="utf-8")
    assert "Текстовая часть" in lp_text
    assert "BLOCK [IMAGE]" not in lp_text
    assert "ИЗОБРАЖЕНИЕ" not in lp_text


def test_rebuild_findings_does_not_block_match_text(tmp_path):
    """rebuild_findings строит text-findings по changes[] из LLM,
    а не по block-to-block matching между MD-блоками."""
    from backend.app.services.stage_comparison import (
        text_llm, findings, paths as paths_mod,
    )

    md = (
        "### BLOCK [TEXT]: T1\nСтарый текст.\n\n"
        "### BLOCK [TEXT]: T2\nЕщё блок.\n"
    )
    left = _write_md(tmp_path, "a.md", md)
    right = _write_md(tmp_path, "b.md", md.replace("Старый", "Новый"))
    _make_pair("filt_rb1", "p1", left_md=left, right_md=right)

    # Mock LLM возвращает один change — rebuild должен сделать ровно один text finding.
    cfg = _make_cfg()
    text_llm.run_text_comparison(
        "filt_rb1", "p1",
        provider=_MockOkProvider(VALID_RESPONSE), config=cfg,
    )
    paths_mod.findings_path("filt_rb1").write_text(
        json.dumps({"version": 1, "items": []}), encoding="utf-8",
    )
    findings.rebuild_findings("filt_rb1")
    all_findings = json.loads(paths_mod.findings_path("filt_rb1").read_text(encoding="utf-8"))
    text_findings = [f for f in all_findings["items"] if f.get("category") == "text"]
    # Только тот change, что вернул мок — никакого block-to-block сопоставления
    assert len(text_findings) == 1
    assert text_findings[0]["type"] == "text_equipment_changed"
