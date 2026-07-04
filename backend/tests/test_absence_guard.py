"""Тесты «умного стража отсутствия» (Stage 01 post-pass).

Логика двухтактная: детектор-пре-фильтр + подтверждение присутствия по документу.
Понижаются ТОЛЬКО подтверждённо-ложные (verifier=present). Без верификатора —
безопасный режим (не понижаем ничего).
"""
from __future__ import annotations

from backend.app.pipeline.stages.text_analysis.absence_guard import (
    enforce_absence_guard,
    _is_absence_claim,
    build_verification_prompt,
    parse_verification_response,
)

_VERIFY = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"


def _present_all(md, cands):
    # стаб-верификатор: всё «present» (ложное отсутствие)
    return {i: "present" for i in range(len(cands))}


def _absent_all(md, cands):
    return {i: "absent" for i in range(len(cands))}


# ── Детектор (пре-фильтр) ──

def test_detector_absence_vs_value_claims():
    assert _is_absence_claim({"finding": "Не указана площадь помещения."})
    assert _is_absence_claim({"finding": "Отсутствует узел примыкания."})
    assert _is_absence_claim({"finding": "Не показан узел примыкания к плите."})
    # претензии к значению — НЕ отсутствие
    assert not _is_absence_claim({"finding": "Значение не соответствует ГОСТ."})
    assert not _is_absence_claim({"finding": "Площадь указана неверно."})
    assert not _is_absence_claim({"finding": "Пропущена цифра в шифре СП (опечатка)."})
    assert not _is_absence_claim({"finding": "Недостаточная площадь тамбура по СП."})


# ── Безопасный режим: без верификатора не понижаем ничего ──

def test_no_verifier_no_downgrade():
    findings = [{"id": "T-1", "severity": "КРИТИЧЕСКОЕ",
                 "finding": "Не указана марка двери."}]
    stats = enforce_absence_guard(findings)  # verifier=None
    assert stats["candidates"] == 1
    assert stats["verified"] is False
    assert stats["downgraded"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


def test_no_md_no_downgrade():
    findings = [{"id": "T-1", "severity": "КРИТИЧЕСКОЕ",
                 "finding": "Отсутствует спецификация."}]
    stats = enforce_absence_guard(findings, verifier=_present_all)  # md_text=None
    assert stats["downgraded"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


# ── С верификатором ──

def test_present_verdict_downgrades():
    findings = [{"id": "T-1", "severity": "КРИТИЧЕСКОЕ",
                 "finding": "Не указана огнестойкость."}]
    stats = enforce_absence_guard(findings, md_text="...", verifier=_present_all)
    assert stats["verified"] is True
    assert stats["downgraded"] == 1
    assert findings[0]["severity"] == _VERIFY
    assert findings[0]["absence_guard_downgraded"] is True
    assert findings[0]["absence_guard_original_severity"] == "КРИТИЧЕСКОЕ"


def test_absent_verdict_keeps_finding():
    # Верное отсутствие (реально нет в документе) — НЕ трогаем.
    findings = [{"id": "T-1", "severity": "КРИТИЧЕСКОЕ",
                 "finding": "Не указана огнестойкость."}]
    stats = enforce_absence_guard(findings, md_text="...", verifier=_absent_all)
    assert stats["downgraded"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


def test_verifier_exception_is_failsoft():
    def _boom(md, cands):
        raise RuntimeError("llm down")
    findings = [{"id": "T-1", "severity": "КРИТИЧЕСКОЕ",
                 "finding": "Не указана огнестойкость."}]
    stats = enforce_absence_guard(findings, md_text="...", verifier=_boom)
    assert stats["verified"] is False
    assert stats["downgraded"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


def test_absence_checked_filled_not_candidate():
    findings = [{"id": "T-1", "severity": "КРИТИЧЕСКОЕ",
                 "finding": "Отсутствует спецификация перемычек.",
                 "absence_checked": ["Общие указания л.1", "Спецификация л.3"]}]
    stats = enforce_absence_guard(findings, md_text="...", verifier=_present_all)
    assert stats["candidates"] == 0
    assert stats["downgraded"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


def test_already_verify_severity_not_candidate():
    findings = [{"id": "T-1", "severity": _VERIFY,
                 "finding": "Не указана высота — проверить по АР."}]
    stats = enforce_absence_guard(findings, md_text="...", verifier=_present_all)
    assert stats["candidates"] == 0
    assert "absence_guard_downgraded" not in findings[0]


def test_mixed_verdicts_downgrade_only_present():
    findings = [
        {"id": "T-1", "severity": "КРИТИЧЕСКОЕ", "finding": "Не указана деталь А."},
        {"id": "T-2", "severity": "КРИТИЧЕСКОЕ", "finding": "Отсутствует деталь Б."},
    ]
    def _mixed(md, cands):
        return {0: "present", 1: "absent"}
    stats = enforce_absence_guard(findings, md_text="...", verifier=_mixed)
    assert stats["downgraded"] == 1
    assert findings[0]["severity"] == _VERIFY   # present → понижено
    assert findings[1]["severity"] == "КРИТИЧЕСКОЕ"  # absent → сохранено


def test_malformed_entries_skipped():
    findings = ["строка", None, 42,
                {"id": "T-1", "severity": "КРИТИЧЕСКОЕ", "finding": "не указан диаметр"}]
    stats = enforce_absence_guard(findings, md_text="...", verifier=_present_all)
    assert stats["scanned"] == 1
    assert stats["downgraded"] == 1


# ── Промпт/парсер верификатора ──

def test_build_prompt_lists_candidates():
    p = build_verification_prompt("DOC", [{"finding": "Нет данных X"}])
    assert "DOC" in p and "0)" in p and "Нет данных X" in p


def test_parse_verification_response():
    parsed = {"verdicts": [
        {"i": 0, "verdict": "present", "evidence": "есть на л.5"},
        {"i": 1, "verdict": "absent"},
        {"i": 2, "verdict": "мусор"},
    ]}
    out = parse_verification_response(parsed)
    assert out[0] == "present" and out["0_evidence"] == "есть на л.5"
    assert out[1] == "absent"
    assert 2 not in out


# ── Stage 02 пост-проход (block findings) ──

def test_stage02_guard_downgrades_present_block_finding(tmp_path, monkeypatch):
    import json as _json
    import backend.app.pipeline.stages.text_analysis.absence_guard as ag
    import backend.app.pipeline.stages.prepare.task_builder as tbmod
    from backend.app.pipeline.stages.block_analysis.runner import _apply_stage02_absence_guard

    data = {"block_analyses": [
        {"block_id": "B1", "findings": [
            {"id": "G-1", "severity": "КРИТИЧЕСКОЕ", "finding": "Не указана огнестойкость."},
            {"id": "G-2", "severity": "КРИТИЧЕСКОЕ", "finding": "Марка бетона неверная."},
        ]},
    ]}
    p = tmp_path / "02_blocks_analysis.json"
    p.write_text(_json.dumps(data, ensure_ascii=False), encoding="utf-8")
    md = tmp_path / "doc.md"
    md.write_text("огнестойкость REI 90 указана на листе 3", encoding="utf-8")

    monkeypatch.setattr(tbmod, "_get_md_file_path", lambda pi, pid: str(md))
    # верификатор: первый кандидат present (ложное), больше кандидатов нет (G-2 не absence)
    monkeypatch.setattr(ag, "run_claude_verification", lambda md_text, cands, **k: {0: "present"})

    msg = _apply_stage02_absence_guard(tmp_path, {}, "proj")
    saved = _json.loads(p.read_text(encoding="utf-8"))
    f1 = saved["block_analyses"][0]["findings"][0]
    assert f1["severity"] == "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
    assert f1["absence_guard_downgraded"] is True
    # не-absence осталось
    assert saved["block_analyses"][0]["findings"][1]["severity"] == "КРИТИЧЕСКОЕ"
    assert "понижено 1/1" in msg


def test_stage02_guard_no_md_safe(tmp_path, monkeypatch):
    import json as _json
    import backend.app.pipeline.stages.text_analysis.absence_guard as ag
    import backend.app.pipeline.stages.prepare.task_builder as tbmod
    from backend.app.pipeline.stages.block_analysis.runner import _apply_stage02_absence_guard

    data = {"block_analyses": [{"block_id": "B1", "findings": [
        {"id": "G-1", "severity": "КРИТИЧЕСКОЕ", "finding": "Отсутствует узел."}]}]}
    p = tmp_path / "02_blocks_analysis.json"
    p.write_text(_json.dumps(data, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(tbmod, "_get_md_file_path", lambda pi, pid: "(нет)")
    monkeypatch.setattr(ag, "run_claude_verification", lambda *a, **k: {0: "present"})
    _apply_stage02_absence_guard(tmp_path, {}, "proj")
    saved = _json.loads(p.read_text(encoding="utf-8"))
    # без MD — безопасный режим, не понижаем
    assert saved["block_analyses"][0]["findings"][0]["severity"] == "КРИТИЧЕСКОЕ"
