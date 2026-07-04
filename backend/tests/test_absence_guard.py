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
