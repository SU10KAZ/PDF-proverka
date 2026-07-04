"""Тесты детерминированного «стража отсутствия» (Stage 01 post-pass)."""
from __future__ import annotations

from backend.app.pipeline.stages.text_analysis.absence_guard import (
    enforce_absence_guard,
    _is_absence_claim,
)

_VERIFY = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"


def test_absence_claim_without_evidence_downgraded():
    findings = [
        {"id": "T-001", "severity": "КРИТИЧЕСКОЕ",
         "finding": "На плане не указана марка двери Д-3."},
    ]
    stats = enforce_absence_guard(findings)
    assert stats["downgraded"] == 1
    assert findings[0]["severity"] == _VERIFY
    assert findings[0]["absence_guard_downgraded"] is True
    assert findings[0]["absence_guard_original_severity"] == "КРИТИЧЕСКОЕ"


def test_absence_claim_with_evidence_kept():
    findings = [
        {"id": "T-001", "severity": "КРИТИЧЕСКОЕ",
         "finding": "Отсутствует спецификация перемычек.",
         "absence_checked": ["Общие указания л.1", "Спецификация л.3"]},
    ]
    stats = enforce_absence_guard(findings)
    assert stats["downgraded"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


def test_absence_claim_empty_evidence_downgraded():
    findings = [
        {"id": "T-001", "severity": "ЭКОНОМИЧЕСКОЕ",
         "finding": "Нет данных о нагрузках на КР.",
         "absence_checked": ["", "  "]},
    ]
    stats = enforce_absence_guard(findings)
    assert stats["downgraded"] == 1
    assert findings[0]["severity"] == _VERIFY


def test_non_absence_finding_untouched():
    findings = [
        {"id": "T-001", "severity": "КРИТИЧЕСКОЕ",
         "finding": "Марка бетона B25 указана неверно, должна быть B30."},
    ]
    stats = enforce_absence_guard(findings)
    assert stats["downgraded"] == 0
    assert stats["absence_claims"] == 0
    assert findings[0]["severity"] == "КРИТИЧЕСКОЕ"


def test_already_verify_severity_not_touched():
    findings = [
        {"id": "T-001", "severity": _VERIFY,
         "finding": "Не указана высота помещения — проверить по АР."},
    ]
    stats = enforce_absence_guard(findings)
    assert stats["downgraded"] == 0
    assert "absence_guard_downgraded" not in findings[0]


def test_mismatch_claims_not_flagged_as_absence():
    # «не соответствует» / «указано неверно» — претензии к значению, НЕ отсутствие.
    assert not _is_absence_claim({"finding": "Значение не соответствует ГОСТ."})
    assert not _is_absence_claim({"finding": "Площадь указана неверно."})
    # «пропущенная цифра/буква» и «недостаточно» — тоже НЕ отсутствие (претензия к значению).
    assert not _is_absence_claim({"finding": "Пропущена цифра в шифре СП (опечатка)."})
    assert not _is_absence_claim({"finding": "Недостаточная площадь тамбура по СП."})
    # А это — отсутствие.
    assert _is_absence_claim({"finding": "Не указана площадь помещения."})
    assert _is_absence_claim({"finding": "Отсутствует узел примыкания."})
    assert _is_absence_claim({"finding": "Не показан узел примыкания к плите."})


def test_malformed_entries_are_skipped():
    findings = ["строка", None, 42,
                {"id": "T-1", "severity": "КРИТИЧЕСКОЕ", "finding": "не указан диаметр"}]
    stats = enforce_absence_guard(findings)
    assert stats["scanned"] == 1
    assert stats["downgraded"] == 1


def test_stats_shape():
    findings = [
        {"severity": "КРИТИЧЕСКОЕ", "finding": "отсутствует лестница"},
        {"severity": "КРИТИЧЕСКОЕ", "finding": "марка неверная"},
    ]
    stats = enforce_absence_guard(findings)
    assert stats == {"scanned": 2, "absence_claims": 1, "downgraded": 1}
