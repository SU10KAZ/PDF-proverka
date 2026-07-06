"""Тесты «умного стража отсутствия» (Stage 01 post-pass).

Логика двухтактная: детектор-пре-фильтр + подтверждение присутствия по документу.
Понижаются ТОЛЬКО подтверждённо-ложные (verifier=present). Без верификатора —
безопасный режим (не понижаем ничего).
"""
from __future__ import annotations

from backend.app.pipeline.stages.text_analysis.absence_guard import (
    enforce_absence_guard,
    _is_absence_claim,
    _candidate_text,
    build_verification_prompt,
    parse_verification_response,
    _split_md_into_chunks,
    _merge_chunk_verdicts,
    run_claude_verification_chunked,
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


# ── Текст кандидата (A1: слитый 03_findings.json без поля finding) ──

def test_candidate_text_prefers_finding_then_problem_then_description():
    assert _candidate_text({"finding": "F", "problem": "P"}) == "F"
    assert _candidate_text({"problem": "P", "description": "D"}) == "P"
    assert _candidate_text({"description": "D"}) == "D"
    assert _candidate_text({}) == ""


def test_detector_reads_problem_field():
    # В слитом 03_findings.json суть замечания в problem/description, не в finding.
    assert _is_absence_claim({"problem": "Отсутствует спецификация перемычек."})
    assert _is_absence_claim({"description": "Не указана огнестойкость."})


# ── Промпт/парсер верификатора ──

def test_build_prompt_lists_candidates():
    p = build_verification_prompt("DOC", [{"finding": "Нет данных X"}])
    assert "DOC" in p and "0)" in p and "Нет данных X" in p


def test_build_prompt_uses_problem_when_no_finding():
    # A1: у слитого замечания нет finding — промпт должен взять problem.
    p = build_verification_prompt("DOC", [{"problem": "Отсутствует лист АР-5"}])
    assert "Отсутствует лист АР-5" in p


# ── Чанкинг больших MD (A2) ──

def test_split_md_into_chunks_by_size():
    md = "".join(f"строка {i}\n" for i in range(100))
    chunks = _split_md_into_chunks(md, 50)
    assert len(chunks) > 1
    assert "".join(chunks) == md  # без потерь и нахлёста
    assert all(len(c) <= 50 or "\n" not in c[:-1] for c in chunks)


def test_split_md_hard_splits_long_line():
    md = "x" * 250  # одна строка длиннее целевого размера
    chunks = _split_md_into_chunks(md, 100)
    assert len(chunks) == 3
    assert "".join(chunks) == md


def test_merge_chunk_verdicts_present_wins():
    # кусок 0 не нашёл (absent), кусок 1 нашёл (present) → present по ИЛИ
    r0 = {0: "absent", "0_evidence": "нет здесь"}
    r1 = {0: "present", "0_evidence": "есть на л.5"}
    out = _merge_chunk_verdicts([r0, r1], 1)
    assert out[0] == "present"
    assert out["0_evidence"] == "есть на л.5"


def test_merge_chunk_verdicts_all_absent():
    out = _merge_chunk_verdicts([{0: "absent"}, {}], 1)
    assert out[0] == "absent"


def test_chunked_small_md_single_call():
    calls = []
    def _fn(md, cands):
        calls.append(md)
        return {0: "absent"}
    out = run_claude_verification_chunked(
        "короткий MD", [{"problem": "нет X"}], verify_fn=_fn, threshold_chars=1000,
    )
    assert len(calls) == 1  # не резали
    assert out[0] == "absent"


def test_chunked_large_md_splits_and_or_aggregates():
    md = "".join(f"строка {i}\n" for i in range(500))  # достаточно крупный
    seen = []
    def _fn(chunk, cands):
        seen.append(chunk)
        # present только если в куске есть «строка 400»
        return {0: "present", "0_evidence": "тут"} if "строка 400" in chunk else {0: "absent"}
    out = run_claude_verification_chunked(
        md, [{"problem": "нет строки 400"}],
        verify_fn=_fn, threshold_chars=100, target_tokens=50, chars_per_token=1.0, workers=2,
    )
    assert len(seen) > 1              # порезали на куски
    assert out[0] == "present"        # OR-агрегация нашла present в одном из кусков


def test_chunked_retries_empty_chunk_once():
    md = "".join(f"строка {i}\n" for i in range(500))
    attempts = {"n": 0}
    def _fn(chunk, cands):
        attempts["n"] += 1
        if attempts["n"] == 1:
            return {}  # первый вызов — сбой (пусто) → должен ретрайнуться
        return {0: "absent"}
    run_claude_verification_chunked(
        md, [{"problem": "нет X"}],
        verify_fn=_fn, threshold_chars=100, target_tokens=50, chars_per_token=1.0, workers=1,
    )
    # хотя бы один кусок ретрайнулся: вызовов больше, чем кусков
    chunks = _split_md_into_chunks(md, 50)
    assert attempts["n"] > len(chunks)


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


# Пост-проходы absence guard в Stage 01/02 сняты — механизм вынесен в отдельный
# этап «Верификатор» (findings_verify). Интеграционные тесты этапа (детерм. проверки +
# absence-понижение + безопасный режим) — в test_findings_verify_stage.py.
