"""reserc.md #74 — детекция усечения для локального /api/v1/chat.

Раньше _run_local_chandra_chat ставил finish_reason='stop' при наличии content →
усечённый JSON на structured-этапах проходил как успех (в отличие от
_run_local_chat_completions, где это ловится). Логика вынесена в чистый
_is_local_structured_truncation.
"""
from __future__ import annotations

from backend.app.services.llm import llm_runner as lr


def test_valid_json_never_truncation():
    assert lr._is_local_structured_truncation(
        finish_reason="length", json_data={"ok": 1},
        expects_json=True, out_tokens=999, max_tokens=100,
    ) is False


def test_plain_text_stage_not_flagged():
    # Не structured-этап (нет response_format) — длинный текст не ошибка.
    assert lr._is_local_structured_truncation(
        finish_reason="length", json_data=None,
        expects_json=False, out_tokens=5000, max_tokens=5000,
    ) is False


def test_structured_finish_length_no_json_is_truncation():
    assert lr._is_local_structured_truncation(
        finish_reason="length", json_data=None,
        expects_json=True, out_tokens=10, max_tokens=4000,
    ) is True


def test_structured_hit_cap_no_finish_reason_is_truncation():
    # chandra не отдал finish_reason, но упёрлись в max_tokens и JSON нет.
    assert lr._is_local_structured_truncation(
        finish_reason="", json_data=None,
        expects_json=True, out_tokens=4000, max_tokens=4000,
    ) is True


def test_structured_under_cap_no_json_not_truncation():
    # JSON нет, но и усечения нет (вышли по eos под лимитом) — не ошибка усечения.
    assert lr._is_local_structured_truncation(
        finish_reason="stop", json_data=None,
        expects_json=True, out_tokens=120, max_tokens=4000,
    ) is False
