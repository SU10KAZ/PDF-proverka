"""
test_norm_clause_binding.py
---------------------------
Нормативная привязка: пункт в ссылке появляется только после сверки с базой.

Почему этап понадобился: глубина ссылки оказалась свойством модели свода, а не
промпта — claude-opus давал номер пункта у 71% ссылок, codex/gpt-5.4 в шести
прогонах из семи не давал вовсе. Узкая задача с обязательным ответом и возвратом
неподтверждённых пунктов выравнивает поведение: замер 06.08.2026 на 20
замечаниях дал opus 20/20, codex 20/20 (4 номера исправлены во втором раунде),
sonnet 19/20.

Run: python -m pytest tests/test_norm_clause_binding.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.norms import clause_binding as cb  # noqa: E402


class _FakeApi:
    """База знает ГОСТ 21.110-2013 п. 4.2 и ГОСТ Р 21.101-2020 п. 5.1.6."""

    KNOWN = {
        ("ГОСТ 21.110-2013", "4.2"): "4.2 Спецификацию выполняют на бумажном носителе...",
        ("ГОСТ Р 21.101-2020", "5.1.6"): "5.1.6 Рабочие чертежи, предназначенные для...",
    }

    def get_paragraph(self, code, paragraph, max_lines=50):
        text = self.KNOWN.get((code, paragraph))
        if text:
            return {"found": True, "text": text}
        return {"found": False, "text": None, "resolution_reason": "paragraph_not_found"}

    def get_norm_status(self, code):
        return {"matched_code": code, "status": "active"}


def _resolve(api, code):
    return code if code.startswith(("ГОСТ", "СП")) else None


@pytest.fixture()
def api():
    return _FakeApi()


# ─── Отбор ───

def test_targets_include_findings_without_clause():
    findings = [
        {"id": "F-001", "norm": "ГОСТ 21.110-2013 (действует)"},          # только документ
        {"id": "F-002", "norm": "ГОСТ 21.110-2013 (действует), п. 4.2"},  # уже полная
        {"id": "F-003", "norm": None},                                     # нормы нет вовсе
    ]
    ids = [f["id"] for f in cb.select_targets(findings)]
    assert ids == ["F-001", "F-003"]


# ─── Промпт ───

def test_prompt_requires_answer_for_every_finding():
    """Разрешение промолчать модели используют на 100% — его быть не должно."""
    assert "Ответ обязателен для КАЖДОГО" in cb.SYSTEM_PROMPT
    assert "не включай" not in cb.SYSTEM_PROMPT


def test_rejected_clauses_are_fed_back_to_model():
    targets = [{"id": "F-001", "problem": "п", "description": "о"}]
    messages = cb.build_messages(targets, rejected={"F-001": "в «ГОСТ 21.110-2013» нет пункта 9.9"})
    user = messages[1]["content"]
    assert "ОТКЛОНЁН" in user and "нет пункта 9.9" in user


def test_prompt_carries_document_hint_from_merge():
    targets = [{"id": "F-001", "problem": "п", "description": "о", "norm": "ГОСТ 21.110-2013"}]
    assert "свод предположил: ГОСТ 21.110-2013" in cb.build_messages(targets)[1]["content"]


# ─── Разбор ответа ───

def test_parse_plain_json():
    answers = cb.parse_answer('[{"id":"F-001","doc":"ГОСТ 21.110-2013","clause":"4.2"}]')
    assert answers == {"F-001": {"doc": "ГОСТ 21.110-2013", "clause": "4.2"}}


def test_parse_survives_markdown_and_chatter():
    text = 'Вот результат:\n```json\n[{"id":"F-001","doc":"ГОСТ 21.110-2013","clause":"4.2"}]\n```\nГотово.'
    assert "F-001" in cb.parse_answer(text)


def test_parse_rejects_non_clause_values():
    """«таблица 5», диапазоны и разделы целиком — не номер пункта."""
    answers = cb.parse_answer(
        '[{"id":"F-001","doc":"ГОСТ 21.110-2013","clause":"таблица 5"},'
        '{"id":"F-002","doc":"ГОСТ 21.110-2013","clause":"4.2-4.5"}]'
    )
    assert answers == {}


def test_parse_empty_on_garbage():
    assert cb.parse_answer("не знаю") == {}
    assert cb.parse_answer("") == {}


# ─── Сверка с базой ───

def test_validate_accepts_only_existing_clauses(api):
    answers = {
        "F-001": {"doc": "ГОСТ 21.110-2013", "clause": "4.2"},    # есть
        "F-002": {"doc": "ГОСТ 21.110-2013", "clause": "9.9"},    # выдумка
        "F-003": {"doc": "ВСН 999", "clause": "1.1"},             # документа нет
    }
    accepted, rejected = cb.validate(answers, api, _resolve)

    assert set(accepted) == {"F-001"}
    assert accepted["F-001"]["text"].startswith("4.2 Спецификацию")
    assert "нет пункта 9.9" in rejected["F-002"]
    assert "нет в базе" in rejected["F-003"]


def test_validate_survives_broken_index(api):
    """Сбой базы не должен ронять этап — замечание просто не получит пункт."""
    class _Broken(_FakeApi):
        def get_paragraph(self, code, paragraph, max_lines=50):
            raise RuntimeError("индекс недоступен")

    accepted, rejected = cb.validate(
        {"F-001": {"doc": "ГОСТ 21.110-2013", "clause": "4.2"}}, _Broken(), _resolve,
    )
    assert accepted == {} and "не удалась" in rejected["F-001"]


# ─── Запись ───

def test_apply_writes_clause_quote_and_state():
    findings = [{"id": "F-001", "norm": "ГОСТ 21.110-2013 (действует)"}]
    accepted = {
        "F-001": {
            "doc": "ГОСТ 21.110-2013", "canon": "ГОСТ 21_110-2013", "clause": "4.2",
            "text": "4.2 Спецификацию выполняют...", "status": "active",
        }
    }
    assert cb.apply(findings, accepted) == 1
    f = findings[0]
    assert f["norm"] == "ГОСТ 21.110-2013 (действует), п. 4.2"
    assert f["norm_quote"].startswith("4.2 Спецификацию")
    assert f["norm_quote_source"] == "norms_index"
    assert f["norm_paragraph_state"] == "paragraph_verified"


def test_apply_keeps_existing_quote():
    findings = [{"id": "F-001", "norm": "ГОСТ 21.110-2013", "norm_quote": "своя цитата"}]
    cb.apply(findings, {"F-001": {
        "doc": "ГОСТ 21.110-2013", "canon": "ГОСТ 21_110-2013", "clause": "4.2",
        "text": "текст из базы", "status": "active",
    }})
    assert findings[0]["norm_quote"] == "своя цитата"


def test_apply_ignores_unknown_finding_ids():
    findings = [{"id": "F-001", "norm": "ГОСТ 21.110-2013"}]
    assert cb.apply(findings, {"F-777": {
        "doc": "ГОСТ 21.110-2013", "canon": "x", "clause": "4.2", "text": "t", "status": "active",
    }}) == 0
