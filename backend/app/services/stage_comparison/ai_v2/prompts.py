"""Whole-document prompts.  The model sees sheet context plus focused evidence."""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from ..ai import identity as table_identity
from . import schemas

SYSTEM_PROMPT = (
    "Ты — инженерный аналитик сравнения проектной документации. У тебя нет "
    "инструментов и доступа к файлам. Истина ограничена переданными "
    "структурированными доказательствами; при пробеле доказательств откажись."
)

_RULES = """ПРАВИЛА AI ANALYST V2

1. Сначала используй SHEET CONTEXT: секции, функциональные узлы, таблицы,
   графовые связи, уже доказанные FAST изменения и внутренние противоречия.
   Затем отвечай на минимальную задачу из FOCUSED EVIDENCE.
2. FAST findings не переписывай и не отменяй. Решения инженера не создавай.
3. selected_candidate_refs выбираются только из candidate_refs задачи.
   Каждый evidence_ref и каждый ref внутри claim обязан существовать во входе.
4. Любое утверждение выражай структурированным claim. Убедительный текст без
   проверяемого claim не является доказательством.
5. Для IDENTITY_FEATURE называй точный attribute и значение, которое реально
   есть у обоих выбранных кандидатов. Номер аппарата сам по себе недостаточен.
6. Для GRAPH_RELATION называй реально существующую связь. Для VALUE копируй
   значение и единицу из evidence. Для ARITHMETIC перечисляй операнды;
   backend сам пересчитает результат.
7. Разные расчётные режимы не объявляй эквивалентными без одинаковой явной
   привязки режима на обеих сторонах. Пустой режим не доказывает равенство.
8. Возможную опечатку разрешено классифицировать как DOCUMENT_ERROR, но
   исходную подпись не исправляй и не подменяй.
9. Если нужен дополнительный контекст, status=NEED_MORE_EVIDENCE, verdict=
   NEED_MORE_EVIDENCE, а requested_evidence — только из закрытого списка.
   Если точного запроса нет, status=UNRESOLVABLE и verdict=UNRESOLVABLE.
10. RESOLVED недопустим с INS UFFICIENT_EVIDENCE, UNKNOWN confidence, пустыми
    evidence_refs или claims. Ответь ровно один раз на каждый task_id.
11. engineering_summary — одно-два предложения по-русски, без новых фактов.
12. Ни один ответ не означает Engineer APPROVED.""".replace(
    "INS UFFICIENT", "INSUFFICIENT"
)


def analyst_prompt(
    *,
    sheet_context: Mapping[str, Any],
    tasks: Sequence[Mapping[str, Any]],
) -> str:
    return "\n\n".join([
        "ЗАДАЧА. Разреши инженерные неоднозначности двух редакций одного "
        "листа по совокупности листа, не по изолированному атому.",
        _RULES,
        "ТИПЫ ЗАДАЧ И ДОПУСТИМЫЕ ВЕРДИКТЫ\n" + json.dumps(
            schemas.VERDICTS_BY_TYPE, ensure_ascii=False, indent=1
        ),
        "SHEET CONTEXT (LEVEL 1)\n" + json.dumps(
            sheet_context, ensure_ascii=False, indent=1
        ),
        "FOCUSED EVIDENCE (LEVEL 2)\n" + json.dumps(
            list(tasks), ensure_ascii=False, indent=1
        ),
        "Ответь строго по JSON Schema.",
    ])


def table_identity_prompt(
    *, sheet_context: Mapping[str, Any], package_view: Mapping[str, Any],
) -> str:
    """Reuse the proven minimal identity contract, with whole-sheet Level 1."""
    return "\n\n".join([
        "SHEET CONTEXT (LEVEL 1). Используй его, чтобы понять секции, "
        "симметричные группы и суммарные строки. Ссылки вопроса ниже имеют "
        "приоритет; контекст не разрешает выдумывать новую строку.",
        json.dumps(sheet_context, ensure_ascii=False, indent=1),
        "FOCUSED EVIDENCE (LEVEL 2)",
        table_identity.identity_prompt(package_view),
    ])


__all__ = ["SYSTEM_PROMPT", "analyst_prompt", "table_identity_prompt"]
