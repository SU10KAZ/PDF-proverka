"""Транспортная адаптация этапа `text_analysis` под ProviderAdapter (этап 11D).

Разделение, ради которого модуль существует, — §13 задания:

    A. ИНЖЕНЕРНОЕ СОДЕРЖАНИЕ    B. ТРАНСПОРТНАЯ ОБОЛОЧКА
    роль дисциплины             «прочитай файл через Read»
    чек-лист                    «запиши результат через Write»
    норм-база                   «не выводи в чат»
    страж отсутствия            путь выходного файла
    pre-scan                    абсолютные пути к артефактам
    JSON-схема
    правила severity
    тело MD

A обязано дойти до модели дословно. B в provider-режиме не просто лишнее — оно
ВРЕДНО: адаптер запускает CLI с `--tools=` и полным `--disallowed-tools`, у
модели нет ни Read, ни Write, ни Bash. Инструкция «прочитай {MD_FILE_PATH}»
адресована инструменту, которого не существует, и единственный её эффект —
модель отвечает сводкой о невыполнимой задаче вместо JSON.

Откуда берётся A. Не из нового промпта, написанного «с нуля» (§13 это прямо
запрещает), а из УЖЕ БОЕВОГО сборщика `prompt_builder.build_text_analysis_messages`
— того самого, которым сегодня работает ветка OpenRouter. Он уже читает MD
силами конвейера, уже вкладывает норм-базу и pre-scan inline и уже снимает
CLI-инструкции через `_clean_template_for_api`. Задача этого модуля — привести
его двухсообщенный вид к одному тексту для stdin и дочистить то, чего
`_clean_template_for_api` не снимает.

Что дочищается и почему это не косметика. `_clean_template_for_api` удаляет
СТРОКИ, содержащие «Read tool»/«Write tool», но упоминания
`{BLOCKS_ANALYSIS_PATH}` в шагах задачи (строки 65 и 128 шаблона) в эти правила
не попадают — и в промпт уезжает абсолютный путь вида
`/home/…/_output/01_blocks_analysis.json`. Для HTTP-транспорта это безвредно,
для 11D — прямое нарушение §14 («модель не получает путь проекта»). Поэтому
system-часть проходит детерминированную зачистку файловых ссылок.

Тело документа (user-часть) НЕ зачищается никогда. Это данные аудита, а не
инструкции: правка внутри них была бы искажением исходного текста, который
модель обязана проверять дословно.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Optional

#: Чем заменяется абсолютный путь в инструкциях. Формулировка отвечает на
#: вопрос, который иначе возник бы у модели: «а где же тогда данные».
FILESYSTEM_PLACEHOLDER = "(inlined below; no filesystem access)"

#: Абсолютный POSIX-путь. Требование «слэш не после словесного символа»
#: отсекает дроби и конструкции вроде `м3/ч`, `и/или`, `01/02/2026`: у них
#: перед слэшем стоит буква или цифра. Требование «хотя бы один внутренний
#: сегмент» отсекает одиночный корень и не трогает `/` как знак.
_ABS_PATH_RE = re.compile(r"(?<![\w])/(?:[^\s/`'\"<>]+/)+[^\s/`'\"<>]*")

#: Транспортный контракт, заменяющий блок B. Английский — как и весь боевой
#: шаблон этапа; смешивать языки в одном промпте незачем.
TRANSPORT_CONTRACT = """## OUTPUT TRANSPORT

You have NO tools in this run: no file reading, no file writing, no shell, no
search. Everything you need is already inlined above — there is nothing to open
and nothing to look up.

Return your result as ONE JSON object in your reply, matching the schema above.
- no markdown code fences,
- no explanation before or after the JSON,
- no summary text.

The pipeline itself parses your reply, validates it and persists it to the
output file. Your only job in this run is the analysis and the JSON."""

#: Поля, отсутствие которых делает артефакт непригодным дальше по конвейеру.
#: Список короткий намеренно: сюда попадает только то, что реально читают
#: следующие этапы и проверяет боевой раннер (`text_findings` он проверяет
#: сам, отдельной строкой кода).
REQUIRED_RESULT_FIELDS: tuple[str, ...] = (
    "text_source",
    "project_params",
    "normative_refs_found",
    "text_findings",
)

FIELD_TYPES: dict[str, Any] = {
    "text_source": str,
    "project_params": dict,
    "normative_refs_found": list,
    "text_findings": list,
    "items_verified_from_blocks": list,
}

#: `text_source` — единственное смысловое ожидание, и оно жёсткое: правило
#: платформы «production-аудит принимает только md» не терпит исключений, а
#: значение прямо продиктовано модели в user-сообщении.
EXPECTED_SEMANTICS: dict[str, Any] = {
    "text_source": "md",
}

#: Поля, чьё отсутствие фиксируется, но НЕ роняет этап. Они косметические:
#: конвейер знает и проект, и имя этапа без модели. Тянуть их в жёсткий
#: контракт значило бы превратить исправный аудит в отказ из-за подписи.
SOFT_RESULT_FIELDS: tuple[str, ...] = ("stage", "project_id", "timestamp")


def strip_filesystem_references(text: str) -> tuple[str, int]:
    """Убрать абсолютные пути из ИНСТРУКЦИЙ. Возвращает (текст, сколько убрано)."""
    raw = str(text or "")
    replaced = 0

    def _sub(match: "re.Match[str]") -> str:
        nonlocal replaced
        replaced += 1
        return FILESYSTEM_PLACEHOLDER

    return _ABS_PATH_RE.sub(_sub, raw), replaced


def _message_text(content: Any) -> str:
    """Текст сообщения. Мультимодальные части здесь невозможны, но форма общая."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts)
    return ""


def split_messages(messages: Iterable[dict]) -> tuple[str, str]:
    """Разложить messages на инструкции (system) и документ (user)."""
    system_parts: list[str] = []
    user_parts: list[str] = []
    for message in messages or []:
        role = str((message or {}).get("role") or "")
        text = _message_text((message or {}).get("content"))
        if not text:
            continue
        if role == "system":
            system_parts.append(text)
        else:
            user_parts.append(text)
    return "\n\n".join(system_parts), "\n\n".join(user_parts)


def build_provider_prompt(messages: Iterable[dict]) -> dict[str, Any]:
    """Собрать один текст для stdin `claude -p` из боевых messages этапа.

    Возвращает не только промпт, но и КАРТУ сборки: сколько символов пришло из
    инструкций, сколько из документа, сколько путей вычищено. Карта уезжает в
    артефакт прогона — без неё «промпт собран правильно» пришлось бы принимать
    на слово, а разбирать чужой прогон было бы нечем.
    """
    system_raw, user_text = split_messages(messages)
    system_text, stripped = strip_filesystem_references(system_raw)
    prompt = (
        f"{system_text}\n\n"
        "===== SOURCE DOCUMENT (inlined by the pipeline) =====\n\n"
        f"{user_text}\n\n"
        "===== END OF SOURCE DOCUMENT =====\n\n"
        f"{TRANSPORT_CONTRACT}\n"
    )
    return {
        "prompt": prompt,
        "system_chars": len(system_text),
        "document_chars": len(user_text),
        "prompt_chars": len(prompt),
        "filesystem_refs_stripped": stripped,
        "absolute_paths_remaining_in_instructions": count_absolute_paths(system_text),
    }


def count_absolute_paths(text: str) -> int:
    """Сколько абсолютных путей осталось. Для проверки, а не для зачистки."""
    return len(_ABS_PATH_RE.findall(str(text or "")))


#: Опорные признаки инженерного содержания. Проверка semantic preservation
#: (§13) сверяет НАЛИЧИЕ каждого в двух промптах — боевом API-промпте и
#: provider-промпте. Сравнивать тексты целиком нельзя: они и обязаны
#: различаться транспортной частью; сравнивать нужно то, что различаться НЕ
#: имеет права.
ENGINEERING_MARKERS: tuple[tuple[str, str], ...] = (
    ("json_schema", '"text_findings"'),
    ("severity_enum", "РЕКОМЕНДАТЕЛЬНОЕ"),
    ("stage_name", "02_text_analysis"),
    ("arithmetic_rule", "Recalculate sums in EACH load table"),
    ("cross_reference_rule", "Cross-reference verification"),
    ("spec_image_crosscheck", "Specification vs [IMAGE] cross-check"),
    ("norm_quote_rule", "norm_quote"),
    ("adjacent_discipline_rule", "ПРОВЕРИТЬ ПО СМЕЖНЫМ"),
    ("text_source_field", '"text_source"'),
    ("output_language_rule", "OUTPUT LANGUAGE"),
)

#: Признаки транспортной оболочки, которых в provider-промпте быть НЕ должно.
FORBIDDEN_TRANSPORT_MARKERS: tuple[tuple[str, str], ...] = (
    ("read_tool", "Read tool"),
    ("write_tool", "Write tool"),
    ("write_via", "WRITE via"),
    ("read_via", "READ via"),
    ("no_chat_output", "DO NOT output to chat"),
    ("brief_summary", "After writing, output a brief summary"),
)


def engineering_markers_present(text: str) -> dict[str, bool]:
    """Какие опорные признаки инженерной части присутствуют в тексте."""
    raw = str(text or "")
    return {name: (needle in raw) for name, needle in ENGINEERING_MARKERS}


def transport_markers_present(text: str) -> dict[str, bool]:
    """Какие признаки транспортной оболочки присутствуют в тексте."""
    raw = str(text or "")
    return {name: (needle in raw) for name, needle in FORBIDDEN_TRANSPORT_MARKERS}


def semantic_preservation_report(
    *, api_prompt: str, provider_prompt: str
) -> dict[str, Any]:
    """Сверка «инженерное сохранено, транспортное снято».

    Базой сравнения служит ИМЕННО API-промпт (ветка OpenRouter), а не сырой
    CLI-шаблон. Причина простая: API-промпт — уже боевой, уже прошедший
    `_clean_template_for_api`, и разница между ним и provider-промптом
    показывает вклад ровно этого этапа. Сравнение с сырым шаблоном смешало бы
    правку 11D с давно принятым решением о ветке API.
    """
    api_markers = engineering_markers_present(api_prompt)
    provider_markers = engineering_markers_present(provider_prompt)
    lost = sorted(
        name for name, present in api_markers.items()
        if present and not provider_markers.get(name)
    )
    transport = transport_markers_present(provider_prompt)
    leaked = sorted(name for name, present in transport.items() if present)
    return {
        "engineering_markers_api": api_markers,
        "engineering_markers_provider": provider_markers,
        "engineering_lost": lost,
        "transport_markers_leaked": leaked,
        "absolute_paths_in_provider_instructions": count_absolute_paths(
            provider_prompt.split("===== SOURCE DOCUMENT", 1)[0]
        ),
        "passed": not lost and not leaked,
    }


def soft_contract_report(payload: Optional[dict]) -> dict[str, Any]:
    """Мягкая часть контракта: что есть, чего нет. Ничего не роняет."""
    data = payload if isinstance(payload, dict) else {}
    return {
        "present": sorted(name for name in SOFT_RESULT_FIELDS if name in data),
        "missing": sorted(name for name in SOFT_RESULT_FIELDS if name not in data),
    }
