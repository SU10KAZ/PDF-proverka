"""Схемы ответов ИИ. Перечисления берутся из политики, а не дублируются.

Схема — это контракт, а не пожелание: обе CLI умеют принудительный
структурированный вывод, поэтому модель физически не может ответить полем,
которого здесь нет, или значением вне множества.

Чего в схемах НЕТ намеренно:
  * внутренних идентификаторов проекта (project_entity_ref, subject_ref) —
    модель называет объект словами («помещение 24.5»), а стабильную ссылку
    детерминированно чеканит бэкенд тем же кодом, что и для ответа человека;
  * рамок (bbox) — координаты модель не измеряет, она их только цитирует;
  * решения инженера — ИИ не подтверждает находки, он их формирует.
"""
from __future__ import annotations

from ..unified_change_policy.contract import (
    CONFIDENCE_LEVELS,
    DIRECTIONS,
    EVIDENCE_DIMENSIONS,
    OUTCOMES,
)

SCHEMA_VERSION = "stage-comparison-ai.v2"
PROMPT_VERSION = "stage-comparison-ai-analyst.v2"
CRITIC_PROMPT_VERSION = "stage-comparison-ai-critic.v2"
VISION_PROMPT_VERSION = "stage-comparison-ai-vision.v4"

RESOLUTION_STATUSES = (
    "AI_RESOLVED",
    "HUMAN_REQUIRED",
    "INSUFFICIENT_EVIDENCE",
    "CONTRADICTORY",
)

#: Причины, по которым модель обязана отказаться вместо того, чтобы угадать.
REVIEW_REASONS = (
    "SHEET_RELATION_WRONG",       # пара листов не соответствует друг другу
    "ROW_ALIGNMENT_ARTIFACT",     # строки таблиц сопоставлены не с теми
    "FORMATTING_ONLY",            # различие только в записи/OCR
    "EVIDENCE_TRUNCATED",         # видимого текста недостаточно
    "ENTITY_AMBIGUOUS",           # непонятно, о каком объекте речь
    "CONTRADICTORY_EVIDENCE",
    "ENGINEERING_JUDGEMENT_REQUIRED",
    "GRAPHIC_EVIDENCE_REQUIRED",  # текста мало, нужен чертёж
    "NOT_APPLICABLE",             # причин для отказа нет
)

CRITIC_VERDICTS = ("ACCEPT", "REJECT", "HUMAN_REQUIRED")
CRITIC_PROBLEMS = (
    "HALLUCINATED_VALUE",
    "WRONG_ENTITY",
    "WRONG_DIMENSION",
    "WRONG_DIRECTION",
    "LEFT_RIGHT_INVERTED",
    "MISSED_CONTRADICTION",
    "UNSUPPORTED_CONCLUSION",
    "OVERCONFIDENT",
    "NONE",
)

_QUOTE = {
    "type": "object",
    "additionalProperties": False,
    "required": ["side", "evidence_ref", "quote"],
    "properties": {
        "side": {"type": "string", "enum": ["LEFT", "RIGHT"]},
        "evidence_ref": {
            "type": "string",
            "description": (
                "Ссылка строки из пакета: L1, R3… Цитата обязана лежать"
                " именно в ней, а не «где-то на этой стороне»."
            ),
        },
        "quote": {
            "type": "string",
            "description": "Дословная строка из пакета доказательств.",
        },
    },
}

_RESOLUTION = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "item_id", "resolution_status", "dimension", "direction", "outcome",
        "object_label", "object_evidence_ref", "facet_label",
        "before_value", "before_evidence_ref",
        "after_value", "after_evidence_ref",
        "confidence", "evidence_quotes", "needs_human_review",
        "human_reason", "human_question", "engineering_summary",
    ],
    "properties": {
        "item_id": {
            "type": "string",
            "description": "Идентификатор элемента из пакета. Копируется дословно.",
        },
        "resolution_status": {"type": "string", "enum": list(RESOLUTION_STATUSES)},
        "dimension": {"type": "string", "enum": list(EVIDENCE_DIMENSIONS)},
        "direction": {"type": "string", "enum": list(DIRECTIONS)},
        "outcome": {"type": "string", "enum": list(OUTCOMES)},
        "object_label": {
            "type": ["string", "null"],
            "description": (
                "Объект на языке проекта: «помещение 24.5», «кровля К5»."
                " Внутренние идентификаторы не возвращать."
            ),
        },
        "object_evidence_ref": {
            "type": ["string", "null"],
            "description": (
                "Ссылка строки пакета, в которой этот объект НАЗВАН."
                " Без неё разрешение не принимается: «объект где-то тут» —"
                " это не привязка."
            ),
        },
        "facet_label": {
            "type": ["string", "null"],
            "description": "Какое свойство объекта изменилось: «площадь», «толщина слоя».",
        },
        "before_value": {
            "type": ["string", "null"],
            "description": "ТОЧНАЯ подстрока из доказательств LEFT. Не пересказ.",
        },
        "before_evidence_ref": {
            "type": ["string", "null"],
            "description": "Ссылка строки LEFT, в которой лежит before_value.",
        },
        "after_value": {
            "type": ["string", "null"],
            "description": "ТОЧНАЯ подстрока из доказательств RIGHT. Не пересказ.",
        },
        "after_evidence_ref": {
            "type": ["string", "null"],
            "description": "Ссылка строки RIGHT, в которой лежит after_value.",
        },
        "confidence": {"type": "string", "enum": list(CONFIDENCE_LEVELS)},
        "evidence_quotes": {
            "type": "array",
            "description": "Дословные цитаты из пакета, обосновывающие вывод.",
            "items": _QUOTE,
        },
        "needs_human_review": {"type": "boolean"},
        "human_reason": {"type": "string", "enum": list(REVIEW_REASONS)},
        "human_question": {
            "type": ["string", "null"],
            "description": (
                "Если нужен человек — конкретный вопрос по-русски,"
                " на который инженер отвечает за десять секунд."
            ),
        },
        "engineering_summary": {
            "type": "string",
            "description": (
                "Одно-два предложения по-русски: что это значит для проекта."
                " Не рассуждения модели, а обоснование для инженера."
            ),
        },
    },
}

ANALYST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["resolutions"],
    "properties": {
        "resolutions": {
            "type": "array",
            "description": "Ровно по одному разрешению на каждый элемент пакета.",
            "items": _RESOLUTION,
        },
    },
}

CRITIC_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "problems", "explanation"],
    "properties": {
        "verdict": {"type": "string", "enum": list(CRITIC_VERDICTS)},
        "problems": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["code", "detail"],
                "properties": {
                    "code": {"type": "string", "enum": list(CRITIC_PROBLEMS)},
                    "detail": {"type": "string"},
                },
            },
        },
        "explanation": {
            "type": "string",
            "description": "Два-три предложения по-русски для инженера.",
        },
    },
}

VISION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "item_id", "observed_left", "observed_left_image_ref",
        "observed_right", "observed_right_image_ref", "verdict",
        "confidence", "explanation",
    ],
    "properties": {
        "item_id": {"type": "string"},
        "observed_left": {
            "type": ["string", "null"],
            "description": "Что действительно видно на левом фрагменте.",
        },
        "observed_left_image_ref": {
            "type": ["string", "null"],
            "description": (
                "Адрес изображения (IMG-…), НА КОТОРОМ это видно. Копируется"
                " из списка изображений дословно. Без него наблюдение не"
                " принимается: сторону задаёт показанный кадр, а не ключ"
                " ответа."
            ),
        },
        "observed_right": {
            "type": ["string", "null"],
            "description": "Что действительно видно на правом фрагменте.",
        },
        "observed_right_image_ref": {
            "type": ["string", "null"],
            "description": (
                "Адрес изображения (IMG-…), на котором это видно. Копируется"
                " из списка изображений дословно."
            ),
        },
        "verdict": {
            "type": "string",
            "enum": [
                "CONFIRMS_TEXT",       # чертёж подтверждает текстовый вывод
                "CONTRADICTS_TEXT",    # чертёж ему противоречит
                "INSUFFICIENT_IMAGE",  # по картинке не видно
            ],
        },
        "confidence": {"type": "string", "enum": list(CONFIDENCE_LEVELS)},
        "explanation": {"type": "string"},
    },
}

__all__ = [
    "ANALYST_SCHEMA",
    "CRITIC_PROBLEMS",
    "CRITIC_PROMPT_VERSION",
    "CRITIC_SCHEMA",
    "CRITIC_VERDICTS",
    "PROMPT_VERSION",
    "RESOLUTION_STATUSES",
    "REVIEW_REASONS",
    "SCHEMA_VERSION",
    "VISION_PROMPT_VERSION",
    "VISION_SCHEMA",
]
