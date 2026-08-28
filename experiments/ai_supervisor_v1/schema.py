"""Схема ответа AI Analyst. Согласована с закрытыми enum'ами G2.4.5.

Источник enum'ов: backend/app/services/stage_comparison/unified_change_policy/contract.py
"""
from __future__ import annotations

DIMENSIONS = [
    "PRINCIPLE", "METHOD", "OPERATION", "STRUCTURE", "CONNECTION",
    "TYPE", "PARAMETER", "QUANTITY", "SPACE", "UNKNOWN_DIMENSION",
]
OUTCOMES = ["MATERIAL_CHANGE", "DETAIL_ONLY", "REVIEW_REQUIRED"]
DIRECTIONS = ["ADDED", "REMOVED", "REPLACED", "INCREASED", "DECREASED", "ALTERED"]
RESOLUTION_STATUS = ["AI_RESOLVED", "HUMAN_REQUIRED", "INSUFFICIENT_EVIDENCE", "CONTRADICTORY"]
CONFIDENCE = ["HIGH", "MEDIUM", "LOW", "UNKNOWN"]

# Причины, по которым модель обязана отказаться, вместо того чтобы угадывать.
REVIEW_REASONS = [
    "SHEET_RELATION_WRONG",      # пара листов не соответствует друг другу
    "ROW_ALIGNMENT_ARTIFACT",    # строки таблиц сопоставлены не с теми
    "FORMATTING_ONLY",           # различие только в форматировании/OCR
    "EVIDENCE_TRUNCATED",        # видимого текста недостаточно
    "ENTITY_AMBIGUOUS",          # непонятно, о каком объекте речь
    "CONTRADICTORY_EVIDENCE",
    "ENGINEERING_JUDGEMENT_REQUIRED",
    "NOT_APPLICABLE",            # причин для отказа нет
]

ANALYST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "resolution_status", "dimension", "direction", "outcome",
        "before_value", "after_value", "subject_label",
        "engineering_significance", "confidence", "evidence_quotes",
        "needs_human_review", "review_reason", "review_question", "reasoning_summary",
    ],
    "properties": {
        "resolution_status": {"type": "string", "enum": RESOLUTION_STATUS},

        "dimension": {"type": "string", "enum": DIMENSIONS},
        "direction": {"type": "string", "enum": DIRECTIONS},
        "outcome": {"type": "string", "enum": OUTCOMES},

        "before_value": {
            "type": ["string", "null"],
            "description": "ТОЧНАЯ подстрока из доказательств LEFT. Не пересказ, не нормализация.",
        },
        "after_value": {
            "type": ["string", "null"],
            "description": "ТОЧНАЯ подстрока из доказательств RIGHT.",
        },
        "subject_label": {
            "type": ["string", "null"],
            "description": "О каком объекте речь на языке проекта: помещение 24.5, кровля К5.",
        },
        "engineering_significance": {
            "type": "string",
            "description": "Одно предложение по-русски: что это значит для проекта.",
        },
        "confidence": {"type": "string", "enum": CONFIDENCE},

        "evidence_quotes": {
            "type": "array",
            "description": "Дословные цитаты из пакета, обосновывающие вывод. Только то, что есть в пакете.",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["side", "quote"],
                "properties": {
                    "side": {"type": "string", "enum": ["LEFT", "RIGHT"]},
                    "quote": {"type": "string"},
                },
            },
        },

        "needs_human_review": {"type": "boolean"},
        "review_reason": {"type": "string", "enum": REVIEW_REASONS},
        "review_question": {
            "type": ["string", "null"],
            "description": "Если нужен человек — конкретный вопрос по-русски, на который он ответит за 10 секунд.",
        },
        "reasoning_summary": {
            "type": "string",
            "description": "2-4 предложения по-русски. Не внутренние рассуждения, а обоснование для инженера.",
        },
    },
}


SHEET_MATCH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["pairs", "unmatched_left", "unmatched_right", "reasoning_summary"],
    "properties": {
        "pairs": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["left_page", "right_page", "confidence", "basis"],
                "properties": {
                    "left_page": {"type": "integer"},
                    "right_page": {"type": "integer"},
                    "confidence": {"type": "string", "enum": CONFIDENCE},
                    "basis": {"type": "string", "description": "дословный признак из входа"},
                },
            },
        },
        "unmatched_left": {"type": "array", "items": {"type": "integer"}},
        "unmatched_right": {"type": "array", "items": {"type": "integer"}},
        "reasoning_summary": {"type": "string"},
    },
}


CRITIC_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "problems", "reasoning_summary"],
    "properties": {
        "verdict": {"type": "string", "enum": ["ACCEPT", "RETRY", "REQUEST_EVIDENCE", "HUMAN_REQUIRED"]},
        "problems": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["code", "detail"],
                "properties": {
                    "code": {
                        "type": "string",
                        "enum": [
                            "HALLUCINATED_VALUE", "WRONG_ENTITY", "WRONG_DIMENSION",
                            "WRONG_DIRECTION", "LEFT_RIGHT_INVERTED", "MISSED_CONTRADICTION",
                            "UNSUPPORTED_CONCLUSION", "OVERCONFIDENT", "NONE",
                        ],
                    },
                    "detail": {"type": "string"},
                },
            },
        },
        "reasoning_summary": {"type": "string"},
    },
}
