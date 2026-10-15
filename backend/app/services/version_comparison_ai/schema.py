"""Схема ответа модели на часть листа (план §5.3).

Подмножество JSON Schema, которое проверяет ``stage_comparison.ai.response_contract``
(тот же валидатор, что у ИИ-слоя стадий): type/enum/required/properties/
additionalProperties/items. Координат в схеме нет намеренно — модель только
ссылается на ID записей, рамки берёт программа.
"""
from __future__ import annotations

from . import settings

CATEGORIES = ("content", "formatting", "service", "needs_check")
FATES = ("described", "merged", "service", "formatting", "needs_check", "not_reviewed")

CATEGORY_LABELS = {
    "content": "Изменение содержания",
    "formatting": "Оформление",
    "service": "Служебное",
    "needs_check": "Нужна проверка",
}
FATE_LABELS = {
    "described": "описана",
    "merged": "объединена с другим пунктом",
    "service": "служебная",
    "formatting": "оформление",
    "needs_check": "неоднозначна — нужна проверка",
    "not_reviewed": "не просмотрена",
}


def _str(desc: str = "") -> dict:
    return {"type": "string", "description": desc} if desc else {"type": "string"}


def _ids(desc: str) -> dict:
    return {"type": "array", "items": {"type": "string"}, "description": desc}


ANSWER_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["packet_id", "chunk_id", "items", "dispositions", "unreadable_images", "context_requests"],
    "properties": {
        "packet_id": _str("packet_id из задания, дословно"),
        "chunk_id": _str("chunk_id из задания, дословно"),
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["item_id", "category", "cross_check", "summary", "location", "values",
                             "evidence_ids", "context_ids", "images_used", "limitation"],
                "properties": {
                    "item_id": _str("I1, I2, … внутри ответа"),
                    "category": {"type": "string", "enum": list(CATEGORIES)},
                    "cross_check": {"type": "boolean",
                                    "description": "true — расхождение между разными листами/документами"},
                    "summary": _str("1–2 предложения: что, где, с чего на что; числа, марки и единицы дословно"),
                    "location": _str("где на листе, словами"),
                    "values": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["evidence_id", "before", "after", "unit"],
                            "properties": {"evidence_id": _str(), "before": _str("значение в A дословно или пусто"),
                                           "after": _str("значение в B дословно или пусто"), "unit": _str()},
                        },
                    },
                    "evidence_ids": _ids("ID записей T/G/M/S/Z/W/N"),
                    "context_ids": _ids("ID контекстов C/X"),
                    "images_used": _ids("ID изображений I, которые действительно просмотрены"),
                    "limitation": _str("ограничение вывода или пусто"),
                },
            },
        },
        "dispositions": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "fate", "item_id", "reason"],
                "properties": {"id": _str(), "fate": {"type": "string", "enum": list(FATES)},
                               "item_id": _str("пункт, к которому отнесена запись, или пусто"),
                               "reason": _str()},
            },
        },
        "unreadable_images": {
            "type": "array",
            "items": {"type": "object", "additionalProperties": False, "required": ["image_id", "reason"],
                      "properties": {"image_id": _str(), "reason": _str()}},
        },
        "context_requests": {
            "type": "array",
            "items": {"type": "object", "additionalProperties": False, "required": ["for_ids", "query", "reason"],
                      "properties": {"for_ids": _ids(""), "query": _str(), "reason": _str()}},
        },
    },
}


def schema_digest() -> str:
    return settings.digest(ANSWER_SCHEMA)
