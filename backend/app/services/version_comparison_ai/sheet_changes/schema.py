"""Схема ответа модели «Изменения в листе» (подмножество JSON Schema для ``--output-schema``).

Координат в схеме нет: рамки доказательств берёт программа из кандидатов.
Все свойства обязательны (строгий структурный вывод); «нет значения» — пустая строка.
"""
from __future__ import annotations

from .. import settings

KINDS = ("replacement", "added", "removed", "quantity", "parameter", "requirement", "route", "relocation",
         "text_correction", "document")
KIND_LABELS = {
    "replacement": "замена", "added": "добавлено", "removed": "удалено", "quantity": "количество",
    "parameter": "параметр", "requirement": "требование", "route": "трасса", "relocation": "перенос",
    "text_correction": "исправлен текст", "document": "документ",
}
MAPPINGS = ("one_to_one", "one_to_many", "many_to_one", "none")
SOURCES = ("text", "image", "text_and_image")
CERTAINTY = ("confirmed", "unclear")
EXCLUDE_REASONS = ("service", "formatting", "label_move", "revision_mark", "duplicate", "not_construction")
EXCLUDE_LABELS = {
    "service": "служебное", "formatting": "оформление", "label_move": "перемещение надписи",
    "revision_mark": "пометка изменения (облако, отметка)", "duplicate": "повтор другого пункта",
    "not_construction": "не строительное",
}
REQUEST_KINDS = ("mark", "zoom", "rows")


def _s(desc: str = "") -> dict:
    return {"type": "string", "description": desc} if desc else {"type": "string"}


def _ids(desc: str) -> dict:
    return {"type": "array", "items": {"type": "string"}, "description": desc}


FIELD = {
    "type": "object", "additionalProperties": False,
    "required": ["name", "before", "after", "unit", "source", "certainty"],
    "properties": {
        "name": _s("что изменилось: марка, размер, количество, длина, сечение, номинал …"),
        "before": _s("значение в A дословно; пусто — не было"),
        "after": _s("значение в B дословно; пусто — нет в B"),
        "unit": _s(),
        "source": {"type": "string", "enum": list(SOURCES)},
        "certainty": {"type": "string", "enum": list(CERTAINTY),
                      "description": "unclear — значение не удалось надёжно прочитать или привязать"},
    },
}

SHEET_CHANGES_SCHEMA: dict = {
    "type": "object", "additionalProperties": False,
    "required": ["packet_id", "part_id", "changes", "unresolved", "excluded", "context_requests"],
    "properties": {
        "packet_id": _s("из задания, дословно"),
        "part_id": _s("из задания, дословно"),
        "changes": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["object", "place", "kind", "summary", "fields", "members", "mapping", "candidate_ids",
                         "image_ids", "other_representation", "limitation"],
            "properties": {
                "object": _s("строительный объект: люк, тип пола, кабельная линия, аппарат, узел …"),
                "place": _s("где: помещение, оси, вид, таблица"),
                "kind": {"type": "string", "enum": list(KINDS)},
                "summary": _s("1–2 предложения «объект/место — было → стало» с марками, размерами и единицами"),
                "fields": {"type": "array", "items": FIELD},
                "members": {"type": "array", "description": "объекты одинаковой или однотипной замены",
                            "items": {"type": "object", "additionalProperties": False,
                                      "required": ["object", "place", "fields"],
                                      "properties": {"object": _s(), "place": _s(),
                                                     "fields": {"type": "array", "items": FIELD}}}},
                "mapping": {"type": "string", "enum": list(MAPPINGS)},
                "candidate_ids": _ids("кандидаты O…, на которых основан пункт"),
                "image_ids": _ids("изображения I…, которые действительно рассмотрены для пункта"),
                "other_representation": {
                    "type": "object", "additionalProperties": False,
                    "required": ["what", "value_a", "value_b", "image_id"],
                    "description": "только для text_correction: то же значение того же объекта в другом "
                                   "представлении (схема, план) в A и в B; иначе пустые строки",
                    "properties": {"what": _s(), "value_a": _s(), "value_b": _s(), "image_id": _s()}},
                "limitation": _s("что осталось неясным в этом пункте, или пусто"),
            }}},
        "unresolved": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["object", "place", "question", "candidate_ids", "image_ids"],
            "properties": {"object": _s(), "place": _s(), "question": _s("что именно не установить"),
                           "candidate_ids": _ids(""), "image_ids": _ids("")}}},
        "excluded": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["candidate_ids", "reason", "note"],
            "properties": {"candidate_ids": _ids(""), "reason": {"type": "string", "enum": list(EXCLUDE_REASONS)},
                           "note": _s()}}},
        "context_requests": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["kind", "value", "side", "candidate_ids", "reason"],
            "properties": {"kind": {"type": "string", "enum": list(REQUEST_KINDS)},
                           "value": _s("mark — обозначение; zoom — ID кандидата; rows — обозначение строки"),
                           "side": {"type": "string", "enum": ["A", "B", "AB"]},
                           "candidate_ids": _ids(""), "reason": _s()}}},
    },
}


def schema_digest() -> str:
    return settings.digest(SHEET_CHANGES_SCHEMA)
