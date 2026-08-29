"""Проверка ответа модели против ТОЙ ЖЕ схемы, которая была ей отправлена.

Зачем второй рубеж, если обе CLI умеют принудительный структурированный
вывод. Потому что «модель физически не может ответить неправильно» — это
утверждение о ЖИВОМ вызове, а бэкенд читает ответы не только оттуда:

  * из кэша, записанного прошлой версией слоя;
  * из артефакта прогона, сделанного месяц назад;
  * из stdout, который мог быть обрезан по дороге;
  * из будущей версии CLI, где флаг переименовали;
  * из подменённого провайдера в тестах и в резервном пути.

В каждом из этих случаев структура не гарантирована никем, а решение по
ответу принимается настоящее. Поэтому контракт проверяется здесь — один раз,
по данным самой схемы, а не выборочными `if` по отдельным полям: иначе
добавленное в схему поле остаётся непроверенным ровно до первого инцидента.

Поддерживается подмножество JSON Schema, которым описаны схемы слоя. Всё, что
за его пределами, — не «пропускаем», а UnsupportedSchemaError: валидатор,
молча игнорирующий незнакомое ограничение, хуже отсутствующего, потому что
создаёт ложную уверенность. Полнота покрытия закреплена тестом.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

#: Ключевые слова схемы, которые валидатор действительно проверяет.
#: `description` и `$schema` носят пояснительный характер и ничего не
#: ограничивают, поэтому перечислены как заведомо безопасные.
SUPPORTED_KEYWORDS = frozenset({
    "type",
    "enum",
    "const",
    "required",
    "properties",
    "additionalProperties",
    "items",
    "description",
    "title",
    "$schema",
})

_TYPE_CHECKS = {
    "object": lambda value: isinstance(value, Mapping),
    "array": lambda value: (
        isinstance(value, Sequence) and not isinstance(value, (str, bytes))
    ),
    "string": lambda value: isinstance(value, str),
    # bool — подкласс int в Python; для схемы это разные типы.
    "number": lambda value: (
        isinstance(value, (int, float)) and not isinstance(value, bool)
    ),
    "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "boolean": lambda value: isinstance(value, bool),
    "null": lambda value: value is None,
}


class UnsupportedSchemaError(ValueError):
    """Схема содержит ограничение, которое этот валидатор не проверяет."""


def _keyword_guard(schema: Mapping[str, Any], path: str) -> None:
    unknown = sorted(set(schema) - SUPPORTED_KEYWORDS)
    if unknown:
        raise UnsupportedSchemaError(
            f"{path or 'корень'}: валидатор не проверяет {', '.join(unknown)}"
        )


def _describe(value: Any) -> str:
    if value is None:
        return "null"
    return {
        bool: "boolean", int: "number", float: "number", str: "string",
        list: "array", tuple: "array", dict: "object",
    }.get(type(value), type(value).__name__)


def _validate(
    value: Any,
    schema: Mapping[str, Any],
    path: str,
    errors: list[str],
) -> None:
    if not isinstance(schema, Mapping):
        raise UnsupportedSchemaError(f"{path or 'корень'}: схема не объект")
    _keyword_guard(schema, path)
    where = path or "ответ"

    declared = schema.get("type")
    if declared is not None:
        allowed = [declared] if isinstance(declared, str) else list(declared)
        for name in allowed:
            if name not in _TYPE_CHECKS:
                raise UnsupportedSchemaError(f"{where}: неизвестный тип {name}")
        if not any(_TYPE_CHECKS[name](value) for name in allowed):
            errors.append(
                f"{where}: ожидался тип {'|'.join(allowed)},"
                f" получен {_describe(value)}"
            )
            # Дальше проверять нечего: ограничения ниже говорят о другом типе.
            return

    if "const" in schema and value != schema["const"]:
        errors.append(f"{where}: допустимо только {schema['const']!r}")
        return

    if "enum" in schema:
        allowed_values = list(schema["enum"])
        if value not in allowed_values:
            errors.append(
                f"{where}: значение {value!r} вне списка допустимых"
                f" ({', '.join(repr(item) for item in allowed_values)})"
            )
            return

    if isinstance(value, Mapping):
        properties = schema.get("properties") or {}
        if not isinstance(properties, Mapping):
            raise UnsupportedSchemaError(f"{where}: properties не объект")
        for name in schema.get("required") or ():
            if name not in value:
                errors.append(f"{where}: отсутствует обязательное поле {name}")
        if schema.get("additionalProperties") is False:
            for name in sorted(set(value) - set(properties)):
                errors.append(f"{where}: поле {name} схемой не предусмотрено")
        elif "additionalProperties" in schema and schema["additionalProperties"] is not True:
            raise UnsupportedSchemaError(
                f"{where}: additionalProperties как схема не поддерживается"
            )
        for name, sub_schema in properties.items():
            if name in value:
                _validate(value[name], sub_schema, f"{where}.{name}", errors)
        return

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        items = schema.get("items")
        if items is None:
            return
        if not isinstance(items, Mapping):
            raise UnsupportedSchemaError(
                f"{where}: items кортежем не поддерживается"
            )
        for index, entry in enumerate(value):
            _validate(entry, items, f"{where}[{index}]", errors)


def validate(payload: Any, schema: Mapping[str, Any]) -> list[str]:
    """Список нарушений контракта по-русски. Пустой список = ответ пригоден.

    Незнакомое схеме ограничение — не «пропускаем», а нарушение: проверить
    его нечем, значит гарантии нет, а отсутствие гарантии обязано выглядеть
    как отказ, а не как успех.
    """
    errors: list[str] = []
    try:
        _validate(payload, schema, "", errors)
    except UnsupportedSchemaError as error:
        return [f"контракт схемы не проверяем: {error}"]
    return errors


def is_valid(payload: Any, schema: Mapping[str, Any]) -> bool:
    return not validate(payload, schema)


__all__ = [
    "SUPPORTED_KEYWORDS",
    "UnsupportedSchemaError",
    "is_valid",
    "validate",
]
