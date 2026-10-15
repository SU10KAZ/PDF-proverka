"""Идентификаторы записей пакета (план §4.1).

Короткий ID в пакете уникален внутри пары листов и воспроизводим: номер берётся
из ID движка (``t134`` → ``T134``, ``r254`` → ``G254``, ``b1`` → ``M1``, ``s1`` →
``S1``). Служебным полям, наложениям и аннотациям движок ID не даёт — их номер
задаёт порядок в ``result.json``. Полный ID записи —
``<object_id>/<run_id>/<spid>/<ref>``; соответствие хранится в манифесте.
"""
from __future__ import annotations

import re

PREFIX = {
    "text": "T", "region": "G", "block": "M", "spec": "S", "service": "Z", "overlay": "W",
    "annotation": "N", "context": "C", "xref": "X", "image": "I",
}
GROUP_LABELS = {
    "T": "текстовое изменение", "G": "регион отличия", "M": "перемещённый блок", "S": "позиция СО",
    "Z": "служебное поле или зона", "W": "наложенный объект", "N": "пометка (аннотация)",
    "C": "контекст", "X": "межлистовой контекст", "I": "изображение",
}
_NUM = re.compile(r"(\d+)$")
LOCAL_ID = re.compile(r"^[TGMSZWNCXI]\d+$")


def local(kind: str, engine_id: str) -> str:
    m = _NUM.search(str(engine_id))
    if not m:
        raise ValueError(f"нет номера в ID движка: {engine_id!r}")
    return f"{PREFIX[kind]}{int(m.group(1))}"


def image(n: int) -> str:
    return f"I{n:03d}"


def full(object_id: str, run_id: str, spid: str, ref: str) -> str:
    return f"{object_id}/{run_id}/{spid}/{ref}"


def is_local(value: str) -> bool:
    return bool(LOCAL_ID.match(str(value or "")))
