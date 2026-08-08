"""Слоты исполнения на стороне ВОРКЕРА.

Зеркало `backend/app/services/distributed_workers/slots.py`, и это намеренное
дублирование, а не забытый рефакторинг: пакет `audit_worker` ставится на чужой
VPS отдельным комплектом (`requirements-worker.txt`) и не имеет и не должен
иметь ни одного импорта из `backend`. Общая константа, вынесенная «куда-нибудь
в общий модуль», означала бы, что воркеру нужен код центра.

Правило то же самое: доказанный максимум — 2. Значение вне диапазона не роняет
воркер, а зажимается с явным предупреждением — молчаливое зажатие оставило бы
оператора в уверенности, что у него пять слотов.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

MAX_VERIFIED_SLOTS = 2
DEFAULT_MAX_SLOTS = 1


@dataclass(frozen=True)
class SlotLimit:
    value: int
    notice: Optional[str] = None


def normalize_max_slots(raw: Any, *, source: str = "AUDIT_WORKER_MAX_SLOTS") -> SlotLimit:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return SlotLimit(DEFAULT_MAX_SLOTS)
    if isinstance(raw, bool):
        return SlotLimit(
            DEFAULT_MAX_SLOTS,
            f"{source}: логическое значение вместо числа — принято {DEFAULT_MAX_SLOTS}",
        )
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return SlotLimit(
            DEFAULT_MAX_SLOTS,
            f"{source}: нечисловое значение {raw!r} — принято {DEFAULT_MAX_SLOTS}",
        )
    if value < 1:
        return SlotLimit(
            DEFAULT_MAX_SLOTS,
            f"{source}: значение {value} меньше единицы — принято {DEFAULT_MAX_SLOTS}",
        )
    if value > MAX_VERIFIED_SLOTS:
        return SlotLimit(
            MAX_VERIFIED_SLOTS,
            f"{source}: запрошено {value}, но доказанный максимум этого этапа — "
            f"{MAX_VERIFIED_SLOTS}. Принято {MAX_VERIFIED_SLOTS}; поддержка 3–5 "
            "слотов НЕ проверялась и не заявляется.",
        )
    return SlotLimit(value)
