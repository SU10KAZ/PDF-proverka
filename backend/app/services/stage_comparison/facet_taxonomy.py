"""Справочник свойств объекта: что вообще может меняться и как это назвать.

Детерминированный слой знает свойство точно — он его и распознал: колонка
экспликации «площадь» становится ``room_area_m2``, колонка таблицы нагрузок —
``demand_active_power_kw``. Модель же присылает свойство человеческим словом
(«площадь», «высота потолка»), и без этого справочника два названия нечем
сопоставить: строка «высота потолка» ничем не хуже строки «площадь», если
сравнивать их со свободным текстом.

Отсюда два правила.

* Если детерминированный слой УЖЕ назвал свойство, спорить с ним нельзя.
  Доказана площадь — «высота потолка» не оттенок формулировки, а другое
  свойство того же объекта, и публиковать его нельзя ни при какой
  уверенности модели.
* Если свойство не установлено, модель может его предложить — но только
  словом из этого справочника. Иначе backend чеканил бы ``facet_ai_<что
  угодно>``: идентификатор, за которым не стоит ничего, кроме того, что так
  написала модель.

Справочник закрыт и намеренно грубый: он существует, чтобы ловить ПОДМЕНУ
свойства, а не чтобы заменять распознавание.
"""
from __future__ import annotations

from typing import Any, Iterable

from .object_identity import canonical, tokens, words_agree

#: Канонические свойства: машинные ключи детерминированного слоя и слова, по
#: которым это же свойство называет человек. Ключи сверяются по началу строки,
#: чтобы `assembly_layer_thickness_mm_<хеш>` попадал в «толщину».
FACETS: dict[str, dict[str, tuple[str, ...]]] = {
    "area": {
        "keys": ("room_area_m2", "area"),
        "words": ("площадь", "area"),
    },
    "cross_section": {
        "keys": ("cross_section",),
        "words": ("сечение", "section"),
    },
    "height": {
        "keys": ("height", "ceiling_height"),
        "words": ("высота", "height"),
    },
    "length": {
        "keys": ("length",),
        "words": ("длина", "length"),
    },
    "width": {
        "keys": ("width",),
        "words": ("ширина", "width"),
    },
    "diameter": {
        "keys": ("diameter",),
        "words": ("диаметр", "diameter"),
    },
    "elevation": {
        "keys": ("elevation", "level"),
        "words": ("отметка", "уровень", "elevation"),
    },
    "thickness": {
        "keys": ("assembly_layer_thickness_mm", "thickness"),
        "words": ("толщина", "thickness"),
    },
    "material": {
        "keys": ("assembly_layer", "material"),
        "words": ("материал", "состав", "слой", "material"),
    },
    "name": {
        "keys": ("room_name", "name"),
        "words": ("наименование", "название", "назначение", "name"),
    },
    "fire_category": {
        "keys": ("room_fire_category", "fire_category"),
        "words": ("категория", "пожарная", "взрывопожарная", "category"),
    },
    "quantity": {
        "keys": ("quantity", "count"),
        "words": ("количество", "кол-во", "число", "quantity", "count"),
    },
    "power": {
        "keys": (
            "power", "installed_power", "unit_installed_power_kw",
            "installed_power_kw", "demand_active_power_kw",
            "demand_reactive_power_kvar", "demand_apparent_power_kva",
            "total_demand_active_power_kw", "total_demand_reactive_power_kvar",
            "total_demand_apparent_power_kva",
        ),
        "words": ("мощность", "power"),
    },
    "current": {
        "keys": (
            "current", "maximum_calculated_current_a",
            "total_maximum_calculated_current_a",
        ),
        "words": ("ток", "current"),
    },
    "voltage": {
        "keys": ("voltage",),
        "words": ("напряжение", "voltage"),
    },
    "frequency": {
        "keys": ("frequency",),
        "words": ("частота", "frequency"),
    },
    "coefficient": {
        "keys": (
            "utilization_coefficient", "demand_coefficient",
            "coincidence_coefficient", "power_factor_cos_phi",
            "reactive_factor_tan_phi", "total_demand_coefficient",
            "total_power_factor_cos_phi",
        ),
        "words": ("коэффициент", "factor", "cos", "tg", "tan"),
    },
    "protection_degree": {
        "keys": ("protection_degree",),
        "words": ("степень", "защиты", "защита", "protection"),
    },
    "temperature": {
        "keys": ("temperature_range", "temperature"),
        "words": ("температура", "temperature"),
    },
    "device_type": {
        "keys": ("device_type", "type"),
        "words": ("тип", "марка", "модель", "type", "model"),
    },
}


def facet_from_ref(facet_ref: Any) -> str | None:
    """Каноническое свойство по машинному ключу детерминированного слоя."""
    key = canonical(facet_ref).replace(" ", "_")
    if not key:
        return None
    best: tuple[int, str] | None = None
    for name, spec in FACETS.items():
        for candidate in spec["keys"]:
            if key == candidate or key.startswith(f"{candidate}_"):
                score = len(candidate)
                if best is None or score > best[0]:
                    best = (score, name)
    return best[1] if best else None


def facet_from_label(label: Any) -> str | None:
    """Каноническое свойство по человеческому названию.

    Побеждает свойство, к которому подошло больше слов названия. Ничья —
    значит, названо неоднозначно, и предлагать такое свойство нельзя.
    """
    words = [token for token in tokens(label) if len(token) >= 2]
    if not words:
        return None
    scores: dict[str, int] = {}
    for name, spec in FACETS.items():
        hits = sum(
            1 for word in words
            if any(words_agree(word, known) for known in spec["words"])
        )
        if hits:
            scores[name] = hits
    if not scores:
        return None
    best = max(scores.values())
    winners = [name for name, hits in scores.items() if hits == best]
    return winners[0] if len(winners) == 1 else None


def contradiction(facet_ref: Any, facet_label: Any) -> str | None:
    """Спорит ли предложенное свойство с уже доказанным. ``None`` — нет.

    Доказанное свойство не обсуждается: если справочник узнал ключ, название
    от модели обязано указывать на то же свойство. «Справочник не знает такого
    слова» — тоже отказ, а не молчаливое согласие: иначе подмену достаточно
    было бы назвать словом посложнее.
    """
    known = facet_from_ref(facet_ref)
    if known is None:
        return None
    if not canonical(facet_label):
        return None
    proposed = facet_from_label(facet_label)
    if proposed is None:
        return (
            f"детерминированный слой доказал свойство «{known}», а модель"
            f" назвала {str(facet_label)!r} — такого свойства справочник"
            " не знает"
        )
    if proposed != known:
        return (
            f"детерминированный слой доказал свойство «{known}», а модель"
            f" вернула «{proposed}» ({str(facet_label)!r}): это другое"
            " свойство того же объекта, а не иная формулировка"
        )
    return None


def known_facets() -> Iterable[str]:
    return tuple(FACETS)


__all__ = [
    "FACETS",
    "contradiction",
    "facet_from_label",
    "facet_from_ref",
    "known_facets",
]
