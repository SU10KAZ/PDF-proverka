"""Консервативная карта инженерных интерфейсов оптимизации раздела."""
from __future__ import annotations

from typing import Any


DEPENDENCY_ASSESSMENT_VERSION = 1

_RULES = (
    ({"кабел", "щит", "автомат", "светиль", "электр", "мощност"}, (
        ("power_supply", "Питание и расчётная мощность", "Однолинейная схема или расчёт нагрузок"),
        ("protection", "Защита и селективность", "Расчёт токов и уставок аппаратов защиты"),
        ("automation", "Автоматика и диспетчеризация", "Функциональная схема и перечень сигналов"),
        ("fire_safety", "Пожарное исполнение", "Требования к огнестойкости и категории цепи"),
    )),
    ({"креп", "кроншт", "опор", "лоток", "подвес"}, (
        ("supports", "Крепления и несущая способность", "Расчёт крепления и данные основания"),
        ("penetrations", "Проходки и пересечения", "План трасс и узлы проходок"),
        ("access", "Монтажный и эксплуатационный доступ", "План или узел с габаритами зон доступа"),
    )),
    ({"труб", "воздуховод", "клапан", "насос", "вентил"}, (
        ("flow", "Гидравлический или аэродинамический режим", "Расчёт расхода, потерь и рабочих точек"),
        ("supports", "Крепления и нагрузки", "Схема креплений и расчёт нагрузок"),
        ("automation", "Автоматика и управление", "Функциональная схема и алгоритм управления"),
        ("access", "Доступ для обслуживания", "План или узел с сервисными зонами"),
    )),
)


def _text(passport: dict) -> str:
    values: list[str] = [str(passport.get("action") or "")]
    for subject in passport.get("subjects") or []:
        values.extend(str(subject.get(field) or "") for field in ("name", "type_mark", "code"))
    return " ".join(values).lower()


def build_dependency_assessment(passport: dict) -> dict:
    text = _text(passport)
    interfaces: dict[str, dict[str, Any]] = {}
    matched_terms: set[str] = set()
    for keywords, rules in _RULES:
        hits = sorted(keyword for keyword in keywords if keyword in text)
        if not hits:
            continue
        matched_terms.update(hits)
        for key, title, required_evidence in rules:
            interfaces.setdefault(key, {
                "interface_id": key,
                "title": title,
                "status": "unknown",
                "relation": "possible_impact",
                "required_evidence": required_evidence,
                "basis": "Совпадение инженерных терминов: " + ", ".join(hits),
            })

    if not interfaces:
        interfaces["adjacent_systems"] = {
            "interface_id": "adjacent_systems",
            "title": "Интерфейсы со смежными системами",
            "status": "unknown",
            "relation": "impact_not_classified",
            "required_evidence": "Указать затрагиваемые системы или обосновать отсутствие влияния",
            "basis": "Тип изменения пока не классифицирован",
        }
    return {
        "assessment_version": DEPENDENCY_ASSESSMENT_VERSION,
        "status": "requires_review",
        "interfaces": list(interfaces.values()),
        "matched_terms": sorted(matched_terms),
        "rule": "Отсутствие сведений сохраняется как unknown и не означает отсутствие влияния.",
    }


__all__ = ["DEPENDENCY_ASSESSMENT_VERSION", "build_dependency_assessment"]
