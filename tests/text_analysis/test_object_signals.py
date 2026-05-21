"""Tests for backend/app/services/text_analysis/object_signals.py."""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.object_signals import (
    KNOWN_SIGNALS,
    detect_object_signals,
    has_required_signals,
    known_signal_names,
    missing_required_signals,
    signal_rules_by_name,
)


# ---------------------------------------------------------------------------
# Detector — null / empty / non-str safety.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [None, "", 123, object(), [], {}])
def test_detect_null_safe_returns_all_false(bad):
    result = detect_object_signals(bad)
    assert isinstance(result, dict)
    assert set(result) == set(KNOWN_SIGNALS)
    assert not any(result.values())


def test_detect_result_always_has_all_known_signals():
    result = detect_object_signals("какой-то текст без сигналов")
    assert set(result) == set(KNOWN_SIGNALS)


# ---------------------------------------------------------------------------
# Each rule fires on at least one canonical Russian phrase.
# ---------------------------------------------------------------------------


SIGNAL_POSITIVES = {
    "motors_present": [
        "В системе используются электродвигатели мощностью 5 кВт",
        "промышленные нагрузки cos φ = 0.8",
    ],
    "high_rise": [
        "Высотное здание высотой 75 м и более",
        "Жилое здание высотой ≥ 28 м",
        "Высота здания свыше 75 м",
    ],
    "fire_system_present": [
        "Проектом предусмотрена АПС по СП 484.1311500.2020",
        "Автоматическая пожарная сигнализация на всех этажах",
        "АУПТ установка пожаротушения",
    ],
    "lightning_protection_required": [
        "Молниезащита по СО-153-34.21.122-2003",
        "Категория молниезащиты II",
    ],
    "category_1_power": [
        "Электроприёмники I категории надёжности электроснабжения",
        "Используется АВР для I-особой категории",
    ],
    "smoke_ventilation_required": [
        "Противодымная вентиляция для лестничной клетки",
        "Подпор воздуха в шахту лифта",
        "ПДВ на этажах",
    ],
    "underground_structure": [
        "Подземная автостоянка на 2 этажа",
        "Цокольный этаж под жилой частью",
        "Подвал с техническими помещениями",
    ],
    "seismic_region": [
        "Сейсмический район по карте ОСР-2015",
        "Расчёт на сейсмическое воздействие 7 баллов",
        "СП 14.13330 учтён в расчётах",
    ],
    "residential_building": [
        "Многоквартирный жилой дом класс Ф1.3",
        "МКД на 12 этажей",
        "Жилые квартиры с балконами",
    ],
    "public_building": [
        "Торговый центр класс Ф3.1",
        "Объект с массовым пребыванием людей",
        "Школа на 500 учеников",
        "Детский сад на 150 мест",
    ],
    "ventilation_system_present": [
        "Вентустановка П1 в подвале",
        "Воздуховоды из оцинкованной стали",
        "Приточная вентиляция для офисов",
    ],
    "pumps_present": [
        "Повысительная насосная станция",
        "Насос марки Wilo",
    ],
    "facade_present": [
        "Витраж в атриуме первого этажа",
        "Навесной фасад с керамогранитом",
        "СПФ-фасад",
    ],
    "roof_operated": [
        "Эксплуатируемая кровля с озеленением",
        "Выход на кровлю из лестничной клетки",
    ],
    "automation_present": [
        "ИТП с автоматизацией",
        "Диспетчеризация инженерных систем",
        "АХЗ в подвале",
    ],
    "cable_lines_present": [
        "Кабельный журнал отходящих линий",
        "Кабель ВВГнг(А)-FRLS 5x10",
    ],
    "wet_zone_present": [
        "Ванная комната с УЗО 10 мА",
        "Санузел отдельный",
        "Розеточная группа в кухне",
    ],
    "elevators_present": [
        "Лифт грузопассажирский 1000 кг",
        "Лифтовая шахта в осях 1-2",
    ],
    "generators_present": [
        "ДГУ резервного питания 200 кВА",
        "Дизельный генератор для I категории",
        "ИБП с временем работы 30 мин",
    ],
}


@pytest.mark.parametrize(
    "signal, text",
    [(sig, txt) for sig, txts in SIGNAL_POSITIVES.items() for txt in txts],
)
def test_signal_fires_on_canonical_phrase(signal, text):
    result = detect_object_signals(text)
    assert result[signal] is True, (
        f"signal {signal!r} did not fire on {text!r}"
    )


# ---------------------------------------------------------------------------
# Conservative: signals do NOT fire on unrelated text.
# ---------------------------------------------------------------------------


NEGATIVE_TEXT = (
    "Это обычный документ. Никаких систем, никаких этажей, никаких "
    "номеров норм. Просто текст."
)


def test_negative_text_fires_nothing():
    result = detect_object_signals(NEGATIVE_TEXT)
    fired = [k for k, v in result.items() if v]
    assert fired == [], f"unexpected signals fired: {fired}"


# ---------------------------------------------------------------------------
# has_required_signals + missing_required_signals.
# ---------------------------------------------------------------------------


def test_no_required_signals_means_pass():
    item = {"object_signals": []}
    assert has_required_signals(item, {}) is True
    assert missing_required_signals(item, {}) == []


def test_required_signal_present_passes():
    item = {"object_signals": ["high_rise"]}
    detected = detect_object_signals("Высотное здание ≥ 75 м")
    assert has_required_signals(item, detected) is True
    assert missing_required_signals(item, detected) == []


def test_required_signal_missing_fails():
    item = {"object_signals": ["high_rise"]}
    detected = detect_object_signals("Малоэтажный жилой дом без указаний")
    assert has_required_signals(item, detected) is False
    assert missing_required_signals(item, detected) == ["high_rise"]


def test_multiple_required_signals_all_must_be_present():
    item = {"object_signals": ["high_rise", "smoke_ventilation_required"]}
    detected = detect_object_signals("Высотное здание ≥ 75 м, прочая обвязка.")
    # only high_rise present
    assert has_required_signals(item, detected) is False
    assert "smoke_ventilation_required" in missing_required_signals(item, detected)


def test_iterable_of_fired_signals_accepted():
    item = {"object_signals": ["high_rise"]}
    assert has_required_signals(item, ["high_rise", "other"]) is True


def test_unknown_required_signal_silently_ignored():
    # Defensive: if metadata accidentally lists a signal not in the
    # allow-list, the gate should ignore it (not crash, not block).
    item = {"object_signals": ["nonexistent_signal"]}
    assert has_required_signals(item, {}) is True


def test_known_signal_names_sorted():
    names = known_signal_names()
    assert names == sorted(names)
    assert set(names) == set(KNOWN_SIGNALS)


def test_signal_rules_by_name_covers_known_signals():
    rules = signal_rules_by_name()
    assert set(rules.keys()) == set(KNOWN_SIGNALS)
    for name, rule in rules.items():
        assert rule.name == name
        assert rule.patterns  # at least one pattern per signal
