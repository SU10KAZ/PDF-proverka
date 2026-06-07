"""Unit-тесты r6 self-check на основном пути сравнения (enriched_comparison).

Покрывают:
  * `_salient_numbers` — извлечение значимых числовых токенов (сечения/номиналы),
    канонизация запятой/× и отсев коротких шумовых токенов;
  * `_numeric_grounded` — числовой re-cite значения change против MD стороны
    (практичная версия r3 для CAD-чертежей без векторного текст-слоя);
  * `_apply_selfcheck` — мягкий режим (mark) и strict-режим (drop), числовой
    rescue негрунтованной по цитате дельты, fail-soft диагностика.

Чистые функции — фикстуры/окружение не нужны (живой Qwen/Opus не вызывается).
"""
from __future__ import annotations

from backend.app.services.stage_comparison import enriched_comparison as ec


LEFT_MD = "Кабель ВВГнг(А)-FRLS 5х10 для ВРУ-1. Вводной автомат 160А."
RIGHT_MD = "Кабель ВВГнг(А)-FRLS 5х16 для ВРУ-1. Вводной автомат 250А."


def _cfg(*, drop: bool = False) -> ec.EnrichedCompareConfig:
    return ec.EnrichedCompareConfig(
        enabled=True, provider="claude_code", model="opus",
        timeout_sec=900, max_chars=600_000,
        selfcheck_enabled=True, selfcheck_drop_ungrounded=drop,
    )


# ─── _salient_numbers ──────────────────────────────────────────────────────


def test_salient_numbers_cable_section_and_rating():
    nums = ec._salient_numbers("ВВГнг 5х10, автомат 160А")
    assert "5x10" in nums   # кириллическая х → x
    assert "160а" in nums


def test_salient_numbers_decimal_and_unit():
    nums = ec._salient_numbers("Класс точности 0,5S")
    assert "0.5s" in nums   # запятая → точка


def test_salient_numbers_drops_short_noise():
    nums = ec._salient_numbers("позиция 12 шт, лист 7")
    # 1-2 символьные / однозначные токены — шум, не извлекаются
    assert "12" not in nums
    assert "7" not in nums
    assert nums == set()


def test_salient_numbers_multiplier_variants_canonicalize():
    assert ec._salient_numbers("5×185") == {"5x185"}
    assert ec._salient_numbers("5x185") == {"5x185"}
    assert ec._salient_numbers("5х185") == {"5x185"}


# ─── _numeric_grounded ─────────────────────────────────────────────────────


def test_numeric_grounded_old_new_values_present():
    left_nums = ec._salient_numbers(LEFT_MD)    # {"5x10","160а"}
    right_nums = ec._salient_numbers(RIGHT_MD)  # {"5x16","250а"}
    change = {"old_value": "160А", "new_value": "250А",
              "evidence_left": {"quote": ""}, "evidence_right": {"quote": ""}}
    assert ec._numeric_grounded(change, left_nums, right_nums) is True


def test_numeric_grounded_phantom_value_absent():
    left_nums = ec._salient_numbers(LEFT_MD)
    right_nums = ec._salient_numbers(RIGHT_MD)
    change = {"old_value": "", "new_value": "999А",
              "evidence_left": {"quote": ""}, "evidence_right": {"quote": ""}}
    assert ec._numeric_grounded(change, left_nums, right_nums) is False


# ─── _apply_selfcheck ──────────────────────────────────────────────────────


def _changes():
    return [
        # grounded дословной цитатой (присутствует в LEFT/RIGHT MD)
        {"id": "chg_quote", "type": "material_changed", "title": "Кабель ввода",
         "old_value": "5х10", "new_value": "5х16",
         "evidence_left": {"quote": "ВВГнг(А)-FRLS 5х10"},
         "evidence_right": {"quote": "ВВГнг(А)-FRLS 5х16"}},
        # цитата НЕ grounded (короткие/частичные), но число есть в MD → rescue
        {"id": "chg_number", "type": "changed", "title": "Номинал ввода",
         "old_value": "160А", "new_value": "250А",
         "evidence_left": {"quote": "ном."}, "evidence_right": {"quote": "ном."}},
        # ни цитаты, ни числа в MD → ungrounded (галлюцинация)
        {"id": "chg_phantom", "type": "added", "title": "Фантомный щит",
         "old_value": "", "new_value": "ЩО-7 на 999А",
         "evidence_left": {"quote": ""},
         "evidence_right": {"quote": "фантомный щит ЩО-7 999А"}},
    ]


def test_apply_selfcheck_mark_mode_marks_only_phantom():
    changes, diag = ec._apply_selfcheck(_changes(), LEFT_MD, RIGHT_MD, _cfg(drop=False))
    assert diag["mode"] == "mark"
    assert diag["total"] == 3
    assert diag["verified"] == 2
    assert diag["rescued_by_number"] == 1
    assert diag["ungrounded"] == 1
    assert diag["marked_review"] == 1
    assert diag["dropped"] == 0
    # ничего не удалено в мягком режиме
    assert len(changes) == 3
    by_id = {c["id"]: c for c in changes}
    assert by_id["chg_quote"]["evidence_verified"] is True
    assert by_id["chg_quote"]["evidence_verified_by"] == "quote"
    assert by_id["chg_number"]["evidence_verified"] is True
    assert by_id["chg_number"]["evidence_verified_by"] == "number"
    assert by_id["chg_phantom"]["evidence_verified"] is False
    assert by_id["chg_phantom"]["requires_human_review"] is True
    assert "selfcheck_note" in by_id["chg_phantom"]


def test_apply_selfcheck_drop_mode_removes_phantom():
    changes, diag = ec._apply_selfcheck(_changes(), LEFT_MD, RIGHT_MD, _cfg(drop=True))
    assert diag["mode"] == "drop"
    assert diag["dropped"] == 1
    assert diag["ungrounded"] == 1
    assert len(changes) == 2
    ids = {c["id"] for c in changes}
    assert ids == {"chg_quote", "chg_number"}


def test_apply_selfcheck_empty_changes_safe():
    changes, diag = ec._apply_selfcheck([], LEFT_MD, RIGHT_MD, _cfg())
    assert changes == []
    assert diag["total"] == 0
    assert diag["ungrounded"] == 0
