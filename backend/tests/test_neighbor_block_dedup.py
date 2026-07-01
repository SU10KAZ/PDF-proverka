"""Тесты дедупа соседних текст-блоков против текст-слоя блока-схемы."""
from backend.app.services.common.neighbor_block_dedup import (
    bigram_containment,
    filter_neighbor_blocks,
    _bigrams,
    _tokens,
)


TEXT_LAYER = (
    "QF3.9 ВА-300 1Р 15кА 10А Освещение ЛК1.1 1-12 эт. "
    "Примечание 1. Степень защиты IP31. 2. Климатическое исполнение УХЛ4. "
    "3. Ввод кабелей сверху, вывод кабелей сверху. "
    "Проверка коэффициентов трансформации ВП1 319.23 477.1 "
    "ГРЩ с.ш.1 ВП1 РП1 РП5 ПЭСПЗ РП4 АВР"
)


def test_notes_block_detected_as_duplicate():
    notes = ("Примечание 1. Степень защиты IP31. 2. Климатическое исполнение УХЛ4. "
             "3. Ввод кабелей сверху, вывод кабелей сверху.")
    ref = _bigrams(_tokens(TEXT_LAYER))
    assert bigram_containment(notes, ref) >= 0.8


def test_unique_block_kept():
    uniq = ("Расчет нагрузок стадия РД Электроприемник Руст кВт Ко Кс Рр квар "
            "Sr кВА жилые квартиры 152 шт кладовые ИТП водоснабжение")
    ref = _bigrams(_tokens(TEXT_LAYER))
    assert bigram_containment(uniq, ref) < 0.5


def test_power_tree_line_not_false_positive():
    # строка дерева питания: слова есть в текст-слое, но как биграмма-последовательность
    # это уникум (не таблица-сосед) → низкое покрытие
    line = "ГРЩ с.ш.2 -> ВП2 -> РП2 / РП3 (ОДН) / РП5(ПЭСПЗ) / РП4(АВР)"
    other_neighbor = "Проверка коэффициентов трансформации ВП1 319.23 Iр.раб Iр.авар"
    ref = _bigrams(_tokens(other_neighbor))
    assert bigram_containment(line, ref) < 0.5


def test_filter_splits_send_and_dropped():
    neighbors = [
        {"block_id": "NOTES", "text": "Примечание 1. Степень защиты IP31. "
                                      "2. Климатическое исполнение УХЛ4. 3. Ввод кабелей сверху."},
        {"block_id": "UNIQ", "text": "Расчет нагрузок Электроприемник Руст Ко Кс Рр квар Sr "
                                     "жилые квартиры кладовые ИТП"},
    ]
    send, dropped = filter_neighbor_blocks(TEXT_LAYER, neighbors)
    assert [b["block_id"] for b in dropped] == ["NOTES"]
    assert [b["block_id"] for b in send] == ["UNIQ"]
    assert dropped[0]["reason"] == "in_text_layer"
    assert "bigram_in_text_layer" in send[0]


def test_short_block_not_matched():
    # <4 токенов → не судим (0.0), уходит в send
    ref = _bigrams(_tokens(TEXT_LAYER))
    assert bigram_containment("QF3.9 ВА-300", ref) == 0.0


def test_html_stripped_before_compare():
    html_notes = ('<div data-bbox="1 2 3 4" data-label="Notes"></div>'
                  "Примечание 1. Степень защиты IP31. 2. Климатическое исполнение УХЛ4. "
                  "3. Ввод кабелей сверху.")
    ref = _bigrams(_tokens(TEXT_LAYER))
    # HTML-обёртка не должна мешать распознать дубль
    assert bigram_containment(html_notes, ref) >= 0.8
