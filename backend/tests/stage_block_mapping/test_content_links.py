"""Offline candidate: ambiguity, evidence type and no all-to-all expansion."""
from copy import deepcopy

from backend.app.services.stage_block_mapping.content_links import propose_region_links

PUMP = ("Насосная станция противопожарного водопровода расположена в помещении подземного этажа. "
        "Проектом предусмотрены рабочий резервный агрегаты запорная арматура обратные клапаны "
        "датчики давления автоматический шкаф управления подключение электропитания контроль "
        "уровня воды передача сигнала диспетчеру испытание трубопроводов после монтажа.")
AIR = ("Вентиляционная установка обслуживает помещения офисов верхнего этажа. Воздух подается "
       "через утепленные воздуховоды регулирующие клапаны распределительные решетки фильтры "
       "очистки нагреватели шумоглушители. Предусмотрены автоматическое отключение при пожаре "
       "система удаленного управления регулирование производительности проверка состояния оборудования.")


def block(id, text, type="TEXT"):
    return {"id": id, "type": type, "page": 1, "structured_md": text}


def region(olds, news):
    return {"id": "r", "old_blocks": olds, "new_blocks": news}


def test_matches_content_instead_of_order_and_leaves_input_unchanged():
    r = region([block("o1", PUMP), block("o2", AIR)], [block("n1", AIR), block("n2", PUMP)])
    before = deepcopy(r)
    links = propose_region_links(r)
    assert {(e["old_block_id"], e["new_block_id"]) for e in links} == {("o1", "n2"), ("o2", "n1")}
    assert all(e["method"] == "EXACT_TEXT" and "требуется проверка" in e["reason"] for e in links)
    assert r == before


def test_repeated_boilerplate_abstains_in_both_directions():
    r = region([block("o1", PUMP), block("o2", AIR)], [block("n1", PUMP), block("n2", PUMP)])
    assert propose_region_links(r) == []
    assert propose_region_links(region(r["new_blocks"], r["old_blocks"])) == []


def test_changed_values_are_suggestions_not_exact_or_confirmed():
    r = region([block("o1", PUMP + " Производительность 100 м3/ч."), block("o2", AIR)],
               [block("n1", PUMP + " Производительность 150 м3/ч."), block("n2", "Другой блок")])
    links = propose_region_links(r)
    assert len(links) == 1 and links[0]["method"] == "TEXT_OVERLAP"
    assert "требуется проверка" in links[0]["reason"]


def test_negative_and_positive_values_are_not_reported_as_identical_text():
    r = region([block("o1", PUMP + " Температура -5 градусов."), block("o2", AIR)],
               [block("n1", PUMP + " Температура 5 градусов."), block("n2", "Другой блок")])
    assert propose_region_links(r)[0]["method"] == "TEXT_OVERLAP"


def test_graphic_descriptions_short_titles_and_cross_type_matches_do_not_link():
    for kind, other, text in [("GRAPHIC", "GRAPHIC", PUMP), ("TEXT", "TABLE", PUMP),
                               ("TEXT", "TEXT", "Общие данные насосная станция")]:
        assert propose_region_links(region([block("a", text, kind), block("b", AIR)],
                                           [block("c", text, other), block("d", "Иное")])) == []


def test_tables_match_by_content_and_markup_does_not_claim_engineering_equality():
    r = region([block("a", PUMP, "TABLE"), block("b", AIR, "TABLE")],
               [block("c", "<table><tr><td>" + PUMP + "</td></tr></table>", "TABLE"), block("d", "Иное")])
    links = propose_region_links(r)
    assert len(links) == 1 and links[0]["old_block_id"] == "a"
    assert "распознанный текст" in links[0]["reason"]
