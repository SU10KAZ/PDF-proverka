"""Эксперимент по docnorm: правила сопоставления и замороженный вариант промпта."""

from __future__ import annotations

from backend.app.services.algorithm_improvement import docnorm_match as M
from backend.app.services.algorithm_improvement import docnorm_variant as V


def _f(fid: str, problem: str, description: str = "") -> dict:
    return {"id": fid, "problem": problem, "description": description}


def test_norm_cores_ignore_edition_year_and_keep_kind():
    assert M.norm_cores("ГОСТ 6465-76 и ГОСТ 6465-2023") == {"ГОСТ:6465"}
    assert M.norm_cores("СП 10.13330.2017") == {"СП:10.13330"}
    assert "ФЗ:123" in M.norm_cores("№123-ФЗ")


def test_same_norm_different_essence_is_not_a_match():
    obsolete = _f("a", "В ведомости ссылочных документов указан отменённый ГОСТ 9573-2012 на плиты.")
    title = _f("b", "В ведомости ссылочных документов неверно указано наименование ГОСТ 9573-2012 на плиты.")
    assert M.compare(obsolete, title) == M.UNDETERMINED
    res = M.match([obsolete], [title])
    assert res["pairs"] == [] and res["left"]["a"] == M.UNDETERMINED


def test_same_norm_essence_but_different_subject_is_not_a_match():
    a = _f("a", "В спецификации материалов указан отменённый ГОСТ 6465-76 для эмали.")
    b = _f("b", "В спецификации материалов указан отменённый ГОСТ 6465-76 для раствора.")
    assert M.compare(a, b) == M.UNDETERMINED


def test_full_agreement_is_strong_and_missing_place_is_weak():
    a = _f("a", "В общих указаниях сохранена ссылка на отменённый ГОСТ Р 53313-2009 для труб ПВХ.")
    b = _f("b", "В общих указаниях использована отменённая ссылка на ГОСТ Р 53313-2009, трубы ПВХ.")
    assert M.compare(a, b) == M.STRONG
    c = _f("c", "Отменённый ГОСТ Р 53313-2009 для труб ПВХ.")
    assert M.compare(a, c) == M.WEAK


def test_two_equal_candidates_are_ambiguous():
    ref = _f("r", "В ведомости ссылочных документов указан отменённый ГОСТ 6428-83 на плиты.")
    x1 = _f("x1", "В ведомости ссылочных документов указан отменённый ГОСТ 6428-83 на плиты гипсовые.")
    x2 = _f("x2", "В ведомости ссылочных документов указан отменённый ГОСТ 6428-83 на плиты пазогребневые.")
    res = M.match([x1, x2], [ref])
    assert res["pairs"] == []
    assert res["right"]["r"] == M.AMBIGUOUS
    assert res["left"]["x1"] == res["left"]["x2"] == M.AMBIGUOUS


def test_unconfirmed_claim_is_its_own_essence():
    assert M.essence("Замена ГОСТ 9573-2012 на ГОСТ 9573-2019 не подтверждена.") == "unconfirmed"
    assert M.essence("В ведомости указан отменённый ГОСТ 9573-2012.") == "status"


def test_variant_hash_is_frozen():
    # Хэш записан в манифест эксперимента до подбора положительных контролей (20.10.2026).
    assert V.variant_sha256() == "6c7ef0298f2f104c4913e32db3bcdf48f9d2ff5f0ed3bde0b2ab7f65d019d787"


def test_variant_keeps_arm_a_untouched_and_rewrites_only_b():
    prod = [{"role": "system", "content": "SYS"}, {"role": "user", "content": "USER"}]
    assert V.messages_for_arm("A", prod) == prod
    b = V.messages_for_arm("B", prod)
    assert b[0]["content"] == V.SYSTEM_B_V1
    assert b[1]["content"].startswith("USER") and b[1]["content"].endswith(V.USER_ADDENDUM_B_V1)
    # Нет общего запрета на устаревшие редакции: значимая устаревшая норма публикуется.
    assert "НЕ основание для пропуска" in V.SYSTEM_B_V1
