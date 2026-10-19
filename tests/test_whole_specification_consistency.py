"""Согласование разметки одинакового текста двух версий (без модели)."""
from pathlib import Path

from backend.app.services.version_comparison import specification_consistency as cons
from backend.app.services.version_comparison.specification_source import load_source
from backend.app.services.version_comparison.specification_structure import validate

HEADER = "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |"
SEP = "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
BODY = [HEADER, SEP, "Кабели",
        "|  | Кабель ВВГ 3х2.5 |  |  |  | м | 10 |  |  |",
        "|  | Гибкий медный провод (ПВХ) | ПВС |  |  |  |  |  | подключение конвекторов |",
        "|  | сечением, мм2: |  |  |  | м | 500 |  | (для проведения ПНР |",
        "|  | 3х1.5 |  |  |  |  |  |  | с послед. демонтажом) |",
        "|  | Лоток 100х50 |  |  |  | м | 90 |  |  |"]


def src(tmp_path: Path, name: str, extra: list[str]):
    p = tmp_path / f"{name}.md"
    p.write_text("# Document: x.pdf\n## Page 1\n### BLOCK #1 [TEXT]: blk_aaaaaaaa\n" + "\n".join(extra + BODY) + "\n",
                 encoding="utf-8")
    return load_source(p, label=name)


def at(s, text):
    return next(ln["line"] for b in s["blocks"] for ln in b["lines"] if ln["text"] == text)


def test_same_text_different_markup_is_found_and_unified(tmp_path):
    a = src(tmp_path, "V10", [])
    b = src(tmp_path, "V11", ["Примечание версии"])       # строки B сдвинуты на одну
    pvs, sech, size = BODY[4], BODY[5], BODY[6]
    pay_a = {"sections": [{"line": at(a, "Кабели")}], "notes": [], "parents": [], "continuations": []}
    pay_b = {"sections": [{"line": at(b, "Кабели")}], "notes": [{"line": at(b, "Примечание версии")}], "parents": [],
             "continuations": [{"lines": [at(b, pvs), at(b, sech), at(b, size)]}]}
    assert validate(a, pay_a)["complete"] and validate(b, pay_b)["complete"]
    wins = cons.windows(a, pay_a, b, pay_b)
    assert len(wins) == 1 and at(a, pvs) in wins[0]["lines"] and wins[0]["a2b"][at(a, pvs)] == at(b, pvs)
    prompt = cons.prompt_for(a, pay_a, pay_b, wins, "V10", "V11")
    assert "variant_1" in prompt and "variant_2" in prompt
    # Луна выбрала вариант второй версии (одна позиция ПВС) — в нумерации A.
    sections = [{"line": at(a, "Кабели")}] if at(a, "Кабели") in wins[0]["lines"] else []
    answer = {"windows": [{"window_id": wins[0]["window_id"], "sections": sections, "notes": [], "parents": [],
                           "continuations": [{"lines": [at(a, pvs), at(a, sech), at(a, size)]}]}]}
    new_a, new_b, issues = cons.apply(pay_a, pay_b, wins, answer)
    assert not issues
    ca, cb = validate(a, new_a), validate(b, new_b)
    assert ca["complete"] and cb["complete"]
    assert [r["fields"]["name"] for r in ca["rows"]] == [r["fields"]["name"] for r in cb["rows"] if r["fields"]["name"]]
    assert cons.windows(a, new_a, b, new_b) == []                    # после согласования расхождений нет


def test_answer_referencing_lines_outside_window_is_rejected(tmp_path):
    a, b = src(tmp_path, "V10", []), src(tmp_path, "V11", [])
    pay_a = {"sections": [{"line": at(a, "Кабели")}], "notes": [], "parents": [], "continuations": []}
    pay_b = {"sections": [{"line": at(b, "Кабели")}], "notes": [], "parents": [],
             "continuations": [{"lines": [at(b, BODY[4]), at(b, BODY[5]), at(b, BODY[6])]}]}
    wins = cons.windows(a, pay_a, b, pay_b)
    bad = {"windows": [{"window_id": wins[0]["window_id"], "sections": [{"line": 999}], "notes": [], "parents": [],
                        "continuations": []}]}
    _, _, issues = cons.apply(pay_a, pay_b, wins, bad)
    assert issues and "вне места" in issues[0]
