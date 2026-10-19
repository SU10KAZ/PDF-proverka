"""Разметка полной СО по частям: склейка частей = разметка всей СО одним заданием (подменная Луна, без модели)."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from backend.app.services.version_comparison import specification_portions as portions
from backend.app.services.version_comparison import specification_run as runner
from backend.app.services.version_comparison import specification_structure as structure
from backend.app.services.version_comparison.specification_source import load_source
from backend.app.services.version_comparison_ai.executor import DescribeResult

HEADER = "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |"
SEP = "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"


def row(name, qty="", unit="", mark=""):
    return f"|  | {name} | {mark} |  |  | {unit} | {qty} |  |  |"


def pages() -> list[list[str]]:
    """6 страниц: раздел через границу, родитель «в составе» через границу, позиция разорвана между страницами."""
    out = []
    out.append([HEADER, SEP, "Электрооборудование", *[row(f"Щит ЩР-{i}", "1", "шт.") for i in range(1, 9)]])
    out.append([row(f"Щит ЩР-{i}", "1", "шт.") for i in range(9, 15)]
               + [row("Шкаф ШУ-ДП9 в составе:"), row("Автомат QF1", "2", "шт."), row("Автомат QF2", "1", "шт.")])
    out.append([row("Автомат QF3", "1", "шт."), row("Автомат QF4", "1", "шт."),           # продолжение состава
                "Кабели", *[row(f"Кабель ППГнг(А)-FRHF 5х{s}", str(10 * s), "м") for s in (6, 10, 16)],
                row("Кабель ППГнг(А)-FRHF 4х2.5 для", "650", "м")])
    out.append([row("противодымной вентиляции"),                                            # разорванная позиция
                *[row(f"Кабель ВВГнг(А)-LS 3х{s}", str(s), "м") for s in (1.5, 2.5, 4, 6)], "Примечание: длины уточнить"])
    out.append(["Лотки", *[row(f"Лоток {w}х50х3000", "90", "м") for w in (100, 200, 300, 400, 500, 600)]])
    out.append([row(f"Крепёж К-{i}", "10", "шт.") for i in range(1, 9)])
    return out


def write_md(tmp_path: Path) -> Path:
    lines = ["# Document: spec.pdf"]
    for n, body in enumerate(pages(), 1):
        lines += [f"## Page {n}", f"### BLOCK #{n} [TEXT]: blk_{n:08d}", *body]
    p = tmp_path / "spec.md"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def truth(src: dict) -> dict:
    """Правильная разметка всей СО (как её дала бы Луна одним заданием)."""
    at = {ln["text"]: ln["line"] for b in src["blocks"] for ln in b["lines"]}
    parent = at[row("Шкаф ШУ-ДП9 в составе:")]
    return {"sections": [{"line": at[x]} for x in ("Электрооборудование", "Кабели", "Лотки")],
            "parents": [{"parent_line": parent, "first_child_line": at[row("Автомат QF1", "2", "шт.")],
                         "last_child_line": at[row("Автомат QF4", "1", "шт.")]}],
            "continuations": [{"lines": [at[row("Кабель ППГнг(А)-FRHF 4х2.5 для", "650", "м")],
                                         at[row("противодымной вентиляции")]]}],
            "notes": [{"line": at["Примечание: длины уточнить"]}]}


class PortionLuna:
    """Честная Луна, которая видит только свою часть и контекст: отвечает правильной разметкой своих строк."""
    model = runner.MODEL

    def __init__(self, full: dict):
        self.full = full
        self.calls = 0
        self.prompts: list[str] = []

    def describe_runtime(self):
        return {"executor": "fake", "model": self.model, "effort": "high"}

    def validate_runtime(self):
        return {"ok": True, "problems": []}

    def run(self, task, cancel):
        self.calls += 1
        self.prompts.append(task.prompt)
        packet = json.loads(task.prompt.split("Вход (JSON):\n", 1)[1].split("\n\nПРЕДЫДУЩИЙ ОТВЕТ", 1)[0])
        own = {ln["line"] for b in packet["blocks"] for ln in b["lines"]}
        m = re.search(r"Контекст \(JSON\):\n(.*?)\n\nРазметь", task.prompt, re.S)
        ctx = json.loads(m.group(1)) if m else {}
        seen = {x["line"] for x in ctx.get("previous_lines") or []} | (
            {ctx["open_parent"]["line"]} if ctx.get("open_parent") else set())
        t = self.full
        ans = {"sections": [s for s in t["sections"] if s["line"] in own],
               "notes": [s for s in t["notes"] if s["line"] in own], "parents": [], "continuations": []}
        for p in t["parents"]:
            kids = sorted(n for n in own if p["first_child_line"] <= n <= p["last_child_line"])
            if kids and (p["parent_line"] in own or p["parent_line"] in seen):
                ans["parents"].append({"parent_line": p["parent_line"], "first_child_line": kids[0],
                                       "last_child_line": kids[-1]})
        for c in t["continuations"]:
            ids = [x for x in c["lines"] if x in own or x in seen]
            if any(x in own for x in ids) and len(ids) >= 2:
                ans["continuations"].append({"lines": ids})
        return DescribeResult(True, parsed=ans, usage={"input_tokens": len(task.prompt) // 3})


def third(src: dict) -> int:
    """Лимит задания на ~треть СО: постоянная часть задания + треть блоков + запас на контекст."""
    base = len(structure.prompt_for({**src, "blocks": []}))
    return base + (len(structure.prompt_for(src)) - base) // 3 + 400 + 800


def manifest(tmp_path: Path) -> Path:
    write_md(tmp_path)
    m = tmp_path / "manifest.json"
    m.write_text(json.dumps({"schema": "whole-specification-input/v1", "pairs": [
        {"id": "p", "old": {"path": "spec.md", "label": "V10", "all_blocks": True},
         "new": {"path": "spec.md", "label": "V11", "all_blocks": True}}]}), encoding="utf-8")
    return m


def test_split_keeps_pages_whole_and_within_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    src = load_source(write_md(tmp_path), label="V10")
    full = len(structure.prompt_for(src))
    parts = portions.split(src, full // 2)
    assert len(parts) >= 2
    assert [b for p in parts for b in p] == [b["block_id"] for b in src["blocks"]]     # все страницы, по порядку
    for p in parts:
        assert len(structure.prompt_for(portions.portion_source(src, p))) <= full // 2 - 800


def test_portioned_markup_equals_whole_markup(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    src = load_source(write_md(tmp_path), label="V10")
    whole = structure.validate(src, truth(src))
    assert whole["complete"], whole["issues"]
    limit = third(src)
    prepared = runner.prepare(manifest(tmp_path), max_source_chars=limit, structure=True)
    assert len(prepared["pairs"][0]["old"]["portions"]) >= 3 and prepared["pairs"][0]["old"]["complete"]
    luna = PortionLuna(truth(src))
    result = runner.run(prepared, tmp_path / "out", execute=True, executor=luna)
    assert result["extractions_complete"], result["incomplete_versions"]
    calls_first = luna.calls
    assert calls_first == 2 * len(prepared["pairs"][0]["old"]["portions"])
    # Склеенная разметка = разметка всей СО: те же строки, разделы, родители, продолжения.
    merged = json.loads(next((tmp_path / "out" / "calls").glob("*/answer.json")).read_text())
    norm = lambda d: {k: sorted(json.dumps(x, sort_keys=True) for x in d[k]) for k in d}   # noqa: E731
    assert norm(merged) == norm(truth(src))
    # Контекст дошёл: открытый родитель и действующий раздел переданы следующей части.
    assert any("open_parent" in p and "Шкаф ШУ-ДП9 в составе:" in p.split("Вход (JSON)")[0] for p in luna.prompts)
    assert result["reports"][0]["comparison"]["status"] != "incomplete"
    # Повтор: части берутся из сохранённых ответов, обращений нет.
    again = runner.run(prepared, tmp_path / "out", execute=True, executor=luna)
    assert again["extractions_complete"] and luna.calls == calls_first


def test_part_referencing_foreign_lines_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    src = load_source(write_md(tmp_path), label="V10")
    parts = portions.split(src, third(src))
    sub = portions.portion_source(src, parts[1])
    far = src["blocks"][-1]["lines"][-1]["line"]                     # строка последней страницы
    bad = {"sections": [{"line": far}], "parents": [], "continuations": [], "notes": []}
    issues = portions.check(sub, None, bad)
    assert any("не из этой части" in x for x in issues) and any("не размечены" in x for x in issues)


def test_page_larger_than_part_is_explicit_error(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 10)
    src = load_source(write_md(tmp_path), label="V10")
    with pytest.raises(portions.PortionError):
        portions.split(src, 200)


def test_part_with_missed_line_is_retried_once_with_feedback(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    src = load_source(write_md(tmp_path), label="V10")
    full = truth(src)
    note = full["notes"][0]["line"]

    class Forgetful(PortionLuna):
        def run(self, task, cancel):
            res = super().run(task, cancel)
            if "ОТКЛОНЁН ПРОВЕРКОЙ" not in task.prompt:          # первая попытка: примечание пропущено
                res.parsed["notes"] = [x for x in res.parsed["notes"] if x["line"] != note]
            return res
    luna = Forgetful(full)
    prepared = runner.prepare(manifest(tmp_path), max_source_chars=third(src), structure=True)
    result = runner.run(prepared, tmp_path / "out", execute=True, executor=luna)
    assert result["extractions_complete"], result["incomplete_versions"]
    retried = [p for p in luna.prompts if "ОТКЛОНЁН ПРОВЕРКОЙ" in p]
    assert len(retried) == 2 and all(f"[{note}]" in p for p in retried)       # по одному повтору на версию


def test_strict_issue_after_merge_goes_back_to_its_part_only(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    src = load_source(write_md(tmp_path), label="V10")
    full = truth(src)
    at = {ln["text"]: ln["line"] for b in src["blocks"] for ln in b["lines"]}
    wrong = at[row("Лоток 300х50х3000", "90", "м")]                 # позиция с количеством — не раздел

    class Sloppy(PortionLuna):
        def run(self, task, cancel):
            res = super().run(task, cancel)
            own = task.prompt.split("Вход (JSON):\n", 1)[1]
            if f'"line": {wrong},' in own and "ОТКЛОНЁН ПРОВЕРКОЙ" not in task.prompt:
                res.parsed["sections"].append({"line": wrong})
            return res
    luna = Sloppy(full)
    prepared = runner.prepare(manifest(tmp_path), max_source_chars=third(src), structure=True)
    parts = len(prepared["pairs"][0]["old"]["portions"])
    result = runner.run(prepared, tmp_path / "out", execute=True, executor=luna)
    assert result["extractions_complete"], result["incomplete_versions"]
    fixed = [p for p in luna.prompts if "ОТКЛОНЁН ПРОВЕРКОЙ" in p]
    assert len(fixed) == 2 and all(f"строка {wrong} отмечена разделом" in p for p in fixed)
    # Повторена часть с замечанием; следующая — только если от исправления изменился её контекст
    # (здесь — действующий раздел на конце части).
    assert 2 * parts + 2 <= luna.calls <= 2 * parts + 4


def test_autofix_handles_a_few_missed_lines_but_not_a_broken_answer(tmp_path, monkeypatch):
    src = load_source(write_md(tmp_path), label="V10")
    full = truth(src)
    note = full["notes"][0]["line"]
    lost = {**full, "notes": []}
    fixed, auto = portions.autofix(src, None, lost)
    assert [a["line"] for a in auto] == [note] and {"line": note} in fixed["notes"]
    assert portions.autofix(src, None, {"bad": "answer"}) == ({"bad": "answer"}, [])
    nothing = {"sections": [], "parents": [], "continuations": [], "notes": []}
    assert portions.autofix(src, None, nothing)[1] == []            # всё не размечено — не мелочь


def test_strict_autofix_counts_one_fix_per_issue_not_per_block(tmp_path):
    src = load_source(write_md(tmp_path), label="V10")
    t = truth(src)
    p = t["parents"][0]["parent_line"]
    issue = f"Parent range must reference row-start lines: {{'parent_line': {p}, 'first_child_line': 1, 'last_child_line': 2}}"
    fixed, fixes = portions.strict_autofix(src, t, [f"blk_{k:08d}: {issue}" for k in range(1, 40)])
    assert len(fixes) == 1 and not fixed["parents"]
