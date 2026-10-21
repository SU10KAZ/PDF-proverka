"""Наследование разметки СО от соседней версии (docs/whole_specification_v2_plan.md, этап 3)."""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.version_comparison import specification_inherit as inheritance
from backend.app.services.version_comparison import specification_portions as portions
from backend.app.services.version_comparison import specification_run as runner
from backend.app.services.version_comparison.specification_source import load_source
from backend.app.services.version_comparison.specification_structure import validate
from tests.test_whole_specification_portions import PortionLuna, pages, row, truth

HEADER = "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |"


def _write(tmp_path: Path, name: str, page_list: list[list[str]]) -> Path:
    lines = ["# Document: spec.pdf"]
    for n, body in enumerate(page_list, 1):
        lines += [f"## Page {n}", f"### BLOCK #{n} [TEXT]: blk_{n:08d}", *body]
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _v2() -> list[list[str]]:
    """Следующая версия: изменено количество одного кабеля и добавлен лоток."""
    out = [list(p) for p in pages()]
    out[2] = [x.replace(row("Кабель ППГнг(А)-FRHF 5х10", "100", "м"), row("Кабель ППГнг(А)-FRHF 5х10", "120", "м"))
              for x in out[2]]
    out[4] = [*out[4], row("Лоток 700х50х3000", "30", "м")]
    return out


def test_markup_of_unchanged_lines_is_carried_over_and_only_changes_are_redone(tmp_path):
    a = load_source(_write(tmp_path, "a.md", pages()), label="V1")
    b = load_source(_write(tmp_path, "b.md", _v2()), label="V2")
    base, segments = inheritance.plan(a, truth(a), b)
    at = {ln["text"]: ln["line"] for blk in b["blocks"] for ln in blk["lines"]}
    # Количество 100 → 120 не меняет роль строки: она сопоставлена по форме и не размечается заново.
    assert at[row("Кабель ППГнг(А)-FRHF 5х10", "120", "м")] not in {n for s in segments for n in s}
    assert segments == [[at[row("Лоток 700х50х3000", "30", "м")]]]
    merged = inheritance.overlay(b, base, {"sections": [], "parents": [], "continuations": [], "notes": []},
                                 {n for s in segments for n in s})
    assert validate(b, merged)["complete"]
    assert [x["line"] for x in base["sections"]] == [at[t] for t in ("Электрооборудование", "Кабели", "Лотки")]
    assert len(base["parents"]) == 1 and len(base["continuations"]) == 1


def test_model_markup_over_unchanged_lines_is_not_taken(tmp_path):
    """Ответ модели по изменённой строке не может переразметить неизменные строки (иначе соседние версии
    размечены по-разному и сравнение показывает ложные удаления/добавления)."""
    a = load_source(_write(tmp_path, "a.md", pages()), label="V1")
    b = load_source(_write(tmp_path, "b.md", _v2()), label="V2")
    base, segments = inheritance.plan(a, truth(a), b)
    at = {ln["text"]: ln["line"] for blk in b["blocks"] for ln in blk["lines"]}
    new = at[row("Лоток 700х50х3000", "30", "м")]
    lotki = [at[row(f"Лоток {w}х50х3000", "90", "м")] for w in (100, 200)]
    answer = {"sections": [], "notes": [], "continuations": [],
              "parents": [{"parent_line": lotki[0], "first_child_line": lotki[1], "last_child_line": new}]}
    merged = inheritance.overlay(b, base, answer, {new})
    assert all(p["parent_line"] != lotki[0] for p in merged["parents"])


def test_chain_sends_only_changed_lines_to_the_model(tmp_path, monkeypatch):
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    _write(tmp_path, "a.md", pages())
    _write(tmp_path, "b.md", _v2())
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"schema": "whole-specification-input/v1", "pairs": [
        {"id": "p", "old": {"path": "a.md", "label": "V1", "all_blocks": True},
         "new": {"path": "b.md", "label": "V2", "all_blocks": True}}]}), encoding="utf-8")
    a = load_source(tmp_path / "a.md", label="V1")
    b = load_source(tmp_path / "b.md", label="V2")
    full_b = truth(b)

    class Luna(PortionLuna):
        def run(self, task, cancel):
            # Первая версия — разметка V1, задание по изменениям второй — разметка V2.
            self.full = full_b if "ЧАСТЬ СПЕЦИФИКАЦИИ: изменённые строки" in task.prompt else truth(a)
            return super().run(task, cancel)
    luna = Luna(truth(a))
    prepared = runner.prepare(manifest, structure=True)
    result = runner.run(prepared, tmp_path / "out", execute=True, executor=luna, inherit=True)
    assert result["extractions_complete"], result["incomplete_versions"]
    changed = [p for p in luna.prompts if "ЧАСТЬ СПЕЦИФИКАЦИИ: изменённые строки" in p]
    assert len(changed) == 1 and "Лоток 700х50х3000" in changed[0] and "Щит ЩР-1 " not in changed[0]
    cmp = result["reports"][0]["comparison"]
    texts = [e["text"] for e in __import__(
        "backend.app.services.version_comparison.specification_compare", fromlist=["x"]).change_entries(cmp)]
    assert any("Лоток 700х50х3000" in t and t.startswith("Добавлено") for t in texts)
    assert any("5х10" in t and "100" in t and "120" in t for t in texts)
    assert cmp["counts"]["added"] == 1 and cmp["counts"]["removed"] == 0
    key_b = {t["label"]: t["key"] for t in json.loads((tmp_path / "out" / "plan.json").read_text())["tasks"]}["V2"]
    info = json.loads((tmp_path / "out" / "calls" / key_b / "inherited.json").read_text())
    assert info["from"] == "V1" and info["origin"] == "модель по изменениям" and info["complete"]


def test_mostly_new_version_is_marked_whole_not_by_changes(tmp_path, monkeypatch):
    """Крупная редакция (изменено больше половины строк) — версия размечается целиком, как без наследования."""
    monkeypatch.setattr(portions, "CONTEXT_RESERVE", 800)
    _write(tmp_path, "a.md", pages())
    renamed = [[x.replace("Щит ЩР", "Шкаф ШР").replace("Лоток", "Короб").replace("Крепёж", "Крепление")
                for x in p] for p in pages()]
    _write(tmp_path, "b.md", renamed)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"schema": "whole-specification-input/v1", "pairs": [
        {"id": "p", "old": {"path": "a.md", "label": "V1", "all_blocks": True},
         "new": {"path": "b.md", "label": "V2", "all_blocks": True}}]}), encoding="utf-8")
    a, b = load_source(tmp_path / "a.md", label="V1"), load_source(tmp_path / "b.md", label="V2")

    class Luna(PortionLuna):
        def run(self, task, cancel):
            self.full = truth(b) if '"label": "V2"' in task.prompt else truth(a)
            return super().run(task, cancel)
    luna = Luna(truth(a))
    result = runner.run(runner.prepare(manifest, structure=True), tmp_path / "out", execute=True, executor=luna,
                        inherit=True)
    assert result["extractions_complete"], result["incomplete_versions"]
    assert not any("ЧАСТЬ СПЕЦИФИКАЦИИ: изменённые строки" in p for p in luna.prompts)


def test_kept_markup_is_already_normalized_and_fixes_stay_visible(tmp_path):
    a = load_source(_write(tmp_path, "a.md", pages()), label="V1")
    at = {ln["text"]: ln["line"] for b in a["blocks"] for ln in b["lines"]}
    wrong = at[row("Лоток 300х50х3000", "90", "м")]
    sloppy = truth(a) | {"notes": [*truth(a)["notes"], {"line": wrong}]}
    checked, payload = runner.best_markup(a, sloppy, validate)
    assert checked["complete"] and {"line": wrong} not in payload["notes"]
    assert [f["line"] for f in checked["autocorrections"]] == [wrong]
