"""Whole-specification changes belong to source pages, not the sheet matching."""
from copy import deepcopy
import json

import pytest

from backend.app.services.version_comparison import paths, specification_export as export, specification_view
from backend.app.services.version_comparison.specification_compare import FIELDS, change_entries, compare, render_entry
from backend.app.services.version_comparison_ai.sheet_changes import view


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def row(name, qty, pages, *, side, pos="1"):
    return {"id": name, "section": "Кабели", "parent": "Щит 1", "fields": dict.fromkeys(FIELDS, "") | {
        "name": name, "code": name, "qty": qty, "unit": "м", "pos": pos},
        "sources": [{"document": side + ".pdf", "page": p, "sheet": str(p + 10), "block_id": side + str(p),
                     "line_start": 1, "line_end": 1, "quote": name + " " + qty} for p in pages]}


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    run = {"object_id": "obj", "project_id": "proj", "run_id": "run", "state": "done",
           "a": {"version_id": "va", "label": "V1", "sha256": "a" * 64, "file_name": "old.pdf"},
           "b": {"version_id": "vb", "label": "V2", "sha256": "b" * 64, "file_name": "new.pdf"},
           "items": [{"spid": spid, "a_page": a, "b_page": b, "title": "Спецификация", "spec": {}}
                     for spid, a, b in (("from", 2, 5), ("to", 3, 6), ("quiet", 3, 5),
                                        ("other", 4, 7), ("wrong_side", 6, 2),
                                        ("only_a", 4, None), ("only_b", None, 7))]}
    write(paths.run_dir("obj", "run") / "run.json", run)
    old_rows = [row("Перенесённый кабель", "10", [2], side="old"),
                row("Не менялся", "7", [3], side="old"), row("Удалённый", "3", [4], side="old")]
    new_rows = [row("Перенесённый кабель", "12", [6], side="new"),
                row("Не менялся", "7", [5], side="new", pos="22"), row("Добавленный", "4", [7], side="new")]
    snapshots = []
    for name, side, pages_, rows in (("old", "a", [2, 3, 4, 6], old_rows), ("new", "b", [2, 5, 6, 7], new_rows)):
        write(paths.profile_dir("obj", run[side]["sha256"]) / "pages.json",
              {"pages": [{"page": p, "kind": "spec"} for p in pages_]})
        snapshots.append({"label": run[side]["label"], "document": name + ".pdf",
                          "pdf_sha256": run[side]["sha256"], "source_sha256": name * 20,
                          "complete": True, "issues": [], "rows": rows,
                          "selection": {"mode": "pages", "pages": pages_, "block_ids": [name + str(p) for p in pages_]}})
    report = {"schema": "whole-specification/1", "reports": [{"id": "spec", "comparison": compare(*snapshots)}]}
    export.publish("obj", "run", report)
    # Opening a specification must never fall through to sheet AI or recovery jobs.
    def forbidden(*args, **kwargs):
        pytest.fail("whole-spec view must only read its saved result")
    monkeypatch.setattr(view, "load_result", forbidden)
    monkeypatch.setattr(view, "_active_job", forbidden)
    monkeypatch.setattr(view.vc_jobs, "get_run", forbidden)
    return run, report


def test_changed_row_shown_on_either_touched_sheet_once_with_full_locations(prepared):
    from_page, to_page = [view.panel("obj", "run", spid) for spid in ("from", "to")]
    assert from_page["source"] == specification_view.SOURCE
    assert from_page["changes"] == to_page["changes"]
    change, = from_page["changes"]
    assert change["fields"] == [{"name": "Количество", "before": "10 м", "after": "12 м"}]
    assert "л.12 (стр. PDF 2)" in change["place"] and "л.16 (стр. PDF 6)" in change["place"]
    assert "entries" not in from_page and "diagnostics" not in from_page
    assert not view.focus("obj", "run", "from", change["id"])["available"]
    # The exact same event, not a second reconstructed comparison, supplies MD.
    *_, report = prepared
    event = next(e for e in change_entries(report["reports"][0]["comparison"]) if e["kind"] == "changed")
    assert change["id"] == event["id"]
    md = view.md("obj", "run", "from")
    assert md.count(render_entry(event)) == 1
    assert "Добавленный" not in md and "Удалённый" not in md


@pytest.mark.parametrize("spid", ["quiet", "wrong_side"])
def test_other_sheets_and_cross_side_numeric_coincidence_do_not_leak(prepared, spid):
    selected = view.panel("obj", "run", spid)
    assert selected["changes"] == [] and selected["unresolved"] == []
    assert selected["state"] == "no_construction_changes"
    assert specification_view.NO_CHANGES in view.md("obj", "run", spid)


@pytest.mark.parametrize("spid,kinds", [("other", {"added", "removed"}), ("only_a", {"removed"}), ("only_b", {"added"})])
def test_addition_and_removal_are_local_to_their_existing_side(prepared, spid, kinds):
    changes = view.panel("obj", "run", spid)["changes"]
    assert {c["kind"] for c in changes} == kinds
    for change in changes:
        assert all(not field["before" if change["kind"] == "added" else "after"] for field in change["fields"])


def test_multi_page_row_and_repeated_report_do_not_duplicate_event(prepared):
    _, report = prepared
    pair = next(p for p in report["reports"][0]["comparison"]["pairs"] if p["field_changes"])
    pair["old"]["sources"] += deepcopy(pair["old"]["sources"])
    pair["new"]["sources"].append(pair["new"]["sources"][0] | {"page": 5})
    report["reports"].append(deepcopy(report["reports"][0]))
    export.publish("obj", "run", report)
    assert len(view.panel("obj", "run", "from")["changes"]) == 1


@pytest.mark.parametrize("problem", ["missing", "stale", "corrupt", "partial_scope"])
def test_incomplete_saved_result_never_claims_unchanged_or_uses_old_ai(prepared, problem):
    run, report = prepared
    artifact = paths.run_dir("obj", "run") / export.FILENAME
    if problem == "missing":
        artifact.unlink()
    elif problem == "corrupt":
        artifact.write_text("{broken", encoding="utf-8")
    elif problem == "stale":
        run["b"]["sha256"] = "c" * 64
        write(paths.run_dir("obj", "run") / "run.json", run)
    else:
        report["reports"][0]["comparison"]["old"]["selection"]["pages"].remove(6)
        export.publish("obj", "run", report)
    selected = view.panel("obj", "run", "quiet")
    assert selected["state"] == "specification_unavailable" and selected["changes"] == []
    assert "Отсутствие изменений" in view.md("obj", "run", "quiet")


def test_unresolved_positions_are_scoped_to_the_current_sheet(prepared):
    _, report = prepared
    cmp = report["reports"][0]["comparison"]
    cmp["unresolved"] = [{"old_rows": cmp.pop("removed"), "new_rows": cmp.pop("added"), "reason": "Неоднозначно"}]
    export.publish("obj", "run", report)
    assert view.panel("obj", "run", "quiet")["state"] == "no_construction_changes"
    selected = view.panel("obj", "run", "other")
    assert selected["state"] == "specification_incomplete" and not selected["changes"]
    assert len(selected["unresolved"]) == 1
    assert "Требует уточнения" in view.md("obj", "run", "other")


def test_drawing_retains_existing_sheet_result_flow(prepared, monkeypatch):
    run, _ = prepared
    run["items"].append({"spid": "plan", "title": "План этажа со спецификацией", "a_page": 1,
                         "b_page": 1, "result_status": "same_zones_changed"})
    write(paths.run_dir("obj", "run") / "run.json", run)
    monkeypatch.setattr(view.vc_jobs, "get_run", lambda *args: run)
    monkeypatch.setattr(view, "load_result", lambda *args: None)
    monkeypatch.setattr(view, "_active_job", lambda *args: None)
    monkeypatch.setattr(view, "_old_result", lambda *args: None)
    assert view.panel("obj", "run", "plan")["state"] == "engine_same"


def test_every_changed_field_and_side_specific_unit_survives_projection(prepared):
    _, report = prepared
    pair = next(p for p in report["reports"][0]["comparison"]["pairs"] if p["field_changes"])
    pair["old"]["fields"].update(mark="AB-1", note="прежнее", unit="м")
    pair["new"]["fields"].update(mark="AB-2", note="новое", unit="шт.")
    pair["field_changes"] += [{"field": "mark", "old": "AB-1", "new": "AB-2"},
                              {"field": "note", "old": "прежнее", "new": "новое"},
                              {"field": "unit", "old": "м", "new": "шт."}]
    export.publish("obj", "run", report)
    change, = view.panel("obj", "run", "from")["changes"]
    assert [field["name"] for field in change["fields"]] == ["Количество", "Марка", "Примечание", "Ед. изм."]
    assert change["fields"][0]["before"] == "10 м" and change["fields"][0]["after"] == "12 шт."
