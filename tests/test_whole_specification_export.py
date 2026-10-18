"""A single specification source in series MD; no PDF or model calls."""
from copy import deepcopy
import json

import pytest

from backend.app.services.version_comparison import paths, series as vc_series, specification_export as export
from backend.app.services.version_comparison.specification_compare import compare
from backend.app.services.version_comparison_ai.sheet_changes import series_summary
from backend.app.services.version_comparison_ai.md_report import parse


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def row(name, qty, pages, *, suffix="", pos="1"):
    fields = {key: "" for key in ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")}
    fields.update(pos=pos, name=name, qty=qty, unit="м", code=name)
    return {"id": name + suffix, "section": "Кабели", "parent": "", "fields": fields,
            "sources": [{"document": "OLD.pdf" if suffix == "old" else "NEW.pdf", "page": p,
                         "sheet": str(p + 10), "block_id": f"blk_{suffix}{p}", "line_start": p * 10,
                         "line_end": p * 10, "quote": f"{name} {qty}"} for p in pages]}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    run = {"object_id": "obj", "project_id": "project", "run_id": "run", "state": "done",
           "a": {"version_id": "va", "label": "V1", "sha256": "a" * 64, "file_name": "OLD.pdf"},
           "b": {"version_id": "vb", "label": "V2", "sha256": "b" * 64, "file_name": "NEW.pdf"},
           "items": [{"spid": "plan", "title": "План этажа", "a_page": 1, "b_page": 1},
                     {"spid": "spec1", "title": "СО л.12 · Спецификация", "a_page": 2, "b_page": 5, "spec": {}},
                     {"spid": "spec2", "title": "Спецификация (продолжение)", "a_page": 3, "b_page": 6, "spec": {}}]}
    write(paths.run_dir("obj", "run") / "run.json", run)
    for side, spec_pages in (("a", [2, 3]), ("b", [5, 6])):
        write(paths.profile_dir("obj", run[side]["sha256"]) / "pages.json",
              {"pages": [{"page": 1, "kind": "plan"}] + [{"page": p, "kind": "spec"} for p in spec_pages]})
    job = {"job_id": "job", "a": run["a"], "b": run["b"],
           "sheets": [{**item, "ai_status": "done", "item_kind": "compare"} for item in run["items"]]}
    rec = {"pairs": [{"index": 0, "run_id": "run", "job_id": "job", "a_label": "V1", "b_label": "V2"}]}
    monkeypatch.setattr(series_summary.series_ai, "_load", lambda *a: rec)
    monkeypatch.setattr(series_summary.vc_series, "get", lambda *a: {"project_id": "project"})
    monkeypatch.setattr(series_summary.old_packet, "_project", lambda *a: {"code": "ТЕСТ"})
    monkeypatch.setattr(series_summary.jobs, "get_job", lambda *a: job)
    answers = {
        "plan": {"job_id": "job", "changes": [{"object": "Стена", "summary": "Толщина 100 → 120 мм."}]},
        "spec1": {"job_id": "job", "changes": [{"object": "Кабель", "summary": "ПОСТРАНИЧНЫЙ ДУБЛЬ"}]},
        "spec2": {"job_id": "job", "changes": [{"object": "Кабель", "summary": "ПОСТРАНИЧНЫЙ ДУБЛЬ"}]},
    }
    monkeypatch.setattr(series_summary.view, "load_result", lambda obj, rid, spid: answers.get(spid))
    sides = []
    for name, side, pages_, qty in (("old", "a", [2, 3], "10"), ("new", "b", [5, 6], "12")):
        sides.append({"label": run[side]["label"], "document": run[side]["file_name"],
                      "source_sha256": name[0] * 64, "pdf_sha256": run[side]["sha256"],
                      "complete": True, "issues": [],
                      "selection": {"mode": "pages", "pages": pages_, "block_ids": [f"blk_{name}{p}" for p in pages_]},
                      "rows": [row("Кабель", qty, pages_, suffix=name),
                               row("Без изменения", "7", pages_[:1], suffix=name, pos="2" if side == "a" else "3")]})
    report = {"schema": "whole-specification/1", "settings": {"model": "gpt-6-luna"},
              "reports": [{"id": "spec", "comparison": compare(*sides)}]}
    return run, job, rec, answers, report


def test_series_includes_whole_spec_once_and_preserves_other_sheets(setup):
    _, _, _, _, report = setup
    report["reports"].append(deepcopy(report["reports"][0]))
    export.publish("obj", "run", report)
    text = series_summary.build("obj", "series")["text"]
    assert text.count("Количество: 10 м → 12 м") == 1
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text
    assert "Без изменения" not in text and "SHA-256" not in text and "blk_" not in text
    assert "Стена" in text and "Толщина 100 → 120 мм." in text
    assert "л.12 (стр. PDF 2), л.13 (стр. PDF 3)" in text
    assert "л.15 (стр. PDF 5), л.16 (стр. PDF 6)" in text
    parsed = parse.parse(text.encode())
    assert not parsed["issues"]
    assert parsed["transitions"][0]["items"] == 2


def test_missing_full_spec_result_is_explicit_not_no_changes(setup):
    _, _, _, answers, _ = setup
    answers["plan"]["changes"] = []
    text = series_summary.build("obj", "series")["text"]
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text
    assert "Спецификация не описана полностью" in text
    assert "изменений содержания нет" not in text
    parsed = parse.parse(text.encode())
    assert parsed["transitions"][0]["source_incomplete"]


def test_whole_spec_export_does_not_require_sheet_ai_job(setup):
    _, _, rec, _, report = setup
    export.publish("obj", "run", report)
    rec["pairs"][0]["job_id"] = None
    text = series_summary.build("obj", "series")["text"]
    assert text.count("Количество: 10 м → 12 м") == 1
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text


def test_whole_spec_export_works_without_series_ai_record(setup, monkeypatch):
    _, _, _, _, report = setup
    export.publish("obj", "run", report)
    monkeypatch.setattr(series_summary.series_ai, "_load", lambda *a: None)
    monkeypatch.setattr(series_summary.vc_series, "get", lambda *a: {
        "object_id": "obj", "series_id": "series", "project_id": "project",
        "pairs": [{"a": "va", "b": "vb", "a_label": "V1", "b_label": "V2", "run_id": "run"}],
    })
    text = series_summary.build("obj", "series")["text"]
    assert "Количество: 10 м → 12 м" in text
    assert "ИИ-описание остальных листов не запускалось" in text


def test_series_manifest_routes_each_transition_to_its_bound_run(setup, monkeypatch):
    run, _, _, _, report = setup
    second = deepcopy(run)
    second.update(run_id="run2")
    second["a"], second["b"] = deepcopy(run["b"]), {
        "version_id": "vc", "label": "V3", "sha256": "c" * 64, "file_name": "V3.pdf"
    }
    write(paths.run_dir("obj", "run2") / "run.json", second)
    # Both reports must be bound to the exact old/new PDF identity of their run.
    second_report = deepcopy(report)
    cmp = second_report["reports"][0]["comparison"]
    cmp["old"]["source_sha256"] = cmp["new"]["source_sha256"]
    for side, run_side in (("old", "a"), ("new", "b")):
        cmp[side]["pdf_sha256"] = second[run_side]["sha256"]
        cmp[side]["document"] = second[run_side]["file_name"]
    write(paths.series_path("obj", "series"), {
        "object_id": "obj", "series_id": "series", "project_id": "project",
        "pairs": [{"a": "va", "b": "vb", "run_id": "run"}, {"a": "vb", "b": "vc", "run_id": "run2"}],
    })
    monkeypatch.setattr(vc_series, "get", lambda *a: pytest.fail("publication must not restart comparisons"))
    manifest = {"schema": export.SERIES_SCHEMA, "transitions": [
        {"run_id": "run", "report": report}, {"run_id": "run2", "report": second_report}
    ]}
    paths_out = export.publish_series("obj", "series", manifest)
    assert [path.parent.name for path in paths_out] == ["run", "run2"]
    assert export.context("obj", "run")["reports"]
    assert export.context("obj", "run2")["reports"]


def test_series_manifest_rejects_wrong_transition_before_any_write(setup, monkeypatch):
    _, _, _, _, report = setup
    write(paths.series_path("obj", "series"), {
        "object_id": "obj", "series_id": "series", "project_id": "project",
        "pairs": [{"a": "wrong", "b": "vb", "run_id": "run"}],
    })
    manifest = {"schema": export.SERIES_SCHEMA, "transitions": [{"run_id": "run", "report": report}]}
    with pytest.raises(ValueError, match="версия A"):
        export.publish_series("obj", "series", manifest)
    assert not (paths.run_dir("obj", "run") / export.FILENAME).exists()


def test_summary_status_is_read_only_and_requires_every_transition(setup, monkeypatch):
    run, _, _, _, report = setup
    series_record = {"object_id": "obj", "series_id": "series", "project_id": "project",
                     "pairs": [{"a": "va", "b": "vb", "run_id": "run"},
                               {"a": "vb", "b": "missing", "run_id": None}]}
    write(paths.series_path("obj", "series"), series_record)
    monkeypatch.setattr(vc_series, "get", lambda *a: pytest.fail("summary_status must not call mutating get"))
    assert export.summary_status("obj", "series") == {"ready": False, "transitions": 2, "complete": 0}
    # A series cannot be ready while an unstarted transition remains.
    series_record["pairs"] = series_record["pairs"][:1]
    write(paths.series_path("obj", "series"), series_record)
    export.publish("obj", "run", report)
    assert export.summary_status("obj", "series") == {"ready": True, "transitions": 1, "complete": 1}


@pytest.mark.parametrize("side", ["old", "new"])
def test_report_binding_rejects_wrong_or_missing_pdf_identity(setup, side):
    *_, report = setup
    for bad in (None, "f" * 64):
        report["reports"][0]["comparison"][side]["pdf_sha256"] = bad
        with pytest.raises(ValueError, match="pdf_sha256"):
            export.publish("obj", "run", report)
    assert not (paths.run_dir("obj", "run") / export.FILENAME).exists()


def test_stale_report_is_not_used_after_run_version_changes(setup):
    run, _, _, _, report = setup
    export.publish("obj", "run", report)
    run["b"]["sha256"] = "c" * 64
    write(paths.run_dir("obj", "run") / "run.json", run)
    text = series_summary.build("obj", "series")["text"]
    assert "Количество: 10 м → 12 м" not in text
    assert "Спецификация не описана" in text
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text


def test_added_removed_unmatched_and_not_described_spec_sheets_are_suppressed(setup):
    _, job, _, answers, report = setup
    export.publish("obj", "run", report)
    job["sheets"][1].update(item_kind="only_b", a_page=None)
    job["sheets"][2].update(item_kind="only_a", b_page=None, ai_status="failed")
    answers["spec1"] = {"job_id": "job", "single": {"sheet_kind": "спецификация", "summary": "ЛИСТОВОЕ ДОБАВЛЕНИЕ"}}
    answers.pop("spec2")
    text = series_summary.build("obj", "series")["text"]
    assert "ЛИСТОВОЕ ДОБАВЛЕНИЕ" not in text and "ошибка ИИ" not in text
    assert text.count("Количество: 10 м → 12 м") == 1


def test_plan_with_spec_table_is_not_suppressed_by_single_model_label(setup):
    _, job, _, answers, _ = setup
    job["sheets"][0]["item_kind"] = "only_b"
    answers["plan"] = {"job_id": "job", "single": {"sheet_kind": "спецификация", "summary": "Описание плана"}}
    text = series_summary.build("obj", "series")["text"]
    assert "Описание плана" in text


def test_repeated_overlapping_scope_and_incomplete_reports_cannot_publish(setup):
    *_, report = setup
    other = deepcopy(report["reports"][0])
    next(p for p in other["comparison"]["pairs"] if p["field_changes"])["field_changes"][0]["new"] = "99"
    report["reports"].append(other)
    with pytest.raises(ValueError, match="пересекаются"):
        export.publish("obj", "run", report)
    report["reports"] = [other]
    other["comparison"]["complete"] = False
    with pytest.raises(ValueError, match="не разобрана"):
        export.publish("obj", "run", report)


def test_partial_spec_coverage_is_not_presented_as_full(setup):
    *_, report = setup
    report["reports"][0]["comparison"]["new"]["selection"]["pages"] = [5]
    for pair in report["reports"][0]["comparison"]["pairs"]:
        pair["new"]["sources"] = [ref for ref in pair["new"]["sources"] if ref["page"] == 5]
    export.publish("obj", "run", report)
    text = series_summary.build("obj", "series")["text"]
    assert "Спецификация не описана полностью" in text
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text


def test_publishing_rejects_drawing_pages_and_source_refs_outside_scope(setup):
    run, _, _, _, report = setup
    report["reports"][0]["comparison"]["new"]["selection"]["pages"] = [5]
    with pytest.raises(ValueError, match="вне области"):
        export.publish("obj", "run", report)
    report["reports"][0]["comparison"]["new"]["selection"]["pages"] = [5, 6]
    write(paths.profile_dir("obj", run["b"]["sha256"]) / "pages.json",
          {"pages": [{"page": 5, "kind": "spec", "stamp": {"title": "План и спецификация"}}]})
    with pytest.raises(ValueError, match="чертёж"):
        export.publish("obj", "run", report)


def test_one_selected_block_cannot_claim_coverage_of_the_whole_page(setup):
    *_, report = setup
    selection = report["reports"][0]["comparison"]["old"]["selection"]
    selection["mode"] = "block_ids"
    selection["block_ids"] = selection["block_ids"][:1]
    with pytest.raises(ValueError, match="целые страницы"):
        export.publish("obj", "run", report)


def test_unknown_unmatched_spec_pages_classified_from_specific_title(setup):
    run, job, _, answers, _ = setup
    run["items"] = []
    write(paths.run_dir("obj", "run") / "run.json", run)
    for side in ("a", "b"):
        (paths.profile_dir("obj", run[side]["sha256"]) / "pages.json").unlink()
    job["sheets"] = [job["sheets"][1]]
    text = series_summary.build("obj", "series")["text"]
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text
    assert "Спецификация не описана" in text


def test_only_page_moves_or_renumbering_produce_no_spec_entries(setup):
    _, _, _, answers, report = setup
    answers["plan"]["changes"] = []
    for pair in report["reports"][0]["comparison"]["pairs"]:
        pair["field_changes"] = []
    export.publish("obj", "run", report)
    text = series_summary.build("obj", "series")["text"]
    assert "### Спецификация" not in text
    assert "ПОСТРАНИЧНЫЙ ДУБЛЬ" not in text
    assert "изменений содержания нет" in text
    assert parse.parse(text.encode())["transitions"][0]["explicit_no_content_changes"]


def test_skipped_transition_without_run_remains_exportable(setup):
    _, _, rec, _, _ = setup
    rec["pairs"][0].update(run_id=None, job_id=None, reason="сравнение не запускалось")
    text = series_summary.build("obj", "series")["text"]
    assert "сравнение не запускалось" in text


def test_series_cli_manifest_binds_report_to_explicit_adjacent_versions():
    import hashlib
    from scripts.publish_whole_specifications import series_manifest

    inputs = {'schema': 'whole-specification-input/v1', 'pairs': [
        {'id': 'p1', 'old': {'version_id': 'v1'}, 'new': {'version_id': 'v2'}},
        {'id': 'p2', 'old': {'version_id': 'v2'}, 'new': {'version_id': 'v3'}}]}
    raw = json.dumps(inputs).encode()
    report = {'schema': 'whole-specification/1', 'manifest_sha256': hashlib.sha256(raw).hexdigest(),
              'reports': [{'id': 'p2'}, {'id': 'p1'}]}
    record = {'pairs': [{'a': 'v1', 'b': 'v2', 'run_id': 'r1'}, {'a': 'v2', 'b': 'v3', 'run_id': 'r2'}]}
    result = series_manifest(report, raw, record)
    assert [x['run_id'] for x in result['transitions']] == ['r1', 'r2']
    assert [x['report']['reports'][0]['id'] for x in result['transitions']] == ['p1', 'p2']
    with pytest.raises(ValueError, match='исходному манифесту'):
        series_manifest(report, raw + b' ', record)
    with pytest.raises(ValueError, match='все переходы'):
        series_manifest(report, raw, {'pairs': record['pairs'][:1]})
