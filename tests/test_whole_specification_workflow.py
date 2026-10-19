"""One sequential launch, distinct evidence routes and one MD; offline executors only."""
import json
import time
from pathlib import Path

import pytest

from backend.app.services.version_comparison import paths, specification_export as export
from backend.app.services.version_comparison_ai import executor, jobs, sheet_match, stage_queue as queue
from backend.app.services.version_comparison_ai import whole_specifications as whole
from backend.app.services.version_comparison_ai.sheet_changes import series_summary


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def corpus(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    versions = {vid: {"version_id": vid, "sha256": char * 64, "file_name": vid + ".pdf", "user_label": label}
                for vid, char, label in (("va", "a", "V1"), ("vb", "b", "V2"))}
    write(paths.catalog_path("obj"), {"projects": [{"project_id": "proj", "code": "TEST", "versions": list(versions)}],
                                     "versions": versions, "object_id": "obj"})
    run = {"object_id": "obj", "run_id": "run", "project_id": "proj", "state": "done",
           "a": versions["va"] | {"label": "V1"}, "b": versions["vb"] | {"label": "V2"},
           "items": [{"spid": "plan", "title": "План со встроенной спецификацией", "kind": "compare", "status": "done",
                      "a_page": 1, "b_page": 1, "result_dir": "plan", "result_status": "different"},
                     {"spid": "spec", "title": "Спецификация", "kind": "compare", "status": "done", "spec": {},
                      "a_page": 2, "b_page": 3, "result_dir": "spec", "result_status": "different"}]}
    write(paths.run_dir("obj", "run") / "run.json", run)
    write(paths.series_path("obj", "series"), {"object_id": "obj", "series_id": "series", "project_id": "proj", "state": "done",
        "pairs": [{"a": "va", "b": "vb", "run_id": "run", "run_state": "done", "a_label": "V1", "b_label": "V2"}]})
    for vid, page in (("va", 2), ("vb", 3)):
        write(paths.profile_dir("obj", versions[vid]["sha256"]) / "pages.json", {"pages": [
            {"page": 1, "kind": "spec", "stamp": {"title": "План со встроенной спецификацией"}},
            {"page": page, "kind": "spec", "stamp": {"title": "Спецификация"}}]})
    def prepare(odir, codes, output):
        output.mkdir(parents=True)
        sides = []
        for vid, page, qty in (("va", 2, "10"), ("vb", 3, "12")):
            text = (f"# Document: {vid}.pdf\n## Page {page} — Sheet 1 — Спецификация\n"
                    f"### BLOCK #1 [TEXT]: blk_{vid[-1] * 8}\n"
                    "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |\n"
                    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
                    f"| 1 | Кабель | AB-1 | К1 | Завод | м | {qty} | | |\n")
            (output / (vid + ".md")).write_text(text)
            sides.append({"path": vid + ".md", "version_id": vid, "label": versions[vid]["user_label"],
                          "pdf_sha256": versions[vid]["sha256"], "pages": [page]})
        write(output / "manifest.json", {"schema": "whole-specification-input/v1",
            "pairs": [{"id": "va--vb", "old": sides[0], "new": sides[1]}]})
    monkeypatch.setattr(whole.specification_prepare, "prepare", prepare)
    fake = executor.FakeDescriber(lambda task: {"sections": [], "parents": [], "continuations": [], "notes": []},
                                  model="gpt-6-luna")
    executor.set_override(fake)
    yield run, fake
    executor.set_override(None)


def finish_spec():
    for _ in range(500):
        st = whole.status("obj", "series")
        if st["state"] not in ("running", "pausing"):
            return st
        time.sleep(.01)
    pytest.fail("spec stage did not finish")


def test_full_spec_stage_builds_publishes_and_reuses_without_model_or_pdf(corpus, monkeypatch):
    _, fake = corpus
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done", result
    assert len(fake.calls) == 2 and whole.ready("obj", "series")
    cmp = export.context("obj", "run")["reports"][0]["comparison"]
    assert cmp["counts"]["changed"] == 1
    monkeypatch.setattr(executor, "get", lambda *args: pytest.fail("published data must not probe Codex"))
    assert whole.start("obj", "series", consent=True)["reused"]
    assert len(fake.calls) == 2
    md = series_summary.build("obj", "series")["text"]
    assert md.count("Количество: 10 м → 12 м") == 1


def test_sheet_jobs_and_estimation_exclude_specifications_even_when_explicit(corpus, monkeypatch):
    run, _ = corpus
    items, skipped = jobs._sc_items(run, None)
    assert [it["spid"] for it in items] == ["plan"]
    assert [it["spid"] for it in skipped] == ["spec"]
    assert jobs._sc_items(run, ["spec"])[0] == []
    mixed = run | {"items": [run["items"][0] | {"b_page": 3}]}
    assert jobs._sc_items(mixed, None)[0] == [], "an incorrect cross-kind pair cannot send a spec page"
    monkeypatch.setattr(jobs.vc_jobs, "get_run", lambda *args: run)
    monkeypatch.setattr(executor, "get", lambda *args: pytest.fail("spec sheet must not reach any executor"))
    with pytest.raises(jobs.JobError, match="нет листов"):
        jobs.create_job("obj", "run", spids=["spec"], method="sheet_changes", consent=True)


def test_old_queued_spec_job_is_skipped_before_packet_or_session(corpus, monkeypatch, tmp_path):
    job = {"job_id": "old", "sheets": [{"spid": "spec", "ai_status": "queued", "chunks": []}], "progress": {}}
    write(jobs.job_dir("obj", "run", "old") / "job.json", job)
    from backend.app.services.version_comparison_ai.sheet_changes import run as sheet_run
    monkeypatch.setattr(sheet_run, "run_sheet", lambda *a, **k: pytest.fail("old spec job must be suppressed"))
    jobs._run_sheet_changes("obj", "run", job, job["sheets"][0], None, None, {}, tmp_path / "packet")
    saved = json.loads((jobs.job_dir("obj", "run", "old") / "job.json").read_text())
    assert saved["sheets"][0]["ai_status"] == "engine_only"
    assert not (tmp_path / "packet").exists()


def test_luna_questions_exclude_specs_but_keep_drawings_with_tables(corpus):
    run, _ = corpus
    versions = [run["a"], run["b"]]
    rows = [{"cells": {"va": {"state": "matched", "page": page},
                       "vb": {"state": "unmatched", "candidates": [{"page": 1}, {"page": 3}]}}}
            for page in (1, 2)]
    qs = sheet_match.questions("obj", {"project_id": "proj", "code": "TEST"}, versions, {"rows": rows}, {})
    assert len(qs) == 1 and qs[0]["a"] == 1 and qs[0]["cands"] == [1]


def test_reverse_question_cannot_send_a_spec_candidate(corpus, monkeypatch):
    _, fake = corpus
    monkeypatch.setattr(sheet_match, "_labeled", lambda *args: pytest.fail("spec must not be rasterized"))
    result = sheet_match.ask("obj", "TEST", {"sha": "a" * 64, "page": 2, "label": "V1"},
                            {"sha": "b" * 64, "label": "V2"}, [3], True, fake, None, {})
    assert result["cached"] and not fake.calls


@pytest.fixture
def staged(corpus, monkeypatch):
    states = {key: "todo" for key in queue.STAGES}
    started, paused = [], []
    monkeypatch.setattr(queue, "GRACE_S", 0)
    monkeypatch.setattr(queue, "_stages", lambda *a: {key: {"state": value} for key, value in states.items()})
    def start(oid, item):
        started.append(item["stage"])
        states[item["stage"]] = "running"
    monkeypatch.setattr(queue, "_start_stage", start)
    monkeypatch.setattr(queue, "_pause_stage", lambda oid, item: paused.append(item["stage"]))
    return states, started, paused


def test_one_luna_launch_runs_all_five_stages_strictly_in_order(staged):
    states, started, _ = staged
    result = queue.start_luna("obj", project_ids=["proj"], consent=True)
    assert [it["stage"] for it in result["pipeline"]["items"]] == list(queue.PIPELINE)
    assert started == ["luna"]
    for stage in queue.PIPELINE:
        queue.dispatch("obj")
        assert started[-1] == stage
        assert not whole.summary_gate("obj", "series")["ready"]
        states[stage] = "done"
        queue.dispatch("obj")
    assert started == list(queue.PIPELINE)
    assert queue.luna_status("obj")["state"] == "done"
    assert whole.summary_gate("obj", "series")["ready"]


@pytest.mark.parametrize("stage,state", [("luna", "failed"), ("ai", "partial"), ("specifications", "failed")])
def test_pipeline_failure_never_starts_next_stage_or_publishes_md(staged, monkeypatch, stage, state):
    states, started, _ = staged
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {"job": {"sheet"}})
    if stage == "ai":
        def start(oid, item):           # доразбор неполных листов снова заканчивается «частично»
            started.append(item["stage"])
            states[item["stage"]] = "partial" if item["stage"] == "ai" and started.count("ai") > 1 else "running"
        monkeypatch.setattr(queue, "_start_stage", start)
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE:
        states[key] = state if key == stage else "done"
        queue.dispatch("obj")
        if key == stage:
            break
    if stage == "ai":
        queue.dispatch("obj")                                    # первый «частично» с неполными — один доразбор
        queue.dispatch("obj")
        assert started.count("ai") == 2
    before = list(started)
    queue.dispatch("obj")
    assert started == before and queue.status("obj")["state"] == "failed"
    assert not whole.summary_gate("obj", "series")["ready"]
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    assert started[-1] == stage and len(queue.status("obj")["items"]) == 5


def test_partial_only_by_engine_notes_continues_pipeline(staged, monkeypatch):
    """Модель разобрала все записи, «частично» — от пометок движка (дубль листа, нет текстового слоя)."""
    states, started, _ = staged
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {})
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "partial" if key == "ai" else "done"
        queue.dispatch("obj")
    assert started[-1] == "specifications" and started.count("ai") == 1
    ai = next(it for it in queue.status("obj")["items"] if it["stage"] == "ai")
    assert ai["state"] == "done" and ai["result"] == "partial"


def test_incomplete_sheets_fixed_by_retry_continue_pipeline(staged, monkeypatch):
    states, started, _ = staged
    bad = {"job": {"sheet"}}
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: bad)
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "partial" if key == "ai" else "done"
        queue.dispatch("obj")
    queue.dispatch("obj")
    assert started.count("ai") == 2 and queue.status("obj")["state"] == "running"
    bad.clear()
    states["ai"] = "done"
    queue.dispatch("obj")
    assert started[-1] == "specifications"


def test_pause_blocks_later_stages_and_resume_keeps_scope(staged):
    states, started, paused = staged
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    queue.pause_luna("obj")
    states["luna"] = "paused"
    queue.dispatch("obj")
    assert started == ["luna"] and paused == ["luna"]
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    assert started == ["luna", "luna"]


def test_empty_spec_scope_finishes_without_model(corpus, monkeypatch):
    run, fake = corpus
    run["items"] = run["items"][:1]
    write(paths.run_dir("obj", "run") / "run.json", run)
    for side in ("a", "b"):
        write(paths.profile_dir("obj", run[side]["sha256"]) / "pages.json", {"pages": [
            {"page": 1, "kind": "drawing", "stamp": {"title": "План"}}]})
    monkeypatch.setattr(executor, "get", lambda *a: pytest.fail("no specifications, no calls"))
    assert whole.start("obj", "series", consent=True)["state"] == "done"


def test_incomplete_extraction_stops_publication(corpus):
    executor.set_override(executor.FakeDescriber(lambda t: {"bad": "answer"}, model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "failed"
    assert not (paths.run_dir("obj", "run") / export.FILENAME).exists()


def test_rematching_same_versions_rebinds_saved_spec_without_another_call(corpus, monkeypatch):
    run, fake = corpus
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "done"
    write(paths.run_dir("obj", "newrun") / "run.json", run | {"run_id": "newrun"})
    ser = json.loads(paths.series_path("obj", "series").read_text())
    ser["pairs"][0]["run_id"] = "newrun"
    write(paths.series_path("obj", "series"), ser)
    monkeypatch.setattr(executor, "get", lambda *a: pytest.fail("same PDFs must reuse their specification"))
    assert whole.start("obj", "series", consent=True)["reused"]
    assert len(fake.calls) == 2 and export.context("obj", "newrun")["reports"]


def test_final_stage_writes_one_md_from_drawings_and_full_specifications(corpus, monkeypatch):
    run, _ = corpus
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "done"
    ser = json.loads(paths.series_path("obj", "series").read_text())
    monkeypatch.setattr(queue.vc_series, "get", lambda *a: ser)
    monkeypatch.setattr(queue.vc_jobs, "get_run", lambda *a: run)
    monkeypatch.setattr(queue, "_stages", lambda *a: {"series": {"state": "done", "series_id": "series"},
                                                    "summary": {"state": "todo"}})
    monkeypatch.setattr(queue.series_ai, "status", lambda *a: {"state": "done"})
    monkeypatch.setattr(queue.series_ai, "_load", lambda *a: {"pairs": [
        {"index": 0, "run_id": "run", "job_id": "draw", "a_label": "V1", "b_label": "V2"}]})
    monkeypatch.setattr(jobs, "get_job", lambda *a: {"job_id": "draw", "a": run["a"], "b": run["b"],
        "sheets": [it | {"ai_status": "done", "item_kind": "compare"} for it in run["items"]]})
    monkeypatch.setattr(series_summary.view, "load_result", lambda o, r, spid: {
        "job_id": "draw", "changes": [{"object": "Стена" if spid == "plan" else "ДУБЛЬ",
                                      "summary": "100 → 120 мм" if spid == "plan" else "ПОЛИСТОВАЯ СПЕЦИФИКАЦИЯ"}]})
    queue._start_stage("obj", {"project_id": "proj", "stage": "summary", "pipeline": True})
    text = (paths.series_path("obj", "series").parent / "summary.md").read_text()
    assert text.count("100 → 120 мм") == 1 and text.count("Количество: 10 м → 12 м") == 1
    assert "ДУБЛЬ" not in text and "ПОЛИСТОВАЯ" not in text


def test_user_pause_stops_submitting_further_full_versions(corpus, tmp_path):
    import threading
    from backend.app.services.version_comparison import specification_run as runner
    whole.specification_prepare.prepare(None, None, tmp_path / "input")
    pause = threading.Event()
    def answer(task):
        pause.set()
        return {"sections": [], "parents": [], "continuations": [], "notes": []}
    fake = executor.FakeDescriber(answer, model="gpt-6-luna")
    result = runner.run(runner.prepare(tmp_path / "input/manifest.json", structure=True), tmp_path / "out",
                        execute=True, parallel=1, executor=fake, pause=pause)
    assert result["paused"] and len(fake.calls) == 1 and not result["extractions_complete"]


def test_other_dispatcher_cannot_start_the_same_stage(staged):
    _, started, _ = staged
    with queue._dispatch_owner("obj") as owned:
        assert owned
        queue.start_luna("obj", project_ids=["proj"], consent=True)
        assert started == []
    queue.dispatch("obj")
    assert started == ["luna"]


def test_match_reader_preserves_live_worker_and_detects_stopped_worker(corpus, monkeypatch):
    write(sheet_match.job_path("obj"), {"state": "running", "owner": "another-worker", "owner_process": {"pid": 999}})
    monkeypatch.setattr(sheet_match.recovery, "owner_alive", lambda owner: True)
    assert sheet_match.status("obj")["state"] == "running"
    with pytest.raises(sheet_match.SheetMatchError, match="уже идёт"):
        sheet_match.start("obj", consent=True)
    monkeypatch.setattr(sheet_match.recovery, "owner_alive", lambda owner: False)
    assert sheet_match.status("obj")["state"] == "interrupted"


def test_existing_luna_api_starts_pipeline_and_md_waits_for_its_completion(staged, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import version_comparison_ai as api
    monkeypatch.setenv("VERSION_COMPARISON_ENABLED", "1")
    states, started, _ = staged
    app = FastAPI()
    app.include_router(api.router)
    base = "/api/version-comparison/objects/obj"
    with TestClient(app) as client:
        assert client.post(base + "/ai-sheet-match", json={"consent": False, "project_ids": ["proj"]}).status_code == 400
        response = client.post(base + "/ai-sheet-match", json={"consent": True, "project_ids": ["proj"]})
        assert response.status_code == 200 and started == ["luna"]
        assert response.json()["pipeline"]["progress"]["total"] == 5
        assert client.get(base + "/series/series/ai-sheet-changes/summary.md").status_code == 409
        assert client.get(base + "/series/series/whole-specifications/status").json()["pipeline"]["ready"] is False


def _profile(sha, pages, words=None, inventory=None):
    folder = paths.profile_dir("obj", sha)
    write(folder / "pages.json", {"pages": pages})
    for page, ws in (words or {}).items():
        (folder / "words").mkdir(parents=True, exist_ok=True)
        import gzip
        (folder / "words" / f"p{page:04d}.json.gz").write_bytes(gzip.compress(json.dumps(ws).encode()))
    if inventory is not None:
        write(folder / "spec_so3.json", {"pages": inventory, "items": [], "parents": []})


def _stamp_page(page, title, kind="spec"):
    return {"page": page, "kind": kind, "stamp": {"title": title, "zone": [500, 700, 600, 800]}}


def test_drawing_with_truncated_stamp_title_is_not_a_specification(corpus):
    """Профиль хранит нижнюю строку названия: «ЩУАХП» от «Монтажная схема ЩУАХП» — чертёж, а не СО."""
    run, _ = corpus
    stamp = lambda *texts: [[510 + 10 * i, 750, 515 + 10 * i, 760, t] for i, t in enumerate(texts)]
    for sha, so_page in ((run["a"]["sha256"], 2), (run["b"]["sha256"], 3)):
        _profile(sha, [_stamp_page(1, "ЩУАХП"), _stamp_page(so_page, "изделий и материалов")],
                 words={1: stamp("Монтажная", "схема", "ЩУАХП"),
                        so_page: stamp("Спецификация", "оборудования,", "изделий", "и", "материалов")})
    export._drawing_stamp.cache_clear()
    ctx = export.context("obj", "run")
    assert ctx["kinds"]["a"][1] == "drawing" and ctx["spec_pages"] == {"a": {2}, "b": {3}}
    assert [it["spid"] for it in jobs._sc_items(run, None)[0]] == ["plan"]


def test_transition_without_text_spec_goes_to_sheet_ai_not_full_spec(corpus, monkeypatch):
    """Версия, где СО только в кривых (перечень пуст): листы СО перехода описывает листовой ИИ."""
    run, _ = corpus
    _profile(run["a"]["sha256"], [_stamp_page(2, "Спецификация")], inventory=[{"page": 2, "parsed": True}])
    _profile(run["b"]["sha256"], [_stamp_page(3, "", kind="title")], inventory=[])
    ctx = export.context("obj", "run")
    assert not ctx["whole_spec"] and ctx["spec_pages"] == {"a": set(), "b": set()}
    assert [it["spid"] for it in jobs._sc_items(run, None)[0]] == ["plan", "spec"]
    monkeypatch.setattr(executor, "get", lambda *a: pytest.fail("no text spec — no full-spec call"))
    assert whole.start("obj", "series", consent=True)["state"] == "done" and whole.ready("obj", "series")


def test_skip_marker_is_refused_for_transition_with_text_spec(corpus):
    with pytest.raises(ValueError, match="пропуск допустим только без текстовой СО"):
        export.publish_series("obj", "series", {"schema": export.SERIES_SCHEMA,
                                                "transitions": [{"run_id": "run", "skipped": "no_text_spec"}]})


def test_sheets_outside_ai_jobs_are_described_once_more_then_stop(staged, monkeypatch):
    states, started, _ = staged
    left = [3]
    monkeypatch.setattr(queue.series_ai, "missing", lambda *a: left[0])
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {})
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "done"
        queue.dispatch("obj")
    queue.dispatch("obj")
    assert started.count("ai") == 2
    states["ai"] = "done"
    queue.dispatch("obj")
    item = next(it for it in queue.status("obj")["items"] if it["stage"] == "ai")
    assert item["state"] == "failed" and "3" in item["reason"]
    left[0] = 0
    queue.start_luna("obj", project_ids=["proj"], consent=True)        # «Продолжить» — снова дозапуск
    states["ai"] = "done"
    queue.dispatch("obj")
    assert started[-1] == "specifications"
